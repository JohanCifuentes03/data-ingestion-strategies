package org.tesis.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.tesis.common.ConfigLoader;
import org.tesis.common.Event;

import java.util.Map;

/**
 * Flink Streaming job — continuously consumes events from Kafka and
 * writes them to PostgreSQL with exactly-once checkpointing.
 * <p>
 * {@code visible_at} is <b>not</b> set by this job; PostgreSQL fills it
 * via its column DEFAULT at INSERT time, consistent with the Spark jobs.
 */
public final class FlinkStreamingJob {
        private FlinkStreamingJob() {
        }

        public static void main(String[] args) throws Exception {
                Map<String, String> config = ConfigLoader.parseArgs(args);
                String scenario = config.getOrDefault("scenario", "low-load");
                String runId = config.getOrDefault("run.id", "run_1");
                String kafkaBootstrap = config.getOrDefault("kafka.bootstrap.servers", "kafka:9092");
                String topic = config.getOrDefault("kafka.topic", "events");
                String jdbcUrl = config.getOrDefault("postgres.url", "jdbc:postgresql://postgres:5432/benchmark");
                String jdbcUser = config.getOrDefault("postgres.user", "benchmark");
                String jdbcPassword = config.getOrDefault("postgres.password", "benchmark");
                int parallelism = Integer.parseInt(config.getOrDefault("parallelism", "1"));

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);
                env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
                CheckpointConfig checkpointConfig = env.getCheckpointConfig();
                checkpointConfig.setMinPauseBetweenCheckpoints(5_000L);
                checkpointConfig.setCheckpointTimeout(60_000L);
                checkpointConfig.setMaxConcurrentCheckpoints(1);
                checkpointConfig.enableUnalignedCheckpoints(true);

                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers(kafkaBootstrap)
                                .setTopics(topic)
                                .setGroupId("flink-streaming-" + scenario)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                // Capture as effectively-final for use inside lambda
                final String strategyVal = "streaming";
                final String scenarioVal = scenario;
                final String runIdVal = runId;

                env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")
                                .map(Event::fromJson)
                                .name("ParseEvent")
                                .rebalance()
                                .addSink(JdbcSink.sink(
                                                "INSERT INTO events(event_id, produced_at, payload, strategy, scenario, run_id) "
                                                                + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING",
                                                (statement, event) -> {
                                                        statement.setObject(1, event.getEventId());
                                                        statement.setLong(2, event.getProducedAt());
                                                        statement.setString(3, event.getPayload());
                                                        statement.setString(4, strategyVal);
                                                        statement.setString(5, scenarioVal);
                                                        statement.setString(6, runIdVal);
                                                },
                                                JdbcExecutionOptions.builder()
                                                                .withBatchSize(500)
                                                                .withBatchIntervalMs(200)
                                                                .withMaxRetries(5)
                                                                .build(),
                                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                                .withUrl(jdbcUrl)
                                                                .withDriverName("org.postgresql.Driver")
                                                                .withUsername(jdbcUser)
                                                                .withPassword(jdbcPassword)
                                                                .build()))
                                .name("PostgresSink");

                env.execute("FlinkStreamingJob-" + scenario);
        }
}
