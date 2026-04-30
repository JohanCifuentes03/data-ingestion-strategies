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
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.tesis.common.ConfigLoader;
import org.tesis.common.Event;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Flink Streaming job — continuously consumes events from Kafka and
 * writes them to PostgreSQL with exactly-once checkpointing.
 * <p>
 * {@code visible_at} is <b>not</b> set by this job; PostgreSQL fills it
 * via its column DEFAULT at INSERT time, consistent with the Spark jobs.
 * <p>
 * Improvements for high-load scenarios:
 * - Graceful shutdown without System.exit()
 * - Increased JDBC batch size (2000) and flush interval (200ms)
 * - Reduced checkpoint interval for stability
 * - Detailed logging for debugging
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
                String parallelismStr = config.getOrDefault("parallelism", "4");
                int parallelism = Integer.parseInt(parallelismStr);
                long runDurationMs = Long.parseLong(
                                config.getOrDefault("run.duration.seconds", "1200")) * 1000L;

                System.out.println("[streaming] Starting Flink job:");
                System.out.println("[streaming]   scenario=" + scenario + " runId=" + runId);
                System.out.println("[streaming]   parallelism=" + parallelism + " duration=" + runDurationMs + "ms");
                System.out.println("[streaming]   kafka=" + kafkaBootstrap + " topic=" + topic);
                System.out.println("[streaming]   jdbc=" + jdbcUrl);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);

                env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
                CheckpointConfig checkpointConfig = env.getCheckpointConfig();
                checkpointConfig.setMinPauseBetweenCheckpoints(10_000L);
                checkpointConfig.setCheckpointTimeout(120_000L);
                checkpointConfig.setMaxConcurrentCheckpoints(1);
                checkpointConfig.enableUnalignedCheckpoints(true);
                checkpointConfig.setTolerableCheckpointFailureNumber(5);

                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers(kafkaBootstrap)
                                .setTopics(topic)
                                .setGroupId("flink-streaming-" + scenario + "-" + runId)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .setProperty("fetch.min.bytes", "1")
                                .setProperty("fetch.max.wait.ms", "100")
                                .build();

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
                                                        statement.setString(4, event.getStrategy());
                                                        statement.setString(5, event.getScenario());
                                                        statement.setString(6, event.getRunId());
                                                },
                                                JdbcExecutionOptions.builder()
                                                                .withBatchSize(2000)
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

                if (runDurationMs > 0) {
                        final ExecutorService executor = Executors.newSingleThreadExecutor();
                        final StreamExecutionEnvironment finalEnv = env;

                        CompletableFuture<Void> execution = CompletableFuture.runAsync(() -> {
                                try {
                                        System.out.println("[streaming] Starting Flink job execution...");
                                        finalEnv.execute("FlinkStreamingJob-" + scenario);
                                } catch (Exception e) {
                                        if (!e.getMessage().contains("Job was cancelled") && 
                                            !e.getMessage().contains("Executing job")) {
                                                System.err.println("[streaming] Job execution error: " + e.getMessage());
                                                throw new RuntimeException(e);
                                        }
                                }
                        });

                        try {
                                System.out.println("[streaming] Waiting for run duration: " + runDurationMs + "ms");
                                execution.get(runDurationMs, TimeUnit.MILLISECONDS);
                                System.out.println("[streaming] Run duration reached normally.");
                        } catch (java.util.concurrent.TimeoutException ignored) {
                                System.out.println("[streaming] Requesting graceful stop...");
                                finalEnv.getExecutionEnvironment().getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
                                execution.cancel(true);
                                
                                try {
                                        Thread.sleep(5000);
                                } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                }
                                
                                System.out.println("[streaming] Flink job stopped gracefully.");
                        }
                        
                        executor.shutdown();
                        try {
                                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                                        executor.shutdownNow();
                                }
                        } catch (InterruptedException e) {
                                executor.shutdownNow();
                                Thread.currentThread().interrupt();
                        }
                        
                        System.out.println("[streaming] Shutdown complete. Exiting.");
                } else {
                        env.execute("FlinkStreamingJob-" + scenario);
                }
        }
}
