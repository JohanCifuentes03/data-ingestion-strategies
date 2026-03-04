package org.tesis.microbatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.tesis.common.ConfigLoader;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Spark Structured Streaming job — consumes events from Kafka using
 * micro-batch semantics with a configurable trigger interval.
 * <p>
 * {@code visible_at} is <b>not</b> set by this job; PostgreSQL fills it
 * via its column DEFAULT at INSERT time.
 */
public final class SparkStructuredJob {
        private SparkStructuredJob() {
        }

        private static StructType eventSchema() {
                return new StructType(new StructField[] {
                                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                                DataTypes.createStructField("produced_at", DataTypes.LongType, false),
                                DataTypes.createStructField("payload", DataTypes.StringType, false)
                });
        }

        public static void main(String[] args) throws StreamingQueryException, TimeoutException {
                Map<String, String> config = ConfigLoader.parseArgs(args);
                String kafkaBootstrap = config.getOrDefault("kafka.bootstrap.servers", "kafka:9092");
                String topic = config.getOrDefault("kafka.topic", "events");
                String checkpointLocation = config.getOrDefault("checkpoint.location",
                                "/opt/spark/checkpoints/microbatch");
                String triggerInterval = config.getOrDefault("trigger.interval", "5 seconds");
                String jdbcUrl = config.getOrDefault("postgres.url", "jdbc:postgresql://postgres:5432/benchmark");
                String jdbcUser = config.getOrDefault("postgres.user", "benchmark");
                String jdbcPassword = config.getOrDefault("postgres.password", "benchmark");
                String scenario = config.getOrDefault("scenario", "low-load");
                String runId = config.getOrDefault("run.id", "run_1");
                // run.duration.seconds: how long to keep the streaming query running.
                // Default = 1200 s (20 min). Use 0 to run indefinitely (manual stop).
                long runDurationMs = Long.parseLong(
                                config.getOrDefault("run.duration.seconds", "1200")) * 1000L;

                SparkSession spark = SparkSession.builder()
                                .appName("SparkStructuredStreaming-" + scenario)
                                .getOrCreate();

                Dataset<Row> streamingDataset = spark.readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBootstrap)
                                .option("subscribe", topic)
                                .option("startingOffsets", "earliest")
                                .load();

                StructType schema = eventSchema();
                Dataset<Row> events = streamingDataset
                                .selectExpr("CAST(value AS STRING) AS json")
                                .select(functions.from_json(functions.col("json"), schema).alias("event"))
                                .select("event.*")
                                .withColumn("strategy", functions.lit("microbatch"))
                                .withColumn("scenario", functions.lit(scenario))
                                .withColumn("run_id", functions.lit(runId));

                Properties properties = ConfigLoader.jdbcProperties(jdbcUser, jdbcPassword);

                StreamingQuery query = events.writeStream()
                                .outputMode("append")
                                .option("checkpointLocation", checkpointLocation)
                                .trigger(Trigger.ProcessingTime(triggerInterval))
                                .foreachBatch((batchDataset, batchId) -> {
                                        batchDataset.write()
                                                        .mode("append")
                                                        .jdbc(jdbcUrl, "events", properties);
                                })
                                .start();

                if (runDurationMs > 0) {
                        query.awaitTermination(runDurationMs);
                        query.stop();
                } else {
                        query.awaitTermination();
                }
        }
}
