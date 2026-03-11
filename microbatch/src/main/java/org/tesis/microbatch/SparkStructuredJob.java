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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spark Structured Streaming job — consumes events from Kafka using
 * micro-batch semantics with a configurable trigger interval.
 * <p>
 * {@code visible_at} is <b>not</b> set by this job; PostgreSQL fills it
 * via its column DEFAULT at INSERT time.
 * <p>
 * Improved for high-load scenarios:
 * - Retry logic with exponential backoff
 * - Batch size configuration for better throughput
 * - Detailed logging for debugging
 */
public final class SparkStructuredJob {
        private static final int MAX_RETRIES = 3;
        private static final long RETRY_BACKOFF_MS = 1000;

        private SparkStructuredJob() {
        }

        private static StructType eventSchema() {
                return new StructType(new StructField[] {
                                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                                DataTypes.createStructField("produced_at", DataTypes.LongType, false),
                                DataTypes.createStructField("payload", DataTypes.StringType, false)
                });
        }

        private static void writeBatchWithRetry(Dataset<Row> batch, String jdbcUrl, Properties props, int attempt) {
                int maxAttempts = MAX_RETRIES;
                long backoffMs = RETRY_BACKOFF_MS;

                for (int i = 1; i <= maxAttempts; i++) {
                        try {
                                long rowCount = batch.count();
                                System.out.println("[microbatch] Batch " + batch.hashCode() + " writing " + rowCount + " rows (attempt " + i + "/" + maxAttempts + ")");

                                batch.write()
                                        .mode("append")
                                        .option("batchsize", 1000)
                                        .option("numPartitions", 4)
                                        .jdbc(jdbcUrl, "events", props);

                                System.out.println("[microbatch] Batch write successful: " + rowCount + " rows");
                                return;
                        } catch (Exception e) {
                                System.err.println("[microbatch] Write attempt " + i + " failed: " + e.getMessage());
                                if (i < maxAttempts) {
                                        try {
                                                Thread.sleep(backoffMs);
                                                backoffMs *= 2;
                                        } catch (InterruptedException ie) {
                                                Thread.currentThread().interrupt();
                                                throw new RuntimeException("Interrupted during retry", ie);
                                        }
                                } else {
                                        System.err.println("[microbatch] All retries exhausted. Batch lost: " + batch.count() + " rows");
                                        throw new RuntimeException("Failed to write batch after " + maxAttempts + " attempts", e);
                                }
                        }
                }
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
                long runDurationMs = Long.parseLong(
                                config.getOrDefault("run.duration.seconds", "1200")) * 1000L;

                System.out.println("[microbatch] Starting with config:");
                System.out.println("[microbatch]   scenario=" + scenario + " runId=" + runId);
                System.out.println("[microbatch]   trigger=" + triggerInterval + " duration=" + runDurationMs + "ms");
                System.out.println("[microbatch]   kafka=" + kafkaBootstrap + " topic=" + topic);

                SparkSession spark = SparkSession.builder()
                                .appName("SparkStructuredStreaming-" + scenario)
                                .config("spark.sql.adaptive.enabled", "true")
                                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                                .config("spark.sql.shuffle.partitions", "8")
                                .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

                Dataset<Row> streamingDataset = spark.readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBootstrap)
                                .option("subscribe", topic)
                                .option("startingOffsets", "earliest")
                                .option("failOnDataLoss", "false")
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

                final String finalJdbcUrl = jdbcUrl;
                final Properties finalProps = properties;

                StreamingQuery query = events.writeStream()
                                .outputMode("append")
                                .option("checkpointLocation", checkpointLocation)
                                .trigger(Trigger.ProcessingTime(triggerInterval))
                                .foreachBatch((batchDataset, batchId) -> {
                                        long count = batchDataset.count();
                                        if (count > 0) {
                                                System.out.println("[microbatch] Processing batch " + batchId + " with " + count + " rows");
                                                writeBatchWithRetry(batchDataset, finalJdbcUrl, finalProps, batchId.intValue());
                                        } else {
                                                System.out.println("[microbatch] Batch " + batchId + " empty, skipping");
                                        }
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
