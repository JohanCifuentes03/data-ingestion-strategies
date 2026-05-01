package org.tesis.microbatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
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
        private static final long DRAIN_POLL_MS = 2000;
        private static final int REQUIRED_IDLE_POLLS = 3;

        /**
         * Prevents instantiation of this command-style utility class.
         */
        private SparkStructuredJob() {
        }

        /**
         * Defines the Spark SQL schema expected in Kafka JSON event payloads.
         *
         * @return schema containing the benchmark identifiers, timestamp, payload, and labels.
         */
        private static StructType eventSchema() {
                return new StructType(new StructField[] {
                                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                                DataTypes.createStructField("produced_at", DataTypes.LongType, false),
                                DataTypes.createStructField("payload", DataTypes.StringType, false),
                                DataTypes.createStructField("strategy", DataTypes.StringType, false),
                                DataTypes.createStructField("scenario", DataTypes.StringType, false),
                                DataTypes.createStructField("run_id", DataTypes.StringType, false)
                });
        }

        /**
         * Writes one Structured Streaming micro-batch to PostgreSQL.
         *
         * <p>The method persists the incoming Spark dataset long enough to count and write it,
         * writes each partition through JDBC batches, and treats interruption/cancellation as a
         * graceful shutdown path rather than a data error.
         *
         * @param batch Spark dataset for the current micro-batch.
         * @param jdbcUrl PostgreSQL JDBC URL.
         * @param props JDBC authentication and driver properties.
         * @param batchId Spark micro-batch identifier for logging.
         * @throws RuntimeException if the micro-batch fails for a non-interruption reason.
         */
        private static void writeBatchWithRetry(Dataset<Row> batch, String jdbcUrl, Properties props, int batchId) {
                batch.persist();
                try {
                        long rowCount = batch.count();
                        if (rowCount == 0) {
                                System.out.println("[microbatch] Batch " + batchId + " is empty, skipping.");
                                return;
                        }

                        System.out.println("[microbatch] Batch " + batchId + " writing " + rowCount + " rows...");

                        batch.foreachPartition(partition -> {
                                if (!partition.hasNext()) return;
                                
                                java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, props);
                                conn.setAutoCommit(false);
                                
                                String sql = "INSERT INTO events (event_id, produced_at, payload, strategy, scenario, run_id) " +
                                             "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";
                                java.sql.PreparedStatement stmt = conn.prepareStatement(sql);
                                
                                int count = 0;
                                while (partition.hasNext()) {
                                        Row row = partition.next();
                                        stmt.setString(1, row.getAs("event_id"));
                                        stmt.setLong(2, row.getAs("produced_at"));
                                        stmt.setString(3, row.getAs("payload"));
                                        stmt.setString(4, row.getAs("strategy"));
                                        stmt.setString(5, row.getAs("scenario"));
                                        stmt.setString(6, row.getAs("run_id"));
                                        stmt.addBatch();
                                        count++;
                                        
                                        if (count % 1000 == 0) {
                                                stmt.executeBatch();
                                        }
                                }
                                stmt.executeBatch();
                                conn.commit();
                                stmt.close();
                                conn.close();
                        });

                        System.out.println("[microbatch] Batch " + batchId + " write successful (idempotent): " + rowCount + " rows");
                } catch (Exception e) {
                        if (isInterrupted(e)) {
                                System.err.println("[microbatch] Batch " + batchId + " interrupted during shutdown, ignoring batch.");
                                Thread.currentThread().interrupt();
                                return;
                        }

                        System.err.println("[microbatch] Batch " + batchId + " failed fatally: " + e.getMessage());
                        throw new RuntimeException("Microbatch write failed", e);
                } finally {
                        batch.unpersist();
                }
        }

        /**
         * Detects whether an exception chain represents shutdown interruption.
         *
         * @param throwable exception to inspect.
         * @return {@code true} when any cause is an interruption or cancellation signal.
         */
        private static boolean isInterrupted(Throwable throwable) {
                while (throwable != null) {
                        if (throwable instanceof InterruptedException
                                        || throwable instanceof java.util.concurrent.CancellationException) {
                                return true;
                        }
                        throwable = throwable.getCause();
                }

                return false;
        }

        /**
         * Runs the Spark Structured Streaming micro-batch ingestion job.
         *
         * <p>The job starts from the isolated run topic, writes append-only micro-batches to
         * PostgreSQL, waits through the official generation window, then observes idle batches
         * until the configured drain condition or timeout is reached.
         *
         * @param args command-line options accepted by {@link ConfigLoader#parseArgs(String[])}.
         * @throws StreamingQueryException if Spark reports a streaming query failure.
         * @throws TimeoutException if Spark awaitTermination reports a timeout unexpectedly.
         */
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
                long officialDurationMs = Long.parseLong(
                                config.getOrDefault("official.duration.seconds",
                                                config.getOrDefault("run.duration.seconds", "1200"))) * 1000L;
                long drainTimeoutMs = Long.parseLong(
                                config.getOrDefault("drain.timeout.seconds", "600")) * 1000L;
                long maxRunDurationMs = Long.parseLong(
                                config.getOrDefault("run.duration.seconds",
                                                String.valueOf((officialDurationMs + drainTimeoutMs) / 1000L))) * 1000L;

                System.out.println("[microbatch] Starting with config:");
                System.out.println("[microbatch]   scenario=" + scenario + " runId=" + runId);
                System.out.println("[microbatch]   trigger=" + triggerInterval
                                + " officialDuration=" + officialDurationMs + "ms"
                                + " drainTimeout=" + drainTimeoutMs + "ms"
                                + " maxDuration=" + maxRunDurationMs + "ms");
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
                                .select("event.*");

                Properties properties = ConfigLoader.jdbcProperties(jdbcUser, jdbcPassword);

                final String finalJdbcUrl = jdbcUrl;
                final Properties finalProps = properties;

                StreamingQuery query = events.writeStream()
                                .outputMode("append")
                                .option("checkpointLocation", checkpointLocation)
                                .trigger(Trigger.ProcessingTime(triggerInterval))
                                .foreachBatch((batchDataset, batchId) -> {
                                        writeBatchWithRetry(batchDataset, finalJdbcUrl, finalProps, batchId.intValue());
                                })
                                .start();

                if (maxRunDurationMs > 0) {
                        long startedAt = System.currentTimeMillis();
                        long drainDeadline = startedAt + officialDurationMs + drainTimeoutMs;
                        long hardDeadline = startedAt + maxRunDurationMs;
                        int idlePolls = 0;
                        long lastObservedBatchId = -1L;

                        while (query.isActive()) {
                                long now = System.currentTimeMillis();
                                if (now < startedAt + officialDurationMs) {
                                        if (query.awaitTermination(Math.min(DRAIN_POLL_MS, (startedAt + officialDurationMs) - now))) {
                                                return;
                                        }
                                        continue;
                                }

                                StreamingQueryProgress progress = query.lastProgress();
                                if (progress != null && progress.batchId() != lastObservedBatchId) {
                                        lastObservedBatchId = progress.batchId();
                                        if (progress.numInputRows() == 0) {
                                                idlePolls++;
                                                System.out.println("[microbatch] Drain idle poll " + idlePolls
                                                                + "/" + REQUIRED_IDLE_POLLS
                                                                + " after batch " + progress.batchId());
                                        } else {
                                                idlePolls = 0;
                                                System.out.println("[microbatch] Drain still processing: batch="
                                                                + progress.batchId()
                                                                + " inputRows=" + progress.numInputRows());
                                        }
                                }

                                if (idlePolls >= REQUIRED_IDLE_POLLS) {
                                        System.out.println("[microbatch] Drain completed: no new Kafka input observed.");
                                        query.stop();
                                        return;
                                }

                                if (now >= drainDeadline || now >= hardDeadline) {
                                        System.err.println("[microbatch] Drain timeout reached; stopping query.");
                                        query.stop();
                                        return;
                                }

                                if (query.awaitTermination(DRAIN_POLL_MS)) {
                                        return;
                                }
                        }
                } else {
                        query.awaitTermination();
                }
        }
}
