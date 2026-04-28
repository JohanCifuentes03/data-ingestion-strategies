package org.tesis.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.tesis.common.ConfigLoader;
import org.tesis.common.JdbcEventWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Spark Batch job — reads all available events from Kafka that were
 * accumulated during the observation window and writes them to PostgreSQL
 * in a single batch execution.
 * <p>
 * Uses {@code ON CONFLICT (event_id) DO NOTHING} to safely handle retries
 * without failing on duplicate key violations.
 * <p>
 * {@code visible_at} is <b>not</b> set by this job; PostgreSQL fills it
 * via its column DEFAULT at INSERT time.
 */
public final class SparkBatchJob {
        private SparkBatchJob() {
        }

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

        public static void main(String[] args) {
                Map<String, String> config = ConfigLoader.parseArgs(args);
                String kafkaBootstrap = config.getOrDefault("kafka.bootstrap.servers", "kafka:9092");
                String topic = config.getOrDefault("kafka.topic", "events");
                String startingOffsets = config.getOrDefault("kafka.startingOffsets", "earliest");
                String endingOffsets = config.getOrDefault("kafka.endingOffsets", "latest");
                String jdbcUrl = config.getOrDefault("postgres.url", "jdbc:postgresql://postgres:5432/benchmark");
                String jdbcUser = config.getOrDefault("postgres.user", "benchmark");
                String jdbcPassword = config.getOrDefault("postgres.password", "benchmark");
                String scenario = config.getOrDefault("scenario", "low-load");

                SparkSession spark = SparkSession.builder()
                                .appName("SparkBatchJob-" + scenario)
                                .getOrCreate();

                Dataset<Row> kafkaDataset = spark.read()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBootstrap)
                                .option("subscribe", topic)
                                .option("startingOffsets", startingOffsets)
                                .option("endingOffsets", endingOffsets)
                                .load();

                StructType schema = eventSchema();
                Dataset<Row> parsed = kafkaDataset
                                .selectExpr("CAST(value AS STRING) AS json")
                                .select(functions.from_json(functions.col("json"), schema).alias("event"))
                                .select("event.*");

        // Stream rows to JDBC in mini-batches to avoid OOM on large partitions.
        // Each partition is flushed every 500 rows instead of materializing all rows
        // into an ArrayList first.
        final int miniBatchSize = 500;
        parsed.foreachPartition(rows -> {
            Properties jdbcProps = ConfigLoader.jdbcProperties(jdbcUser, jdbcPassword);
            List<org.tesis.common.Event> batch = new ArrayList<>(miniBatchSize);
            while (rows.hasNext()) {
                try {
                    var row = rows.next();
                    java.util.UUID eventId = java.util.UUID.fromString(row.getString(0));
                    long producedAt = row.getLong(1);
                    String payload = row.isNullAt(2) ? "" : row.getString(2);
                    String eventStrategy = row.getString(3);
                    String eventScenario = row.getString(4);
                    String eventRunId = row.getString(5);
                    batch.add(new org.tesis.common.Event(eventId, producedAt, payload, eventStrategy, eventScenario, eventRunId));
                    if (batch.size() >= miniBatchSize) {
                        JdbcEventWriter.writeBatch(jdbcUrl, jdbcProps, batch);
                        batch.clear();
                    }
                } catch (Exception ignored) {
                    // skip malformed rows
                }
            }
            if (!batch.isEmpty()) {
                JdbcEventWriter.writeBatch(jdbcUrl, jdbcProps, batch);
            }
        });

                spark.close();
        }
}
