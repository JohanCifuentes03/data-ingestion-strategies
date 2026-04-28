package org.tesis.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.tesis.common.ConfigLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
        private static final int JDBC_BATCH_SIZE = 500;
        private static final String INSERT_SQL = """
                        INSERT INTO events(event_id, produced_at, payload, strategy, scenario, run_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (event_id) DO NOTHING
                        """;

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

                parsed.foreachPartition(rows -> {
                        Properties jdbcProps = ConfigLoader.jdbcProperties(jdbcUser, jdbcPassword);
                        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProps);
                                        PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
                                int pending = 0;
                                int malformed = 0;
                                while (rows.hasNext()) {
                                        Row row = rows.next();
                                        try {
                                                statement.setObject(1, java.util.UUID.fromString(row.getString(0)));
                                                statement.setLong(2, row.getLong(1));
                                                statement.setString(3, row.isNullAt(2) ? "" : row.getString(2));
                                                statement.setString(4, row.getString(3));
                                                statement.setString(5, row.getString(4));
                                                statement.setString(6, row.getString(5));
                                                statement.addBatch();
                                                pending++;
                                                if (pending >= JDBC_BATCH_SIZE) {
                                                        statement.executeBatch();
                                                        pending = 0;
                                                }
                                        } catch (IllegalArgumentException | ClassCastException malformedRow) {
                                                malformed++;
                                        }
                                }
                                if (pending > 0) {
                                        statement.executeBatch();
                                }
                                if (malformed > 0) {
                                        System.err.println("Skipped malformed batch rows: " + malformed);
                                }
                        } catch (SQLException e) {
                                throw new RuntimeException("Failed to stream partition to PostgreSQL", e);
                        }
                });

                spark.close();
        }
}
