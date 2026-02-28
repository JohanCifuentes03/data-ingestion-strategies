package org.tesis.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.tesis.common.ConfigLoader;

import java.util.Map;
import java.util.Properties;

public final class SparkBatchJob {
    private SparkBatchJob() {
    }

    private static StructType eventSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                DataTypes.createStructField("produced_at", DataTypes.LongType, false),
                DataTypes.createStructField("payload", DataTypes.StringType, false)
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

        SparkSession spark = SparkSession.builder()
                .appName("SparkBatchJob")
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
                .select("event.*")
                .withColumn("visible_at", functions.expr("(unix_timestamp(current_timestamp()) * 1000)"));

        Properties properties = ConfigLoader.jdbcProperties(jdbcUser, jdbcPassword);
        parsed.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, "events", properties);

        spark.close();
    }
}
