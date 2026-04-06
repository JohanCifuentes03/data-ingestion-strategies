package org.tesis.common;

import java.util.Map;

/**
 * Configuration interface for ingestion jobs.
 * Provides common configuration keys and utilities for all processing strategies.
 */
public interface JobConfig {
    // Kafka configuration keys
    String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    String KAFKA_TOPIC = "kafka.topic";
    String KAFKA_STARTING_OFFSETS = "kafka.startingOffsets";
    String KAFKA_ENDING_OFFSETS = "kafka.endingOffsets";
    
    // PostgreSQL configuration keys
    String POSTGRES_URL = "postgres.url";
    String POSTGRES_USER = "postgres.user";
    String POSTGRES_PASSWORD = "postgres.password";
    
    // Experiment configuration keys
    String SCENARIO = "scenario";
    String RUN_ID = "run.id";
    
    // Default values
    String DEFAULT_KAFKA_BOOTSTRAP = "kafka:9092";
    String DEFAULT_KAFKA_TOPIC = "events";
    String DEFAULT_POSTGRES_URL = "jdbc:postgresql://postgres:5432/benchmark";
    String DEFAULT_POSTGRES_USER = "benchmark";
    String DEFAULT_POSTGRES_PASSWORD = "benchmark";
    String DEFAULT_SCENARIO = "low-load";
    String DEFAULT_RUN_ID = "run_1";
    
    /**
     * Get configuration value with default fallback.
     * 
     * @param config Configuration map
     * @param key Configuration key
     * @param defaultValue Default value if key not found
     * @return Configuration value or default
     */
    static String getOrDefault(Map<String, String> config, String key, String defaultValue) {
        return config.getOrDefault(key, defaultValue);
    }
}
