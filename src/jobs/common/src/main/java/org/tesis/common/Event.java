package org.tesis.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

/**
 * Lightweight POJO that represents a generated event.
 * <p>
 * {@code visible_at} is intentionally <b>not</b> part of this class.
 * The authoritative visibility timestamp is set by PostgreSQL via its
 * column DEFAULT ({@code EXTRACT(EPOCH FROM NOW()) * 1000}) at INSERT
 * time, ensuring a consistent measurement point across all three
 * ingestion strategies (Batch, Micro‑batch, Streaming).
 * <p>
 * JSON parsing uses Jackson (bundled with Flink/Spark runtimes) instead
 * of regex, which was fragile for large or complex payloads.
 */
public class Event implements Serializable {
    @Serial
    private static final long serialVersionUID = 3L;

    /**
     * Shared, thread-safe ObjectMapper. Jackson's ObjectMapper is thread-safe
     * once configured, so sharing it avoids repeated instantiation overhead.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final UUID eventId;
    private final long producedAt;
    private final String payload;
    private final String strategy;
    private final String scenario;
    private final String runId;

    /**
     * Creates an immutable event value used by streaming jobs and JDBC writers.
     *
     * @param eventId unique event identifier.
     * @param producedAt producer timestamp in epoch milliseconds.
     * @param payload synthetic payload body.
     * @param strategy ingestion strategy label.
     * @param scenario workload scenario label.
     * @param runId benchmark run identifier.
     */
    public Event(UUID eventId, long producedAt, String payload, String strategy, String scenario, String runId) {
        this.eventId = eventId;
        this.producedAt = producedAt;
        this.payload = payload;
        this.strategy = strategy;
        this.scenario = scenario;
        this.runId = runId;
    }

    /**
     * Parse an event from a JSON string. Only {@code event_id},
     * {@code produced_at}, and {@code payload} are extracted; all other
     * domain fields (temperature, heart_rate, etc.) are intentionally
     * ignored at this layer — they remain in the raw Kafka message.
     *
     * @throws IllegalArgumentException if required fields are missing or malformed
     */
    public static Event fromJson(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);

            JsonNode idNode = node.get("event_id");
            JsonNode tsNode = node.get("produced_at");
            if (idNode == null || tsNode == null) {
                throw new IllegalArgumentException(
                        "Missing required field(s) in event JSON: " + json);
            }

            UUID id = UUID.fromString(idNode.asText());
            long producedAt = tsNode.asLong();
            JsonNode strategyNode = node.get("strategy");
            JsonNode scenarioNode = node.get("scenario");
            JsonNode runIdNode = node.get("run_id");
            if (strategyNode == null || scenarioNode == null || runIdNode == null) {
                throw new IllegalArgumentException(
                        "Missing required label field(s) in event JSON: " + json);
            }

            String payload = node.has("payload") ? node.get("payload").asText("") : "";
            String strategy = strategyNode.asText();
            String scenario = scenarioNode.asText();
            String runId = runIdNode.asText();

            return new Event(id, producedAt, payload, strategy, scenario, runId);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse event JSON: " + json, e);
        }
    }

    /**
     * @return unique event identifier.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * @return producer timestamp in epoch milliseconds.
     */
    public long getProducedAt() {
        return producedAt;
    }

    /**
     * @return synthetic payload body.
     */
    public String getPayload() {
        return payload;
    }

    /**
     * @return ingestion strategy label.
     */
    public String getStrategy() {
        return strategy;
    }

    /**
     * @return workload scenario label.
     */
    public String getScenario() {
        return scenario;
    }

    /**
     * @return benchmark run identifier.
     */
    public String getRunId() {
        return runId;
    }
}
