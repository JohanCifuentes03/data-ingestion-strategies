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

    public Event(UUID eventId, long producedAt, String payload) {
        this.eventId = eventId;
        this.producedAt = producedAt;
        this.payload = payload;
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
            // payload field is optional (some domain schemas may omit it)
            String payload = node.has("payload") ? node.get("payload").asText("") : "";

            return new Event(id, producedAt, payload);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse event JSON: " + json, e);
        }
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getProducedAt() {
        return producedAt;
    }

    public String getPayload() {
        return payload;
    }
}
