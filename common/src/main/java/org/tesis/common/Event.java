package org.tesis.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lightweight POJO that represents a generated event.
 * <p>
 * {@code visible_at} is intentionally <b>not</b> part of this class.
 * The authoritative visibility timestamp is set by PostgreSQL via its
 * column DEFAULT ({@code EXTRACT(EPOCH FROM NOW()) * 1000}) at INSERT
 * time, ensuring a consistent measurement point across all three
 * ingestion strategies (Batch, Micro‑batch, Streaming).
 */
public class Event implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    private static final Pattern EVENT_ID_PATTERN = Pattern.compile("\"event_id\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern PRODUCED_AT_PATTERN = Pattern.compile("\"produced_at\"\\s*:\\s*(\\d+)");
    private static final Pattern PAYLOAD_PATTERN = Pattern.compile("\"payload\"\\s*:\\s*\"([^\"]*)\"");

    private final UUID eventId;
    private final long producedAt;
    private final String payload;

    public Event(UUID eventId, long producedAt, String payload) {
        this.eventId = eventId;
        this.producedAt = producedAt;
        this.payload = payload;
    }

    public static Event fromJson(String json) {
        UUID id = UUID.fromString(extract(EVENT_ID_PATTERN, json));
        long producedAt = Long.parseLong(extract(PRODUCED_AT_PATTERN, json));
        String payload = extract(PAYLOAD_PATTERN, json);
        return new Event(id, producedAt, payload);
    }

    private static String extract(Pattern pattern, String json) {
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Missing field in event payload: " + pattern);
        }
        return matcher.group(1);
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
