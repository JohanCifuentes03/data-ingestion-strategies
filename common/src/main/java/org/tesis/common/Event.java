package org.tesis.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Event implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final UUID eventId;
    private final long producedAt;
    private final String payload;
    private final long visibleAt;

    public Event(UUID eventId, long producedAt, String payload, long visibleAt) {
        this.eventId = eventId;
        this.producedAt = producedAt;
        this.payload = payload;
        this.visibleAt = visibleAt;
    }

    public static Event fromJson(String json) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(json);
        UUID id = UUID.fromString(node.get("event_id").asText());
        long producedAt = node.get("produced_at").asLong();
        String payload = node.get("payload").asText();
        long visibleAt = System.currentTimeMillis();
        return new Event(id, producedAt, payload, visibleAt);
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

    public long getVisibleAt() {
        return visibleAt;
    }
}
