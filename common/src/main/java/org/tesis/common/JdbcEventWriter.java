package org.tesis.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public final class JdbcEventWriter {
    private JdbcEventWriter() {
    }

    private static final String INSERT_SQL = """
            INSERT INTO events(event_id, produced_at, visible_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    public static void writeBatch(String url, Properties properties, List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(url, properties);
             PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            for (Event event : events) {
                statement.setObject(1, event.getEventId());
                statement.setLong(2, event.getProducedAt());
                statement.setLong(3, event.getVisibleAt());
                statement.setString(4, event.getPayload());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write batch", e);
        }
    }
}
