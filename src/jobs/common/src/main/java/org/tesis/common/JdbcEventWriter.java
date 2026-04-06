package org.tesis.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Utility for writing events to PostgreSQL via JDBC batch insert.
 * <p>
 * {@code visible_at} is <b>not</b> included in the INSERT statement;
 * PostgreSQL fills it automatically via its column DEFAULT, ensuring
 * a consistent measurement point across all ingestion strategies.
 */
public final class JdbcEventWriter {
    private JdbcEventWriter() {
    }

    private static final int JDBC_BATCH_SIZE = 5_000;

    private static final String INSERT_SQL = """
            INSERT INTO events(event_id, produced_at, payload, strategy, scenario, run_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    public static void writeBatch(String url, Properties properties, List<Event> events,
            String strategy, String scenario, String runId) {
        if (events.isEmpty()) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(url, properties);
                PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            int pending = 0;
            for (Event event : events) {
                statement.setObject(1, event.getEventId());
                statement.setLong(2, event.getProducedAt());
                statement.setString(3, event.getPayload());
                statement.setString(4, strategy);
                statement.setString(5, scenario);
                statement.setString(6, runId);
                statement.addBatch();
                pending++;
                if (pending >= JDBC_BATCH_SIZE) {
                    statement.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write batch", e);
        }
    }
}
