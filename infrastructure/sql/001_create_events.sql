CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    produced_at BIGINT NOT NULL,
    visible_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_visible_at ON events (visible_at);
