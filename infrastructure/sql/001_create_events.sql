CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS events (
    event_id    UUID        PRIMARY KEY,
    produced_at BIGINT      NOT NULL,
    visible_at  BIGINT      NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    payload     TEXT        NOT NULL,
    strategy    VARCHAR(32) NOT NULL DEFAULT 'unknown',
    scenario    VARCHAR(32) NOT NULL DEFAULT 'unknown',
    run_id      VARCHAR(64) NOT NULL DEFAULT 'unset'
);

CREATE INDEX IF NOT EXISTS idx_events_visible_at ON events (visible_at);
CREATE INDEX IF NOT EXISTS idx_events_run        ON events (strategy, scenario, run_id);
