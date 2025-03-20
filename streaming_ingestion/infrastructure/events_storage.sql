\c events_storage;

CREATE TABLE IF NOT EXISTS event_counts (
    event_type VARCHAR(100) NOT NULL,
    occurrences BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_event_counts (
    event_type VARCHAR(100) NOT NULL,
    user_id BIGINT NOT NULL,
    occurrences BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_avg_waiting_time (
    user_id BIGINT NOT NULL,
    avg_waiting_time DECIMAL(10, 2) NOT NULL
);
