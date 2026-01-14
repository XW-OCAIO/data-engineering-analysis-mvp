CREATE TABLE IF NOT EXISTS raw_events (
    event_time TIMESTAMP,
    user_id INTEGER,
    event_name VARCHAR,
    category VARCHAR,
    amount DOUBLE
);
