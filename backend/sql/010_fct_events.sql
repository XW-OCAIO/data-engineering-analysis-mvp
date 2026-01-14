CREATE OR REPLACE TABLE fct_events AS
WITH typed AS (
    SELECT
        CAST(event_time AS TIMESTAMP) AS event_time,
        CAST(user_id AS INTEGER) AS user_id,
        CAST(event_name AS VARCHAR) AS event_name,
        CAST(category AS VARCHAR) AS category,
        CAST(amount AS DOUBLE) AS amount
    FROM raw_events
),
deduped AS (
    SELECT
        event_time,
        user_id,
        event_name,
        category,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY event_time, user_id, event_name, category, amount
            ORDER BY event_time, user_id, event_name, category, amount
        ) AS rn
    FROM typed
)
SELECT
    event_time,
    user_id,
    event_name,
    category,
    amount
FROM deduped
WHERE rn = 1;
