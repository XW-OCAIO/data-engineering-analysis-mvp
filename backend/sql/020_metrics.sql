SELECT
    COUNT(*)::BIGINT AS total_events,
    COUNT(DISTINCT user_id)::BIGINT AS total_users,
    COALESCE(SUM(amount), 0)::DOUBLE AS total_amount
FROM fct_events;
