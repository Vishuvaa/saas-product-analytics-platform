-- GRAIN: one row per (user, activity day) with at least one accepted event.
-- Late-arriving events are placed on their EVENT date (event_ts), not the day they arrived.
-- plan_id = the user's paid plan at the END of that day ('free' if none / canceled / not converted).
WITH daily AS (
    SELECT user_id, CAST(event_ts AS DATE) AS activity_date, COUNT(*) AS events_count,
           COUNT(DISTINCT event_type) AS distinct_event_types,
           SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_events
    FROM s_events GROUP BY 1, 2
)
SELECT d.user_id, d.activity_date, DATE_TRUNC('month', d.activity_date)::DATE AS activity_month,
       d.events_count, d.distinct_event_types, d.late_events,
       COALESCE(pp.plan_id, 'free') AS plan_id, u.segment
FROM daily d
JOIN s_users u ON u.user_id = d.user_id
LEFT JOIN gold_fact_subscription_plan_period pp
       ON pp.user_id = d.user_id
      AND pp.valid_from < CAST(d.activity_date AS TIMESTAMP) + INTERVAL 1 DAY
      AND pp.valid_to  >= CAST(d.activity_date AS TIMESTAMP) + INTERVAL 1 DAY
