-- Daily active users: distinct users with >= 1 accepted event on the day.
SELECT activity_date, COUNT(DISTINCT user_id) AS dau, SUM(events_count) AS events
FROM gold_fact_product_activity_daily
GROUP BY 1 ORDER BY 1
