-- Monthly product activity by the plan the user was on at the end of each active day.
SELECT activity_month AS month, plan_id, COUNT(DISTINCT user_id) AS active_users, SUM(events_count) AS events
FROM gold_fact_product_activity_daily GROUP BY 1, 2 ORDER BY 1, 2
