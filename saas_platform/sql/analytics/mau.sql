-- Monthly active users: distinct users with >= 1 accepted event in the calendar month.
SELECT activity_month, COUNT(DISTINCT user_id) AS mau
FROM gold_fact_product_activity_daily
GROUP BY 1 ORDER BY 1
