-- GRAIN: one row per calendar day from the first signup to the last observed activity.
WITH bounds AS (
    SELECT CAST(MIN(signup_ts) AS DATE) AS d0,
           CAST(GREATEST(
               COALESCE((SELECT MAX(event_ts) FROM s_events), TIMESTAMP '1970-01-01'),
               COALESCE((SELECT MAX(paid_at) FROM s_payments), TIMESTAMP '1970-01-01'),
               MAX(signup_ts)) AS DATE) AS d1
    FROM s_users
)
SELECT CAST(d AS DATE) AS date_day,
       DATE_TRUNC('month', d)::DATE AS month_start,
       (DATE_TRUNC('month', d) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE AS month_end,
       EXTRACT(year FROM d)::INT AS year,
       EXTRACT(month FROM d)::INT AS month,
       DAYNAME(d) AS day_name
FROM bounds, generate_series(bounds.d0, bounds.d1, INTERVAL 1 DAY) AS g(d)
