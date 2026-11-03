-- Sign-up -> paid conversion by signup month cohort.
-- A user converts if their first paid subscription started within 30 days of signup.
-- window_complete = every user in the cohort has had a full 30 days of observation.
SELECT u.signup_month,
       COUNT(*) AS signups,
       SUM(CASE WHEN u.first_paid_at IS NOT NULL AND u.first_paid_at <= u.signup_ts + INTERVAL 30 DAY THEN 1 ELSE 0 END) AS converted_30d,
       ROUND(100.0 * SUM(CASE WHEN u.first_paid_at IS NOT NULL AND u.first_paid_at <= u.signup_ts + INTERVAL 30 DAY THEN 1 ELSE 0 END) / COUNT(*), 1) AS conversion_pct_30d,
       MAX(u.signup_ts) + INTERVAL 30 DAY <= (SELECT MAX(date_day) FROM gold_dim_date) AS window_complete
FROM gold_dim_user u
GROUP BY 1 ORDER BY 1
