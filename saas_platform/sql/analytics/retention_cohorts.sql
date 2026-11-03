-- Activity retention by signup-month cohort: share of the cohort active in month N after signup month (N=0 is signup month).
WITH cohort AS (
    SELECT signup_month, COUNT(*) AS cohort_size FROM gold_dim_user GROUP BY 1
),
act AS (
    SELECT u.signup_month, DATEDIFF('month', u.signup_month, a.activity_month) AS months_since_signup,
           COUNT(DISTINCT a.user_id) AS active_users
    FROM gold_fact_product_activity_daily a JOIN gold_dim_user u USING (user_id)
    GROUP BY 1, 2
)
SELECT c.signup_month, c.cohort_size, a.months_since_signup, a.active_users,
       ROUND(100.0 * a.active_users / c.cohort_size, 1) AS retention_pct
FROM cohort c JOIN act a USING (signup_month)
WHERE a.months_since_signup >= 0
ORDER BY 1, 3
