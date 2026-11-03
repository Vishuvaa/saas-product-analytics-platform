-- GRAIN: one row per accepted user.
-- first_paid_at = start of the user's first paid subscription (NULL if never converted).
-- current_plan  = plan of the user's most recent paid subscription ('free' if never paid).
WITH paid AS (
    SELECT user_id, subscription_id, started_at, plan_id, status,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY started_at DESC, subscription_id DESC) AS rn_last,
           MIN(started_at) OVER (PARTITION BY user_id) AS first_started
    FROM s_subscriptions
    WHERE started_at IS NOT NULL
)
SELECT u.user_id, u.email, u.country, u.segment, u.acquisition_channel, u.signup_ts,
       CAST(u.signup_ts AS DATE) AS signup_date,
       DATE_TRUNC('month', u.signup_ts)::DATE AS signup_month,
       p.first_started AS first_paid_at,
       COALESCE(p.plan_id, 'free') AS current_plan,
       COALESCE(p.status = 'canceled', FALSE) AS is_churned
FROM s_users u
LEFT JOIN paid p ON p.user_id = u.user_id AND p.rn_last = 1
