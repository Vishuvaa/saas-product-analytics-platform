-- GRAIN: one row per (subscription, plan period). A period starts at the paid start or a plan change and
-- ends at the next change, at cancellation, or at 9999-12-31 while still active. Half-open: [valid_from, valid_to).
-- Assumption: a user holds at most one paid subscription at a time.
WITH first_hist AS (
    SELECT subscription_id, ARG_MIN(old_plan, change_ts) AS first_old_plan, ARG_MIN(old_mrr, change_ts) AS first_old_mrr
    FROM s_history GROUP BY subscription_id
),
periods AS (
    SELECT s.subscription_id, s.user_id, s.started_at AS valid_from,
           COALESCE(fh.first_old_plan, s.plan_id) AS plan_id,
           COALESCE(fh.first_old_mrr, s.mrr_amount) AS mrr_amount, s.canceled_at
    FROM s_subscriptions s LEFT JOIN first_hist fh USING (subscription_id)
    WHERE s.started_at IS NOT NULL
    UNION ALL
    SELECT h.subscription_id, s.user_id, h.change_ts, h.new_plan, h.new_mrr, s.canceled_at
    FROM s_history h JOIN s_subscriptions s USING (subscription_id)
    WHERE s.started_at IS NOT NULL
)
SELECT subscription_id, user_id, plan_id, mrr_amount, valid_from,
       COALESCE(LEAD(valid_from) OVER (PARTITION BY subscription_id ORDER BY valid_from),
                canceled_at, TIMESTAMP '9999-12-31') AS valid_to
FROM periods
