-- GRAIN: one row per MRR-changing event of a subscription.
-- MRR = contracted recurring revenue of ACTIVE paid subscriptions, derived from the subscription
-- lifecycle (NOT from payments), so renewals are never double counted and refunds do not change MRR.
--   new         = + initial monthly price at first paid start
--   expansion   = + (new_mrr - old_mrr) on upgrade
--   contraction = - (old_mrr - new_mrr) on downgrade
--   churn       = - current MRR at cancellation
-- Trials never carry MRR.
WITH first_hist AS (
    SELECT subscription_id, ARG_MIN(old_mrr, change_ts) AS first_old_mrr FROM s_history GROUP BY subscription_id
),
new_ AS (
    SELECT s.subscription_id, s.user_id, s.started_at AS event_ts, 'new' AS movement_type,
           COALESCE(fh.first_old_mrr, s.mrr_amount) AS mrr_delta
    FROM s_subscriptions s LEFT JOIN first_hist fh USING (subscription_id)
    WHERE s.started_at IS NOT NULL
),
changes AS (
    SELECT h.subscription_id, s.user_id, h.change_ts AS event_ts,
           CASE WHEN h.new_mrr > h.old_mrr THEN 'expansion' ELSE 'contraction' END AS movement_type,
           h.new_mrr - h.old_mrr AS mrr_delta
    FROM s_history h JOIN s_subscriptions s USING (subscription_id)
    WHERE h.new_mrr <> h.old_mrr AND s.started_at IS NOT NULL
),
churn AS (
    SELECT subscription_id, user_id, canceled_at AS event_ts, 'churn' AS movement_type, -mrr_amount AS mrr_delta
    FROM s_subscriptions WHERE status = 'canceled' AND canceled_at IS NOT NULL
)
SELECT subscription_id, user_id, event_ts, CAST(event_ts AS DATE) AS event_date,
       DATE_TRUNC('month', event_ts)::DATE AS event_month, movement_type, mrr_delta
FROM (SELECT * FROM new_ UNION ALL SELECT * FROM changes UNION ALL SELECT * FROM churn)
