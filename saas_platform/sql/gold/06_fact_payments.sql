-- GRAIN: one row per payment record (charge or refund).
-- signed_amount (cash view): successful charge = +amount, successful refund = -amount, failed = 0.
-- This is BOOKED CASH, not recurring revenue; see fact_mrr_movement for MRR.
SELECT p.payment_id, p.subscription_id, p.user_id, p.kind, p.status, p.amount,
       CASE WHEN p.status <> 'succeeded' THEN 0.0
            WHEN p.kind = 'charge' THEN p.amount ELSE -p.amount END AS signed_amount,
       p.paid_at, CAST(p.paid_at AS DATE) AS payment_date,
       DATE_TRUNC('month', p.paid_at)::DATE AS payment_month,
       COALESCE(pp.plan_id, 'unknown') AS plan_id
FROM s_payments p
LEFT JOIN gold_fact_subscription_plan_period pp
       ON pp.subscription_id = p.subscription_id AND p.paid_at >= pp.valid_from AND p.paid_at < pp.valid_to
