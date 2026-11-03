-- Per month and plan: month-end MRR (contracted, from plan periods) next to booked cash (payments).
-- gross_payments = successful charges, refunds = successful refunds (positive number), net_cash = gross - refunds.
-- Cash and MRR differ by design: MRR is contracted run-rate; cash is what was charged/refunded in the month.
WITH mrr AS (
    SELECT d.month_start AS month, p.plan_id, SUM(p.mrr_amount) AS ending_mrr, COUNT(*) AS paying_subs
    FROM (SELECT DISTINCT month_start, month_end FROM gold_dim_date) d
    JOIN gold_fact_subscription_plan_period p
      ON p.valid_from < CAST(d.month_end AS TIMESTAMP) + INTERVAL 1 DAY
     AND p.valid_to  >= CAST(d.month_end AS TIMESTAMP) + INTERVAL 1 DAY
    GROUP BY 1, 2
),
cash AS (
    SELECT payment_month AS month, plan_id,
           SUM(CASE WHEN kind = 'charge' AND status = 'succeeded' THEN amount ELSE 0 END) AS gross_payments,
           SUM(CASE WHEN kind = 'refund' AND status = 'succeeded' THEN amount ELSE 0 END) AS refunds,
           SUM(signed_amount) AS net_cash
    FROM gold_fact_payments GROUP BY 1, 2
)
SELECT COALESCE(m.month, c.month) AS month, COALESCE(m.plan_id, c.plan_id) AS plan_id,
       COALESCE(m.ending_mrr, 0) AS ending_mrr, COALESCE(m.paying_subs, 0) AS paying_subs,
       COALESCE(c.gross_payments, 0) AS gross_payments, COALESCE(c.refunds, 0) AS refunds,
       COALESCE(c.net_cash, 0) AS net_cash
FROM mrr m FULL OUTER JOIN cash c ON m.month = c.month AND m.plan_id = c.plan_id
ORDER BY 1, 2
