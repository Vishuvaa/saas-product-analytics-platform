-- Monthly churn and cancellations.
-- logo_churn_pct    = subscriptions canceled in month / paying subscriptions at start of month
-- revenue_churn_pct = churned MRR / MRR at start of month   (gross; excludes expansion)
WITH ev AS (
    SELECT event_month,
           SUM(CASE WHEN movement_type = 'new' THEN 1 WHEN movement_type = 'churn' THEN -1 ELSE 0 END) AS net_subs,
           SUM(CASE WHEN movement_type = 'churn' THEN 1 ELSE 0 END) AS canceled_subs,
           SUM(CASE WHEN movement_type = 'churn' THEN -mrr_delta ELSE 0 END) AS churned_mrr,
           SUM(mrr_delta) AS net_mrr
    FROM gold_fact_mrr_movement GROUP BY 1
),
m AS (
    SELECT d.month_start AS month, COALESCE(canceled_subs, 0) AS canceled_subs, COALESCE(churned_mrr, 0) AS churned_mrr,
           COALESCE(net_subs, 0) AS net_subs, COALESCE(net_mrr, 0) AS net_mrr
    FROM (SELECT DISTINCT month_start FROM gold_dim_date) d LEFT JOIN ev ON ev.event_month = d.month_start
)
SELECT month, canceled_subs, churned_mrr,
       SUM(net_subs) OVER (ORDER BY month) - net_subs AS starting_subs,
       SUM(net_mrr) OVER (ORDER BY month) - net_mrr AS starting_mrr,
       ROUND(100.0 * canceled_subs / NULLIF(SUM(net_subs) OVER (ORDER BY month) - net_subs, 0), 1) AS logo_churn_pct,
       ROUND(100.0 * churned_mrr / NULLIF(SUM(net_mrr) OVER (ORDER BY month) - net_mrr, 0), 1) AS revenue_churn_pct
FROM m ORDER BY month
