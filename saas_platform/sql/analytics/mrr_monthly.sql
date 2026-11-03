-- Month-end MRR and the movements that explain it. new + expansion + contraction + churn = net_new_mrr,
-- and ending_mrr is the running sum of net_new_mrr. MRR is NOT payment volume.
WITH m AS (
    SELECT event_month,
           SUM(CASE WHEN movement_type = 'new' THEN mrr_delta ELSE 0 END) AS new_mrr,
           SUM(CASE WHEN movement_type = 'expansion' THEN mrr_delta ELSE 0 END) AS expansion_mrr,
           SUM(CASE WHEN movement_type = 'contraction' THEN mrr_delta ELSE 0 END) AS contraction_mrr,
           SUM(CASE WHEN movement_type = 'churn' THEN mrr_delta ELSE 0 END) AS churned_mrr
    FROM gold_fact_mrr_movement GROUP BY 1
)
SELECT d.month_start AS month, COALESCE(new_mrr, 0) AS new_mrr, COALESCE(expansion_mrr, 0) AS expansion_mrr,
       COALESCE(contraction_mrr, 0) AS contraction_mrr, COALESCE(churned_mrr, 0) AS churned_mrr,
       COALESCE(new_mrr + expansion_mrr + contraction_mrr + churned_mrr, 0) AS net_new_mrr,
       SUM(COALESCE(new_mrr + expansion_mrr + contraction_mrr + churned_mrr, 0)) OVER (ORDER BY d.month_start) AS ending_mrr
FROM (SELECT DISTINCT month_start FROM gold_dim_date) d LEFT JOIN m ON m.event_month = d.month_start
ORDER BY 1
