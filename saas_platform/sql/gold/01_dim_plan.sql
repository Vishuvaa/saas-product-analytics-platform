-- GRAIN: one row per subscription plan. Prices are list prices (USD / month).
SELECT * FROM (VALUES
    ('starter', 29.0, 1),
    ('pro', 99.0, 2),
    ('business', 299.0, 3)
) AS t(plan_id, list_price, tier_rank)
