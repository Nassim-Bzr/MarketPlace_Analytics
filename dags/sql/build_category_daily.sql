-- analytics.category_daily : CA par catégorie et par jour (US-04).

BEGIN;

DELETE FROM analytics.category_daily;

INSERT INTO analytics.category_daily (dt, category, total_orders, total_revenue, total_commission)
SELECT
    f.dt,
    p.category,
    COUNT(*),
    SUM(f.total_amount),
    SUM(f.commission_amount)
FROM dwh.fact_orders AS f
JOIN dwh.dim_product AS p USING (product_id)
WHERE f.is_revenue
GROUP BY f.dt, p.category;

COMMIT;
