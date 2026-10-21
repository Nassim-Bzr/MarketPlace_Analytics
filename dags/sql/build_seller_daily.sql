-- analytics.seller_daily : CA par vendeur et par jour (US-02 top vendeurs).

BEGIN;

DELETE FROM analytics.seller_daily;

INSERT INTO analytics.seller_daily (dt, seller_id, seller_name, total_orders, total_revenue, total_commission)
SELECT
    f.dt,
    f.seller_id,
    s.name,
    COUNT(*),
    SUM(f.total_amount),
    SUM(f.commission_amount)
FROM dwh.fact_orders AS f
JOIN dwh.dim_seller AS s USING (seller_id)
WHERE f.is_revenue
GROUP BY f.dt, f.seller_id, s.name;

COMMIT;
