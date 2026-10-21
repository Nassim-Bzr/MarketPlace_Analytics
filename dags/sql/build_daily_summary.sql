-- analytics.daily_summary : 1 ligne par jour (US-01 CA journalier, US-03 commissions).
-- Reconstruction complète dans une transaction (quelques centaines de lignes) :
-- toujours cohérente avec dwh.fact_orders, quelle que soit la date rejouée.
-- CA / commissions / panier moyen : commandes is_revenue uniquement (hors cancelled / refunded).

BEGIN;

DELETE FROM analytics.daily_summary;

INSERT INTO analytics.daily_summary (
    dt, total_orders, cancelled_orders, total_revenue, total_commission,
    avg_basket, active_sellers, active_customers, top_seller_id
)
WITH seller_revenue AS (
    SELECT dt, seller_id, SUM(total_amount) AS revenue
    FROM dwh.fact_orders
    WHERE is_revenue
    GROUP BY dt, seller_id
),
top_seller AS (
    SELECT DISTINCT ON (dt) dt, seller_id
    FROM seller_revenue
    ORDER BY dt, revenue DESC, seller_id
)
SELECT
    f.dt,
    COUNT(*)                                            FILTER (WHERE f.is_revenue),
    COUNT(*)                                            FILTER (WHERE NOT f.is_revenue),
    COALESCE(SUM(f.total_amount)      FILTER (WHERE f.is_revenue), 0),
    COALESCE(SUM(f.commission_amount) FILTER (WHERE f.is_revenue), 0),
    ROUND(AVG(f.total_amount)         FILTER (WHERE f.is_revenue), 2),
    COUNT(DISTINCT f.seller_id)                         FILTER (WHERE f.is_revenue),
    COUNT(DISTINCT f.customer_id)                       FILTER (WHERE f.is_revenue),
    t.seller_id
FROM dwh.fact_orders AS f
LEFT JOIN top_seller AS t USING (dt)
GROUP BY f.dt, t.seller_id;

COMMIT;
