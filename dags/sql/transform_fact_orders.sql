-- staging.orders -> dwh.fact_orders pour la partition dt = {{ ds }}.
-- Pattern d'idempotence obligatoire : DELETE + INSERT de la partition dans une transaction.
-- Rejouer la même date = même résultat en base.

BEGIN;

DELETE FROM dwh.fact_orders
WHERE dt = '{{ ds }}';

INSERT INTO dwh.fact_orders (
    order_id, dt, seller_id, customer_id, product_id, order_ts,
    quantity, unit_price, total_amount, commission_amount, status, is_revenue
)
SELECT DISTINCT ON (s.order_id)
    s.order_id,
    s.dt,
    s.seller_id,
    s.customer_id,
    s.product_id,
    s.order_ts,
    s.quantity,
    s.unit_price,
    COALESCE(s.total_amount, s.quantity * s.unit_price),
    ROUND(COALESCE(s.total_amount, s.quantity * s.unit_price) * {{ params.commission_rate }}, 2),
    s.status,
    s.status NOT IN ('cancelled', 'refunded')
FROM staging.orders AS s
WHERE s.dt = '{{ ds }}'
ORDER BY s.order_id, s.loaded_at DESC;

COMMIT;
