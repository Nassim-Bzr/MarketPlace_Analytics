-- Garantit que chaque clé référencée par les commandes du jour existe dans les dimensions,
-- sinon l'INSERT dans fact_orders échoue sur les FOREIGN KEY.
-- Les membres inconnus sont complétés au prochain run de marketplace_dims_refresh_daily.
-- Idempotent : ON CONFLICT DO NOTHING.

BEGIN;

INSERT INTO dwh.dim_seller (seller_id, name)
SELECT DISTINCT seller_id, 'Inconnu (' || seller_id || ')'
FROM staging.orders
WHERE dt = '{{ ds }}' AND seller_id IS NOT NULL
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO dwh.dim_product (product_id, name, category, seller_id)
SELECT DISTINCT ON (product_id) product_id, 'Inconnu (' || product_id || ')', 'Inconnue', seller_id
FROM staging.orders
WHERE dt = '{{ ds }}' AND product_id IS NOT NULL
ORDER BY product_id
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO dwh.dim_customer (customer_id)
SELECT DISTINCT customer_id
FROM staging.orders
WHERE dt = '{{ ds }}' AND customer_id IS NOT NULL
ON CONFLICT (customer_id) DO NOTHING;

COMMIT;
