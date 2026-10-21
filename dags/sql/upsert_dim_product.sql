-- staging.products (snapshot du jour) -> dwh.dim_product, SCD type 1.
-- Exécuté APRÈS upsert_dim_seller (FK dim_product.seller_id).

BEGIN;

INSERT INTO dwh.dim_product (product_id, name, category, base_price, seller_id, updated_at)
SELECT DISTINCT ON (product_id)
    product_id, name, category, base_price, seller_id, now()
FROM staging.products
WHERE dt = '{{ ds }}'
ORDER BY product_id, loaded_at DESC
ON CONFLICT (product_id) DO UPDATE SET
    name       = EXCLUDED.name,
    category   = EXCLUDED.category,
    base_price = EXCLUDED.base_price,
    seller_id  = EXCLUDED.seller_id,
    updated_at = now();

COMMIT;
