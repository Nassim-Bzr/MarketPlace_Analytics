-- staging.sellers (snapshot du jour) -> dwh.dim_seller, SCD type 1 (on écrase).
-- Jamais de DELETE sur une dimension : fact_orders y fait référence.

BEGIN;

INSERT INTO dwh.dim_seller (seller_id, name, country, city, main_category, joined_date, updated_at)
SELECT DISTINCT ON (seller_id)
    seller_id, name, country, city, main_category, joined_date, now()
FROM staging.sellers
WHERE dt = '{{ ds }}'
ORDER BY seller_id, loaded_at DESC
ON CONFLICT (seller_id) DO UPDATE SET
    name          = EXCLUDED.name,
    country       = EXCLUDED.country,
    city          = EXCLUDED.city,
    main_category = EXCLUDED.main_category,
    joined_date   = EXCLUDED.joined_date,
    updated_at    = now();

COMMIT;
