-- staging.customers (snapshot du jour) -> dwh.dim_customer, SCD type 1.

BEGIN;

INSERT INTO dwh.dim_customer (customer_id, first_name, last_name, email, city, signup_date, updated_at)
SELECT DISTINCT ON (customer_id)
    customer_id, first_name, last_name, email, city, signup_date, now()
FROM staging.customers
WHERE dt = '{{ ds }}'
ORDER BY customer_id, loaded_at DESC
ON CONFLICT (customer_id) DO UPDATE SET
    first_name  = EXCLUDED.first_name,
    last_name   = EXCLUDED.last_name,
    email       = EXCLUDED.email,
    city        = EXCLUDED.city,
    signup_date = EXCLUDED.signup_date,
    updated_at  = now();

COMMIT;
