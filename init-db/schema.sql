-- =====================================================================
-- MarketPlace Analytics — modèle de données du Data Warehouse
--
-- Exécuté automatiquement par postgres-dwh au PREMIER démarrage (volume vide),
-- après 00_schemas.sql. Rejouable à la main sans erreur :
--   psql -h localhost -p 5433 -U dwh_user -d dwh -f init-db/schema.sql
--
--   staging   : données brutes typées, 1 table par endpoint, partitionnées par dt
--   dwh       : modèle en étoile (Kimball) — 4 dimensions + 1 table de faits
--   analytics : agrégats pré-calculés lus par Metabase
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =====================================================================
-- STAGING — dt = date du run Airflow ({{ ds }}) qui a chargé la ligne
-- =====================================================================

CREATE TABLE IF NOT EXISTS staging.orders (
    dt            DATE          NOT NULL,
    order_id      TEXT          NOT NULL,
    order_ts      TIMESTAMP,
    customer_id   TEXT,
    seller_id     TEXT,
    product_id    TEXT,
    quantity      INTEGER,
    unit_price    NUMERIC(12, 2),
    total_amount  NUMERIC(12, 2),
    status        TEXT,
    loaded_at     TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_staging_orders_dt ON staging.orders (dt);

CREATE TABLE IF NOT EXISTS staging.sellers (
    dt            DATE          NOT NULL,
    seller_id     TEXT          NOT NULL,
    name          TEXT,
    country       TEXT,
    city          TEXT,
    main_category TEXT,
    joined_date   DATE,
    active_until  DATE,
    loaded_at     TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_staging_sellers_dt ON staging.sellers (dt);

CREATE TABLE IF NOT EXISTS staging.products (
    dt            DATE          NOT NULL,
    product_id    TEXT          NOT NULL,
    name          TEXT,
    category      TEXT,
    base_price    NUMERIC(12, 2),
    seller_id     TEXT,
    loaded_at     TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_staging_products_dt ON staging.products (dt);

CREATE TABLE IF NOT EXISTS staging.customers (
    dt            DATE          NOT NULL,
    customer_id   TEXT          NOT NULL,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    city          TEXT,
    signup_date   DATE,
    loaded_at     TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_staging_customers_dt ON staging.customers (dt);

-- =====================================================================
-- DWH — dimensions (SCD type 1 : on écrase, pas d'historique)
-- =====================================================================

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt            DATE          PRIMARY KEY,
    year          SMALLINT      NOT NULL,
    quarter       SMALLINT      NOT NULL,
    month         SMALLINT      NOT NULL,
    month_name    TEXT          NOT NULL,
    week          SMALLINT      NOT NULL,
    day_of_month  SMALLINT      NOT NULL,
    day_of_week   SMALLINT      NOT NULL,   -- 1 = lundi ... 7 = dimanche (ISO)
    day_name      TEXT          NOT NULL,
    is_weekend    BOOLEAN       NOT NULL
);

-- Pré-remplie une fois pour toutes : aucune maintenance, FK toujours satisfaite
INSERT INTO dwh.dim_date
SELECT d::date,
       EXTRACT(YEAR    FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH   FROM d),
       (ARRAY['janvier','février','mars','avril','mai','juin','juillet',
              'août','septembre','octobre','novembre','décembre'])[EXTRACT(MONTH FROM d)],
       EXTRACT(WEEK    FROM d),
       EXTRACT(DAY     FROM d),
       EXTRACT(ISODOW  FROM d),
       (ARRAY['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') AS d
ON CONFLICT (dt) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id     TEXT          PRIMARY KEY,
    name          TEXT          NOT NULL,
    country       TEXT,
    city          TEXT,
    main_category TEXT,
    joined_date   DATE,
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id   TEXT          PRIMARY KEY,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    city          TEXT,
    signup_date   DATE,
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id    TEXT          PRIMARY KEY,
    name          TEXT          NOT NULL,
    category      TEXT          NOT NULL,
    base_price    NUMERIC(12, 2),
    seller_id     TEXT          REFERENCES dwh.dim_seller (seller_id),
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

-- =====================================================================
-- DWH — table de faits (grain : 1 ligne = 1 commande)
-- =====================================================================

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id          TEXT          PRIMARY KEY,
    dt                DATE          NOT NULL REFERENCES dwh.dim_date     (dt),
    seller_id         TEXT          NOT NULL REFERENCES dwh.dim_seller   (seller_id),
    customer_id       TEXT                   REFERENCES dwh.dim_customer (customer_id),
    product_id        TEXT          NOT NULL REFERENCES dwh.dim_product  (product_id),
    order_ts          TIMESTAMP,
    quantity          INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12, 2) NOT NULL,
    total_amount      NUMERIC(12, 2) NOT NULL,
    commission_amount NUMERIC(12, 2) NOT NULL,   -- total_amount × 12 %
    status            TEXT          NOT NULL,
    is_revenue        BOOLEAN       NOT NULL,    -- FALSE si cancelled / refunded (exclu du CA)
    loaded_at         TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_dt        ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_seller_dt ON dwh.fact_orders (seller_id, dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_product   ON dwh.fact_orders (product_id);

-- =====================================================================
-- ANALYTICS — agrégats pour Metabase (uniquement les commandes is_revenue)
-- =====================================================================

CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt               DATE          PRIMARY KEY,
    total_orders     INTEGER       NOT NULL,
    cancelled_orders INTEGER       NOT NULL,
    total_revenue    NUMERIC(14, 2) NOT NULL,
    total_commission NUMERIC(14, 2) NOT NULL,
    avg_basket       NUMERIC(12, 2),
    active_sellers   INTEGER       NOT NULL,
    active_customers INTEGER       NOT NULL,
    top_seller_id    TEXT,
    built_at         TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt               DATE          NOT NULL,
    seller_id        TEXT          NOT NULL,
    seller_name      TEXT,
    total_orders     INTEGER       NOT NULL,
    total_revenue    NUMERIC(14, 2) NOT NULL,
    total_commission NUMERIC(14, 2) NOT NULL,
    built_at         TIMESTAMP     NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt               DATE          NOT NULL,
    category         TEXT          NOT NULL,
    total_orders     INTEGER       NOT NULL,
    total_revenue    NUMERIC(14, 2) NOT NULL,
    total_commission NUMERIC(14, 2) NOT NULL,
    built_at         TIMESTAMP     NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, category)
);
