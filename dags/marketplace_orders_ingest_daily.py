"""
### marketplace_orders_ingest_daily — pipeline ELT principal

`API /orders?date={{ ds }}` → **bronze** (MinIO, JSON brut) → **staging.orders** → **dwh.fact_orders**

Chaque étape est idempotente : rejouer une date donne exactement le même résultat
(fichier bronze écrasé, DELETE + INSERT de la partition `dt` en staging et dans le DWH).

La fin du DAG met à jour l'Asset `dwh_fact_orders`, ce qui déclenche
`marketplace_analytics_aggregate_daily`.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from hooks.marketplace_api_hook import MarketplaceAPIHook
from marketplace_config import (
    COMMISSION_RATE,
    DEFAULT_ARGS,
    DWH_CONN_ID,
    DWH_FACT_ORDERS,
    MARKETPLACE_CONN_ID,
)
from marketplace_io import load_bronze_to_staging, upload_to_bronze


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "elt", "orders"],
    doc_md=__doc__,
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders_to_bronze(ds: str | None = None) -> int:
        """Appelle l'API pour la date du run et archive le JSON brut dans MinIO.

        Seul le nombre de commandes transite par XCom, pas les données.
        """
        if ds is None:
            raise ValueError("Run sans date logique : déclencher le DAG avec une logical date")
        orders = MarketplaceAPIHook(marketplace_conn_id=MARKETPLACE_CONN_ID).get_orders(ds)
        upload_to_bronze(orders, "orders", ds)
        return len(orders)

    @task
    def load_staging_orders(extracted: int, ds: str | None = None) -> int:
        """Relit bronze et remplace la partition dt = ds de staging.orders."""
        loaded = load_bronze_to_staging("orders", ds)
        if loaded != extracted:
            raise ValueError(f"{extracted} commandes extraites mais {loaded} chargées en staging")
        return loaded

    ensure_dimensions = SQLExecuteQueryOperator(
        task_id="ensure_dimensions",
        conn_id=DWH_CONN_ID,
        sql="sql/ensure_dimensions.sql",
        autocommit=True,  # la transaction est gérée par BEGIN / COMMIT dans le fichier SQL
        show_return_value_in_logs=False,
    )

    transform_fact_orders = SQLExecuteQueryOperator(
        task_id="transform_fact_orders",
        conn_id=DWH_CONN_ID,
        sql="sql/transform_fact_orders.sql",
        params={"commission_rate": COMMISSION_RATE},
        autocommit=True,
        show_return_value_in_logs=False,
        outlets=[DWH_FACT_ORDERS],
    )

    load_staging_orders(extract_orders_to_bronze()) >> ensure_dimensions >> transform_fact_orders


marketplace_orders_ingest_daily()
