"""
### marketplace_dims_refresh_daily — rafraîchissement des dimensions

Pour chaque référentiel (`sellers`, `products`, `customers`) :
`API` → **bronze** (MinIO) → **staging.<entité>** (partition `dt`) → **upsert** dans `dwh.dim_*` (SCD type 1).

`dim_product` est mise à jour après `dim_seller` (clé étrangère `seller_id`).
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from hooks.marketplace_api_hook import MarketplaceAPIHook
from marketplace_config import DEFAULT_ARGS, DWH_CONN_ID, MARKETPLACE_CONN_ID
from marketplace_io import load_bronze_to_staging, upload_to_bronze

# entité de l'API -> fichier SQL d'upsert de la dimension
DIMENSIONS = {
    "sellers": "sql/upsert_dim_seller.sql",
    "products": "sql/upsert_dim_product.sql",
    "customers": "sql/upsert_dim_customer.sql",
}


@dag(
    dag_id="marketplace_dims_refresh_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "elt", "dimensions"],
    doc_md=__doc__,
)
def marketplace_dims_refresh_daily():

    @task
    def extract_to_bronze(entity: str, ds: str | None = None) -> int:
        hook = MarketplaceAPIHook(marketplace_conn_id=MARKETPLACE_CONN_ID)
        fetch = {
            "sellers": hook.get_sellers,
            "products": hook.get_products,
            "customers": hook.get_customers,
        }[entity]
        records = fetch()
        upload_to_bronze(records, entity, ds)
        return len(records)

    @task
    def load_staging(entity: str, extracted: int, ds: str | None = None) -> int:
        loaded = load_bronze_to_staging(entity, ds)
        if loaded != extracted:
            raise ValueError(f"{entity} : {extracted} extraits mais {loaded} chargés en staging")
        return loaded

    upserts = {}
    for entity, sql_file in DIMENSIONS.items():
        extracted = extract_to_bronze.override(task_id=f"extract_{entity}")(entity)
        loaded = load_staging.override(task_id=f"load_staging_{entity}")(entity, extracted)
        upserts[entity] = SQLExecuteQueryOperator(
            task_id=f"upsert_dim_{entity.removesuffix('s')}",
            conn_id=DWH_CONN_ID,
            sql=sql_file,
            autocommit=True,
            show_return_value_in_logs=False,
        )
        loaded >> upserts[entity]

    upserts["sellers"] >> upserts["products"]


marketplace_dims_refresh_daily()
