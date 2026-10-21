"""
### marketplace_analytics_aggregate_daily — tables d'agrégation pour Metabase

Déclenché par l'Asset `dwh_fact_orders` : tourne dès que `marketplace_orders_ingest_daily`
a mis à jour la table de faits, sans horaire fixe.

Un run déclenché par un Asset n'a pas de date logique : chaque table `analytics.*`
est donc reconstruite entièrement (DELETE + INSERT dans une transaction).
À ce volume (quelques milliers de lignes agrégées), c'est instantané et toujours cohérent.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
from marketplace_config import DEFAULT_ARGS, DWH_CONN_ID, DWH_FACT_ORDERS

AGGREGATES = ["daily_summary", "seller_daily", "category_daily"]


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    schedule=[DWH_FACT_ORDERS],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "analytics"],
    doc_md=__doc__,
)
def marketplace_analytics_aggregate_daily():
    for table in AGGREGATES:
        SQLExecuteQueryOperator(
            task_id=f"build_{table}",
            conn_id=DWH_CONN_ID,
            sql=f"sql/build_{table}.sql",
            autocommit=True,
            show_return_value_in_logs=False,
        )


marketplace_analytics_aggregate_daily()
