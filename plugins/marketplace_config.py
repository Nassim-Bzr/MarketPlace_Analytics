"""Constantes partagées par les DAGs Marketplace (identifiants de connexion, buckets, Assets)."""

import os
from datetime import timedelta

from airflow.sdk import Asset

MARKETPLACE_CONN_ID = "marketplace_api"
MINIO_CONN_ID = "minio_s3"
DWH_CONN_ID = "postgres_dwh"

BRONZE_BUCKET = os.environ.get("MINIO_BUCKET_BRONZE", "bronze")

# Mis à jour par le DAG d'ingestion, déclenche le DAG d'agrégation analytics
DWH_FACT_ORDERS = Asset(name="dwh_fact_orders", uri="postgres://postgres-dwh:5432/dwh/dwh/fact_orders")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

COMMISSION_RATE = 0.12


def bronze_key(entity: str, ds: str) -> str:
    """Clé MinIO du JSON brut : ``<entité>/dt=YYYY-MM-DD/data.json``."""
    return f"{entity}/dt={ds}/data.json"
