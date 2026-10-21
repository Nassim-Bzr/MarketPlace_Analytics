"""Entrées / sorties communes aux DAGs : bronze (MinIO) et staging (PostgreSQL)."""

from __future__ import annotations

import json
import logging

from psycopg2.extras import execute_values

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from marketplace_config import BRONZE_BUCKET, DWH_CONN_ID, MINIO_CONN_ID, bronze_key

log = logging.getLogger(__name__)

# Colonnes chargées dans staging.<entité>, dans l'ordre des champs de l'API.
# Sert aussi de liste blanche : seules ces tables peuvent être écrites.
STAGING_COLUMNS: dict[str, list[str]] = {
    "orders": ["order_id", "order_ts", "customer_id", "seller_id", "product_id",
               "quantity", "unit_price", "total_amount", "status"],
    "sellers": ["seller_id", "name", "country", "city", "main_category", "joined_date", "active_until"],
    "products": ["product_id", "name", "category", "base_price", "seller_id"],
    "customers": ["customer_id", "first_name", "last_name", "email", "city", "signup_date"],
}


def upload_to_bronze(records: list[dict], entity: str, ds: str) -> str:
    """Archive le JSON brut dans bronze/<entité>/dt=<ds>/data.json.

    ``replace=True`` : rejouer la même date écrase le même fichier (idempotent).
    """
    key = bronze_key(entity, ds)
    S3Hook(aws_conn_id=MINIO_CONN_ID).load_string(
        json.dumps(records, ensure_ascii=False),
        key=key,
        bucket_name=BRONZE_BUCKET,
        replace=True,
    )
    log.info("%d enregistrement(s) écrits dans s3://%s/%s", len(records), BRONZE_BUCKET, key)
    return key


def load_bronze_to_staging(entity: str, ds: str) -> int:
    """Relit le JSON depuis bronze et remplace la partition dt = ds de staging.<entité>.

    On relit MinIO (et pas l'API) : la transformation est rejouable sans rappeler la source.
    DELETE + INSERT dans UNE transaction : rejouer = même résultat.
    """
    if entity not in STAGING_COLUMNS:
        raise ValueError(f"Entité inconnue : {entity!r}")
    columns = STAGING_COLUMNS[entity]

    raw = S3Hook(aws_conn_id=MINIO_CONN_ID).read_key(key=bronze_key(entity, ds), bucket_name=BRONZE_BUCKET)
    records = json.loads(raw)
    rows = [(ds, *(record.get(column) for column in columns)) for record in records]

    conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
    try:
        # "with conn" : COMMIT si tout passe, ROLLBACK sinon
        with conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM staging.{entity} WHERE dt = %s", (ds,))
            deleted = cur.rowcount
            if rows:
                execute_values(
                    cur,
                    f"INSERT INTO staging.{entity} (dt, {', '.join(columns)}) VALUES %s",
                    rows,
                    page_size=1000,
                )
    finally:
        conn.close()

    log.info("staging.%s dt=%s : %d ligne(s) supprimée(s), %d insérée(s)", entity, ds, deleted, len(rows))
    return len(rows)
