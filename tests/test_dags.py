"""Tests de structure des DAGs (aucun service externe requis)."""

import re
from pathlib import Path

import pytest

from conftest import DAGS_DIR, PLUGINS_DIR

EXPECTED_DAGS = {
    "marketplace_orders_ingest_daily",
    "marketplace_dims_refresh_daily",
    "marketplace_analytics_aggregate_daily",
}
INGEST_DAG_ID = "marketplace_orders_ingest_daily"


def _sql_of(dag, task_id: str) -> str:
    """SQL d'un SQLExecuteQueryOperator.

    Le DagBag résout déjà les fichiers .sql (task.sql contient alors le contenu) ;
    sinon on lit le fichier relativement au fichier du DAG.
    """
    sql = dag.get_task(task_id).sql
    if sql.endswith(".sql"):
        return (Path(dag.fileloc).parent / sql).read_text()
    return sql


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Erreurs d'import : {dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    assert EXPECTED_DAGS <= set(dagbag.dag_ids)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_all_dags_catchup_false(dagbag, dag_id):
    assert dagbag.get_dag(dag_id).catchup is False


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_all_tasks_have_retries(dagbag, dag_id):
    for task in dagbag.get_dag(dag_id).tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} n'a pas de retry"


def test_ingest_dag_structure(dagbag):
    dag = dagbag.get_dag(INGEST_DAG_ID)
    chain = ["extract_orders_to_bronze", "load_staging_orders", "ensure_dimensions", "transform_fact_orders"]

    assert set(chain) <= set(dag.task_ids)
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, f"{upstream} -> {downstream} manquant"


def test_marketplace_ingest_has_idempotent_transform(dagbag):
    """DELETE de la partition {{ ds }} AVANT l'INSERT, le tout dans une transaction."""
    sql = _sql_of(dagbag.get_dag(INGEST_DAG_ID), "transform_fact_orders")
    normalized = re.sub(r"\s+", " ", sql)

    delete = re.search(r"DELETE FROM dwh\.fact_orders WHERE dt = '\{\{ ds \}\}'", normalized)
    insert = re.search(r"INSERT INTO dwh\.fact_orders", normalized)
    assert delete, "Pas de DELETE de la partition dt = '{{ ds }}'"
    assert insert, "Pas d'INSERT dans dwh.fact_orders"
    assert delete.start() < insert.start(), "Le DELETE doit précéder l'INSERT"
    assert "WHERE s.dt = '{{ ds }}'" in normalized, "L'INSERT doit être filtré sur la partition {{ ds }}"
    assert normalized.index("BEGIN;") < delete.start() and insert.start() < normalized.index("COMMIT;")


def test_no_hardcoded_date_in_sql():
    for sql_file in (DAGS_DIR / "sql").glob("*.sql"):
        assert not re.search(r"'20\d\d-\d\d-\d\d'", sql_file.read_text()), f"Date en dur dans {sql_file.name}"


def test_analytics_dag_triggered_by_ingest_asset(dagbag):
    ingest = dagbag.get_dag(INGEST_DAG_ID)
    analytics = dagbag.get_dag("marketplace_analytics_aggregate_daily")

    outlets = {asset.name for asset in ingest.get_task("transform_fact_orders").outlets}
    assert "dwh_fact_orders" in outlets
    assert type(analytics.timetable).__name__ == "AssetTriggeredTimetable"


def test_no_hardcoded_secrets():
    secrets = ["formation-token-2026", "minio_password_2026", "dwh_password_2026"]
    for folder in (DAGS_DIR, PLUGINS_DIR):
        for path in folder.rglob("*"):
            if path.suffix in {".py", ".sql"}:
                content = path.read_text()
                for secret in secrets:
                    assert secret not in content, f"Secret en dur dans {path}"
