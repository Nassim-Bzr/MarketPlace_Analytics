"""Test d'intégration de l'idempotence sur le vrai DWH (ignoré si postgres-dwh est injoignable).

Exécute deux fois le SQL de transformation du DAG sur une partition de test
et vérifie que la table de faits contient exactement la même chose.
Les données de test (dt = 2020-01-01, identifiants TEST-*) sont supprimées à la fin.
"""

from pathlib import Path

import pytest
from jinja2 import Template

from conftest import DAGS_DIR

TEST_DT = "2020-01-01"
SQL_DIR = DAGS_DIR / "sql"
STAGING_ROWS = [
    # order_id, customer_id, seller_id, product_id, quantity, unit_price, total_amount, status
    ("TEST-ORD-1", "TEST-CUS-1", "TEST-SEL-1", "TEST-PRD-1", 2, 10.00, 20.00, "delivered"),
    ("TEST-ORD-2", "TEST-CUS-1", "TEST-SEL-1", "TEST-PRD-1", 1, 10.00, 10.00, "cancelled"),
    ("TEST-ORD-3", "TEST-CUS-2", "TEST-SEL-2", "TEST-PRD-2", 3, 5.50, 16.50, "shipped"),
]


def _render(sql_file: str) -> str:
    return Template(Path(SQL_DIR / sql_file).read_text()).render(ds=TEST_DT, params={"commission_rate": 0.12})


@pytest.fixture
def dwh_cursor():
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        conn = PostgresHook(postgres_conn_id="postgres_dwh").get_conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DWH injoignable : {exc}")
    conn.autocommit = True  # les fichiers SQL gèrent eux-mêmes BEGIN / COMMIT
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.execute("DELETE FROM dwh.fact_orders WHERE dt = %s", (TEST_DT,))
        cur.execute("DELETE FROM staging.orders WHERE dt = %s", (TEST_DT,))
        cur.execute("DELETE FROM dwh.dim_product WHERE product_id LIKE 'TEST-%%'")
        cur.execute("DELETE FROM dwh.dim_customer WHERE customer_id LIKE 'TEST-%%'")
        cur.execute("DELETE FROM dwh.dim_seller WHERE seller_id LIKE 'TEST-%%'")
        cur.close()
        conn.close()


def _snapshot(cur):
    cur.execute(
        "SELECT order_id, total_amount, commission_amount, is_revenue "
        "FROM dwh.fact_orders WHERE dt = %s ORDER BY order_id",
        (TEST_DT,),
    )
    return cur.fetchall()


def test_transform_is_idempotent_on_real_dwh(dwh_cursor):
    cur = dwh_cursor
    cur.execute("DELETE FROM staging.orders WHERE dt = %s", (TEST_DT,))
    for row in STAGING_ROWS:
        cur.execute(
            "INSERT INTO staging.orders (dt, order_id, customer_id, seller_id, product_id, "
            "quantity, unit_price, total_amount, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (TEST_DT, *row),
        )

    cur.execute(_render("ensure_dimensions.sql"))
    cur.execute(_render("transform_fact_orders.sql"))
    first = _snapshot(cur)

    cur.execute(_render("ensure_dimensions.sql"))
    cur.execute(_render("transform_fact_orders.sql"))
    second = _snapshot(cur)

    assert len(first) == len(STAGING_ROWS)
    assert first == second, "Rejouer la transformation a modifié la table de faits"

    by_id = {order_id: (total, commission, is_revenue) for order_id, total, commission, is_revenue in first}
    assert float(by_id["TEST-ORD-1"][1]) == pytest.approx(2.40)   # 12 % de 20 €
    assert by_id["TEST-ORD-2"][2] is False                         # cancelled exclu du CA
