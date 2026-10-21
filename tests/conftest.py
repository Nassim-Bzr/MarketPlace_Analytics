"""Fixtures pytest — à lancer dans le conteneur :

    docker compose exec airflow-worker pytest /opt/airflow/tests -v
"""

import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_DIR / "dags"
PLUGINS_DIR = PROJECT_DIR / "plugins"

# Airflow ajoute dags/ et plugins/ au sys.path au runtime ; pytest ne le fait pas
for path in (PLUGINS_DIR, DAGS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
