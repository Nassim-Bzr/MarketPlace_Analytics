"""Custom Hook pour l'API REST Marketplace (authentification Bearer)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.sdk import BaseHook


class MarketplaceAPIHook(BaseHook):
    """Interroge l'API Marketplace à partir de la Connection Airflow ``marketplace_api``.

    - ``host``     : URL de base de l'API (ex. ``http://api-marketplace:5000``)
    - ``password`` : token Bearer

    Les erreurs réseau (timeout, connexion refusée) et les réponses 5xx sont
    réessayées avec un backoff exponentiel ; les 4xx (401, 400…) échouent
    immédiatement, inutile de réessayer.
    """

    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    RETRY_STATUS_CODES = (500, 502, 503, 504)

    def __init__(
        self,
        marketplace_conn_id: str = default_conn_name,
        timeout: float = 30,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> None:
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._session: requests.Session | None = None
        self._base_url: str | None = None

    # ------------------------------------------------------------------
    # Connexion
    # ------------------------------------------------------------------

    def get_conn(self) -> requests.Session:
        """Session HTTP authentifiée, créée à la demande (jamais au parsing du DAG)."""
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.marketplace_conn_id)
        if not conn.host:
            raise ValueError(f"La Connection '{self.marketplace_conn_id}' n'a pas de host")
        if not conn.password:
            raise ValueError(f"La Connection '{self.marketplace_conn_id}' n'a pas de token (champ password)")

        host = conn.host if "://" in conn.host else f"{conn.schema or 'http'}://{conn.host}"
        if conn.port and host.count(":") < 2:
            host = f"{host}:{conn.port}"
        self._base_url = host.rstrip("/")

        retry = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.RETRY_STATUS_CODES,
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {conn.password}", "Accept": "application/json"})
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        self._session = session
        return session

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict]:
        session = self.get_conn()
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        params = {k: v for k, v in (params or {}).items() if v is not None}

        self.log.info("GET %s params=%s", url, params)
        response = session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()
        # L'API renvoie {"count": N, "data": [...]} ; on accepte aussi une liste brute
        records = payload.get("data", []) if isinstance(payload, dict) else payload
        if not isinstance(records, list):
            raise ValueError(f"Réponse inattendue de {url} : {type(records).__name__}")

        self.log.info("GET %s -> %d enregistrement(s)", url, len(records))
        return records

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    def get_orders(self, date: str) -> list[dict]:
        """Commandes d'une journée. ``date`` au format ``YYYY-MM-DD``."""
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Date invalide : {date!r} (attendu YYYY-MM-DD)") from exc
        return self._get("/orders", {"date": date})

    def get_sellers(self, limit: int | None = None) -> list[dict]:
        return self._get("/sellers", {"limit": limit})

    def get_products(self, limit: int | None = None) -> list[dict]:
        return self._get("/products", {"limit": limit})

    def get_customers(self, limit: int | None = None) -> list[dict]:
        return self._get("/customers", {"limit": limit})

    def test_connection(self) -> tuple[bool, str]:
        """Bouton « Test » de l'UI Airflow : appelle /health."""
        try:
            session = self.get_conn()
            response = session.get(f"{self._base_url}/health", timeout=self.timeout)
            response.raise_for_status()
            return True, f"API Marketplace OK : {response.json()}"
        except Exception as exc:  # noqa: BLE001 — on remonte le message tel quel à l'UI
            return False, str(exc)
