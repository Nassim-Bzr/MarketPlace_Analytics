"""Configure Metabase et crée les dashboards MarketPlace Analytics via l'API Metabase.

Idempotent : on peut le relancer autant de fois que nécessaire (les questions et
dashboards existants sont mis à jour, pas dupliqués).

    python3 scripts/setup_metabase.py            # depuis la racine du projet, Metabase démarré

Uniquement la bibliothèque standard Python : aucune dépendance à installer.
Identifiants lus dans .env (voir .env.example).
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
COLLECTION_NAME = "Marketplace Analytics"
DATABASE_NAME = "DWH Marketplace"

LAST_DAY = "(SELECT max(dt) FROM analytics.daily_summary)"

# ----------------------------------------------------------------------
# Questions (cartes) : nom -> (type de visualisation, SQL, réglages)
# ----------------------------------------------------------------------
CARDS: dict[str, dict] = {
    # --- Executive Summary ---
    "CA du dernier jour (€)": {
        "display": "scalar",
        "sql": "SELECT total_revenue AS ca FROM analytics.daily_summary ORDER BY dt DESC LIMIT 1",
    },
    "Commandes du dernier jour": {
        "display": "scalar",
        "sql": "SELECT total_orders AS commandes FROM analytics.daily_summary ORDER BY dt DESC LIMIT 1",
    },
    "Panier moyen du dernier jour (€)": {
        "display": "scalar",
        "sql": "SELECT avg_basket AS panier_moyen FROM analytics.daily_summary ORDER BY dt DESC LIMIT 1",
    },
    "Commissions du dernier jour (€)": {
        "display": "scalar",
        "sql": "SELECT total_commission AS commissions FROM analytics.daily_summary ORDER BY dt DESC LIMIT 1",
    },
    "CA des 30 derniers jours": {
        "display": "line",
        "sql": f"""
            SELECT dt AS jour, total_revenue AS ca
            FROM analytics.daily_summary
            WHERE dt > {LAST_DAY} - 30
            ORDER BY dt""",
        "settings": {"graph.dimensions": ["jour"], "graph.metrics": ["ca"]},
    },
    "Top 5 vendeurs du dernier jour": {
        "display": "row",
        "sql": """
            SELECT seller_name AS vendeur, total_revenue AS ca
            FROM analytics.seller_daily
            WHERE dt = (SELECT max(dt) FROM analytics.seller_daily)
            ORDER BY ca DESC
            LIMIT 5""",
        "settings": {"graph.dimensions": ["vendeur"], "graph.metrics": ["ca"]},
    },
    "Commissions par jour (30 derniers jours)": {
        "display": "bar",
        "sql": f"""
            SELECT dt AS jour, total_commission AS commissions
            FROM analytics.daily_summary
            WHERE dt > {LAST_DAY} - 30
            ORDER BY dt""",
        "settings": {"graph.dimensions": ["jour"], "graph.metrics": ["commissions"]},
    },
    # --- Top Sellers ---
    "Top 10 vendeurs du mois": {
        "display": "row",
        "sql": """
            SELECT seller_name AS vendeur, SUM(total_revenue) AS ca
            FROM analytics.seller_daily
            WHERE dt >= date_trunc('month', (SELECT max(dt) FROM analytics.seller_daily))
            GROUP BY seller_name
            ORDER BY ca DESC
            LIMIT 10""",
        "settings": {"graph.dimensions": ["vendeur"], "graph.metrics": ["ca"]},
    },
    "Évolution du CA des 3 meilleurs vendeurs (30 jours)": {
        "display": "line",
        "sql": """
            WITH last_day AS (SELECT max(dt) AS dt FROM analytics.seller_daily),
            top3 AS (
                SELECT seller_id
                FROM analytics.seller_daily, last_day
                WHERE analytics.seller_daily.dt > last_day.dt - 30
                GROUP BY seller_id
                ORDER BY SUM(total_revenue) DESC
                LIMIT 3
            )
            SELECT sd.dt AS jour, sd.seller_name AS vendeur, sd.total_revenue AS ca
            FROM analytics.seller_daily AS sd
            JOIN top3 USING (seller_id), last_day
            WHERE sd.dt > last_day.dt - 30
            ORDER BY jour, vendeur""",
        "settings": {"graph.dimensions": ["jour", "vendeur"], "graph.metrics": ["ca"]},
    },
    "Vendeurs inactifs (plus de 7 jours sans vente)": {
        "display": "table",
        "sql": """
            WITH last_day AS (SELECT max(dt) AS dt FROM dwh.fact_orders)
            SELECT s.seller_id   AS id_vendeur,
                   s.name        AS vendeur,
                   s.country     AS pays,
                   MAX(f.dt)     AS derniere_vente,
                   last_day.dt - MAX(f.dt) AS jours_sans_vente
            FROM dwh.dim_seller AS s
            CROSS JOIN last_day
            LEFT JOIN dwh.fact_orders AS f ON f.seller_id = s.seller_id
            GROUP BY s.seller_id, s.name, s.country, last_day.dt
            HAVING MAX(f.dt) IS NULL OR MAX(f.dt) < last_day.dt - 7
            ORDER BY derniere_vente NULLS FIRST, vendeur""",
    },
}

# ----------------------------------------------------------------------
# Dashboards : nom -> (description, [(carte, ligne, colonne, largeur, hauteur)]) — grille de 24 colonnes
# ----------------------------------------------------------------------
DASHBOARDS: dict[str, tuple[str, list[tuple[str, int, int, int, int]]]] = {
    "Executive Summary": (
        "KPIs clés pour le CEO : CA, commandes, panier moyen, commissions (US-01, US-02, US-03).",
        [
            ("CA du dernier jour (€)", 0, 0, 6, 3),
            ("Commandes du dernier jour", 0, 6, 6, 3),
            ("Panier moyen du dernier jour (€)", 0, 12, 6, 3),
            ("Commissions du dernier jour (€)", 0, 18, 6, 3),
            ("CA des 30 derniers jours", 3, 0, 16, 7),
            ("Top 5 vendeurs du dernier jour", 3, 16, 8, 7),
            ("Commissions par jour (30 derniers jours)", 10, 0, 24, 6),
        ],
    ),
    "Top Sellers": (
        "Performance des vendeurs : classement, évolution et vendeurs inactifs (US-02).",
        [
            ("Top 10 vendeurs du mois", 0, 0, 10, 8),
            ("Évolution du CA des 3 meilleurs vendeurs (30 jours)", 0, 10, 14, 8),
            ("Vendeurs inactifs (plus de 7 jours sans vente)", 8, 0, 24, 8),
        ],
    ),
}


def load_env() -> dict[str, str]:
    env = {}
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip()
    env.update({k: v for k, v in os.environ.items() if k.startswith(("MB_", "DWH_"))})
    return env


class Metabase:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self.session: str | None = None

    def call(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(f"{self.url}/api{path}", data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code} : {exc.read().decode()[:500]}") from exc
        return json.loads(body) if body else None

    def wait_until_ready(self, timeout: int = 300) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.call("GET", "/health").get("status") == "ok":
                    return
            except Exception:  # noqa: BLE001 — Metabase pas encore démarré
                pass
            print("  … Metabase démarre (≈ 60 s au premier lancement)")
            time.sleep(10)
        sys.exit(f"Metabase injoignable sur {self.url} après {timeout} s")


def main() -> None:
    env = load_env()
    mb = Metabase(env.get("MB_URL", "http://localhost:3000"))
    email = env.get("MB_ADMIN_EMAIL", "admin@maelys.local")
    password = env.get("MB_ADMIN_PASSWORD", "Admin2026!")

    print(f"1/5 Attente de Metabase sur {mb.url}")
    mb.wait_until_ready()

    print("2/5 Compte administrateur")
    properties = mb.call("GET", "/session/properties")
    if not properties.get("has-user-setup"):
        mb.session = mb.call("POST", "/setup", {
            "token": properties["setup-token"],
            "user": {"email": email, "password": password, "first_name": "Admin", "last_name": "Maelys",
                     "site_name": "Maelys Marketplace"},
            "prefs": {"site_name": "Maelys Marketplace", "site_locale": "fr", "allow_tracking": False},
        })["id"]
        print(f"  compte créé : {email}")
    else:
        mb.session = mb.call("POST", "/session", {"username": email, "password": password})["id"]
        print(f"  connecté : {email}")

    print("3/5 Source de données PostgreSQL DWH")
    databases = mb.call("GET", "/database")
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    database = next((db for db in databases if db["name"] == DATABASE_NAME), None)
    if database is None:
        database = mb.call("POST", "/database", {
            "engine": "postgres",
            "name": DATABASE_NAME,
            "details": {
                "host": "postgres-dwh",  # nom du service Docker, pas localhost
                "port": 5432,            # port interne, pas 5433
                "dbname": env.get("DWH_DB", "dwh"),
                "user": env.get("DWH_USER", "dwh_user"),
                "password": env["DWH_PASSWORD"],
                "ssl": False,
            },
        })
        print(f"  base ajoutée (id={database['id']})")
    else:
        print(f"  base déjà présente (id={database['id']})")
    mb.call("POST", f"/database/{database['id']}/sync_schema")

    print("4/5 Questions")
    collections = mb.call("GET", "/collection")
    collection = next((c for c in collections if c.get("name") == COLLECTION_NAME and not c.get("archived")), None)
    if collection is None:
        collection = mb.call("POST", "/collection", {"name": COLLECTION_NAME})

    existing_cards = {
        card["name"]: card for card in mb.call("GET", "/card?f=all")
        if card.get("collection_id") == collection["id"] and not card.get("archived")
    }
    card_ids = {}
    for name, spec in CARDS.items():
        payload = {
            "name": name,
            "collection_id": collection["id"],
            "display": spec["display"],
            "dataset_query": {
                "type": "native",
                "native": {"query": " ".join(spec["sql"].split())},
                "database": database["id"],
            },
            "visualization_settings": spec.get("settings", {}),
        }
        if name in existing_cards:
            card = mb.call("PUT", f"/card/{existing_cards[name]['id']}", payload)
        else:
            card = mb.call("POST", "/card", payload)
        card_ids[name] = card["id"]
        print(f"  ✔ {name}")

    print("5/5 Dashboards")
    existing_dashboards = {
        d["name"]: d for d in mb.call("GET", "/dashboard")
        if d.get("collection_id") == collection["id"] and not d.get("archived")
    }
    for name, (description, layout) in DASHBOARDS.items():
        dashboard = existing_dashboards.get(name) or mb.call(
            "POST", "/dashboard", {"name": name, "description": description, "collection_id": collection["id"]}
        )
        # PUT avec la liste complète : remplace les cartes du dashboard (idempotent)
        mb.call("PUT", f"/dashboard/{dashboard['id']}", {
            "description": description,
            "dashcards": [
                {"id": -(i + 1), "card_id": card_ids[card], "row": row, "col": col,
                 "size_x": width, "size_y": height, "parameter_mappings": [], "series": []}
                for i, (card, row, col, width, height) in enumerate(layout)
            ],
        })
        print(f"  ✔ {name} -> {mb.url}/dashboard/{dashboard['id']}")

    print(f"\nTerminé. Connexion : {mb.url}  ({email} / {password})")


if __name__ == "__main__":
    main()
