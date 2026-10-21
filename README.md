# MarketPlace Analytics — Plateforme data de Maelys Marketplace

> **Projet A — Data Lake / ELT** · IPSSI · Semaine du 19 octobre 2026
> MVP d'une plateforme data qui ingère les commandes d'une marketplace, les archive dans un data lake (MinIO), les modélise en étoile dans PostgreSQL et expose les KPIs dans Metabase.

---

## Sommaire

1. [Contexte et besoin](#1-contexte-et-besoin)
2. [Ce qu'on doit livrer (MVP vs bonus)](#2-ce-quon-doit-livrer-mvp-vs-bonus)
3. [Architecture](#3-architecture)
4. [Stack et services](#4-stack-et-services)
5. [Arborescence du projet](#5-arborescence-du-projet)
6. [Prérequis](#6-prérequis)
7. [Feuille de route étape par étape](#7-feuille-de-route-étape-par-étape)
   - [Étape 0 — Initialiser le dépôt](#étape-0--initialiser-le-dépôt)
   - [Étape 1 — Monter l'infrastructure Docker](#étape-1--monter-linfrastructure-docker)
   - [Étape 2 — L'API Marketplace](#étape-2--lapi-marketplace)
   - [Étape 3 — Explorer l'API à la main](#étape-3--explorer-lapi-à-la-main)
   - [Étape 4 — Concevoir le modèle dimensionnel](#étape-4--concevoir-le-modèle-dimensionnel)
   - [Étape 5 — Le Custom Hook `MarketplaceAPIHook`](#étape-5--le-custom-hook-marketplaceapihook)
   - [Étape 6 — DAG d'ingestion, tâche par tâche](#étape-6--dag-dingestion-tâche-par-tâche)
   - [Étape 7 — Transformation staging → dwh idempotente](#étape-7--transformation-staging--dwh-idempotente)
   - [Étape 8 — Prouver l'idempotence](#étape-8--prouver-lidempotence)
   - [Étape 9 — DAG de refresh des dimensions](#étape-9--dag-de-refresh-des-dimensions)
   - [Étape 10 — DAG d'agrégation analytics](#étape-10--dag-dagrégation-analytics)
   - [Étape 11 — Metabase et dashboards](#étape-11--metabase-et-dashboards)
   - [Étape 12 — Tests pytest](#étape-12--tests-pytest)
   - [Étape 13 — Finaliser la doc et livrer](#étape-13--finaliser-la-doc-et-livrer)
8. [Bonus](#8-bonus)
9. [Definition of Done](#9-definition-of-done)
10. [Planning sur 2 jours](#10-planning-sur-2-jours)
11. [Pièges connus et solutions](#11-pièges-connus-et-solutions)
12. [Aide-mémoire des commandes](#12-aide-mémoire-des-commandes)
13. [Accès et identifiants (environnement de formation)](#13-accès-et-identifiants-environnement-de-formation)
14. [Choix techniques](#14-choix-techniques)
15. [Ce qu'on ferait avec plus de temps](#15-ce-quon-ferait-avec-plus-de-temps)

---

## 1. Contexte et besoin

**Maelys Marketplace** met en relation des vendeurs indépendants (artisans, DNVB, revendeurs) et des consommateurs. Elle prend **12 % de commission** sur chaque vente.

| Indicateur | Valeur (fictive) |
|---|---|
| Vendeurs actifs | 2 400 |
| Produits | 180 000 |
| Commandes / jour | ~8 500 |
| CA mensuel | ~4,2 M€ |
| Équipe interne | 75 personnes, dont 1 analyste qui passe 2 j/semaine à extraire des données à la main |

**Problème actuel :** tout est manuel (exports backoffice → Excel), les rapports sont en retard, les erreurs de copier-coller sont fréquentes, et l'analyste passe 40 % de son temps à extraire au lieu d'analyser.

**Qui a besoin de quoi :**

| ID | En tant que… | Je veux… | Priorité |
|---|---|---|---|
| US-01 | CEO | voir le CA total par jour sur 30 jours | 🟠 Must |
| US-02 | CEO | connaître le top 10 vendeurs par revenu | 🟠 Must |
| US-03 | Finance | un récapitulatif des commissions par jour | 🟠 Must |
| US-04 | Resp. catalogue | la répartition du CA par catégorie | 🟡 Should |
| US-05 | Analyste | être alerté si le CA du jour < 70 % de la moyenne 7 j | 🟡 Should |
| US-06 | Analyste | rejouer le pipeline sur une date passée | 🔵 Could |
| US-07 | Ops | superviser l'état du pipeline (alerte si un DAG échoue) | 🔵 Could |

---

## 2. Ce qu'on doit livrer (MVP vs bonus)

> 🟠 **Règle d'or : MVP à 100 % avant de toucher au moindre bonus.** Un MVP propre et testé vaut mieux qu'un projet ambitieux bancal.

### 🟠 Must have — MVP obligatoire
- [ ] **Custom Hook** `MarketplaceAPIHook` avec `get_orders(date)`, `get_sellers()`, `get_products()`
- [ ] **Pipeline ELT** : Extract (Hook) → raw JSON dans MinIO (`dt=YYYY-MM-DD`) → `staging.orders` → `dwh.fact_orders` (**DELETE + INSERT par partition**)
- [ ] **Modèle en étoile** : au minimum `dim_seller`, `dim_product`, `fact_orders` (on vise 4 dimensions)
- [ ] **2 dashboards Metabase** : CA journalier + top vendeurs
- [ ] **≥ 3 tests pytest**, dont un sur l'idempotence
- [ ] **README** avec schéma Mermaid et instructions de lancement

### 🟡 Should have — bonus +5 pts
- [ ] `DataQualityOperator` avec 3 règles configurables (not null, not empty, pas de date future)
- [ ] **Branching** : si DQ KO → tâche d'alerte au lieu du chargement DWH
- [ ] 2 dashboards Metabase supplémentaires (commissions, catégories)

### 🔵 Could have — bonus +10 pts
- [ ] Stack observabilité Prometheus + Grafana (+ statsd-exporter, postgres-exporter)
- [ ] DAG de détection d'anomalies (écart à la moyenne mobile 7 j)
- [ ] Backfill manuel documenté sur 7 jours
- [ ] Tests pytest avec mock de la DB

### 🔴 Won't have — hors périmètre
- Dimensions SCD2, CI/CD GitHub Actions, migration cloud, dbt, Spark.

---

## 3. Architecture

### 3.1 Vue d'ensemble

```mermaid
flowchart LR
    subgraph SRC["Sources"]
        API[("API Marketplace<br/>Flask simulée")]
    end

    subgraph ORCH["Orchestration"]
        HOOK["MarketplaceAPIHook"]
        AF[["Airflow 3.1.8<br/>4-5 DAGs"]]
    end

    subgraph STORE["Stockage"]
        MINIO[("MinIO<br/>raw / dt=")]
        PG[("PostgreSQL 18<br/>staging · dwh · analytics")]
    end

    subgraph SERV["Serving BI"]
        MB["Metabase<br/>dashboards"]
    end

    subgraph OBS["Observabilité (bonus)"]
        PROM[("Prometheus")]
        GRAF["Grafana"]
    end

    API --> HOOK --> AF
    AF -- "upload raw JSON" --> MINIO
    AF -- "load + transform" --> PG
    PG --> MB
    AF -. "StatsD" .-> PROM
    PG -. "postgres-exporter" .-> PROM
    PROM --> GRAF
```

### 3.2 Déroulé d'un run daté `{{ ds }}`

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant H as MarketplaceAPIHook
    participant A as API Marketplace
    participant M as MinIO
    participant P as PostgreSQL DWH
    participant MB as Metabase

    S->>H: extract(date = {{ ds }})
    H->>A: GET /orders?date={{ ds }} (Bearer)
    A-->>H: JSON (N commandes)
    H->>M: PUT bronze/orders/dt={{ ds }}/data.json
    S->>P: DELETE + INSERT staging.orders WHERE dt = {{ ds }}
    S->>P: DELETE + INSERT dwh.fact_orders WHERE dt = {{ ds }}
    S->>P: rebuild analytics.* pour {{ ds }}
    MB->>P: SELECT pour les dashboards
```

### 3.3 Chaînage des DAGs

```mermaid
flowchart TD
    D0["marketplace_dims_refresh_daily<br/>@daily"]
    D1["marketplace_orders_ingest_daily<br/>@daily"]
    D2["marketplace_analytics_aggregate_daily<br/>schedule = Asset dwh_orders"]
    D3["marketplace_anomaly_detect_daily<br/>@daily (bonus)"]

    D0 -. "dimensions à jour" .-> D1
    D1 -- "Asset dwh_orders" --> D2
    D2 -. "lit analytics.daily_summary" .-> D3
```

### 3.4 Pourquoi cette architecture

- **MinIO = raw layer (« data lake first »)** : on garde le JSON brut tel que reçu. Si la transformation a un bug, on peut la rejouer à partir de MinIO sans rappeler l'API source.
- **Architecture médaillon** : le cahier des charges prévoit un bucket unique `data-lake/raw/`. Nous l'avons remplacé par 3 buckets :

  | Couche | Où | Contenu | MVP ? |
  |---|---|---|---|
  | **Bronze** | MinIO `bronze/` | JSON brut de l'API, partitionné `entité/dt=YYYY-MM-DD/data.json` | ✅ |
  | **Silver** | Postgres `staging` + `dwh` | données typées, nettoyées, modèle en étoile | ✅ |
  | | MinIO `silver/` | export Parquet des données nettoyées | bonus |
  | **Gold** | Postgres `analytics` | agrégats lus par Metabase | ✅ |
  | | MinIO `gold/` | export Parquet / CSV des KPIs | bonus |
- **3 schémas PostgreSQL séparés** :
  - `staging` : données brutes mais typées, 1 table par endpoint
  - `dwh` : modèle dimensionnel propre (Kimball)
  - `analytics` : agrégats pré-calculés pour les dashboards (requêtes Metabase rapides)
- **Metabase plutôt que Superset** : installation Docker triviale, suffisant pour 4 dashboards.
- **Pas de Spark / dbt** : 8 500 commandes/jour, c'est petit. Airflow exécute le SQL directement via `PostgresHook.run()`.

---

## 4. Stack et services

| Service | Image | Port hôte | Volume | Rôle |
|---|---|---|---|---|
| `airflow-apiserver` | `apache/airflow:3.1.8` | 8080 | — | UI + API REST Airflow |
| `airflow-scheduler` | `apache/airflow:3.1.8` | — | — | Planifie les runs |
| `airflow-dag-processor` | `apache/airflow:3.1.8` | — | — | Parse les fichiers DAG |
| `airflow-triggerer` | `apache/airflow:3.1.8` | — | — | Opérateurs deferrables |
| `airflow-worker` | `apache/airflow:3.1.8` | — | — | Exécute les tâches (Celery) |
| `airflow-init` | `apache/airflow:3.1.8` | — | — | Migration DB + création de l'utilisateur (one-shot) |
| `postgres-airflow` | `postgres:16` | — (non exposé) | `pg-airflow-data` | Base de métadonnées Airflow |
| `postgres-dwh` | `postgres:18` | **5433** → 5432 | `pg-dwh-data` | Data Warehouse |
| `redis` | `redis:7.2-bookworm` | — (non exposé) | — | Broker Celery |
| `minio` | `cgr.dev/chainguard/minio` (épinglée par digest) | 9000 (API) / 9001 (console) | `minio-data` | Stockage objet S3 |
| `minio-init` | même image que `minio` | — | — | Crée les buckets `bronze`, `silver`, `gold` à chaque démarrage |
| `api-marketplace` | build local (Flask) | 5000 | — | API simulée |
| `metabase` | `metabase/metabase:v0.59.6.1` | 3000 | `metabase-data` | BI |
| `prometheus` *(bonus)* | `prom/prometheus` | 9090 | `prometheus-data` | Collecte des métriques |
| `grafana` *(bonus)* | `grafana/grafana` | **3001** → 3000 | `grafana-data` | Dashboards techniques |
| `statsd-exporter` *(bonus)* | `prom/statsd-exporter` | 9102 | — | Métriques Airflow → Prometheus |
| `postgres-exporter` *(bonus)* | `prometheuscommunity/postgres-exporter` | 9187 | — | Métriques PostgreSQL |

> ℹ️ Les données sont **fausses et générées par nous avec Faker**, à l'intérieur de l'API Flask (`api/`). L'API reste indispensable : c'est elle que le Custom Hook interroge avec son token Bearer.
>
> ℹ️ `minio/minio` n'est plus publiée sur Docker Hub : on utilise l'image Chainguard, épinglée par digest.
>
> ℹ️ Les services `api-marketplace` (profil `api`) et `metabase` (profil `bi`) ne démarrent que si on les active dans `.env` : `COMPOSE_PROFILES=api,bi`.

---

## 5. Arborescence du projet

```
MarketPlace_Analytics/
├── docker-compose.yml
├── .env                          # identifiants — NON commité
├── .env.example                  # modèle de .env — commité
├── .gitignore
├── README.md
│
├── api/                          # API Flask simulée (si pas de kit)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── config/                       # airflow.cfg généré par airflow-init — NON commité
│
├── init-db/                      # exécuté par postgres-dwh au 1er démarrage (ordre alphabétique)
│   ├── 00_schemas.sql            # CREATE SCHEMA staging / dwh / analytics
│   └── schema.sql                # CREATE TABLE (Rayan — étape 4)
│
├── scripts/legacy/               # anciens fichiers du TD MinIO (à supprimer une fois validé)
│
├── plugins/
│   ├── hooks/
│   │   ├── __init__.py
│   │   └── marketplace_api_hook.py
│   └── operators/
│       ├── __init__.py
│       └── data_quality_operator.py      # bonus Should
│
├── dags/
│   ├── marketplace_dims_refresh_daily.py
│   ├── marketplace_orders_ingest_daily.py
│   ├── marketplace_analytics_aggregate_daily.py
│   ├── marketplace_anomaly_detect_daily.py   # bonus Could
│   └── sql/                      # requêtes SQL externalisées (templatées Jinja)
│       ├── load_staging_orders.sql
│       ├── transform_fact_orders.sql
│       └── build_daily_summary.sql
│
├── tests/
│   ├── conftest.py               # fixture dagbag
│   ├── test_dags.py              # import, catchup, structure, idempotence
│   └── test_marketplace_hook.py  # Hook avec requests mocké
│
├── observability/                # bonus Could
│   ├── prometheus.yml
│   └── statsd_mapping.yml
│
├── logs/                         # monté par Airflow — NON commité
└── docs/
    └── screenshots/              # captures Metabase / Airflow pour le rendu
```

---

## 6. Prérequis

| Outil | Version | Vérification |
|---|---|---|
| Docker Engine | ≥ 24 | `docker --version` |
| Docker Compose v2 | ≥ 2.20 | `docker compose version` |
| RAM disponible pour Docker | ≥ 8 Go conseillés | Airflow Celery + Metabase consomment beaucoup |
| `curl`, `psql` (ou `pgcli`) | — | pour tester l'API et la base |
| Git | — | `git --version` |

Ports à libérer sur la machine : `3000, 5000, 5433, 8080, 9000, 9001` (+ `3001, 9090, 9102, 9187` pour le bonus).

```bash
# Vérifier qu'aucun service local n'occupe déjà un port (ex. un Postgres local sur 5432)
ss -ltnp | grep -E ':(3000|5000|5433|8080|9000|9001)\b'
```

---

## 7. Feuille de route étape par étape

Chaque étape a un **objectif**, un **à faire** détaillé et une **validation** : on ne passe pas à l'étape suivante tant que la validation n'est pas verte.

---

### Étape 0 — Initialiser le dépôt

**Objectif :** un dépôt Git propre, sans secret commité.

**À faire :**
1. Créer l'arborescence de la section 5 (dossiers vides inclus).
2. `git init` puis un premier commit vide.
3. Écrire le `.gitignore` :
   ```gitignore
   .env
   logs/
   __pycache__/
   *.pyc
   .pytest_cache/
   .ipynb_checkpoints/
   ```
4. Écrire `.env.example` (et le copier en `.env`) :
   ```dotenv
   AIRFLOW_UID=1000

   # Postgres Airflow (métadonnées)
   AIRFLOW_DB_USER=airflow
   AIRFLOW_DB_PASSWORD=airflow
   AIRFLOW_DB_NAME=airflow

   # Postgres DWH
   DWH_USER=dwh_user
   DWH_PASSWORD=dwh_password_2026
   DWH_DB=dwh

   # MinIO
   MINIO_ROOT_USER=minio_admin
   MINIO_ROOT_PASSWORD=minio_password_2026

   # API Marketplace
   MARKETPLACE_API_TOKEN=formation-token-2026
   ```
   > `AIRFLOW_UID` = résultat de `id -u` (évite les fichiers de logs appartenant à root sous Linux).

**Validation :** `git status` ne montre pas `.env`.

---

### Étape 1 — Monter l'infrastructure Docker

**Objectif :** tous les services sont `healthy` et accessibles depuis le navigateur.

**À faire :**

1. **Bloc commun Airflow `x-airflow-common`** (partagé par tous les services Airflow) :
   - `image: apache/airflow:3.1.8`
   - Variables clés :
     | Variable | Valeur | Pourquoi |
     |---|---|---|
     | `AIRFLOW__CORE__EXECUTOR` | `CeleryExecutor` | exécution distribuée via le worker |
     | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow` | DB de métadonnées |
     | `AIRFLOW__CELERY__BROKER_URL` | `redis://:@redis:6379/0` | broker |
     | `AIRFLOW__CELERY__RESULT_BACKEND` | `db+postgresql://airflow:airflow@postgres-airflow/airflow` | résultats Celery |
     | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-apiserver:8080/execution/` | **spécifique Airflow 3** : le worker parle à l'API server |
     | `AIRFLOW__API_AUTH__JWT_SECRET` | une chaîne aléatoire commune | tokens signés entre composants Airflow 3 |
     | `AIRFLOW__CORE__LOAD_EXAMPLES` | `"false"` | UI lisible |
     | `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` | `30` | prise en compte rapide des nouveaux DAGs |
     | `_PIP_ADDITIONAL_REQUIREMENTS` | `apache-airflow-providers-amazon apache-airflow-providers-postgres pytest` | S3Hook + PostgresHook + tests |
   - Volumes : `./dags`, `./plugins`, `./logs`, `./tests` montés sous `/opt/airflow/...`
   - `user: "${AIRFLOW_UID}:0"`

2. **Connections Airflow déclarées par variables d'environnement** (aucun token dans le code, aucun clic manuel dans l'UI) :
   ```yaml
   AIRFLOW_CONN_MARKETPLACE_API: '{"conn_type": "http", "host": "http://api-marketplace:5000", "password": "${MARKETPLACE_API_TOKEN}"}'
   AIRFLOW_CONN_MINIO_S3: '{"conn_type": "aws", "login": "${MINIO_ROOT_USER}", "password": "${MINIO_ROOT_PASSWORD}", "extra": {"endpoint_url": "http://minio:9000"}}'
   AIRFLOW_CONN_POSTGRES_DWH: '{"conn_type": "postgres", "host": "postgres-dwh", "port": 5432, "login": "${DWH_USER}", "password": "${DWH_PASSWORD}", "schema": "${DWH_DB}"}'
   ```
   > Le nom de la Connection = la partie après `AIRFLOW_CONN_`, en minuscules : `marketplace_api`, `minio_s3`, `postgres_dwh`.

3. **Services Airflow** : `airflow-init` (one-shot : `airflow db migrate` + création d'un admin), puis `apiserver` (`command: api-server`), `scheduler`, `dag-processor`, `triggerer`, `worker` (`command: celery worker`), tous avec `depends_on: airflow-init: condition: service_completed_successfully`.

4. **`postgres-dwh`** :
   - `ports: "5433:5432"`
   - monte `./init-db:/docker-entrypoint-initdb.d:ro` → `schema.sql` est exécuté **uniquement au premier démarrage** (volume vide)
   - ⚠️ **Postgres 18** : monter le volume sur `/var/lib/postgresql` (et non plus `/var/lib/postgresql/data`)
   - `healthcheck: pg_isready -U ${DWH_USER} -d ${DWH_DB}`

5. **`minio` + `minio-init`** : `minio-init` attend que MinIO soit healthy, puis :
   ```sh
   mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
   mc mb --ignore-existing lake/bronze lake/silver lake/gold
   ```

6. **Réseau unique** `airflow-net` pour que tous les conteneurs se voient par leur nom de service.

7. Lancer :
   ```bash
   docker compose up -d
   # attendre ~60-90 s
   docker compose ps
   ```

**Validation :**
- [ ] `docker compose ps` → tous les services `running (healthy)` (sauf `airflow-init` et `minio-init` : `exited (0)`)
- [ ] http://localhost:8080 → login Airflow OK
- [ ] http://localhost:9001 → console MinIO, buckets `bronze`, `silver`, `gold` présents
- [ ] `psql -h localhost -p 5433 -U dwh_user -d dwh -c '\dn'` → schémas `staging`, `dwh`, `analytics`
- [ ] Dans le worker : `docker compose exec airflow-worker airflow connections list` → les 3 connections apparaissent

---

### Étape 2 — L'API Marketplace

> **À sauter si le kit fourni contient déjà le service `api-marketplace`.**

**Objectif :** une API Flask **déterministe** : même date ⇒ mêmes données (`seed = md5(date)`). C'est ce qui rend l'idempotence testable.

**Spécification des endpoints :**

| Méthode | Endpoint | Paramètres | Auth | Retour |
|---|---|---|---|---|
| GET | `/health` | — | non | `{"status": "ok", "version": "1.0"}` |
| GET | `/orders` | `date=YYYY-MM-DD` (obligatoire) | Bearer | liste de commandes du jour |
| GET | `/sellers` | `limit` (défaut 100) | Bearer | liste de vendeurs |
| GET | `/products` | — | Bearer | catalogue |
| GET | `/customers` | `limit` (défaut 500) | Bearer | liste de clients |

**À faire :**
1. `api/app.py` :
   - un décorateur `require_token` qui compare `Authorization: Bearer <token>` à `os.environ["API_TOKEN"]` → sinon **401**
   - **référentiels stables** (vendeurs, produits, clients) générés avec une seed fixe (ex. `random.Random(42)`) : ils ne changent pas d'un appel à l'autre
   - `/orders` : `rnd = random.Random(int(hashlib.md5(date.encode()).hexdigest(), 16))`, puis génération de N commandes (N varie avec la date) qui pointent vers des `seller_id` / `product_id` / `customer_id` **existants** dans les référentiels
   - `/orders` sans `date` ou date mal formée → **400**
   - (optionnel, pour tester le retry) un paramètre d'environnement qui renvoie aléatoirement des 503
2. Format proposé d'une commande :
   ```json
   {
     "order_id": "ORD-20260407-000123",
     "order_date": "2026-04-07T14:32:10",
     "customer_id": "CUS-00042",
     "seller_id": "SEL-0007",
     "product_id": "PRD-00315",
     "quantity": 2,
     "unit_price": 24.90,
     "total_amount": 49.80,
     "status": "delivered"
   }
   ```
   Statuts possibles : `pending`, `paid`, `shipped`, `delivered`, `cancelled`, `refunded`.
3. `api/Dockerfile` (`python:3.12-slim`, `pip install flask gunicorn`, `CMD gunicorn -b 0.0.0.0:5000 app:app`).
4. Ajouter le service `api-marketplace` au compose (`build: ./api`, `environment: API_TOKEN: ${MARKETPLACE_API_TOKEN}`, port 5000, healthcheck sur `/health`).

**Validation :**
- [ ] `curl localhost:5000/health` → 200
- [ ] Même appel `/orders?date=2026-04-07` deux fois → réponses **identiques** (`diff` vide)
- [ ] Sans header `Authorization` → 401

---

### Étape 3 — Explorer l'API à la main

**Objectif :** connaître **exactement** le format des données avant d'écrire le moindre SQL ou DAG.

**À faire :**
```bash
TOKEN=formation-token-2026

curl -s http://localhost:5000/health

curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:5000/orders?date=2026-04-07" | python3 -m json.tool | head -40

# Combien de commandes ?
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:5000/orders?date=2026-04-07" | python3 -c "import json,sys; d=json.load(sys.stdin); print(type(d), len(d) if isinstance(d,list) else d.keys())"

curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:5000/sellers?limit=5"   | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:5000/products"          | python3 -m json.tool | head -40
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:5000/customers?limit=5" | python3 -m json.tool

# Sans token → doit renvoyer 401
curl -i "http://localhost:5000/orders?date=2026-04-07"
```

**Questions à se poser et à noter ici :**
- [ ] La réponse est-elle une liste directement, ou enveloppée (`{"data": [...], "count": N}`) ?
- [ ] **Grain** d'une commande : 1 produit par commande, ou une liste `items` ? → si liste, le grain de la table de faits sera **la ligne de commande** (`order_id` + `line_number`)
- [ ] Le montant est-il fourni (`total_amount`) ou à calculer (`quantity × unit_price`) ?
- [ ] Les dates sont-elles en ISO 8601 ? Avec fuseau horaire ?
- [ ] Quels statuts existent ? Lesquels comptent dans le CA (on exclut `cancelled` / `refunded`) ?
- [ ] Tous les `seller_id` / `product_id` des commandes existent-ils dans `/sellers` et `/products` ? (impacte les FK — voir étape 7)
- [ ] Pagination sur `/products` ?

---

### Étape 4 — Concevoir le modèle dimensionnel

**Objectif :** un schéma en étoile (Kimball) **simple et propre**, écrit dans `init-db/schema.sql`.

**Contraintes du cahier des charges :**
- 3 schémas séparés : `staging`, `dwh`, `analytics`
- ≥ 3 dimensions + 1 table de faits
- chaque dimension a une **PRIMARY KEY**
- la table de faits a des **FOREIGN KEY** vers les dimensions
- ≥ 1 table d'agrégation dans `analytics`

**Modèle cible :**

```mermaid
erDiagram
    DIM_SELLER   ||--o{ FACT_ORDERS : vend
    DIM_CUSTOMER ||--o{ FACT_ORDERS : achete
    DIM_PRODUCT  ||--o{ FACT_ORDERS : contient
    DIM_DATE     ||--o{ FACT_ORDERS : datee
    DIM_SELLER   ||--o{ DIM_PRODUCT : propose

    DIM_SELLER {
        text seller_id PK
        text name
        text country
        date joined_date
    }
    DIM_CUSTOMER {
        text customer_id PK
        text email
        text city
        date signup_date
    }
    DIM_PRODUCT {
        text product_id PK
        text name
        text category
        numeric base_price
        text seller_id FK
    }
    DIM_DATE {
        date dt PK
        int year
        int quarter
        int month
        int week
        int day_of_week
        bool is_weekend
    }
    FACT_ORDERS {
        text order_id PK
        date dt FK
        text seller_id FK
        text customer_id FK
        text product_id FK
        int quantity
        numeric unit_price
        numeric total_amount
        numeric commission_amount
        text status
    }
```

**Squelette de `init-db/schema.sql` :**

```sql
-- =========================================================
-- Schémas
-- =========================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================
-- STAGING : brut typé, une ligne par enregistrement API, partitionné logiquement par dt
-- =========================================================
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id      TEXT        NOT NULL,
    dt            DATE        NOT NULL,          -- = {{ ds }} du run
    order_ts      TIMESTAMP,
    customer_id   TEXT,
    seller_id     TEXT,
    product_id    TEXT,
    quantity      INTEGER,
    unit_price    NUMERIC(12,2),
    total_amount  NUMERIC(12,2),
    status        TEXT,
    loaded_at     TIMESTAMP   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_staging_orders_dt ON staging.orders (dt);

CREATE TABLE IF NOT EXISTS staging.sellers   ( /* champs de /sellers   + loaded_at */ );
CREATE TABLE IF NOT EXISTS staging.products  ( /* champs de /products  + loaded_at */ );
CREATE TABLE IF NOT EXISTS staging.customers ( /* champs de /customers + loaded_at */ );

-- =========================================================
-- DWH : dimensions
-- =========================================================
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt          DATE PRIMARY KEY,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    week        SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,   -- 1 = lundi (ISO)
    is_weekend  BOOLEAN  NOT NULL
);

-- Pré-remplissage 2024 → 2030 : dim_date n'a plus jamais besoin d'être rechargée
INSERT INTO dwh.dim_date
SELECT d::date,
       EXTRACT(YEAR    FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH   FROM d),
       EXTRACT(WEEK    FROM d),
       EXTRACT(ISODOW  FROM d),
       EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::date, '2030-12-31'::date, interval '1 day') AS d
ON CONFLICT (dt) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id   TEXT PRIMARY KEY,
    name        TEXT,
    country     TEXT,
    joined_date DATE,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id TEXT PRIMARY KEY,
    email       TEXT,
    city        TEXT,
    signup_date DATE,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id  TEXT PRIMARY KEY,
    name        TEXT,
    category    TEXT,
    base_price  NUMERIC(12,2),
    seller_id   TEXT REFERENCES dwh.dim_seller (seller_id),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================================================
-- DWH : table de faits (grain = 1 commande / 1 ligne de commande)
-- =========================================================
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id          TEXT PRIMARY KEY,
    dt                DATE NOT NULL REFERENCES dwh.dim_date     (dt),
    seller_id         TEXT NOT NULL REFERENCES dwh.dim_seller   (seller_id),
    customer_id       TEXT          REFERENCES dwh.dim_customer (customer_id),
    product_id        TEXT NOT NULL REFERENCES dwh.dim_product  (product_id),
    quantity          INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL,
    total_amount      NUMERIC(12,2) NOT NULL,
    commission_amount NUMERIC(12,2) NOT NULL,   -- total_amount * 0.12
    status            TEXT          NOT NULL,
    loaded_at         TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_dt     ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_seller ON dwh.fact_orders (seller_id, dt);

-- =========================================================
-- ANALYTICS : agrégats pour Metabase
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt               DATE PRIMARY KEY,
    total_orders     INTEGER       NOT NULL,
    total_revenue    NUMERIC(14,2) NOT NULL,
    total_commission NUMERIC(14,2) NOT NULL,
    avg_basket       NUMERIC(12,2),
    active_sellers   INTEGER,
    top_seller_id    TEXT
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt            DATE,
    seller_id     TEXT,
    orders        INTEGER,
    revenue       NUMERIC(14,2),
    commission    NUMERIC(14,2),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt        DATE,
    category  TEXT,
    orders    INTEGER,
    revenue   NUMERIC(14,2),
    PRIMARY KEY (dt, category)
);
```

**Règles métier à figer (et à documenter) :**
- **CA** = somme de `total_amount` des commandes dont le statut **n'est pas** `cancelled` / `refunded`.
- **Commission** = `total_amount × 0.12` (le taux est une constante nommée dans le code, pas un nombre magique éparpillé).
- **Panier moyen** = CA / nombre de commandes valides.

**Validation :**
- [ ] `psql -h localhost -p 5433 -U dwh_user -d dwh -f init-db/schema.sql` passe sans erreur (le script est rejouable grâce aux `IF NOT EXISTS`)
- [ ] `\dt dwh.*` → 5 tables ; `SELECT count(*) FROM dwh.dim_date;` → ~2 557 lignes

> 💡 `schema.sql` ne s'exécute automatiquement qu'au **premier** démarrage de `postgres-dwh`. Après une modification : soit on le rejoue à la main avec `psql`, soit `docker compose down -v` (⚠️ efface toutes les données).

---

### Étape 5 — Le Custom Hook `MarketplaceAPIHook`

**Objectif :** la pièce maîtresse, testable seule. Toute la logique HTTP (URL, auth, retry, erreurs) est encapsulée ici ; les DAGs n'utilisent jamais `requests` directement.

**Fichier :** `plugins/hooks/marketplace_api_hook.py`

**Cahier des charges du Hook :**
- hérite de `airflow.hooks.base.BaseHook` (ou `airflow.sdk.bases.hook.BaseHook` en Airflow 3)
- lit la Connection `marketplace_api` : `host` = URL de base, `password` = token Bearer
- expose `get_orders(date)`, `get_sellers(limit=None)`, `get_products()`, `get_customers(limit=None)`
- gère les erreurs : **timeout**, **5xx** → retry avec backoff ; **4xx** → erreur immédiate (inutile de réessayer un 401)
- testable unitairement (on peut mocker `requests`)

**Design proposé :**

```python
class MarketplaceAPIHook(BaseHook):
    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(self, marketplace_conn_id: str = default_conn_name,
                 timeout: int = 30, max_retries: int = 3) -> None: ...

    def get_conn(self) -> requests.Session:
        """Session avec header Authorization: Bearer <password> et urllib3.Retry
        (total=max_retries, backoff_factor=1, status_forcelist=[500, 502, 503, 504])."""

    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        """GET + raise_for_status() + json(). Gère une éventuelle enveloppe {"data": [...]}."""

    def get_orders(self, date: str) -> list[dict]: ...      # valide le format YYYY-MM-DD
    def get_sellers(self, limit: int | None = None) -> list[dict]: ...
    def get_products(self) -> list[dict]: ...
    def get_customers(self, limit: int | None = None) -> list[dict]: ...

    def test_connection(self) -> tuple[bool, str]:
        """Appelle /health — active le bouton « Test » dans l'UI Airflow."""
```

**Points d'attention :**
- La session est créée **à la demande** (dans `get_conn`), jamais dans `__init__` ni au niveau du module → le parsing des DAGs reste rapide et ne fait aucun appel réseau.
- Le token n'apparaît **jamais** dans les logs (`self.log.info` affiche l'URL et le nombre d'enregistrements, pas les headers).
- Le `host` peut contenir ou non `http://` et un `/` final : normaliser.

**Validation (test isolé avant tout DAG) :**
```bash
docker compose exec airflow-worker python -c "
from hooks.marketplace_api_hook import MarketplaceAPIHook
h = MarketplaceAPIHook()
o = h.get_orders('2026-04-07')
print(len(o), o[0])
print(len(h.get_sellers()), len(h.get_products()))
print(h.test_connection())
"
```
- [ ] Le nombre de commandes s'affiche, identique à ce qu'on a vu avec `curl`
- [ ] En mettant un mauvais token dans la Connection → erreur 401 claire, sans retry

---

### Étape 6 — DAG d'ingestion, tâche par tâche

**Objectif :** `marketplace_orders_ingest_daily` qui fait `extract → raw MinIO → staging`. **On ajoute une tâche à la fois et on la valide avant la suivante.**

**Paramètres du DAG :**
```python
dag_id="marketplace_orders_ingest_daily"
schedule="@daily"
start_date=datetime(2026, 4, 1)
catchup=False                      # exigé par les tests
max_active_runs=1                  # évite deux runs concurrents sur la même table
default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "owner": "data-team"}
tags=["marketplace", "elt"]
```

**6.1 — `extract_orders`**
- Appelle `MarketplaceAPIHook().get_orders(ds)` avec `ds` **issu du contexte** (`{{ ds }}`), jamais `date.today()`.
- Première version : on se contente de `print(len(orders))` et de lancer `airflow dags test`.
- ⚠️ Ne pas faire transiter les ~8 500 commandes par XCom : la tâche **écrit directement dans MinIO** et ne passe à la suite que la **clé S3** (une petite chaîne). En pratique, on fusionne extract + upload dans une seule tâche `extract_to_raw`.

**6.2 — `extract_to_raw` (upload MinIO)**
- `S3Hook(aws_conn_id="minio_s3").load_string(json.dumps(orders), key=..., bucket_name="bronze", replace=True)`
- Clé : `orders/dt={{ ds }}/data.json` dans le bucket `bronze` → `replace=True` rend l'étape **idempotente** (rejouer = écraser le même fichier).
- Retourne la clé (XCom).
- **Validation :** le fichier apparaît dans la console MinIO à `bronze/orders/dt=2026-04-07/data.json`.

**6.3 — `load_staging_orders`**
- Relit le JSON **depuis MinIO** (`S3Hook.read_key`) : c'est ce qui permet de rejouer la transformation sans rappeler l'API.
- Dans **une seule transaction** :
  ```sql
  DELETE FROM staging.orders WHERE dt = %(ds)s;
  -- puis insert_rows / execute_values des enregistrements, avec dt = ds
  ```
- Utiliser `PostgresHook(postgres_conn_id="postgres_dwh")` et `execute_values` (psycopg2) ou `insert_rows` pour un insert en masse (pas une boucle de 8 500 `INSERT`).
- **Validation :** `SELECT dt, count(*) FROM staging.orders GROUP BY dt;`

**6.4 — `ensure_dimensions`** (voir étape 7, piège des FK)

**6.5 — `transform_fact_orders`** (étape 7)

**Graphe final du DAG (MVP) :**
```mermaid
flowchart LR
    A[extract_to_raw] --> B[load_staging_orders] --> C[ensure_dimensions] --> D[transform_fact_orders]
    D -. "outlet: Asset dwh_orders" .-> E((analytics DAG))
```

**Graphe avec bonus DQ (étape B1) :**
```mermaid
flowchart LR
    A[extract_to_raw] --> B[load_staging_orders] --> Q[dq_check_staging]
    Q --> BR{branch_on_dq}
    BR -- OK --> C[ensure_dimensions] --> D[transform_fact_orders]
    BR -- KO --> AL[alert_dq_failure]
```

---

### Étape 7 — Transformation staging → dwh idempotente

**Objectif :** le pattern **obligatoire et non négociable** : rejouer une date = même résultat en base.

**`dags/sql/transform_fact_orders.sql` :**
```sql
BEGIN;

DELETE FROM dwh.fact_orders
WHERE dt = '{{ ds }}';

INSERT INTO dwh.fact_orders (
    order_id, dt, seller_id, customer_id, product_id,
    quantity, unit_price, total_amount, commission_amount, status
)
SELECT
    s.order_id,
    s.dt,
    s.seller_id,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    COALESCE(s.total_amount, s.quantity * s.unit_price),
    ROUND(COALESCE(s.total_amount, s.quantity * s.unit_price) * 0.12, 2),
    s.status
FROM staging.orders AS s
WHERE s.dt = '{{ ds }}';

COMMIT;
```

Exécuté par `SQLExecuteQueryOperator(conn_id="postgres_dwh", sql="sql/transform_fact_orders.sql")` (le template Jinja `{{ ds }}` est rendu automatiquement), ou par `PostgresHook.run(sql, parameters=...)` dans une `@task`.

**Pourquoi DELETE + INSERT plutôt qu'un UPSERT ?**
- plus simple (pas de gestion de conflit ligne par ligne)
- plus performant pour des lots > 1 000 lignes
- gère aussi les **suppressions** (une commande disparue de la source disparaît du DWH)
- état connu et prévisible après chaque run

**⚠️ Piège des clés étrangères :** si une commande référence un `seller_id` absent de `dim_seller`, l'`INSERT` échoue sur la FK. Deux protections :
1. le DAG `marketplace_dims_refresh_daily` (étape 9) remplit les dimensions ;
2. la tâche **`ensure_dimensions`** du DAG d'ingestion insère les clés manquantes vues dans `staging.orders` (« membres inconnus ») avant le chargement des faits :
   ```sql
   INSERT INTO dwh.dim_seller (seller_id, name)
   SELECT DISTINCT seller_id, 'UNKNOWN' FROM staging.orders WHERE dt = '{{ ds }}'
   ON CONFLICT (seller_id) DO NOTHING;
   -- idem pour dim_product et dim_customer
   ```
   Le refresh suivant des dimensions complète ensuite les attributs (`ON CONFLICT DO UPDATE`).

**Validation :**
- [ ] `SELECT count(*) FROM dwh.fact_orders WHERE dt = '2026-04-07';` = nombre de lignes dans `staging.orders` pour cette date
- [ ] Le CA calculé correspond à la somme du JSON brut

---

### Étape 8 — Prouver l'idempotence

**Objectif :** la démonstration que le formateur exécutera lui-même.

```bash
# Run 1
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT COUNT(*), SUM(total_amount) FROM dwh.fact_orders WHERE dt='2026-04-07';"
# → N lignes, X €

# Run 2 et 3 : on relance exactement la même commande
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT COUNT(*), SUM(total_amount) FROM dwh.fact_orders WHERE dt='2026-04-07';"
# → toujours N lignes et X € (PAS 2N ni 3N)

# Bonus : vérifier que les autres dates ne sont pas touchées
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT dt, COUNT(*) FROM dwh.fact_orders GROUP BY dt ORDER BY dt;"
```

- [ ] COUNT identique après 3 runs
- [ ] SUM identique (preuve que les **valeurs** sont aussi identiques, pas seulement le nombre de lignes)
- [ ] Captures d'écran rangées dans `docs/screenshots/`
- [ ] 🎉 **Premier commit Git du pipeline fonctionnel** (`git commit -m "feat: pipeline ELT orders idempotent"`)

---

### Étape 9 — DAG de refresh des dimensions

> 🟡 **En cas de retard, c'est ce DAG qu'on sacrifie en premier** : la tâche `ensure_dimensions` suffit à faire tourner le pipeline.

**Objectif :** `marketplace_dims_refresh_daily` maintient `dim_seller`, `dim_product`, `dim_customer` à jour.

**Tâches (en parallèle par entité) :**
```mermaid
flowchart LR
    S1[extract_sellers_to_raw] --> S2[load_staging_sellers] --> S3[upsert_dim_seller]
    P1[extract_products_to_raw] --> P2[load_staging_products] --> P3[upsert_dim_product]
    C1[extract_customers_to_raw] --> C2[load_staging_customers] --> C3[upsert_dim_customer]
    S3 --> P3
```
- bronze : `sellers/dt={{ ds }}/data.json`, etc.
- upsert (SCD type 1 — on écrase, pas d'historique ; SCD2 est explicitement hors périmètre) :
  ```sql
  INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date, updated_at)
  SELECT seller_id, name, country, joined_date, now() FROM staging.sellers
  ON CONFLICT (seller_id) DO UPDATE
     SET name = EXCLUDED.name, country = EXCLUDED.country,
         joined_date = EXCLUDED.joined_date, updated_at = now();
  ```
- `upsert_dim_product` **après** `upsert_dim_seller` (FK `dim_product.seller_id`).
- Pour les dimensions, on ne fait **jamais** de DELETE : `fact_orders` y fait référence.

**Validation :**
- [ ] `SELECT count(*) FROM dwh.dim_seller WHERE name = 'UNKNOWN';` → 0 après le refresh
- [ ] Rejouer le DAG → mêmes comptes

---

### Étape 10 — DAG d'agrégation analytics

**Objectif :** `marketplace_analytics_aggregate_daily` alimente les tables lues par Metabase. Il est déclenché par un **Asset** (data-aware scheduling d'Airflow 3) : il tourne **dès que** les faits sont à jour, sans horaire fixe.

**Mise en place de l'Asset :**
```python
# fichier partagé, ex. dags/assets.py
from airflow.sdk import Asset
DWH_ORDERS = Asset("postgres://postgres-dwh/dwh/dwh/fact_orders")

# DAG d'ingestion : la tâche transform_fact_orders déclare outlets=[DWH_ORDERS]
# DAG analytics :   schedule=[DWH_ORDERS]
```

> ⚠️ Un run déclenché par un Asset n'a pas de `ds` « métier » égal à la date ingérée. Le plus simple et le plus robuste : **reconstruire une fenêtre glissante** (par ex. les 35 derniers jours présents dans `fact_orders`), ou lire la date ingérée dans les métadonnées de l'événement d'Asset (`triggering_asset_events`).

**Tâches :**
1. `build_daily_summary` : DELETE + INSERT sur la fenêtre
   ```sql
   DELETE FROM analytics.daily_summary WHERE dt >= current_date - 35;
   INSERT INTO analytics.daily_summary
   SELECT f.dt,
          COUNT(*)                                   AS total_orders,
          SUM(f.total_amount)                        AS total_revenue,
          SUM(f.commission_amount)                   AS total_commission,
          ROUND(AVG(f.total_amount), 2)              AS avg_basket,
          COUNT(DISTINCT f.seller_id)                AS active_sellers,
          (SELECT seller_id FROM dwh.fact_orders f2
            WHERE f2.dt = f.dt AND f2.status NOT IN ('cancelled','refunded')
            GROUP BY seller_id ORDER BY SUM(total_amount) DESC LIMIT 1) AS top_seller_id
   FROM dwh.fact_orders f
   WHERE f.dt >= current_date - 35
     AND f.status NOT IN ('cancelled', 'refunded')
   GROUP BY f.dt;
   ```
   > Attention : avec des dates de test comme `2026-04-07`, `current_date - 35` ne les couvre pas. Utiliser plutôt `(SELECT max(dt) FROM dwh.fact_orders) - 35`, ou reconstruire toute la table (très rapide à ce volume).
2. `build_seller_daily` (même principe, `GROUP BY dt, seller_id`)
3. `build_category_daily` (jointure `fact_orders` ⨝ `dim_product`, `GROUP BY dt, category`)

**Validation :**
- [ ] Déclencher le DAG d'ingestion → le DAG analytics démarre **tout seul**
- [ ] `SELECT * FROM analytics.daily_summary ORDER BY dt DESC LIMIT 5;`
- [ ] `SUM(total_revenue)` d'analytics = `SUM(total_amount)` des faits valides

---

### Étape 11 — Metabase et dashboards

**Objectif :** au moins 2 dashboards lisibles alimentés par de vraies données.

**11.1 — Préparer assez de données :** pour que les courbes sur 30 jours aient du sens, charger au moins 30 dates :
```bash
docker compose exec airflow-worker airflow dags backfill marketplace_orders_ingest_daily \
  --start-date 2026-03-08 --end-date 2026-04-07
# ou une boucle de `airflow dags test` sur chaque date
```

**11.2 — Service Docker :**
```yaml
metabase:
  image: metabase/metabase:v0.59.6.1
  ports: ["3000:3000"]
  environment:
    MB_DB_TYPE: h2
    MB_DB_FILE: /metabase-data/metabase.db
  volumes:
    - metabase-data:/metabase-data        # CRUCIAL : sinon dashboards perdus
  depends_on: [postgres-dwh]
  networks: [airflow-net]
```
Ne pas oublier `metabase-data:` dans la section `volumes:` en bas du fichier. Premier démarrage ≈ 60 s (initialisation H2).

**11.3 — Premier login :**
1. http://localhost:3000 → créer l'admin `admin@maelys.local` / `Admin2026!`
2. Ajouter une base de données :
   | Champ | Valeur |
   |---|---|
   | Type | PostgreSQL |
   | Host | **`postgres-dwh`** (nom Docker, pas `localhost`) |
   | Port | **`5432`** (port interne, pas 5433) |
   | Database | `dwh` |
   | Username / Password | ceux du `.env` |
3. Admin → Table Metadata : vérifier que les FK sont détectées et typer les colonnes monétaires en « Currency (EUR) ».

**11.4 — Dashboard 1 : Executive Summary** (US-01, US-02)

| Visualisation | Type | Source |
|---|---|---|
| CA du jour | Big Number (+ variation vs J-1) | `analytics.daily_summary` |
| Nombre de commandes / panier moyen | Big Numbers | `analytics.daily_summary` |
| CA des 30 derniers jours | Line chart | `analytics.daily_summary` |
| Top 5 vendeurs du jour | Bar chart horizontal | `analytics.seller_daily` ⨝ `dim_seller` |

**11.5 — Dashboard 2 : Top Sellers** (US-02)

| Visualisation | Type | Requête SQL indicative |
|---|---|---|
| Top 10 vendeurs du mois | Bar chart | voir ci-dessous |
| Évolution du CA des top 3 sur 30 j | Line chart multi-séries | `seller_daily` filtré sur les 3 meilleurs |
| Vendeurs inactifs (> 7 j sans vente) | Table | voir ci-dessous |

```sql
-- Top 10 vendeurs du mois
SELECT s.name, SUM(sd.revenue) AS ca
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s USING (seller_id)
WHERE sd.dt >= date_trunc('month', (SELECT max(dt) FROM analytics.seller_daily))
GROUP BY s.name
ORDER BY ca DESC
LIMIT 10;

-- Vendeurs inactifs depuis plus de 7 jours
SELECT s.seller_id, s.name, MAX(f.dt) AS derniere_vente,
       (SELECT max(dt) FROM dwh.fact_orders) - MAX(f.dt) AS jours_sans_vente
FROM dwh.dim_seller s
LEFT JOIN dwh.fact_orders f USING (seller_id)
GROUP BY s.seller_id, s.name
HAVING MAX(f.dt) IS NULL
    OR MAX(f.dt) < (SELECT max(dt) FROM dwh.fact_orders) - 7
ORDER BY derniere_vente NULLS FIRST;
```

**11.6 — Dashboards bonus (Should)**
- **Commissions** (US-03) : commissions par jour (bar), cumul du mois (big number), top vendeurs par commission (table).
- **Catégories** (US-04) : répartition du CA par catégorie (donut / bar), évolution par catégorie sur 30 j (stacked area).

**Validation :**
- [ ] 2 dashboards (minimum) affichent des données réelles
- [ ] `docker compose down` puis `up -d` → dashboards toujours là
- [ ] Captures d'écran dans `docs/screenshots/`

---

### Étape 12 — Tests pytest

**Objectif :** ≥ 3 tests verts, lancés **dans le conteneur** (même environnement que la prod).

**`tests/conftest.py` :**
```python
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
```

**`tests/test_dags.py` — les 3 tests minimum exigés :**

| Test | Ce qu'il vérifie |
|---|---|
| `test_no_import_errors(dagbag)` | `dagbag.import_errors == {}` |
| `test_all_dags_catchup_false(dagbag)` | `dag.catchup is False` pour chaque DAG |
| `test_marketplace_ingest_has_idempotent_transform(dagbag)` | la tâche `transform_fact_orders` existe et son SQL contient un `DELETE` **avant** l'`INSERT`, filtrés tous les deux sur `{{ ds }}` |

**Tests supplémentaires conseillés :**
- `test_expected_dags_present` : les 3 DAG ids attendus existent
- `test_ingest_dag_structure` : ordre `extract_to_raw → load_staging_orders → … → transform_fact_orders`
- `test_default_args_have_retries` : `retries >= 1` sur chaque DAG
- `test_no_hardcoded_token` : la chaîne `formation-token-2026` n'apparaît dans aucun fichier de `dags/` ni `plugins/`

**`tests/test_marketplace_hook.py` — Hook avec `requests` mocké** (pas besoin que l'API tourne) :
- `get_orders` envoie bien `Authorization: Bearer <password de la connection>` et `params={"date": ...}`
- une réponse 401 lève une exception **sans retry**
- une date mal formée (`"07/04/2026"`) lève `ValueError` avant tout appel HTTP
- la Connection est mockée avec `unittest.mock.patch.object(MarketplaceAPIHook, "get_connection", ...)`

**Lancer :**
```bash
docker compose exec airflow-worker pytest /opt/airflow/tests -v
```

**Validation :**
- [ ] tous les tests passent (`N passed`), sortie copiée dans le rendu

---

### Étape 13 — Finaliser la doc et livrer

> 💡 Écrire le README **au fil de l'eau**, pas à 16 h 59.

**À faire :**
- [ ] Remplacer la section « feuille de route » de ce README par un **« Démarrage rapide »** (voir 12.1)
- [ ] Vérifier que le schéma Mermaid d'architecture est à jour
- [ ] Documenter les **choix techniques** (section 14) et les **limites connues**
- [ ] Ajouter les captures d'écran (Airflow Graph, MinIO, psql idempotence, dashboards Metabase)
- [ ] Vérifier qu'un `git clone` + `cp .env.example .env` + `docker compose up -d` suffit à tout relancer (**test de reproductibilité sur un dossier vierge**)
- [ ] `grep -rn "formation-token-2026" dags plugins` → aucun résultat
- [ ] Commit final + tag `v1.0-mvp`

---

## 8. Bonus

> À n'attaquer **que** lorsque toute la section 9 (Definition of Done) est cochée.

### B1 — `DataQualityOperator` + branching (🟡 Should, +5 pts)

**Fichier :** `plugins/operators/data_quality_operator.py`

**Design :**
```python
class DataQualityOperator(BaseOperator):
    template_fields = ("table", "partition_filter")

    def __init__(self, *, conn_id: str, table: str, partition_filter: str,
                 checks: list[dict], fail_on_error: bool = False, **kwargs): ...
    # checks = [
    #   {"type": "not_empty"},
    #   {"type": "not_null",       "columns": ["order_id", "seller_id", "product_id", "total_amount"]},
    #   {"type": "no_future_date", "column": "order_ts"},
    # ]
    def execute(self, context) -> dict:
        """Exécute une requête SQL par règle sur la partition (WHERE dt = '{{ ds }}'),
        log le résultat de chaque règle et retourne {"passed": bool, "failures": [...]}"""
```

| Règle | SQL généré | Échec si |
|---|---|---|
| `not_empty` | `SELECT COUNT(*) FROM {table} WHERE {filter}` | = 0 |
| `not_null` | `SELECT COUNT(*) FROM {table} WHERE {filter} AND ({col} IS NULL OR …)` | > 0 |
| `no_future_date` | `SELECT COUNT(*) FROM {table} WHERE {filter} AND {col} > now()` | > 0 |
| *(extension)* `positive` | `… AND {col} <= 0` | > 0 |
| *(extension)* `unique` | `SELECT {col} … GROUP BY {col} HAVING COUNT(*) > 1` | ≥ 1 ligne |

**Branching dans le DAG d'ingestion :**
```python
@task.branch
def branch_on_dq(dq_result: dict) -> str:
    return "ensure_dimensions" if dq_result["passed"] else "alert_dq_failure"
```
- `alert_dq_failure` : log en `ERROR` + écriture dans une table `analytics.dq_incidents` (+ webhook simulé si temps).
- ⚠️ Toute tâche placée **après** la réunion des deux branches doit avoir `trigger_rule="none_failed_min_one_success"`, sinon elle est `skipped`.

**Tests :** opérateur testé avec un `PostgresHook` mocké (renvoie 0 ou N selon le cas).

### B2 — Détection d'anomalies (🔵 Could, +5 pts — US-05)

**DAG :** `marketplace_anomaly_detect_daily`, `@daily` (ou déclenché par l'Asset analytics).

1. `compute_anomalies` :
   ```sql
   WITH s AS (
     SELECT dt, total_revenue,
            AVG(total_revenue) OVER (ORDER BY dt ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_7d
     FROM analytics.daily_summary
   )
   SELECT dt, total_revenue, avg_7d, total_revenue / NULLIF(avg_7d, 0) AS ratio
   FROM s WHERE dt = '{{ ds }}' AND total_revenue < 0.7 * avg_7d;
   ```
   Variante par vendeur : drop > 30 % vs sa propre moyenne 7 j (sur `seller_daily`).
2. Écriture dans `analytics.anomalies (dt, scope, entity_id, value, baseline, ratio, detected_at)` en DELETE + INSERT sur `dt`.
3. `@task.branch` : anomalies trouvées → `notify_anomalies` (webhook simulé / log) ; sinon → `no_anomaly`.
4. Bonus « prix suspects » : produit vendu à un `unit_price` qui s'écarte de plus de ±50 % de son `base_price`.
5. Tuile Metabase « Anomalies des 30 derniers jours ».

### B3 — Backfill documenté sur 7 jours (🔵 Could — US-06)

```bash
docker compose exec airflow-worker airflow dags backfill marketplace_orders_ingest_daily \
  --start-date 2026-04-01 --end-date 2026-04-07
```
Documenter : la commande, le contrôle (`SELECT dt, count(*) … GROUP BY dt`), et la preuve qu'un **second** backfill ne crée aucun doublon.

### B4 — Observabilité Prometheus + Grafana (🔵 Could, +10 pts — US-07)

1. Ajouter au compose : `statsd-exporter` (9102, écoute en UDP 9125), `prometheus` (9090), `grafana` (**3001:3000**), `postgres-exporter` (9187, `DATA_SOURCE_NAME=postgresql://dwh_user:...@postgres-dwh:5432/dwh?sslmode=disable`).
2. Activer les métriques Airflow dans `x-airflow-common` puis `docker compose up -d --force-recreate` :
   ```yaml
   AIRFLOW__METRICS__STATSD_ON: "True"
   AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
   AIRFLOW__METRICS__STATSD_PORT: "9125"
   AIRFLOW__METRICS__STATSD_PREFIX: airflow
   ```
3. `observability/prometheus.yml` : jobs de scrape `statsd-exporter:9102` et `postgres-exporter:9187`.
4. Vérifier http://localhost:9090/targets → toutes les cibles `UP`.
5. Grafana (http://localhost:3001, admin/admin) → datasource Prometheus `http://prometheus:9090` → Import des dashboards **11010** (Airflow Cluster) et **9628** (PostgreSQL Database).
6. (optionnel) Alerte Grafana sur `airflow_dagrun_failed` > 0.

---

## 9. Definition of Done

Le projet est **livrable** quand tout est coché :

- [ ] `docker compose up -d` démarre toute la stack depuis un clone propre
- [ ] `marketplace_orders_ingest_daily` est en **success** dans l'UI Airflow
- [ ] Le JSON brut est visible dans MinIO sous `bronze/orders/dt=YYYY-MM-DD/`
- [ ] `dwh.fact_orders` contient des données pour au moins 1 date (idéalement 30)
- [ ] Rejouer le même run 2× (ou 3×) → **même `COUNT(*)`** dans `dwh.fact_orders`
- [ ] Les FK de `fact_orders` vers les dimensions sont en place et respectées
- [ ] Metabase affiche **≥ 2 dashboards** avec des données réelles
- [ ] **≥ 3 tests pytest** passent
- [ ] Aucun token / mot de passe en dur dans `dags/` ou `plugins/`
- [ ] README avec schéma Mermaid, instructions de lancement et choix techniques

---

## 10. Planning sur 2 jours

### Jour 1 — Pipeline ELT (objectif : des données dans `dwh.fact_orders`, idempotence prouvée)

| Créneau | Étapes | Livrable de fin de créneau |
|---|---|---|
| 09:00 – 10:30 | 0, 1 | Stack Docker healthy, UIs accessibles |
| 10:30 – 11:00 | 2 (si pas de kit), 3 | API comprise, format noté |
| 11:00 – 12:30 | 4 | `schema.sql` qui passe, ERD validé |
| 13:30 – 15:00 | 5 | Hook testé seul dans le worker |
| 15:00 – 16:30 | 6, 7 | DAG d'ingestion complet en success |
| 16:30 – 17:30 | 8 | Idempotence prouvée + **premier commit** |

> 🟡 En retard ? On sacrifie l'étape 9 (refresh des dimensions) : `ensure_dimensions` suffit.

### Jour 2 — Analytics, dashboards, qualité (objectif : 2 dashboards + tests + README)

| Créneau | Étapes | Livrable de fin de créneau |
|---|---|---|
| 09:00 – 10:00 | 9 | Dimensions complètes |
| 10:00 – 11:30 | 10 + backfill 30 j | Tables analytics remplies automatiquement via l'Asset |
| 11:30 – 13:00 | 11 | 2 dashboards Metabase |
| 14:00 – 15:00 | 12 | Tests verts |
| 15:00 – 16:00 | 13 | README final, test de reproductibilité, tag `v1.0-mvp` |
| 16:00 – … | Bonus B1 → B2 → B3 → B4 | selon le temps restant |

---

## 11. Pièges connus et solutions

### Pipeline ELT

| Piège | Symptôme | Solution |
|---|---|---|
| 🔴 INSERT sans DELETE préalable | Doublons à chaque run | DELETE + INSERT par partition `dt` dans une transaction |
| 🔴 Date en dur / `date.today()` | Le run ignore `{{ ds }}`, le backfill charge toujours la même date | Macros Jinja `{{ ds }}`, `{{ ds_nodash }}` ou `context["ds"]` |
| 🔴 Token API dans le code | Refus en code review | Toujours `Connection.password` |
| 🔴 FK violée sur `fact_orders` | `insert or update violates foreign key constraint` | `ensure_dimensions` avant le chargement des faits + refresh des dimensions |
| 🟡 Gros volume dans XCom | DB de métadonnées qui gonfle, lenteur | Passer la **clé S3** en XCom, pas les données |
| 🟡 `staging.orders` qui grossit indéfiniment | Disque qui se remplit | DELETE par `dt` à chaque run + purge des partitions > 30 j |
| 🟡 Appel réseau au niveau module d'un DAG | DAG processor lent, timeouts | Aucun appel d'API / DB hors des tâches |
| 🟡 `schema.sql` modifié mais non appliqué | Colonne inexistante | Il ne tourne qu'au 1er démarrage : le rejouer avec `psql` |

### Docker / Airflow 3

| Piège | Symptôme | Solution |
|---|---|---|
| 🔴 Modification de `x-airflow-common` non prise en compte | Ancienne config active | `docker compose up -d --force-recreate` (tous les services Airflow) |
| 🔴 Worker qui n'arrive pas à exécuter les tâches | Tâches bloquées en `queued` / erreurs d'auth | Vérifier `EXECUTION_API_SERVER_URL` et un `JWT_SECRET` identique partout |
| 🟡 Logs appartenant à root | `Permission denied` dans `logs/` | `AIRFLOW_UID=$(id -u)` dans `.env` |
| 🟡 Plugins non trouvés | `ModuleNotFoundError: hooks` | `./plugins` monté sur `/opt/airflow/plugins` + fichiers `__init__.py` |
| 🟡 Postgres 18 qui refuse de démarrer | Erreur sur le dossier de données | Monter le volume sur `/var/lib/postgresql` |
| 🟡 Bucket absent | `NoSuchBucket` | `docker compose restart minio-init` ou création manuelle dans la console |

### Metabase

| Piège | Symptôme | Solution |
|---|---|---|
| 🔴 Dashboards perdus après `down -v` | Tout a disparu | Volume nommé `metabase-data` |
| 🔴 Mauvaise connexion à Postgres | « Cannot connect » | Host `postgres-dwh`, port **5432** |
| 🟡 Démarrage lent | Page blanche ~60 s | Normal (initialisation H2) |
| 🟡 Conflit de port avec Grafana | `Bind failed` sur 3000 | Grafana sur `3001:3000` |
| 🟡 Nouvelles tables invisibles | Table absente dans l'éditeur | Admin → Databases → « Sync database schema now » |

---

## 12. Aide-mémoire des commandes

### 12.1 Démarrage rapide

```bash
git clone https://github.com/Nassim-Bzr/MarketPlace_Analytics.git && cd MarketPlace_Analytics
cp .env.example .env            # puis ajuster AIRFLOW_UID=$(id -u)
docker compose up -d
docker compose ps               # attendre que tout soit healthy (~60-90 s)

# Charger une journée et vérifier
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT COUNT(*) FROM dwh.fact_orders;"
```

### 12.2 Docker

```bash
docker compose ps                                          # état des services
docker compose logs -f airflow-scheduler --tail=50         # suivre les logs
docker compose exec airflow-worker bash                    # shell dans le worker
docker compose up -d --force-recreate metabase             # recréer un service
docker compose down                                        # arrêter (volumes conservés)
docker compose down -v                                     # ⚠️ RESET COMPLET : supprime toutes les données
```

### 12.3 Airflow

```bash
docker compose exec airflow-worker airflow dags list
docker compose exec airflow-worker airflow dags list-import-errors
docker compose exec airflow-worker airflow dags test   <dag_id> 2026-04-07
docker compose exec airflow-worker airflow tasks test  <dag_id> <task_id> 2026-04-07
docker compose exec airflow-worker airflow dags backfill <dag_id> --start-date 2026-04-01 --end-date 2026-04-07
docker compose exec airflow-worker airflow connections get marketplace_api
docker compose exec airflow-worker pytest /opt/airflow/tests -v
```

### 12.4 PostgreSQL DWH

```bash
# Depuis la machine hôte
psql -h localhost -p 5433 -U dwh_user -d dwh
# Depuis un conteneur
docker compose exec postgres-dwh psql -U dwh_user -d dwh
```
```sql
\dn                                    -- schémas
\dt dwh.*                              -- tables du DWH
SELECT dt, COUNT(*), SUM(total_amount) FROM dwh.fact_orders GROUP BY dt ORDER BY dt;
SELECT * FROM analytics.daily_summary ORDER BY dt DESC LIMIT 7;
```

### 12.5 API

```bash
curl http://localhost:5000/health
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=2026-04-07"
```

---

## 13. Accès et identifiants (environnement de formation)

> ⚠️ Identifiants **de formation uniquement**, définis dans `.env` (non commité). À ne jamais réutiliser ailleurs.

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` (créé par `airflow-init`) |
| MinIO console | http://localhost:9001 | `minio_admin` / `minio_password_2026` |
| API Marketplace | http://localhost:5000 | Bearer `formation-token-2026` |
| PostgreSQL DWH | `localhost:5433` | `dwh_user` / voir `.env` — base `dwh` |
| Metabase | http://localhost:3000 | `admin@maelys.local` / `Admin2026!` |
| Grafana *(bonus)* | http://localhost:3001 | `admin` / `admin` |
| Prometheus *(bonus)* | http://localhost:9090 | — |

---

## 14. Choix techniques

| Choix | Alternative écartée | Justification |
|---|---|---|
| ELT (on charge puis on transforme dans Postgres) | ETL en Python / pandas | Le SQL dans le DWH est plus simple, traçable et suffisant à ce volume |
| JSON brut archivé dans MinIO, partition `dt=` | Charger directement l'API dans Postgres | Rejouabilité sans rappeler la source, audit, pattern « data lake first » |
| DELETE + INSERT par partition | MERGE / UPSERT | Plus simple, plus rapide en batch, gère les suppressions, état prévisible |
| Schéma en étoile Kimball | Table plate unique / 3NF | Lisible par un analyste, jointures simples, Metabase détecte les relations |
| SCD type 1 sur les dimensions | SCD2 | Historisation non demandée (hors périmètre) |
| `dim_date` pré-générée par `generate_series` | Dimension date chargée par le pipeline | Statique, jamais à maintenir, FK garantie |
| Connections Airflow en variables d'environnement | Saisie manuelle dans l'UI | Reproductible, aucun secret dans le code |
| Asset scheduling entre ingestion et analytics | Horaire fixe / `ExternalTaskSensor` | Le DAG en aval démarre dès que les données sont prêtes, sans worker bloqué |
| Metabase (stockage H2 + volume nommé) | Superset / Streamlit | Installation triviale, suffisant pour 4 dashboards |
| Pas de Spark ni dbt | — | 8 500 commandes/jour : surdimensionné pour un MVP de 2 jours |

---

## 15. Ce qu'on ferait avec plus de temps

- **dbt Core** pour les transformations SQL (tests de données, lignage, documentation générée)
- **Great Expectations / Soda** à la place du `DataQualityOperator` maison
- **Metabase sur PostgreSQL** plutôt que H2 pour la persistance de ses métadonnées
- **Partitionnement natif** PostgreSQL de `fact_orders` et `staging.orders` par mois
- **Parquet** dans MinIO (couche `curated/`) en plus du JSON brut
- **CI** (lint `ruff`, `pytest`, validation des DAGs) sur chaque push
- **SCD2** sur `dim_seller` si le métier a besoin d'historiser les changements de pays ou de statut
- Alerting réel (Slack / Teams) sur échec de DAG et sur anomalie
