# MarketPlace Analytics

> MVP de plateforme data pour **Maelys Marketplace** (marketplace e-commerce, 12 % de commission par vente).
> Ingestion quotidienne et idempotente des commandes depuis l'API du backoffice → data lake MinIO → Data Warehouse PostgreSQL en étoile → dashboards Metabase.

**Projet A — Data Lake / ELT** · IPSSI · Semaine du 19 octobre 2026 · Équipe : Medy, Rayan, Nassim

---

## Sommaire

1. [Ce que fait le projet](#1-ce-que-fait-le-projet)
2. [Architecture](#2-architecture)
3. [Démarrage rapide](#3-démarrage-rapide)
4. [Les données (API + Faker)](#4-les-données-api--faker)
5. [Modèle de données](#5-modèle-de-données)
6. [Les DAGs Airflow](#6-les-dags-airflow)
7. [Idempotence — la preuve](#7-idempotence--la-preuve)
8. [Dashboards Metabase](#8-dashboards-metabase)
9. [Tests](#9-tests)
10. [Structure du dépôt](#10-structure-du-dépôt)
11. [Choix techniques](#11-choix-techniques)
12. [Limites et pistes d'amélioration](#12-limites-et-pistes-damélioration)
13. [Accès, commandes utiles et dépannage](#13-accès-commandes-utiles-et-dépannage)

---

## 1. Ce que fait le projet

**Problème :** l'analyste de Maelys passe 2 jours par semaine à extraire le backoffice à la main vers Excel. Les rapports sont en retard et pleins d'erreurs de copier-coller.

**Solution livrée :**

| User story | Besoin | Livré par |
|---|---|---|
| US-01 (Must) | CA total par jour sur 30 jours | Dashboard *Executive Summary* |
| US-02 (Must) | Top 10 vendeurs par revenu | Dashboard *Top Sellers* |
| US-03 (Must) | Récapitulatif des commissions par jour | Dashboard *Executive Summary* + `analytics.daily_summary` |
| US-04 (Should) | CA par catégorie | Table `analytics.category_daily` (prête pour un dashboard) |
| US-06 (Could) | Rejouer le pipeline sur une date passée | `airflow dags test` / `airflow backfill create` (pipeline idempotent) |

**Périmètre MVP couvert à 100 % :**

- [x] Custom Hook `MarketplaceAPIHook` (auth Bearer via Connection Airflow, retry sur timeout / 5xx)
- [x] Pipeline ELT : API → JSON brut dans MinIO (partition `dt=`) → `staging.orders` → `dwh.fact_orders`
- [x] Transformation idempotente **DELETE + INSERT par partition `dt = {{ ds }}`**
- [x] Modèle en étoile : 4 dimensions + 1 table de faits, PK et FK
- [x] 3 tables d'agrégation dans `analytics`
- [x] 3 DAGs (ingestion, dimensions, agrégation déclenchée par Asset)
- [x] 2 dashboards Metabase (10 visualisations), créés automatiquement par script
- [x] 24 tests pytest, dont un test d'idempotence sur le vrai DWH
- [x] Stack reproductible : `docker compose up -d`

Bonus non réalisés (hors périmètre de cette version) : `DataQualityOperator` + branching, détection d'anomalies, Prometheus / Grafana.

---

## 2. Architecture

```mermaid
flowchart LR
    subgraph SRC["Source"]
        API[("API Marketplace<br/>Flask + Faker")]
    end

    subgraph ORCH["Orchestration — Airflow 3.1.8"]
        HOOK["MarketplaceAPIHook"]
        D1[["orders_ingest_daily"]]
        D0[["dims_refresh_daily"]]
        D2[["analytics_aggregate_daily"]]
    end

    subgraph LAKE["Data lake — MinIO"]
        BRONZE[("bronze<br/>JSON brut, dt=")]
        SILVER[("silver")]
        GOLD[("gold")]
    end

    subgraph DWH["Data Warehouse — PostgreSQL 18"]
        STG["staging"]
        STAR["dwh<br/>étoile"]
        ANA["analytics"]
    end

    MB["Metabase<br/>2 dashboards"]

    API -- "GET + Bearer" --> HOOK
    HOOK --> D0 & D1
    D0 & D1 -- "JSON brut" --> BRONZE
    BRONZE -- "relecture" --> STG
    STG -- "DELETE + INSERT dt" --> STAR
    D1 -. "Asset dwh_fact_orders" .-> D2
    STAR --> ANA
    ANA --> MB
```

### Déroulé d'un run (date `{{ ds }}`)

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant H as MarketplaceAPIHook
    participant A as API Marketplace
    participant M as MinIO (bronze)
    participant P as PostgreSQL DWH
    participant MB as Metabase

    S->>H: extract_orders_to_bronze(ds)
    H->>A: GET /orders?date={{ ds }} (Bearer)
    A-->>H: {"count": N, "data": [...]}
    H->>M: PUT bronze/orders/dt={{ ds }}/data.json (écrase)
    S->>P: load_staging_orders : relit MinIO, DELETE + INSERT staging.orders WHERE dt
    S->>P: ensure_dimensions : clés manquantes → membres « Inconnu »
    S->>P: transform_fact_orders : DELETE + INSERT dwh.fact_orders WHERE dt
    P-->>S: Asset dwh_fact_orders mis à jour
    S->>P: analytics_aggregate_daily : reconstruit analytics.*
    MB->>P: SELECT pour les dashboards
```

### Architecture médaillon

Le cahier des charges prévoit un bucket unique `data-lake/raw/`. Nous utilisons 3 buckets, ce qui rend chaque couche explicite :

| Couche | Où | Contenu |
|---|---|---|
| **Bronze** | MinIO `bronze/` | JSON brut de l'API, tel que reçu : `<entité>/dt=YYYY-MM-DD/data.json` |
| **Silver** | PostgreSQL `staging` + `dwh` | Données typées puis modélisées en étoile |
| **Gold** | PostgreSQL `analytics` | Agrégats lus par Metabase |

Les buckets `silver/` et `gold/` sont créés et réservés aux exports Parquet (évolution prévue, voir §12).

### Services

| Service | Port hôte | Rôle |
|---|---|---|
| `airflow-apiserver` | **8080** | UI + API Airflow |
| `airflow-scheduler`, `-dag-processor`, `-triggerer`, `-worker` | — | Orchestration (CeleryExecutor) |
| `airflow-init` | — | Migration de la base Airflow + création de l'admin (one-shot) |
| `postgres-airflow` | — (non exposé) | Métadonnées Airflow |
| `postgres-dwh` | **5433** | Data Warehouse |
| `redis` | — (non exposé) | Broker Celery |
| `minio` | **9000** (S3) / **9001** (console) | Data lake |
| `minio-init` | — | Crée les buckets `bronze`, `silver`, `gold` (idempotent) |
| `api-marketplace` | **5000** | API simulée (profil `api`) |
| `metabase` | **3000** | BI (profil `bi`) |

---

## 3. Démarrage rapide

**Prérequis :** Docker + Docker Compose v2, **≈ 6 Go de RAM libre** (Airflow ≈ 3 Go, Metabase ≈ 1 Go), ports 3000, 5000, 5433, 8080, 9000 et 9001 libres.

### 3.1 Lancer la stack

```bash
git clone https://github.com/Nassim-Bzr/MarketPlace_Analytics.git
cd MarketPlace_Analytics

cp .env.example .env
# Dans .env : AIRFLOW_UID = résultat de `id -u`
# et générer les 2 secrets Airflow :
python3 -c "import secrets; print(secrets.token_urlsafe(32))"   # → AIRFLOW_JWT_SECRET
python3 -c "import secrets; print(secrets.token_urlsafe(32))"   # → AIRFLOW_API_SECRET_KEY

docker compose up -d
docker compose ps          # attendre ~90 s que tout soit (healthy)
```

Au premier démarrage, `postgres-dwh` exécute automatiquement `init-db/` (schémas, tables, `dim_date` préremplie) et `minio-init` crée les buckets.

### 3.2 Charger les données

```bash
# 1. Dimensions (vendeurs, produits, clients)
docker compose exec airflow-worker airflow dags test marketplace_dims_refresh_daily 2026-10-20

# 2. Une journée de commandes (la date de test du cahier des charges)
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07

# 3. Activer les DAGs (planification quotidienne + déclenchement par Asset)
for dag in marketplace_dims_refresh_daily marketplace_orders_ingest_daily marketplace_analytics_aggregate_daily; do
  docker compose exec airflow-worker airflow dags unpause $dag
done

# 4. Historique de 30 jours pour les dashboards (≈ 3 min, 2 runs en parallèle)
docker compose exec airflow-worker airflow backfill create \
  --dag-id marketplace_orders_ingest_daily \
  --from-date 2026-09-21 --to-date 2026-10-20 \
  --max-active-runs 2 --reprocess-behavior completed
```

Le DAG `marketplace_analytics_aggregate_daily` se déclenche tout seul après chaque run d'ingestion.

### 3.3 Créer les dashboards

```bash
python3 scripts/setup_metabase.py
```

Le script (bibliothèque standard Python uniquement) crée le compte admin, ajoute la source PostgreSQL, crée les 10 questions et les 2 dashboards. Il est **idempotent** : on peut le relancer sans créer de doublons.

→ http://localhost:3000 (identifiants en §13).

---

## 4. Les données (API + Faker)

Les données sont **fausses**, générées avec **Faker** (`fr_FR`) à l'intérieur d'une API Flask ([api/app.py](api/app.py)). L'API reste indispensable : c'est elle que le Custom Hook interroge, avec authentification Bearer.

### Endpoints

| Méthode | Endpoint | Paramètres | Auth | Retour |
|---|---|---|---|---|
| GET | `/health` | — | non | `{"status": "ok", "version": "1.0.0"}` |
| GET | `/orders` | `date=YYYY-MM-DD` (obligatoire) | Bearer | `{"date", "count", "data": [...]}` |
| GET | `/sellers` | `limit` (optionnel) | Bearer | `{"count", "data": [...]}` |
| GET | `/products` | `limit` (optionnel) | Bearer | `{"count", "data": [...]}` |
| GET | `/customers` | `limit` (optionnel) | Bearer | `{"count", "data": [...]}` |

Erreurs : `401` sans token valide, `400` si la date est absente ou mal formée. Une date future renvoie une liste vide.

```bash
curl http://localhost:5000/health
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=2026-04-07"
```

### Déterminisme (indispensable pour l'idempotence)

- **Référentiels** (200 vendeurs, 2 000 produits, 5 000 clients) : générés une seule fois au démarrage avec une seed fixe → identiques à chaque lancement et sur chaque machine.
- **Commandes** : `seed = md5(date)` → **une même date renvoie toujours exactement les mêmes commandes**.
- Versions de Faker / Flask / Gunicorn **épinglées** ([api/requirements.txt](api/requirements.txt)) : une autre version de Faker pourrait générer d'autres données pour la même seed.

### Réalisme

| Règle de génération | Ce que ça permet de montrer |
|---|---|
| Poids des vendeurs selon une loi de Pareto | Quelques gros vendeurs font l'essentiel du CA → top vendeurs crédible |
| ~8 % des vendeurs arrêtent de vendre en 2026 | Table « vendeurs inactifs » non vide |
| Plus de commandes le week-end (×1,3) | Courbe de CA réaliste |
| 10 catégories avec leurs fourchettes de prix, ±10 % de variation | Panier moyen ≈ 180 € |
| ~9 % de commandes `cancelled` / `refunded` | Exclues du CA et des commissions |

Volume par défaut : `ORDERS_PER_DAY=1000` (réglable dans `.env` ; le cahier des charges cite ~8 500/jour en production).

---

## 5. Modèle de données

Défini dans [init-db/schema.sql](init-db/schema.sql) (rejouable sans erreur grâce aux `IF NOT EXISTS`).

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
        text city
        text main_category
        date joined_date
    }
    DIM_CUSTOMER {
        text customer_id PK
        text first_name
        text last_name
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
        smallint year
        smallint quarter
        smallint month
        smallint week
        smallint day_of_week
        boolean is_weekend
    }
    FACT_ORDERS {
        text order_id PK
        date dt FK
        text seller_id FK
        text customer_id FK
        text product_id FK
        timestamp order_ts
        int quantity
        numeric unit_price
        numeric total_amount
        numeric commission_amount
        text status
        boolean is_revenue
    }
```

| Schéma | Tables | Rôle |
|---|---|---|
| `staging` | `orders`, `sellers`, `products`, `customers` | Copie typée de l'API, partitionnée par `dt` (date du run) |
| `dwh` | `dim_date`, `dim_seller`, `dim_product`, `dim_customer`, `fact_orders` | Étoile Kimball. Grain de `fact_orders` : 1 ligne = 1 commande |
| `analytics` | `daily_summary`, `seller_daily`, `category_daily` | Agrégats pour Metabase |

**Règles métier :**

- **CA** = somme de `total_amount` des commandes `is_revenue` (statut ni `cancelled` ni `refunded`)
- **Commission** = `total_amount × 12 %` (constante `COMMISSION_RATE` dans [plugins/marketplace_config.py](plugins/marketplace_config.py), injectée dans le SQL)
- **Panier moyen** = CA / nombre de commandes valides
- `dim_date` est préremplie de 2020 à 2030 par `generate_series` : aucune maintenance
- Dimensions en **SCD type 1** (écrasement, sans historique — SCD2 hors périmètre)

---

## 6. Les DAGs Airflow

```mermaid
flowchart LR
    subgraph ingest["marketplace_orders_ingest_daily — @daily"]
        A[extract_orders_to_bronze] --> B[load_staging_orders] --> C[ensure_dimensions] --> D[transform_fact_orders]
    end
    subgraph agg["marketplace_analytics_aggregate_daily — Asset"]
        E[build_daily_summary]
        F[build_seller_daily]
        G[build_category_daily]
    end
    D -. "Asset dwh_fact_orders" .-> agg
```

| DAG | Planification | Rôle |
|---|---|---|
| [marketplace_orders_ingest_daily](dags/marketplace_orders_ingest_daily.py) | `@daily` | Pipeline principal : API → bronze → staging → fact_orders |
| [marketplace_dims_refresh_daily](dags/marketplace_dims_refresh_daily.py) | `@daily` | Vendeurs / produits / clients : API → bronze → staging → upsert `dim_*` |
| [marketplace_analytics_aggregate_daily](dags/marketplace_analytics_aggregate_daily.py) | Asset `dwh_fact_orders` | Reconstruit les 3 tables `analytics.*` |

Réglages communs : `catchup=False`, `max_active_runs=1`, 2 retries espacés d'1 minute. Le run planifié de minuit le jour J traite la journée J-1 (`{{ ds }}`).

### Détail des tâches d'ingestion

| Tâche | Ce qu'elle fait | Pourquoi c'est idempotent |
|---|---|---|
| `extract_orders_to_bronze` | `MarketplaceAPIHook.get_orders(ds)` puis écriture dans `bronze/orders/dt={{ ds }}/data.json`. Seul le nombre de commandes passe par XCom | Le fichier est écrasé (`replace=True`) |
| `load_staging_orders` | **Relit le JSON depuis MinIO** (pas l'API), puis DELETE + INSERT de la partition `dt` en une transaction. Vérifie que le nombre de lignes chargées = nombre extrait | Partition remplacée |
| `ensure_dimensions` | Crée les vendeurs / produits / clients référencés mais encore absents des dimensions (« Inconnu (SEL-…) »), sinon les FK de `fact_orders` bloquent l'insertion. Ils sont complétés au prochain refresh des dimensions | `ON CONFLICT DO NOTHING` |
| `transform_fact_orders` | [transform_fact_orders.sql](dags/sql/transform_fact_orders.sql) : `BEGIN; DELETE … WHERE dt = '{{ ds }}'; INSERT … SELECT … WHERE dt = '{{ ds }}'; COMMIT;` | Pattern imposé par le cahier des charges |

### Le Custom Hook

[plugins/hooks/marketplace_api_hook.py](plugins/hooks/marketplace_api_hook.py) :

- hérite de `BaseHook` et lit la Connection `marketplace_api` (`host` = URL, `password` = token) → **aucun token dans le code**
- méthodes `get_orders(date)`, `get_sellers(limit)`, `get_products(limit)`, `get_customers(limit)`
- `urllib3.Retry` : 3 tentatives avec backoff exponentiel sur les timeouts et les erreurs 500 / 502 / 503 / 504 ; les 4xx échouent immédiatement
- vérifie le format de date **avant** tout appel HTTP
- `test_connection()` : bouton « Test » dans l'UI Airflow (appel à `/health`)
- session HTTP créée à la demande, jamais au parsing du DAG

### Les Connections

Déclarées en variables d'environnement dans [docker-compose.yml](docker-compose.yml), valeurs lues dans `.env` :

| Connection | Type | Cible |
|---|---|---|
| `marketplace_api` | http | `http://api-marketplace:5000` + token Bearer |
| `minio_s3` | aws | `http://minio:9000` (S3Hook) |
| `postgres_dwh` | postgres | `postgres-dwh:5432/dwh` |

---

## 7. Idempotence — la preuve

Le test que le formateur exécutera :

```bash
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT COUNT(*), SUM(total_amount) FROM dwh.fact_orders WHERE dt='2026-04-07';"
# … relancer les deux commandes autant de fois que voulu
```

**Résultat obtenu (3 runs consécutifs) :**

| Run | État | `COUNT(*)` | `SUM(total_amount)` | `SUM(commission_amount)` |
|---|---|---|---|---|
| 1 | success | 1 063 | 205 292,18 € | 24 635,28 € |
| 2 | success | 1 063 | 205 292,18 € | 24 635,28 € |
| 3 | success | 1 063 | 205 292,18 € | 24 635,28 € |

→ Toujours 1 063 lignes, jamais 2 126 ni 3 189. Le CA valide (hors `cancelled` / `refunded`) vaut **188 750,73 €**, exactement la somme calculée directement sur le JSON brut de l'API.

**Cohérence de bout en bout après le backfill de 30 jours :**

| Contrôle | Résultat |
|---|---|
| Runs d'ingestion | 30 backfills + 1 manuel + 1 planifié : 100 % success |
| Runs analytics déclenchés par l'Asset | 26 : 100 % success |
| `staging.orders` vs `dwh.fact_orders` | 33 689 = 33 689 lignes, 32 jours |
| CA : faits / `daily_summary` / `seller_daily` / `category_daily` | 5 510 087 € dans les 4 tables |
| Membres « Inconnu » restants dans les dimensions | 0 |

---

## 8. Dashboards Metabase

Créés par [scripts/setup_metabase.py](scripts/setup_metabase.py) dans la collection **Marketplace Analytics**. Ils lisent uniquement le schéma `analytics` (et `dwh` pour les vendeurs inactifs).

### Executive Summary (US-01, US-02, US-03)

| Visualisation | Type |
|---|---|
| CA du dernier jour | Chiffre clé |
| Commandes du dernier jour | Chiffre clé |
| Panier moyen du dernier jour | Chiffre clé |
| Commissions du dernier jour | Chiffre clé |
| CA des 30 derniers jours | Courbe |
| Top 5 vendeurs du dernier jour | Barres horizontales |
| Commissions par jour (30 derniers jours) | Barres |

### Top Sellers (US-02)

| Visualisation | Type |
|---|---|
| Top 10 vendeurs du mois | Barres horizontales |
| Évolution du CA des 3 meilleurs vendeurs (30 jours) | Courbe multi-séries |
| Vendeurs inactifs (plus de 7 jours sans vente) | Tableau |

« Dernier jour » = dernière date chargée dans le DWH : les dashboards restent pertinents même après un backfill.

Le volume nommé `metabase-data` conserve les dashboards après un `docker compose down`. Après un `down -v`, il suffit de relancer le script.

---

## 9. Tests

```bash
docker compose exec airflow-worker pytest /opt/airflow/tests -v
# → 24 passed
```

| Fichier | Tests | Service requis |
|---|---|---|
| [tests/test_dags.py](tests/test_dags.py) | Aucune erreur d'import · les 3 DAGs présents · `catchup=False` · retries sur toutes les tâches · ordre des tâches d'ingestion · **DELETE avant INSERT sur `{{ ds }}` dans une transaction** · aucune date en dur dans le SQL · DAG analytics déclenché par l'Asset · aucun secret en dur | aucun |
| [tests/test_marketplace_hook.py](tests/test_marketplace_hook.py) | Token Bearer et paramètres envoyés · `limit` · liste brute acceptée · dates invalides rejetées sans appel HTTP · erreurs HTTP remontées · retry configuré sur les 5xx uniquement · token manquant | aucun (`requests` mocké) |
| [tests/test_idempotence_dwh.py](tests/test_idempotence_dwh.py) | Exécute **deux fois** le SQL réel du DAG sur une partition de test du vrai DWH : résultat identique, commission 12 %, commandes annulées exclues du CA. Nettoie ses données ensuite | `postgres-dwh` (ignoré s'il est injoignable) |

---

## 10. Structure du dépôt

```
.
├── docker-compose.yml            # toute la stack
├── .env.example                  # modèle de configuration (copier en .env, jamais commité)
├── api/                          # API Flask + Faker
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── init-db/                      # exécuté par postgres-dwh au 1er démarrage
│   ├── 00_schemas.sql
│   └── schema.sql                # tables staging / dwh / analytics
├── plugins/
│   ├── hooks/marketplace_api_hook.py
│   ├── marketplace_config.py     # identifiants de connexion, buckets, Asset, taux de commission
│   └── marketplace_io.py         # écriture bronze, chargement staging
├── dags/
│   ├── marketplace_orders_ingest_daily.py
│   ├── marketplace_dims_refresh_daily.py
│   ├── marketplace_analytics_aggregate_daily.py
│   └── sql/                      # SQL templaté Jinja ({{ ds }}, {{ params.* }})
├── tests/
├── scripts/
│   ├── setup_metabase.py         # dashboards as code
│   └── legacy/                   # anciens fichiers du TD MinIO, conservés pour référence
├── config/                       # airflow.cfg généré par airflow-init (non commité)
└── docs/
    └── PLAN.md                   # plan de travail détaillé, étape par étape
```

---

## 11. Choix techniques

| Choix | Alternative écartée | Justification |
|---|---|---|
| ELT : transformations en SQL dans PostgreSQL | ETL en pandas | Simple, traçable, largement suffisant à ce volume |
| JSON brut archivé dans MinIO, staging rechargé **depuis MinIO** | Charger l'API directement dans Postgres | Transformation rejouable sans rappeler la source ; audit |
| DELETE + INSERT par partition `dt` | MERGE / UPSERT | Imposé ; plus simple, gère aussi les suppressions, état prévisible |
| Membres « Inconnu » (`ensure_dimensions`) | Faire échouer le run sur la FK | Les DAGs faits / dimensions restent indépendants, aucune perte de commande |
| Agrégats `analytics` reconstruits entièrement | Mise à jour par partition | Un run déclenché par un Asset n'a pas de date logique ; reconstruire quelques milliers de lignes est instantané et toujours cohérent |
| Asset scheduling (Airflow 3) | Horaire fixe / `ExternalTaskSensor` | L'agrégation démarre dès que les faits sont prêts, sans worker bloqué |
| `dim_date` préremplie | Dimension date alimentée par le pipeline | Statique, FK toujours satisfaite |
| Connections en variables d'environnement | Saisie dans l'UI | Reproductible, aucun secret dans le code ni dans Git |
| SQL dans des fichiers `.sql` séparés | SQL inline dans le Python | Lisible, testable, rendu Jinja visible dans l'UI Airflow |
| Dashboards créés par script (API Metabase) | Clics manuels | Reproductibles après un `down -v` ou sur une autre machine |
| MinIO via l'image Chainguard, épinglée par digest | `minio/minio` | `minio/minio` n'est plus publiée sur Docker Hub |
| `postgres-airflow` et `redis` sans port exposé | Ports 5432 / 6379 sur l'hôte | Évite les conflits avec un Postgres / Redis local ; seuls les conteneurs y accèdent |
| `AIRFLOW__API__WORKERS=1`, `WORKER_CONCURRENCY=4` | Valeurs par défaut | Réduit la RAM sur les machines de formation |
| Pas de Spark ni dbt | — | Surdimensionné pour un MVP de 2 jours |

---

## 12. Limites et pistes d'amélioration

- **Bonus non réalisés :** `DataQualityOperator` + branching, détection d'anomalies (CA < 70 % de la moyenne 7 j), Prometheus / Grafana, 2 dashboards supplémentaires (commissions, catégories — les tables sont prêtes).
- `staging.*` grossit indéfiniment : prévoir une purge des partitions de plus de 30 jours.
- Les buckets `silver` / `gold` sont créés mais vides : exporter les tables nettoyées et les KPIs en Parquet.
- Metabase stocke sa configuration dans H2 : passer sur PostgreSQL pour la production.
- `_PIP_ADDITIONAL_REQUIREMENTS` installe `pytest` à chaque démarrage : à remplacer par une image Airflow personnalisée.
- Avec plus de temps : **dbt** pour les transformations (tests, lignage, documentation), **Great Expectations / Soda** pour la qualité, partitionnement natif PostgreSQL de `fact_orders`, CI (ruff + pytest).

---

## 13. Accès, commandes utiles et dépannage

### Accès (environnement de formation uniquement — valeurs définies dans `.env`)

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | `admin@maelys.local` / `Admin2026!` |
| MinIO console | http://localhost:9001 | `minio_admin` / `minio_password_2026` |
| API Marketplace | http://localhost:5000 | Bearer `formation-token-2026` |
| PostgreSQL DWH | `localhost:5433`, base `dwh` | `dwh_user` / `dwh_password_2026` |

### Commandes utiles

```bash
docker compose ps                                              # état des services
docker compose logs -f airflow-scheduler --tail=50             # logs
docker compose exec airflow-worker airflow dags list-import-errors
docker compose exec airflow-worker airflow tasks test marketplace_orders_ingest_daily transform_fact_orders 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh       # SQL sur le DWH
docker compose down                                            # arrêt (données conservées)
docker compose down -v                                         # ⚠️ reset complet : supprime toutes les données
```

### Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| Un service Airflow redémarre en boucle | RAM insuffisante | Libérer de la mémoire ; retirer `bi` de `COMPOSE_PROFILES` pour se passer de Metabase |
| `Permission denied` dans `logs/` | `AIRFLOW_UID` incorrect | Mettre le résultat de `id -u` dans `.env`, puis `docker compose up -d --force-recreate` |
| Modification de `.env` / du compose non prise en compte | Conteneurs non recréés | `docker compose up -d --force-recreate` |
| `401 Unauthorized` dans les logs d'extraction | Token API différent entre l'API et la Connection | Vérifier `MARKETPLACE_API_TOKEN` dans `.env` |
| Table absente après modification de `schema.sql` | `init-db/` n'est exécuté qu'au 1er démarrage | `docker compose exec -T postgres-dwh psql -U dwh_user -d dwh < init-db/schema.sql` |
| Metabase « Cannot connect » | Mauvais host / port | Host `postgres-dwh`, port **5432** (interne) — déjà fait par le script |
| Dashboards vides | Agrégats non construits | Vérifier que `marketplace_analytics_aggregate_daily` est activé et en success |

---

Plan de travail détaillé (étapes, répartition, planning) : [docs/PLAN.md](docs/PLAN.md).
