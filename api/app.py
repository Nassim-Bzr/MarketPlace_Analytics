"""API Marketplace simulée — Maelys Marketplace.

Données fausses générées avec Faker, mais DÉTERMINISTES :
- les référentiels (vendeurs, produits, clients) sont générés une seule fois
  au démarrage avec une seed fixe : ils sont identiques à chaque lancement ;
- les commandes d'une date sont générées avec seed = md5(date) : une même
  date renvoie toujours exactement les mêmes commandes (idempotence testable).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import random
from datetime import date, datetime, timedelta
from functools import wraps

from faker import Faker
from flask import Flask, jsonify, request

API_VERSION = "1.0.0"
API_TOKEN = os.environ.get("API_TOKEN", "")
ORDERS_PER_DAY = int(os.environ.get("ORDERS_PER_DAY", "1000"))

REFERENTIAL_SEED = 42
N_SELLERS = int(os.environ.get("N_SELLERS", "200"))
N_PRODUCTS = int(os.environ.get("N_PRODUCTS", "2000"))
N_CUSTOMERS = int(os.environ.get("N_CUSTOMERS", "5000"))

# Catégorie -> (prix min, prix max, noms de produits)
CATEGORIES: dict[str, tuple[float, float, list[str]]] = {
    "Mode": (15, 120, ["T-shirt", "Robe", "Veste", "Pull", "Jean", "Écharpe"]),
    "Maison & Déco": (10, 250, ["Vase", "Coussin", "Lampe", "Bougie", "Miroir", "Plaid"]),
    "Bijoux": (20, 300, ["Bracelet", "Collier", "Bague", "Boucles d'oreilles", "Pendentif"]),
    "Beauté": (8, 60, ["Savon", "Crème", "Huile", "Parfum", "Baume", "Sérum"]),
    "High-Tech reconditionné": (50, 800, ["Smartphone", "Tablette", "Casque", "Montre connectée", "Enceinte"]),
    "Art & Artisanat": (25, 400, ["Tableau", "Sculpture", "Poterie", "Affiche", "Céramique"]),
    "Sport & Loisirs": (15, 200, ["Tapis de yoga", "Gourde", "Sac de sport", "Raquette", "Haltères"]),
    "Épicerie fine": (5, 50, ["Miel", "Confiture", "Thé", "Café", "Chocolat", "Huile d'olive"]),
    "Enfants": (10, 90, ["Peluche", "Puzzle", "Jeu en bois", "Doudou", "Livre illustré"]),
    "Livres & Papeterie": (5, 40, ["Carnet", "Agenda", "Roman", "Stylo", "Carte postale"]),
}
ADJECTIVES = ["artisanal", "bio", "vintage", "fait main", "premium", "recyclé", "édition limitée", "classique"]

COUNTRIES = ["France"] * 14 + ["Belgique", "Belgique", "Suisse", "Espagne", "Italie", "Allemagne"]

# Statut -> poids (les commandes cancelled / refunded sont exclues du CA côté DWH)
ORDER_STATUSES = {"delivered": 70, "shipped": 10, "paid": 8, "pending": 3, "cancelled": 6, "refunded": 3}
QUANTITIES = {1: 70, 2: 20, 3: 7, 4: 2, 5: 1}
# Lundi = 0 ... dimanche = 6 : plus de ventes le week-end
WEEKDAY_FACTOR = [0.90, 0.95, 0.95, 1.00, 1.05, 1.30, 1.25]


def _build_referentials() -> tuple[list[dict], list[dict], list[dict]]:
    fake = Faker("fr_FR")
    fake.seed_instance(REFERENTIAL_SEED)
    rnd = random.Random(REFERENTIAL_SEED)
    category_names = list(CATEGORIES)

    sellers = []
    for i in range(1, N_SELLERS + 1):
        joined = fake.date_between(start_date=date(2021, 1, 1), end_date=date(2025, 12, 31))
        # ~8 % des vendeurs arrêtent de vendre en 2026 (pour le suivi des vendeurs inactifs)
        active_until = None
        if rnd.random() < 0.08:
            active_until = fake.date_between(start_date=date(2026, 1, 1), end_date=date(2026, 10, 1))
        sellers.append({
            "seller_id": f"SEL-{i:04d}",
            "name": fake.company(),
            "country": rnd.choice(COUNTRIES),
            "city": fake.city(),
            "main_category": rnd.choice(category_names),
            "joined_date": joined.isoformat(),
            "active_until": active_until.isoformat() if active_until else None,
            # Loi de Pareto : quelques gros vendeurs font l'essentiel du CA
            "_weight": rnd.paretovariate(1.16),
        })

    products = []
    for i in range(1, N_PRODUCTS + 1):
        seller = rnd.choice(sellers)
        # 80 % des produits d'un vendeur sont dans sa catégorie principale
        category = seller["main_category"] if rnd.random() < 0.8 else rnd.choice(category_names)
        low, high, names = CATEGORIES[category]
        products.append({
            "product_id": f"PRD-{i:05d}",
            "name": f"{rnd.choice(names)} {rnd.choice(ADJECTIVES)}",
            "category": category,
            "base_price": round(rnd.uniform(low, high), 2),
            "seller_id": seller["seller_id"],
        })

    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        customers.append({
            "customer_id": f"CUS-{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "city": fake.city(),
            "signup_date": fake.date_between(start_date=date(2020, 1, 1), end_date=date(2025, 12, 31)).isoformat(),
        })

    return sellers, products, customers


SELLERS, PRODUCTS, CUSTOMERS = _build_referentials()
PRODUCTS_BY_SELLER: dict[str, list[dict]] = {}
for _product in PRODUCTS:
    PRODUCTS_BY_SELLER.setdefault(_product["seller_id"], []).append(_product)


def _public(rows: list[dict]) -> list[dict]:
    """Retire les champs techniques internes (préfixés par _)."""
    return [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]


def _weighted_choice(rnd: random.Random, weights: dict):
    return rnd.choices(list(weights), weights=list(weights.values()), k=1)[0]


def generate_orders(day: date) -> list[dict]:
    seed = int(hashlib.md5(day.isoformat().encode()).hexdigest(), 16) % 2**32
    rnd = random.Random(seed)

    active_sellers = [
        s for s in SELLERS
        if s["joined_date"] <= day.isoformat()
        and (s["active_until"] is None or day.isoformat() <= s["active_until"])
        and s["seller_id"] in PRODUCTS_BY_SELLER
    ]
    if not active_sellers:
        return []
    seller_weights = [s["_weight"] for s in active_sellers]

    n_orders = round(ORDERS_PER_DAY * WEEKDAY_FACTOR[day.weekday()] * rnd.uniform(0.85, 1.15))
    day_start = datetime.combine(day, datetime.min.time())

    orders = []
    for i in range(1, n_orders + 1):
        seller = rnd.choices(active_sellers, weights=seller_weights, k=1)[0]
        product = rnd.choice(PRODUCTS_BY_SELLER[seller["seller_id"]])
        customer = rnd.choice(CUSTOMERS)
        quantity = _weighted_choice(rnd, QUANTITIES)
        # Petites variations de prix (promotions, frais) autour du prix de base
        unit_price = round(product["base_price"] * rnd.uniform(0.90, 1.10), 2)
        orders.append({
            "order_id": f"ORD-{day:%Y%m%d}-{i:06d}",
            "order_ts": (day_start + timedelta(seconds=rnd.randint(0, 86_399))).isoformat(),
            "customer_id": customer["customer_id"],
            "seller_id": seller["seller_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(unit_price * quantity, 2),
            "status": _weighted_choice(rnd, ORDER_STATUSES),
        })
    return orders


app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False


def require_token(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        if not API_TOKEN or not hmac.compare_digest(token, API_TOKEN):
            return jsonify(error="Unauthorized", message="Header 'Authorization: Bearer <token>' manquant ou invalide"), 401
        return view(*args, **kwargs)
    return wrapper


def _limit_arg() -> int | None:
    limit = request.args.get("limit")
    if limit is None:
        return None
    if not limit.isdigit():
        raise ValueError("limit doit être un entier positif")
    return int(limit)


def _list_response(rows: list[dict]):
    try:
        limit = _limit_arg()
    except ValueError as exc:
        return jsonify(error="Bad Request", message=str(exc)), 400
    data = _public(rows[:limit] if limit is not None else rows)
    return jsonify(count=len(data), data=data)


@app.get("/health")
def health():
    return jsonify(status="ok", version=API_VERSION)


@app.get("/orders")
@require_token
def orders():
    raw_date = request.args.get("date")
    if not raw_date:
        return jsonify(error="Bad Request", message="Paramètre 'date' obligatoire (YYYY-MM-DD)"), 400
    try:
        day = date.fromisoformat(raw_date)
    except ValueError:
        return jsonify(error="Bad Request", message=f"Date invalide : {raw_date!r} (attendu YYYY-MM-DD)"), 400
    # Pas de commandes dans le futur
    data = [] if day > date.today() else generate_orders(day)
    return jsonify(date=day.isoformat(), count=len(data), data=data)


@app.get("/sellers")
@require_token
def sellers():
    return _list_response(SELLERS)


@app.get("/products")
@require_token
def products():
    return _list_response(PRODUCTS)


@app.get("/customers")
@require_token
def customers():
    return _list_response(CUSTOMERS)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
