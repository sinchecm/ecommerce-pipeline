"""
=============================================================================
Data Ingestion Script - TheLook eCommerce Dataset Simulation
=============================================================================
Source: BigQuery Public Dataset - bigquery-public-data.thelook_ecommerce
Method: Synthetic data generation replicating the schema and statistical
        distributions of the original BigQuery dataset.

Tables ingested:
  - users (customers)
  - products
  - orders
  - order_items
  - events (web events)
  - inventory_items
  - distribution_centers

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import os
import random
from faker import Faker
from datetime import datetime, timedelta
import logging

# ─── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/user/ecommerce-pipeline/logs/ingestion.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Ensure logs dir exists
os.makedirs("/home/user/ecommerce-pipeline/logs", exist_ok=True)

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# ─── Constants ───────────────────────────────────────────────────────────────
RAW_DATA_DIR   = "/home/user/ecommerce-pipeline/data/raw"
DB_PATH        = "/home/user/ecommerce-pipeline/data/warehouse/ecommerce.duckdb"
N_USERS        = 10_000
N_PRODUCTS     = 2_000
N_ORDERS       = 35_000
N_EVENTS       = 80_000
START_DATE     = datetime(2020, 1, 1)
END_DATE       = datetime(2024, 12, 31)

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ─── Reference Data ──────────────────────────────────────────────────────────
PRODUCT_CATEGORIES = {
    "Intimates":         ["Bras & Bralettes", "Underwear", "Lingerie"],
    "Swim":              ["Swimwear", "Cover Ups"],
    "Pants & Capris":    ["Jeans", "Chinos", "Leggings", "Shorts"],
    "Shirts & Tops":     ["T-Shirts", "Casual Shirts", "Polo Shirts"],
    "Accessories":       ["Bags", "Belts", "Hats", "Sunglasses", "Jewelry"],
    "Dresses":           ["Mini Dresses", "Maxi Dresses", "Formal Dresses"],
    "Outerwear & Coats": ["Jackets", "Coats", "Vests"],
    "Socks":             ["Crew Socks", "Ankle Socks"],
    "Footwear":          ["Sneakers", "Boots", "Sandals", "Flats"],
    "Tops & Tees":       ["Blouses", "Tank Tops", "Crop Tops"],
    "Suits":             ["Business Suits", "Blazers"],
    "Active":            ["Sports Bras", "Athletic Shorts", "Yoga Pants"],
    "Jeans":             ["Skinny Jeans", "Straight Jeans", "Bootcut Jeans"],
    "Skirts":            ["Mini Skirts", "Midi Skirts", "A-Line Skirts"],
    "Blazers & Jackets": ["Blazers", "Denim Jackets", "Leather Jackets"],
    "Shorts":            ["Bermuda Shorts", "Athletic Shorts"],
    "Leggings":          ["Workout Leggings", "Casual Leggings"],
    "Fashion Hoodies & Sweatshirts": ["Hoodies", "Crewneck Sweatshirts"],
    "Sleep & Lounge":    ["Pajamas", "Robes"],
    "Underwear":         ["Boxers", "Briefs"],
}

BRANDS = [
    "Nike", "Adidas", "Zara", "H&M", "Levi's", "Gap", "Forever 21",
    "Calvin Klein", "Tommy Hilfiger", "Ralph Lauren", "Gucci", "Prada",
    "Versace", "Burberry", "Under Armour", "Puma", "Reebok", "New Balance",
    "Columbia", "Patagonia", "North Face", "Wrangler", "Banana Republic",
    "J.Crew", "American Eagle", "Hollister", "Abercrombie", "UNIQLO",
    "Anthropologie", "Free People"
]

DEPARTMENTS = ["Women", "Men", "Kids"]

TRAFFIC_SOURCES = ["Search", "Organic", "Facebook", "Email", "Display"]
EVENT_TYPES     = ["home", "department", "product", "cart", "purchase", "cancel"]

COUNTRIES_CITIES = {
    "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Germany":        ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France":         ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "China":          ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu"],
    "Australia":      ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Brazil":         ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador"],
    "Japan":          ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"],
    "India":          ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"],
    "Canada":         ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
}

DISTRIBUTION_CENTERS = [
    {"id": 1,  "name": "Memphis TN",         "latitude": 35.1175,  "longitude": -89.9711},
    {"id": 2,  "name": "Chicago IL",          "latitude": 41.8781,  "longitude": -87.6298},
    {"id": 3,  "name": "Houston TX",          "latitude": 29.7604,  "longitude": -95.3698},
    {"id": 4,  "name": "Los Angeles CA",      "latitude": 34.0522,  "longitude": -118.2437},
    {"id": 5,  "name": "New Orleans LA",      "latitude": 29.9511,  "longitude": -90.0715},
    {"id": 6,  "name": "Port Authority of NY/NJ", "latitude": 40.6895, "longitude": -74.1745},
    {"id": 7,  "name": "Savannah GA",         "latitude": 32.0835,  "longitude": -81.0998},
    {"id": 8,  "name": "Philadelphia PA",     "latitude": 39.9526,  "longitude": -75.1652},
    {"id": 9,  "name": "Mobile AL",           "latitude": 30.6954,  "longitude": -88.0399},
    {"id": 10, "name": "Charleston SC",       "latitude": 32.7765,  "longitude": -79.9311},
]

ORDER_STATUSES   = ["Complete", "Returned", "Cancelled", "Processing", "Shipped"]
STATUS_WEIGHTS   = [0.62, 0.12, 0.07, 0.09, 0.10]

# ═══════════════════════════════════════════════════════════════════════════════
# GENERATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def generate_distribution_centers() -> pd.DataFrame:
    log.info("Generating distribution_centers …")
    return pd.DataFrame(DISTRIBUTION_CENTERS)

def generate_users(n: int) -> pd.DataFrame:
    log.info(f"Generating {n} users …")
    records = []
    age_choices = list(range(13, 75))
    age_weights = np.exp(-0.02 * np.array([abs(a - 30) for a in age_choices]))
    age_weights /= age_weights.sum()

    for uid in range(1, n + 1):
        country = random.choice(list(COUNTRIES_CITIES.keys()))
        city    = random.choice(COUNTRIES_CITIES[country])
        gender  = random.choice(["M", "F"])
        age     = int(np.random.choice(age_choices, p=age_weights))
        created = rand_date(START_DATE, END_DATE - timedelta(days=90))

        if gender == "F":
            first = fake.first_name_female()
        else:
            first = fake.first_name_male()

        records.append({
            "id":               uid,
            "first_name":       first,
            "last_name":        fake.last_name(),
            "email":            fake.unique.email(),
            "age":              age,
            "gender":           gender,
            "country":          country,
            "city":             city,
            "state":            fake.state_abbr(),
            "postal_code":      fake.postcode(),
            "street_address":   fake.street_address(),
            "latitude":         round(fake.latitude(), 4),
            "longitude":        round(fake.longitude(), 4),
            "traffic_source":   random.choices(TRAFFIC_SOURCES, weights=[0.35,0.25,0.20,0.12,0.08])[0],
            "created_at":       created.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(records)

def generate_products(n: int) -> pd.DataFrame:
    log.info(f"Generating {n} products …")
    records = []
    cats = list(PRODUCT_CATEGORIES.keys())

    for pid in range(1, n + 1):
        category    = random.choice(cats)
        sub_cat     = random.choice(PRODUCT_CATEGORIES[category])
        brand       = random.choice(BRANDS)
        department  = random.choice(DEPARTMENTS)
        cost        = round(random.uniform(5, 250), 2)
        retail_mult = random.uniform(1.8, 3.2)
        retail      = round(cost * retail_mult, 2)
        dc          = random.choice(DISTRIBUTION_CENTERS)

        records.append({
            "id":                    pid,
            "cost":                  cost,
            "category":              category,
            "name":                  f"{brand} {sub_cat} #{pid}",
            "brand":                 brand,
            "retail_price":          retail,
            "department":            department,
            "sku":                   f"SKU-{pid:06d}",
            "distribution_center_id": dc["id"],
        })
    return pd.DataFrame(records)

def generate_inventory_items(products_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Generating inventory_items …")
    records = []
    iid = 1
    for _, prod in products_df.iterrows():
        qty = random.randint(5, 200)
        for _ in range(qty):
            created = rand_date(START_DATE, END_DATE)
            sold_at = None
            if random.random() < 0.65:
                sold_at = (created + timedelta(days=random.randint(1, 400))).strftime("%Y-%m-%d %H:%M:%S")
            records.append({
                "id":                    iid,
                "product_id":            prod["id"],
                "created_at":            created.strftime("%Y-%m-%d %H:%M:%S"),
                "sold_at":               sold_at,
                "cost":                  prod["cost"],
                "product_category":      prod["category"],
                "product_name":          prod["name"],
                "product_brand":         prod["brand"],
                "product_retail_price":  prod["retail_price"],
                "product_department":    prod["department"],
                "product_sku":           prod["sku"],
                "product_distribution_center_id": prod["distribution_center_id"],
            })
            iid += 1
    return pd.DataFrame(records)

def generate_orders_and_items(users_df: pd.DataFrame, products_df: pd.DataFrame,
                               n_orders: int):
    log.info(f"Generating {n_orders} orders + order_items …")
    orders      = []
    order_items = []
    oi_id       = 1

    # Weight users by account age (older accounts order more)
    user_ages    = pd.to_datetime(users_df["created_at"])
    days_active  = (datetime(2024, 12, 31) - user_ages).dt.days.clip(lower=1)
    user_weights = days_active.values.astype(float)
    user_weights /= user_weights.sum()

    for oid in range(1, n_orders + 1):
        user_row  = users_df.iloc[np.random.choice(len(users_df), p=user_weights)]
        user_created = datetime.strptime(user_row["created_at"], "%Y-%m-%d %H:%M:%S")
        order_date = rand_date(user_created + timedelta(days=1), END_DATE)

        status        = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        num_items     = random.choices([1,2,3,4,5,6], weights=[0.40,0.28,0.16,0.09,0.05,0.02])[0]
        shipped_at    = None
        delivered_at  = None
        returned_at   = None

        if status in ("Complete", "Shipped"):
            shipped_at   = order_date + timedelta(days=random.randint(1, 3))
            delivered_at = shipped_at + timedelta(days=random.randint(2, 7))
        if status == "Returned":
            shipped_at   = order_date + timedelta(days=random.randint(1, 3))
            delivered_at = shipped_at + timedelta(days=random.randint(2, 7))
            returned_at  = delivered_at + timedelta(days=random.randint(1, 30))

        # Gender-biased product selection
        gender = user_row["gender"]
        pool   = products_df[products_df["department"].isin(
            ["Women"] if gender == "F" else ["Men"]
        ) | (products_df["department"] == "Kids")]
        if len(pool) < num_items:
            pool = products_df

        chosen_products = pool.sample(n=min(num_items, len(pool)), replace=False)
        order_total     = 0.0

        for _, prod in chosen_products.iterrows():
            sale_price = round(prod["retail_price"] * random.uniform(0.7, 1.0), 2)
            order_total += sale_price
            order_items.append({
                "id":              oi_id,
                "order_id":        oid,
                "user_id":         user_row["id"],
                "product_id":      prod["id"],
                "inventory_item_id": random.randint(1, 50000),
                "status":          status,
                "created_at":      order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "shipped_at":      shipped_at.strftime("%Y-%m-%d %H:%M:%S") if shipped_at else None,
                "delivered_at":    delivered_at.strftime("%Y-%m-%d %H:%M:%S") if delivered_at else None,
                "returned_at":     returned_at.strftime("%Y-%m-%d %H:%M:%S") if returned_at else None,
                "sale_price":      sale_price,
            })
            oi_id += 1

        orders.append({
            "order_id":        oid,
            "user_id":         user_row["id"],
            "status":          status,
            "gender":          gender,
            "created_at":      order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "returned_at":     returned_at.strftime("%Y-%m-%d %H:%M:%S") if returned_at else None,
            "shipped_at":      shipped_at.strftime("%Y-%m-%d %H:%M:%S") if shipped_at else None,
            "delivered_at":    delivered_at.strftime("%Y-%m-%d %H:%M:%S") if delivered_at else None,
            "num_of_item":     len(chosen_products),
            "order_total":     round(order_total, 2),
        })

    return pd.DataFrame(orders), pd.DataFrame(order_items)

def generate_events(users_df: pd.DataFrame, n_events: int) -> pd.DataFrame:
    log.info(f"Generating {n_events} web events …")
    records   = []
    browsers  = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    oses      = ["Windows", "macOS", "iOS", "Android", "Linux"]
    uris      = ["/home", "/category/tops", "/product/123", "/cart", "/checkout",
                 "/order/confirm", "/account", "/search", "/sale", "/new-arrivals"]

    for eid in range(1, n_events + 1):
        user_row    = users_df.iloc[random.randint(0, len(users_df) - 1)]
        event_type  = random.choices(
            EVENT_TYPES, weights=[0.25, 0.20, 0.25, 0.15, 0.10, 0.05]
        )[0]
        event_time  = rand_date(START_DATE, END_DATE)
        session_id  = f"sess_{random.randint(100000, 999999)}"

        records.append({
            "id":               eid,
            "user_id":          user_row["id"],
            "sequence_number":  random.randint(1, 20),
            "session_id":       session_id,
            "created_at":       event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "ip_address":       fake.ipv4(),
            "city":             user_row["city"],
            "state":            user_row["state"],
            "postal_code":      user_row["postal_code"],
            "browser":          random.choice(browsers),
            "traffic_source":   user_row["traffic_source"],
            "uri":              random.choice(uris),
            "event_type":       event_type,
        })
    return pd.DataFrame(records)

# ═══════════════════════════════════════════════════════════════════════════════
# SAVE + LOAD TO DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def save_raw_csv(df: pd.DataFrame, name: str):
    path = f"{RAW_DATA_DIR}/{name}.csv"
    df.to_csv(path, index=False)
    log.info(f"  Saved {name}.csv  → {len(df):,} rows  ({os.path.getsize(path)/1024:.1f} KB)")

def load_into_duckdb(dfs: dict):
    """Load all raw DataFrames into DuckDB as raw_* tables."""
    import duckdb
    log.info(f"Loading raw tables into DuckDB → {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for name, df in dfs.items():
        tbl = f"raw.{name}"
        con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.execute(f"CREATE TABLE {tbl} AS SELECT * FROM df")
        count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info(f"  Loaded {tbl}: {count:,} rows")
    con.close()
    log.info("DuckDB load complete.")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("TheLook eCommerce — Data Ingestion Pipeline")
    log.info("=" * 60)

    # 1. Generate
    dist_centers_df   = generate_distribution_centers()
    users_df          = generate_users(N_USERS)
    products_df       = generate_products(N_PRODUCTS)
    inventory_df      = generate_inventory_items(products_df)
    orders_df, items_df = generate_orders_and_items(users_df, products_df, N_ORDERS)
    events_df         = generate_events(users_df, N_EVENTS)

    all_dfs = {
        "distribution_centers": dist_centers_df,
        "users":                users_df,
        "products":             products_df,
        "inventory_items":      inventory_df,
        "orders":               orders_df,
        "order_items":          items_df,
        "events":               events_df,
    }

    # 2. Save raw CSVs
    log.info("\n── Saving raw CSVs ──")
    for name, df in all_dfs.items():
        save_raw_csv(df, name)

    # 3. Load into DuckDB
    log.info("\n── Loading into DuckDB ──")
    load_into_duckdb(all_dfs)

    # 4. Summary
    log.info("\n── Ingestion Summary ──")
    for name, df in all_dfs.items():
        log.info(f"  {name:30s}: {len(df):>8,} rows")

    log.info("\n✅ Ingestion complete!")

if __name__ == "__main__":
    main()
