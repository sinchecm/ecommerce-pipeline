"""
=============================================================================
ELT Pipeline — Star Schema Transformation
=============================================================================
Transforms raw ingested data → Staging → Warehouse (Star Schema)

Star Schema Design:
  Fact Tables:
    - fact.FactSales         — grain: one row per order item
    - fact.FactEvents        — grain: one row per web event

  Dimension Tables:
    - dim.DimCustomer        — SCD Type 1 (latest state)
    - dim.DimProduct         — product attributes + pricing
    - dim.DimDate            — calendar spine 2019-2025
    - dim.DimDistributionCenter

  Derived / Mart Tables:
    - mart.CustomerMetrics   — CLV, purchase frequency, RFM scores
    - mart.ProductPerformance — revenue, return rate, margin

Why Star Schema?
  ✓ Denormalized → fewer JOINs → faster BI query performance
  ✓ Intuitive for business users (Tableau, Looker, Power BI)
  ✓ Aggregation-friendly grain design
  ✓ Supports additive facts (SUM, COUNT) across all dimensions

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import duckdb
import logging
import os
from datetime import datetime

# ─── Logging ─────────────────────────────────────────────────────────────────
os.makedirs("/home/user/ecommerce-pipeline/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/user/ecommerce-pipeline/logs/elt.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH = "/home/user/ecommerce-pipeline/data/warehouse/ecommerce.duckdb"


def run_sql(con, label: str, sql: str):
    log.info(f"  → {label}")
    con.execute(sql)


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1: STAGING — Clean & Validate Raw Data
# ═══════════════════════════════════════════════════════════════════════════════

STAGING_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;

-- ── stg_users ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging.stg_users;
CREATE TABLE staging.stg_users AS
SELECT
    id                                            AS user_id,
    TRIM(first_name)                              AS first_name,
    TRIM(last_name)                               AS last_name,
    LOWER(TRIM(email))                            AS email,
    CASE
        WHEN age < 0   THEN NULL
        WHEN age > 100 THEN NULL
        ELSE age
    END                                           AS age,
    CASE gender
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'Unknown'
    END                                           AS gender,
    COALESCE(TRIM(country), 'Unknown')            AS country,
    COALESCE(TRIM(city),    'Unknown')            AS city,
    COALESCE(TRIM(state),   'Unknown')            AS state,
    COALESCE(postal_code,   'Unknown')            AS postal_code,
    street_address,
    latitude,
    longitude,
    COALESCE(traffic_source, 'Unknown')           AS traffic_source,
    CAST(created_at AS TIMESTAMP)                 AS created_at,
    -- derived
    CASE
        WHEN age BETWEEN 13 AND 17 THEN 'Teen (13-17)'
        WHEN age BETWEEN 18 AND 24 THEN 'Young Adult (18-24)'
        WHEN age BETWEEN 25 AND 34 THEN 'Adult (25-34)'
        WHEN age BETWEEN 35 AND 44 THEN 'Mid Adult (35-44)'
        WHEN age BETWEEN 45 AND 54 THEN 'Mature Adult (45-54)'
        WHEN age BETWEEN 55 AND 64 THEN 'Senior Adult (55-64)'
        WHEN age >= 65              THEN 'Senior (65+)'
        ELSE 'Unknown'
    END                                           AS age_group
FROM raw.users
WHERE id IS NOT NULL;

-- ── stg_products ───────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging.stg_products;
CREATE TABLE staging.stg_products AS
SELECT
    id                                            AS product_id,
    TRIM(name)                                    AS product_name,
    TRIM(brand)                                   AS brand,
    TRIM(category)                                AS category,
    TRIM(department)                              AS department,
    sku,
    CASE
        WHEN cost <= 0       THEN NULL
        WHEN cost > 10000    THEN NULL
        ELSE ROUND(cost, 2)
    END                                           AS cost,
    CASE
        WHEN retail_price <= 0    THEN NULL
        WHEN retail_price > 10000 THEN NULL
        ELSE ROUND(retail_price, 2)
    END                                           AS retail_price,
    distribution_center_id,
    -- derived
    ROUND(retail_price - cost, 2)                 AS gross_margin,
    ROUND((retail_price - cost) / NULLIF(retail_price, 0) * 100, 2) AS margin_pct
FROM raw.products
WHERE id IS NOT NULL
  AND cost IS NOT NULL
  AND retail_price IS NOT NULL
  AND retail_price >= cost;

-- ── stg_orders ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging.stg_orders;
CREATE TABLE staging.stg_orders AS
SELECT
    order_id,
    user_id,
    TRIM(status)                                  AS order_status,
    CAST(created_at AS TIMESTAMP)                 AS order_created_at,
    CAST(shipped_at AS TIMESTAMP)                 AS shipped_at,
    CAST(delivered_at AS TIMESTAMP)               AS delivered_at,
    CAST(returned_at AS TIMESTAMP)                AS returned_at,
    num_of_item,
    ROUND(order_total, 2)                         AS order_total,
    -- derived
    CASE
        WHEN status = 'Complete'   THEN 1 ELSE 0
    END                                           AS is_complete,
    CASE
        WHEN status = 'Returned'   THEN 1 ELSE 0
    END                                           AS is_returned,
    CASE
        WHEN status = 'Cancelled'  THEN 1 ELSE 0
    END                                           AS is_cancelled,
    DATEDIFF('day',
        CAST(created_at AS TIMESTAMP),
        CAST(COALESCE(delivered_at, created_at) AS TIMESTAMP)
    )                                             AS fulfillment_days
FROM raw.orders
WHERE order_id IS NOT NULL
  AND user_id  IS NOT NULL
  AND order_total > 0;

-- ── stg_order_items ────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging.stg_order_items;
CREATE TABLE staging.stg_order_items AS
SELECT
    oi.id                                         AS order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    TRIM(oi.status)                               AS item_status,
    CAST(oi.created_at AS TIMESTAMP)              AS item_created_at,
    CAST(oi.shipped_at AS TIMESTAMP)              AS item_shipped_at,
    CAST(oi.delivered_at AS TIMESTAMP)            AS item_delivered_at,
    CAST(oi.returned_at AS TIMESTAMP)             AS item_returned_at,
    ROUND(oi.sale_price, 2)                       AS sale_price,
    p.cost                                        AS product_cost,
    p.retail_price,
    -- derived
    ROUND(oi.sale_price - p.cost, 2)              AS item_profit,
    ROUND((oi.sale_price - p.cost)
          / NULLIF(oi.sale_price, 0) * 100, 2)    AS item_margin_pct,
    ROUND(p.retail_price - oi.sale_price, 2)      AS discount_amount,
    ROUND((p.retail_price - oi.sale_price)
          / NULLIF(p.retail_price, 0) * 100, 2)   AS discount_pct
FROM raw.order_items oi
LEFT JOIN staging.stg_products p
       ON oi.product_id = p.product_id
WHERE oi.id         IS NOT NULL
  AND oi.order_id   IS NOT NULL
  AND oi.product_id IS NOT NULL
  AND oi.sale_price > 0;

-- ── stg_events ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging.stg_events;
CREATE TABLE staging.stg_events AS
SELECT
    id                                            AS event_id,
    user_id,
    session_id,
    sequence_number,
    CAST(created_at AS TIMESTAMP)                 AS event_time,
    TRIM(event_type)                              AS event_type,
    TRIM(uri)                                     AS uri,
    TRIM(browser)                                 AS browser,
    traffic_source,
    city,
    state
FROM raw.events
WHERE id IS NOT NULL;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2: WAREHOUSE — Star Schema Dimensions
# ═══════════════════════════════════════════════════════════════════════════════

DIM_DDL = """
CREATE SCHEMA IF NOT EXISTS dim;

-- ══════════════════════════════════════════════════════
-- DimDate  — Full calendar spine 2019-2025
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dim.DimDate;
CREATE TABLE dim.DimDate AS
WITH date_spine AS (
    SELECT CAST('2019-01-01' AS DATE) + INTERVAL (n) DAY AS dt
    FROM (SELECT ROW_NUMBER() OVER () - 1 AS n
          FROM raw.events LIMIT 2557)   -- ~7 years
)
SELECT
    CAST(STRFTIME(dt, '%Y%m%d') AS INTEGER)          AS date_key,
    dt                                               AS full_date,
    YEAR(dt)                                         AS year,
    QUARTER(dt)                                      AS quarter,
    MONTH(dt)                                        AS month,
    STRFTIME(dt, '%B')                               AS month_name,
    STRFTIME(dt, '%b')                               AS month_short,
    DAY(dt)                                          AS day_of_month,
    DAYOFWEEK(dt)                                    AS day_of_week,
    STRFTIME(dt, '%A')                               AS day_name,
    STRFTIME(dt, '%a')                               AS day_short,
    WEEKOFYEAR(dt)                                   AS week_of_year,
    DAYOFYEAR(dt)                                    AS day_of_year,
    CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN TRUE
         ELSE FALSE END                              AS is_weekend,
    CAST(STRFTIME(dt, '%Y') || '-Q' ||
         CAST(QUARTER(dt) AS VARCHAR)  AS VARCHAR)   AS year_quarter,
    CAST(STRFTIME(dt, '%Y-%m')        AS VARCHAR)    AS year_month,
    -- Fiscal year (April start)
    CASE WHEN MONTH(dt) >= 4
         THEN YEAR(dt)
         ELSE YEAR(dt) - 1 END                       AS fiscal_year,
    CASE
        WHEN MONTH(dt) IN (4,5,6)    THEN 1
        WHEN MONTH(dt) IN (7,8,9)    THEN 2
        WHEN MONTH(dt) IN (10,11,12) THEN 3
        ELSE 4
    END                                              AS fiscal_quarter
FROM date_spine
ORDER BY dt;

-- ══════════════════════════════════════════════════════
-- DimCustomer — SCD Type 1 (latest state)
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dim.DimCustomer;
CREATE TABLE dim.DimCustomer AS
WITH customer_orders AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id)      AS lifetime_orders,
        SUM(order_total)              AS lifetime_revenue,
        MIN(order_created_at)         AS first_order_date,
        MAX(order_created_at)         AS last_order_date,
        AVG(order_total)              AS avg_order_value,
        SUM(is_returned)              AS total_returns,
        SUM(is_cancelled)             AS total_cancellations
    FROM staging.stg_orders
    GROUP BY user_id
)
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.first_name || ' ' || u.last_name  AS full_name,
    u.email,
    u.age,
    u.age_group,
    u.gender,
    u.country,
    u.city,
    u.state,
    u.postal_code,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.created_at                        AS customer_since,
    -- Order metrics (CLV components)
    COALESCE(co.lifetime_orders,    0)  AS lifetime_orders,
    COALESCE(co.lifetime_revenue, 0.0)  AS lifetime_revenue,
    COALESCE(co.avg_order_value,  0.0)  AS avg_order_value,
    co.first_order_date,
    co.last_order_date,
    COALESCE(co.total_returns,      0)  AS total_returns,
    COALESCE(co.total_cancellations,0)  AS total_cancellations,
    -- Customer segment (RFM proxy)
    CASE
        WHEN co.lifetime_revenue >= 1000 AND co.lifetime_orders >= 5  THEN 'Champions'
        WHEN co.lifetime_revenue >= 500  AND co.lifetime_orders >= 3  THEN 'Loyal Customers'
        WHEN co.lifetime_revenue >= 200  AND co.lifetime_orders >= 2  THEN 'Potential Loyalists'
        WHEN co.lifetime_orders = 1      AND co.lifetime_revenue > 0  THEN 'New Customers'
        WHEN co.last_order_date < CURRENT_DATE - INTERVAL '365' DAY   THEN 'At Risk'
        WHEN co.lifetime_orders = 0 OR co.lifetime_orders IS NULL     THEN 'Prospects'
        ELSE 'Regular'
    END                                 AS customer_segment
FROM staging.stg_users u
LEFT JOIN customer_orders co
       ON u.user_id = co.user_id;

-- ══════════════════════════════════════════════════════
-- DimProduct — Product attributes + performance
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dim.DimProduct;
CREATE TABLE dim.DimProduct AS
WITH product_perf AS (
    SELECT
        product_id,
        COUNT(*)                                    AS units_sold,
        SUM(sale_price)                             AS total_revenue,
        AVG(sale_price)                             AS avg_sale_price,
        SUM(item_profit)                            AS total_profit,
        AVG(discount_pct)                           AS avg_discount_pct,
        SUM(CASE WHEN item_status = 'Returned' THEN 1 ELSE 0 END) AS returns
    FROM staging.stg_order_items
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.department,
    p.sku,
    p.cost,
    p.retail_price,
    p.gross_margin,
    p.margin_pct,
    p.distribution_center_id,
    -- Performance metrics
    COALESCE(pp.units_sold,       0)    AS units_sold,
    COALESCE(pp.total_revenue,    0.0)  AS total_revenue,
    COALESCE(pp.avg_sale_price,   0.0)  AS avg_sale_price,
    COALESCE(pp.total_profit,     0.0)  AS total_profit,
    COALESCE(pp.avg_discount_pct, 0.0)  AS avg_discount_pct,
    COALESCE(pp.returns,          0)    AS total_returns,
    ROUND(COALESCE(pp.returns, 0) * 100.0
          / NULLIF(pp.units_sold, 0), 2) AS return_rate_pct,
    -- Price tier
    CASE
        WHEN p.retail_price < 25   THEN 'Budget (<$25)'
        WHEN p.retail_price < 75   THEN 'Mid-Range ($25-75)'
        WHEN p.retail_price < 150  THEN 'Premium ($75-150)'
        ELSE 'Luxury (>$150)'
    END                                 AS price_tier
FROM staging.stg_products p
LEFT JOIN product_perf pp
       ON p.product_id = pp.product_id;

-- ══════════════════════════════════════════════════════
-- DimDistributionCenter
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dim.DimDistributionCenter;
CREATE TABLE dim.DimDistributionCenter AS
SELECT
    id   AS dc_id,
    name AS dc_name,
    latitude,
    longitude
FROM raw.distribution_centers;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3: FACT TABLES
# ═══════════════════════════════════════════════════════════════════════════════

FACT_DDL = """
CREATE SCHEMA IF NOT EXISTS fact;

-- ══════════════════════════════════════════════════════
-- FactSales — Grain: one row per order item
-- Additive facts: sale_price, profit, discount
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS fact.FactSales;
CREATE TABLE fact.FactSales AS
SELECT
    -- Surrogate keys (for BI tools)
    ROW_NUMBER() OVER (ORDER BY oi.order_item_id)       AS fact_id,

    -- Foreign keys → Dimensions
    oi.order_item_id,
    oi.order_id,
    oi.user_id                                          AS customer_key,
    oi.product_id                                       AS product_key,
    CAST(STRFTIME(oi.item_created_at::DATE, '%Y%m%d')
         AS INTEGER)                                    AS date_key,
    p.distribution_center_id                            AS dc_key,

    -- Order attributes
    oi.item_status,
    o.order_status,
    o.num_of_item                                       AS order_items_count,

    -- Additive facts
    oi.sale_price,
    oi.product_cost,
    oi.retail_price,
    oi.item_profit,
    oi.item_margin_pct,
    oi.discount_amount,
    oi.discount_pct,

    -- Derived flags
    CASE WHEN oi.item_status = 'Complete'   THEN 1 ELSE 0 END AS is_complete,
    CASE WHEN oi.item_status = 'Returned'   THEN 1 ELSE 0 END AS is_returned,
    CASE WHEN oi.item_status = 'Cancelled'  THEN 1 ELSE 0 END AS is_cancelled,

    -- Timestamps (for lineage / SLA tracking)
    oi.item_created_at,
    oi.item_shipped_at,
    oi.item_delivered_at,
    oi.item_returned_at,

    -- Fulfillment metrics
    o.fulfillment_days,

    -- Customer dimension attributes (de-normalized for convenience)
    c.country                                           AS customer_country,
    c.gender                                            AS customer_gender,
    c.age_group                                         AS customer_age_group,
    c.customer_segment,
    c.traffic_source,

    -- Product dimension attributes
    p.category,
    p.department,
    p.brand,
    p.price_tier

FROM staging.stg_order_items oi
JOIN staging.stg_orders   o  ON oi.order_id   = o.order_id
JOIN dim.DimCustomer      c  ON oi.user_id    = c.user_id
JOIN dim.DimProduct       p  ON oi.product_id = p.product_id;


-- ══════════════════════════════════════════════════════
-- FactEvents — Grain: one row per web event
-- ══════════════════════════════════════════════════════
DROP TABLE IF EXISTS fact.FactEvents;
CREATE TABLE fact.FactEvents AS
SELECT
    e.event_id,
    e.user_id                                           AS customer_key,
    CAST(STRFTIME(e.event_time::DATE, '%Y%m%d')
         AS INTEGER)                                    AS date_key,
    e.session_id,
    e.sequence_number,
    e.event_type,
    e.uri,
    e.browser,
    e.traffic_source,
    e.event_time,
    -- flags
    CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END AS is_purchase,
    CASE WHEN e.event_type = 'cart'     THEN 1 ELSE 0 END AS is_cart,
    CASE WHEN e.event_type = 'cancel'   THEN 1 ELSE 0 END AS is_cancel,
    -- customer attrs
    c.country                                           AS customer_country,
    c.customer_segment
FROM staging.stg_events e
LEFT JOIN dim.DimCustomer c ON e.user_id = c.user_id;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 4: DATA MARTS — Business-Ready Aggregations
# ═══════════════════════════════════════════════════════════════════════════════

MART_DDL = """
CREATE SCHEMA IF NOT EXISTS mart;

-- ── Monthly Sales Trends ───────────────────────────────────────────────────
DROP TABLE IF EXISTS mart.monthly_sales;
CREATE TABLE mart.monthly_sales AS
SELECT
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    COUNT(DISTINCT fs.order_id)                         AS total_orders,
    COUNT(fs.fact_id)                                   AS total_items_sold,
    ROUND(SUM(fs.sale_price), 2)                        AS gross_revenue,
    ROUND(SUM(CASE WHEN fs.is_returned = 0 THEN fs.sale_price ELSE 0 END), 2) AS net_revenue,
    ROUND(SUM(fs.item_profit), 2)                       AS gross_profit,
    ROUND(AVG(fs.sale_price), 2)                        AS avg_item_price,
    SUM(fs.is_returned)                                 AS total_returns,
    COUNT(DISTINCT fs.customer_key)                     AS unique_customers,
    ROUND(SUM(fs.sale_price) / NULLIF(COUNT(DISTINCT fs.order_id), 0), 2) AS avg_order_value
FROM fact.FactSales fs
JOIN dim.DimDate d ON fs.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, d.year_month
ORDER BY d.year, d.month;

-- ── Product Performance ────────────────────────────────────────────────────
DROP TABLE IF EXISTS mart.product_performance;
CREATE TABLE mart.product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.department,
    p.price_tier,
    p.retail_price,
    p.cost,
    p.margin_pct,
    COUNT(fs.fact_id)                                   AS units_sold,
    ROUND(SUM(fs.sale_price), 2)                        AS total_revenue,
    ROUND(SUM(fs.item_profit), 2)                       AS total_profit,
    ROUND(AVG(fs.sale_price), 2)                        AS avg_sale_price,
    ROUND(AVG(fs.discount_pct), 2)                      AS avg_discount_pct,
    SUM(fs.is_returned)                                 AS total_returns,
    ROUND(SUM(fs.is_returned) * 100.0
          / NULLIF(COUNT(fs.fact_id), 0), 2)            AS return_rate_pct
FROM dim.DimProduct p
LEFT JOIN fact.FactSales fs ON p.product_id = fs.product_key
GROUP BY p.product_id, p.product_name, p.brand, p.category, p.department,
         p.price_tier, p.retail_price, p.cost, p.margin_pct
ORDER BY total_revenue DESC;

-- ── Customer Segmentation & RFM ────────────────────────────────────────────
DROP TABLE IF EXISTS mart.customer_rfm;
CREATE TABLE mart.customer_rfm AS
WITH rfm_base AS (
    SELECT
        c.user_id,
        c.full_name,
        c.email,
        c.gender,
        c.age_group,
        c.country,
        c.customer_segment,
        c.traffic_source,
        DATEDIFF('day', c.last_order_date, CURRENT_DATE)  AS recency_days,
        c.lifetime_orders                                  AS frequency,
        c.lifetime_revenue                                 AS monetary,
        c.avg_order_value
    FROM dim.DimCustomer c
    WHERE c.lifetime_orders > 0
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC)         AS r_score,
        NTILE(5) OVER (ORDER BY frequency     ASC)         AS f_score,
        NTILE(5) OVER (ORDER BY monetary      ASC)         AS m_score
    FROM rfm_base
)
SELECT
    *,
    r_score + f_score + m_score                            AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4                 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND f_score < 3                  THEN 'Potential Loyalists'
        WHEN r_score >= 4 AND f_score < 2                  THEN 'New Customers'
        WHEN r_score < 2  AND f_score >= 3                 THEN 'At Risk'
        WHEN r_score < 2  AND f_score < 2                  THEN 'Lost'
        ELSE 'Regular'
    END                                                    AS rfm_segment
FROM rfm_scored;

-- ── Revenue by Category ────────────────────────────────────────────────────
DROP TABLE IF EXISTS mart.category_revenue;
CREATE TABLE mart.category_revenue AS
SELECT
    fs.category,
    fs.department,
    COUNT(DISTINCT fs.order_id)       AS orders,
    COUNT(fs.fact_id)                 AS items_sold,
    ROUND(SUM(fs.sale_price), 2)      AS total_revenue,
    ROUND(SUM(fs.item_profit), 2)     AS total_profit,
    ROUND(AVG(fs.item_margin_pct), 2) AS avg_margin_pct,
    SUM(fs.is_returned)               AS total_returns,
    ROUND(SUM(fs.is_returned)*100.0/NULLIF(COUNT(fs.fact_id),0), 2) AS return_rate_pct
FROM fact.FactSales fs
GROUP BY fs.category, fs.department
ORDER BY total_revenue DESC;

-- ── Geographic Revenue ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS mart.geo_revenue;
CREATE TABLE mart.geo_revenue AS
SELECT
    fs.customer_country,
    COUNT(DISTINCT fs.customer_key)   AS unique_customers,
    COUNT(DISTINCT fs.order_id)       AS total_orders,
    ROUND(SUM(fs.sale_price), 2)      AS total_revenue,
    ROUND(AVG(fs.sale_price), 2)      AS avg_item_price
FROM fact.FactSales fs
GROUP BY fs.customer_country
ORDER BY total_revenue DESC;

-- ── Cohort Retention ───────────────────────────────────────────────────────
DROP TABLE IF EXISTS mart.cohort_retention;
CREATE TABLE mart.cohort_retention AS
WITH first_orders AS (
    SELECT
        customer_key,
        DATE_TRUNC('month', MIN(item_created_at))   AS cohort_month,
        MIN(item_created_at)                         AS first_order_date
    FROM fact.FactSales
    GROUP BY customer_key
),
customer_activity AS (
    SELECT
        fs.customer_key,
        fo.cohort_month,
        DATE_TRUNC('month', fs.item_created_at)     AS activity_month
    FROM fact.FactSales fs
    JOIN first_orders fo ON fs.customer_key = fo.customer_key
    GROUP BY fs.customer_key, fo.cohort_month, DATE_TRUNC('month', fs.item_created_at)
)
SELECT
    cohort_month,
    activity_month,
    DATEDIFF('month', cohort_month, activity_month) AS months_since_first,
    COUNT(DISTINCT customer_key)                     AS active_customers
FROM customer_activity
GROUP BY cohort_month, activity_month
ORDER BY cohort_month, months_since_first;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 60)
    log.info("ELT Pipeline — Star Schema Transformation")
    log.info("=" * 60)

    con = duckdb.connect(DB_PATH)

    try:
        log.info("\n[1/4] Building Staging Layer …")
        run_sql(con, "Staging tables", STAGING_DDL)
        for tbl in ["stg_users", "stg_products", "stg_orders",
                    "stg_order_items", "stg_events"]:
            cnt = con.execute(f"SELECT COUNT(*) FROM staging.{tbl}").fetchone()[0]
            log.info(f"       staging.{tbl}: {cnt:,} rows")

        log.info("\n[2/4] Building Dimension Tables …")
        run_sql(con, "Dimension tables", DIM_DDL)
        for tbl in ["DimDate", "DimCustomer", "DimProduct", "DimDistributionCenter"]:
            cnt = con.execute(f"SELECT COUNT(*) FROM dim.{tbl}").fetchone()[0]
            log.info(f"       dim.{tbl}: {cnt:,} rows")

        log.info("\n[3/4] Building Fact Tables …")
        run_sql(con, "Fact tables", FACT_DDL)
        for tbl in ["FactSales", "FactEvents"]:
            cnt = con.execute(f"SELECT COUNT(*) FROM fact.{tbl}").fetchone()[0]
            log.info(f"       fact.{tbl}: {cnt:,} rows")

        log.info("\n[4/4] Building Data Marts …")
        run_sql(con, "Mart tables", MART_DDL)
        for tbl in ["monthly_sales", "product_performance", "customer_rfm",
                    "category_revenue", "geo_revenue", "cohort_retention"]:
            cnt = con.execute(f"SELECT COUNT(*) FROM mart.{tbl}").fetchone()[0]
            log.info(f"       mart.{tbl}: {cnt:,} rows")

        # Quick sanity check
        log.info("\n── Fact Table Sanity Checks ──")
        rev = con.execute("SELECT ROUND(SUM(sale_price),2) FROM fact.FactSales").fetchone()[0]
        log.info(f"  Total Gross Revenue:   ${rev:,.2f}")
        orders = con.execute("SELECT COUNT(DISTINCT order_id) FROM fact.FactSales").fetchone()[0]
        log.info(f"  Total Orders:          {orders:,}")
        custs  = con.execute("SELECT COUNT(DISTINCT customer_key) FROM fact.FactSales").fetchone()[0]
        log.info(f"  Unique Customers:      {custs:,}")

        log.info("\n✅ ELT Pipeline complete!")

    finally:
        con.close()

if __name__ == "__main__":
    run_pipeline()
