# TheLook eCommerce — End-to-End Data Pipeline

> **Data Engineering Team | June 2026**
> *Transforming Raw Clicks Into Revenue Intelligence*

---

## Project Overview

A production-grade end-to-end data pipeline and analytics system built on top of the **TheLook eCommerce** dataset (BigQuery Public Data). Covers the full data engineering lifecycle from ingestion through business intelligence.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE ARCHITECTURE                           │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌─────────────────────────────────┐   │
│  │  Source  │    │Ingestion │    │         DuckDB Warehouse         │   │
│  │          │───▶│  Script  │───▶│  raw.*  →  staging.*  →  dim.*  │   │
│  │BigQuery  │    │(Python)  │    │              ↓          fact.*  │   │
│  │ Public   │    │CSV→DuckDB│    │         Quality Tests    mart.* │   │
│  └──────────┘    └──────────┘    └─────────────────────────────────┘   │
│                                                  │                      │
│  ┌──────────────────────────────────────────┐    │                      │
│  │           Pipeline Orchestrator          │    ▼                      │
│  │  schedule lib → Daily 02:00 UTC          │  ┌──────────────────┐    │
│  │  Quality gate → Every 6 hours            │  │  Python Analysis │    │
│  │  Analysis     → Every Sunday             │  │  SQLAlchemy EDA  │    │
│  └──────────────────────────────────────────┘  │  Matplotlib/SNS  │    │
│                                                 └──────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Star Schema Design

```
                    ┌─────────────────┐
                    │   DimProduct    │
                    │─────────────────│
                    │ product_id (PK) │
                    │ name, brand     │
                    │ category, dept  │
                    │ cost, retail    │
                    │ margin_pct      │
                    │ price_tier      │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  DimCustomer    │          │          │    DimDate      │
│─────────────────│          │          │─────────────────│
│ user_id (PK)    │          │          │ date_key (PK)   │
│ name, email     │          │          │ year, quarter   │
│ age, gender     ├──────────┤          │ month, week     │
│ country, city   │          │          │ is_weekend      │
│ traffic_source  │     ┌────┴────┐     │ fiscal_year     │
│ customer_segment│────▶│FactSales│◀────┤                 │
│ lifetime_orders │     │─────────│     └─────────────────┘
│ lifetime_revenue│     │order_id │
└─────────────────┘     │user_id  │     ┌─────────────────────┐
                        │prod_id  │     │ DimDistribution     │
                        │date_key │◀────│ Center              │
                        │dc_key   │     │─────────────────────│
                        │sale_price│    │ dc_id (PK)          │
                        │profit   │     │ name, lat, lon      │
                        │discount │     └─────────────────────┘
                        └─────────┘
                         76,076 rows
```

**Why Star Schema?**
- ✅ Fewer JOINs = faster BI query performance (1 fact + n dims vs. 7 normalized tables)
- ✅ Additive facts: SUM revenue across any dimension slice
- ✅ Native to BI tools (Tableau, Power BI, Looker)
- ✅ Self-documenting: business users understand immediately

## ELT Layers

| Layer | Schema | Description | Tables |
|-------|--------|-------------|--------|
| Raw | `raw.*` | Source data as-is from ingestion | 7 tables |
| Staging | `staging.*` | Cleaned, typed, validated | 5 tables |
| Warehouse | `dim.* / fact.*` | Star schema | 4 dims + 2 facts |
| Mart | `mart.*` | Pre-aggregated for BI | 6 tables |

## Key Results

| Metric | Value |
|--------|-------|
| Gross Revenue | $20,906,184 |
| Net Revenue | $18,385,203 |
| Gross Margin | 52.84% |
| Total Orders | 35,000 |
| Unique Customers | 8,897 |
| Avg Order Value | $597.32 |
| Avg CLV | $2,349.80 |
| Return Rate | 12.0% |
| YoY Growth (2024) | +28.89% |

## Data Quality: 31/31 Tests Pass ✅

| Category | Tests | Result |
|----------|-------|--------|
| Completeness | 7 | ✅ All Pass |
| Uniqueness | 6 | ✅ All Pass |
| Referential Integrity | 4 | ✅ All Pass |
| Validity | 7 | ✅ All Pass |
| Consistency | 5 | ✅ All Pass |
| Freshness/Volume | 2 | ✅ All Pass |

## Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Warehouse | DuckDB | Zero-server OLAP, 10x faster than SQLite for analytics |
| ORM/Query | SQLAlchemy | Database-agnostic, industry standard, migration-friendly |
| Transforms | Pure Python/SQL | Transparent, version-controlled, no black-box SaaS |
| Quality | Custom SQL | Team-readable, no GE overhead, SQL-native |
| Analysis | pandas + matplotlib | Right-sized for 10-100M rows, zero infra |
| Scheduler | schedule library | Lightweight, extensible to Airflow/Dagster |

## Project Structure

```
ecommerce-pipeline/
├── scripts/
│   ├── ingestion/
│   │   └── generate_thelook_data.py    # Data ingestion (7 tables, 408K rows)
│   ├── transformations/
│   │   └── elt_pipeline.py             # ELT: 4 layers, star schema
│   ├── quality/
│   │   └── data_quality_tests.py       # 31 quality tests
│   ├── analysis/
│   │   └── eda_analysis.py             # EDA, KPIs, 6 chart outputs
│   └── orchestration/
│       └── pipeline_orchestrator.py    # DAG orchestrator + scheduler
├── data/
│   ├── raw/                            # CSV exports (7 files)
│   └── warehouse/
│       └── ecommerce.duckdb            # Main data warehouse
├── reports/
│   ├── 01_monthly_revenue.png
│   ├── 02_top_products.png
│   ├── 03_customer_segmentation.png
│   ├── 04_geographic.png
│   ├── 05_cohort_retention.png
│   ├── 06_revenue_breakdown.png
│   ├── insights.json
│   └── quality_report.json
├── logs/
│   ├── ingestion.log
│   ├── elt.log
│   ├── quality.log
│   ├── orchestrator.log
│   └── run_history.json
└── README.md
```

## Running the Pipeline

```bash
# 1. Run full pipeline manually
python3 scripts/orchestration/pipeline_orchestrator.py --mode run

# 2. Run specific steps
python3 scripts/orchestration/pipeline_orchestrator.py --mode run --steps quality_tests analysis

# 3. Dry-run (validate without executing)
python3 scripts/orchestration/pipeline_orchestrator.py --mode dry-run

# 4. Start scheduler (daemonized)
python3 scripts/orchestration/pipeline_orchestrator.py --mode schedule

# 5. View run history
python3 scripts/orchestration/pipeline_orchestrator.py --mode history

# Individual scripts
python3 scripts/ingestion/generate_thelook_data.py
python3 scripts/transformations/elt_pipeline.py
python3 scripts/quality/data_quality_tests.py
python3 scripts/analysis/eda_analysis.py
```

## Business Recommendations

1. **Win-Back Campaign** — 957 At Risk customers (avg CLV $2,757) → 20% recovery = $527K revenue
2. **Champion VIP Program** — Top 2 segments = 61% of revenue → protect with loyalty tier
3. **Market Localization** — Brazil/Australia/Germany each ~$2.1M → 30% growth with localization
4. **Category Mix Optimization** — Socks/Swim (53%+ margin) → increase paid media allocation

## Dependencies

```
duckdb>=1.5.0
duckdb-engine>=0.17.0
sqlalchemy>=2.0
pandas>=2.0
numpy>=1.24
matplotlib>=3.7
seaborn>=0.12
faker>=18.0
schedule>=1.2
scipy>=1.10
scikit-learn>=1.3
```
