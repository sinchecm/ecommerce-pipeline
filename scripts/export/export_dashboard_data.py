"""
=============================================================================
Export Dashboard Data — TheLook eCommerce
=============================================================================
Reads all mart tables from DuckDB and exports a single dashboard_data.json
file that the Cloudflare Pages dashboard reads at runtime.

Output: dashboard/public/data/dashboard_data.json

Run:
    python3 scripts/export/export_dashboard_data.py

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import json
import os
import duckdb
from pathlib import Path
from datetime import datetime, date

BASE_DIR   = Path(__file__).resolve().parents[2]
DB_PATH    = str(BASE_DIR / "data" / "warehouse" / "ecommerce.duckdb")
OUT_DIR    = BASE_DIR / "dashboard" / "public" / "data"
OUT_FILE   = OUT_DIR / "dashboard_data.json"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(BASE_DIR / "logs", exist_ok=True)

# ── JSON serializer for dates / numpy types ──────────────────────────────────
def _serial(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def df_to_records(df):
    """Convert a pandas DataFrame to a list of plain dicts."""
    return json.loads(df.to_json(orient="records", date_format="iso"))


def export():
    print(f"[export] Connecting to {DB_PATH}")
    db = duckdb.connect(DB_PATH, read_only=True)

    payload = {"exported_at": datetime.utcnow().isoformat()}

    # ── 1. KPIs ──────────────────────────────────────────────────────────────
    insights_path = BASE_DIR / "reports" / "insights.json"
    if insights_path.exists():
        with open(insights_path) as f:
            insights = json.load(f)
        payload["kpis"]              = insights.get("kpis", {})
        payload["annual_revenue"]    = insights.get("annual_revenue", [])
        payload["top_categories"]    = insights.get("top_5_categories", [])
        payload["customer_segments_summary"] = insights.get("customer_segments", [])
        payload["top_countries"]     = insights.get("top_3_countries", [])
        payload["peak_month"]        = insights.get("peak_month", "")
        print("[export] ✅ KPIs loaded from insights.json")
    else:
        print("[export] ⚠ insights.json not found, computing from DuckDB")

    # ── 2. Monthly Sales (all months) ────────────────────────────────────────
    df = db.execute("""
        SELECT year, month, month_name, year_month,
               total_orders, gross_revenue, net_revenue,
               gross_profit, avg_order_value, unique_customers,
               total_returns
        FROM mart.monthly_sales
        ORDER BY year, month
    """).fetchdf()
    payload["monthly_sales"] = df_to_records(df)
    print(f"[export] ✅ monthly_sales: {len(df)} rows")

    # ── 3. Category Revenue ──────────────────────────────────────────────────
    df = db.execute("""
        SELECT category, department, orders, items_sold,
               total_revenue, total_profit, avg_margin_pct,
               total_returns, return_rate_pct
        FROM mart.category_revenue
        ORDER BY total_revenue DESC
        LIMIT 20
    """).fetchdf()
    payload["category_revenue"] = df_to_records(df)
    print(f"[export] ✅ category_revenue: {len(df)} rows")

    # ── 4. Geographic Revenue ────────────────────────────────────────────────
    df = db.execute("""
        SELECT customer_country, unique_customers,
               total_orders, total_revenue, avg_item_price
        FROM mart.geo_revenue
        ORDER BY total_revenue DESC
        LIMIT 15
    """).fetchdf()
    payload["geo_revenue"] = df_to_records(df)
    print(f"[export] ✅ geo_revenue: {len(df)} rows")

    # ── 5. RFM Segments (aggregated) ────────────────────────────────────────
    df = db.execute("""
        SELECT rfm_segment,
               COUNT(*)           AS customers,
               SUM(monetary)      AS total_revenue,
               AVG(monetary)      AS avg_clv,
               AVG(frequency)     AS avg_orders,
               AVG(recency_days)  AS avg_recency_days
        FROM mart.customer_rfm
        GROUP BY rfm_segment
        ORDER BY total_revenue DESC
    """).fetchdf()
    payload["rfm_segments"] = df_to_records(df)
    print(f"[export] ✅ rfm_segments: {len(df)} rows")

    # ── 6. Top Products ──────────────────────────────────────────────────────
    df = db.execute("""
        SELECT product_name, brand, category, department,
               price_tier, retail_price, units_sold,
               total_revenue, total_profit, avg_discount_pct,
               return_rate_pct
        FROM mart.product_performance
        ORDER BY total_revenue DESC
        LIMIT 20
    """).fetchdf()
    payload["top_products"] = df_to_records(df)
    print(f"[export] ✅ top_products: {len(df)} rows")

    # ── 7. Cohort Retention (pivot for heatmap) ──────────────────────────────
    df = db.execute("""
        SELECT cohort_month, months_since_first, active_customers
        FROM mart.cohort_retention
        ORDER BY cohort_month, months_since_first
    """).fetchdf()
    # Build cohort sizes (month 0 = cohort size)
    cohort_sizes = (
        df[df["months_since_first"] == 0]
        .set_index("cohort_month")["active_customers"]
        .to_dict()
    )
    records = []
    for _, row in df.iterrows():
        cohort = str(row["cohort_month"])[:7]  # YYYY-MM
        size   = cohort_sizes.get(row["cohort_month"], 1)
        records.append({
            "cohort_month":       cohort,
            "months_since_first": int(row["months_since_first"]),
            "active_customers":   int(row["active_customers"]),
            "retention_rate":     round(int(row["active_customers"]) / max(size, 1) * 100, 1),
        })
    payload["cohort_retention"] = records
    print(f"[export] ✅ cohort_retention: {len(records)} rows")

    # ── 8. Data Quality Summary ──────────────────────────────────────────────
    quality_path = BASE_DIR / "reports" / "quality_report.json"
    if quality_path.exists():
        with open(quality_path) as f:
            qr = json.load(f)
        payload["quality_summary"] = qr.get("summary", {})
        payload["quality_tests"]   = [
            {
                "name":         t["name"],
                "category":     t["category"],
                "table":        t["table"],
                "passed":       t["passed"],
                "failed_rows":  t["failed_rows"],
                "failure_rate": t["failure_rate"],
                "critical":     t["critical"],
            }
            for t in qr.get("tests", [])
        ]
        payload["quality_run_at"] = qr.get("run_timestamp", "")
        print(f"[export] ✅ quality: {len(payload['quality_tests'])} tests")

    db.close()

    # ── Write output ─────────────────────────────────────────────────────────
    with open(OUT_FILE, "w") as f:
        json.dump(payload, f, indent=2, default=_serial)

    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"\n[export] ✅ Written → {OUT_FILE}  ({size_kb:.1f} KB)")
    return str(OUT_FILE)


if __name__ == "__main__":
    export()
