"""
=============================================================================
Data Analysis & EDA — TheLook eCommerce
=============================================================================
Connects to DuckDB via SQLAlchemy, performs comprehensive EDA,
and generates publication-quality charts saved to /reports/

Analysis Sections:
  1. Executive KPI Summary
  2. Monthly Revenue Trends (2020-2024)
  3. Top Products & Categories
  4. Customer Segmentation (RFM)
  5. Geographic Revenue Distribution
  6. Customer Cohort Retention
  7. Traffic Source Analysis
  8. Demand Forecasting (simple linear trend)

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import os
import warnings
import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from scipy import stats
import logging

warnings.filterwarnings("ignore")

# ─── Dynamic Base Directory (works on any machine) ───────────────────────────
# scripts/analysis/eda_analysis.py  →  go up 2 levels = project root
BASE_DIR = Path(__file__).resolve().parents[2]

# ─── Config ──────────────────────────────────────────────────────────────────
DB_PATH      = str(BASE_DIR / "data" / "warehouse" / "ecommerce.duckdb")
REPORTS_DIR  = str(BASE_DIR / "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Styling ──────────────────────────────────────────────────────────────────
PALETTE  = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D",
            "#3B1F2B", "#44BBA4", "#E94F37", "#393E41"]
sns.set_theme(style="whitegrid", palette=PALETTE, font_scale=1.1)
plt.rcParams.update({
    "figure.dpi":        150,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "font.family":       "DejaVu Sans",
})


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION (SQLAlchemy)
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    """Create SQLAlchemy engine for DuckDB."""
    return create_engine(f"duckdb:///{DB_PATH}")


def query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. EXECUTIVE KPI SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

def kpi_summary(engine) -> dict:
    log.info("[1] Computing Executive KPIs …")
    sql = """
    SELECT
        ROUND(SUM(sale_price), 2)                        AS gross_revenue,
        ROUND(SUM(CASE WHEN is_returned=0 THEN sale_price ELSE 0 END), 2) AS net_revenue,
        ROUND(SUM(item_profit), 2)                       AS gross_profit,
        ROUND(SUM(item_profit)*100.0/NULLIF(SUM(sale_price),0),2) AS overall_margin_pct,
        COUNT(DISTINCT order_id)                         AS total_orders,
        COUNT(DISTINCT customer_key)                     AS unique_customers,
        COUNT(fact_id)                                   AS total_items_sold,
        ROUND(AVG(sale_price), 2)                        AS avg_item_price,
        ROUND(SUM(sale_price)/NULLIF(COUNT(DISTINCT order_id),0),2) AS avg_order_value,
        SUM(is_returned)                                 AS total_returns,
        ROUND(SUM(is_returned)*100.0/NULLIF(COUNT(fact_id),0),2)   AS return_rate_pct
    FROM fact.FactSales
    """
    df  = query(engine, sql)
    kpi = df.iloc[0].to_dict()

    # YoY growth (2024 vs 2023)
    yoy_sql = """
    SELECT year,
           ROUND(SUM(net_revenue),2) AS annual_revenue
    FROM mart.monthly_sales
    WHERE year IN (2023, 2024)
    GROUP BY year ORDER BY year
    """
    yoy = query(engine, yoy_sql)
    if len(yoy) == 2:
        r23, r24 = float(yoy.iloc[0]["annual_revenue"]), float(yoy.iloc[1]["annual_revenue"])
        kpi["yoy_growth_pct"] = round((r24 - r23) / max(r23, 1) * 100, 2)
    else:
        kpi["yoy_growth_pct"] = 0.0

    # CLV
    clv_sql = """
    SELECT ROUND(AVG(lifetime_revenue),2) AS avg_clv
    FROM dim.DimCustomer
    WHERE lifetime_orders > 0
    """
    clv = query(engine, clv_sql)
    kpi["avg_clv"] = float(clv.iloc[0]["avg_clv"])

    log.info(f"   Gross Revenue:  ${kpi['gross_revenue']:,.2f}")
    log.info(f"   Net Revenue:    ${kpi['net_revenue']:,.2f}")
    log.info(f"   Gross Margin:   {kpi['overall_margin_pct']}%")
    log.info(f"   Total Orders:   {int(kpi['total_orders']):,}")
    log.info(f"   Unique Customers:{int(kpi['unique_customers']):,}")
    log.info(f"   AOV:            ${kpi['avg_order_value']:.2f}")
    log.info(f"   Avg CLV:        ${kpi['avg_clv']:.2f}")
    log.info(f"   YoY Growth:     {kpi['yoy_growth_pct']}%")

    return kpi


# ═══════════════════════════════════════════════════════════════════════════════
# 2. MONTHLY REVENUE TREND
# ═══════════════════════════════════════════════════════════════════════════════

def plot_monthly_revenue(engine, kpi: dict):
    log.info("[2] Monthly Revenue Trend …")
    df = query(engine, """
        SELECT year, month, year_month, gross_revenue, net_revenue,
               gross_profit, total_orders, avg_order_value, unique_customers
        FROM mart.monthly_sales
        ORDER BY year, month
    """)
    df["period"] = pd.to_datetime(df["year_month"], format="%Y-%m")
    df["rolling_12"] = df["net_revenue"].rolling(12, min_periods=1).mean()

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Monthly Business Performance (2020–2024)",
                 fontsize=16, fontweight="bold", y=1.01)

    # ── Revenue ──
    ax = axes[0, 0]
    ax.fill_between(df["period"], df["net_revenue"], alpha=0.15, color=PALETTE[0])
    ax.plot(df["period"], df["net_revenue"],   color=PALETTE[0], lw=1.5, label="Net Revenue")
    ax.plot(df["period"], df["rolling_12"],    color=PALETTE[2], lw=2,   ls="--", label="12-mo Rolling Avg")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax.set_title("Monthly Net Revenue", fontweight="bold")
    ax.legend(fontsize=9); ax.set_xlabel("")

    # ── Orders ──
    ax = axes[0, 1]
    bars = ax.bar(df["period"], df["total_orders"], color=PALETTE[1], alpha=0.8, width=20)
    ax.set_title("Monthly Order Volume", fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
    ax.set_xlabel("")

    # ── AOV ──
    ax = axes[1, 0]
    ax.plot(df["period"], df["avg_order_value"], color=PALETTE[3], lw=2, marker="o", ms=3)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax.set_title("Average Order Value", fontweight="bold")
    ax.set_xlabel("")

    # ── Unique Customers ──
    ax = axes[1, 1]
    ax.fill_between(df["period"], df["unique_customers"], alpha=0.2, color=PALETTE[5])
    ax.plot(df["period"], df["unique_customers"], color=PALETTE[5], lw=2)
    ax.set_title("Unique Customers per Month", fontweight="bold")
    ax.set_xlabel("")

    for ax in axes.flat:
        ax.tick_params(axis="x", rotation=30)

    plt.tight_layout()
    path = f"{REPORTS_DIR}/01_monthly_revenue.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 3. TOP PRODUCTS & CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════════

def plot_top_products(engine):
    log.info("[3] Top Products & Categories …")

    # Top 15 products by revenue
    top_prod = query(engine, """
        SELECT product_name, brand, category, total_revenue, units_sold,
               total_profit, return_rate_pct, avg_sale_price
        FROM mart.product_performance
        WHERE units_sold > 0
        ORDER BY total_revenue DESC LIMIT 15
    """)

    # Category revenue
    cat_rev = query(engine, """
        SELECT category, total_revenue, total_profit, items_sold,
               avg_margin_pct, return_rate_pct
        FROM mart.category_revenue
        ORDER BY total_revenue DESC LIMIT 15
    """)

    fig, axes = plt.subplots(1, 2, figsize=(18, 8))
    fig.suptitle("Product & Category Performance", fontsize=16, fontweight="bold")

    # Top products (horizontal bar)
    ax = axes[0]
    colors_p = [PALETTE[i % len(PALETTE)] for i in range(len(top_prod))]
    bars = ax.barh(top_prod["product_name"].str[:35],
                   top_prod["total_revenue"],
                   color=colors_p, alpha=0.85)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
    ax.set_title("Top 15 Products by Revenue", fontweight="bold")
    ax.set_xlabel("Total Revenue")
    ax.invert_yaxis()
    for bar, val in zip(bars, top_prod["total_revenue"]):
        ax.text(val + 500, bar.get_y() + bar.get_height()/2,
                f"${val/1e3:.1f}K", va="center", fontsize=7.5)

    # Category revenue (donut + bar combo)
    ax = axes[1]
    cat_plot = cat_rev.head(10)
    colors_c = PALETTE[:len(cat_plot)]
    bars2 = ax.barh(cat_plot["category"],
                    cat_plot["total_revenue"],
                    color=colors_c, alpha=0.85)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax.set_title("Revenue by Category (Top 10)", fontweight="bold")
    ax.invert_yaxis()
    # Add margin % annotation
    for bar, row in zip(bars2, cat_plot.itertuples()):
        ax.text(row.total_revenue + 2000, bar.get_y() + bar.get_height()/2,
                f"  {row.avg_margin_pct:.0f}% margin", va="center", fontsize=8)

    plt.tight_layout()
    path = f"{REPORTS_DIR}/02_top_products.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return top_prod, cat_rev


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CUSTOMER SEGMENTATION
# ═══════════════════════════════════════════════════════════════════════════════

def plot_customer_segmentation(engine):
    log.info("[4] Customer Segmentation …")

    rfm = query(engine, """
        SELECT rfm_segment, gender, age_group, country,
               COUNT(*)                   AS n_customers,
               ROUND(AVG(monetary),2)     AS avg_revenue,
               ROUND(AVG(frequency),2)    AS avg_orders,
               ROUND(AVG(recency_days),0) AS avg_recency_days
        FROM mart.customer_rfm
        GROUP BY rfm_segment, gender, age_group, country
    """)

    seg_summary = query(engine, """
        SELECT rfm_segment,
               COUNT(*)                   AS customers,
               ROUND(SUM(monetary),2)     AS total_revenue,
               ROUND(AVG(monetary),2)     AS avg_clv,
               ROUND(AVG(frequency),2)    AS avg_orders
        FROM mart.customer_rfm
        GROUP BY rfm_segment
        ORDER BY total_revenue DESC
    """)

    age_seg = query(engine, """
        SELECT age_group,
               COUNT(*) AS customers,
               ROUND(AVG(monetary),2) AS avg_revenue
        FROM mart.customer_rfm
        GROUP BY age_group
        ORDER BY avg_revenue DESC
    """)

    fig = plt.figure(figsize=(18, 12))
    gs  = fig.add_gridspec(2, 3, hspace=0.4, wspace=0.35)

    # ── Segment donut ──
    ax1 = fig.add_subplot(gs[0, 0])
    wedge_colors = PALETTE[:len(seg_summary)]
    wedges, texts, autotexts = ax1.pie(
        seg_summary["customers"],
        labels=seg_summary["rfm_segment"],
        colors=wedge_colors,
        autopct="%1.1f%%",
        pctdistance=0.82,
        wedgeprops=dict(width=0.5),
        startangle=90
    )
    ax1.set_title("Customer Segments\n(% of Customers)", fontweight="bold")

    # ── Revenue by segment bar ──
    ax2 = fig.add_subplot(gs[0, 1])
    seg_sorted = seg_summary.sort_values("total_revenue", ascending=True)
    colors_s   = PALETTE[:len(seg_sorted)]
    ax2.barh(seg_sorted["rfm_segment"], seg_sorted["total_revenue"],
             color=colors_s, alpha=0.85)
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax2.set_title("Revenue by Segment", fontweight="bold")

    # ── Avg CLV by segment ──
    ax3 = fig.add_subplot(gs[0, 2])
    seg_clv = seg_summary.sort_values("avg_clv", ascending=False)
    bars = ax3.bar(seg_clv["rfm_segment"], seg_clv["avg_clv"],
                   color=PALETTE[:len(seg_clv)], alpha=0.85)
    ax3.set_title("Avg Customer Lifetime Value", fontweight="bold")
    ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax3.tick_params(axis="x", rotation=25)

    # ── Revenue by age group ──
    ax4 = fig.add_subplot(gs[1, 0])
    age_sorted = age_seg.sort_values("avg_revenue", ascending=True)
    ax4.barh(age_sorted["age_group"], age_sorted["avg_revenue"],
             color=PALETTE[4], alpha=0.8)
    ax4.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax4.set_title("Avg Revenue by Age Group", fontweight="bold")

    # ── Gender split ──
    gender_data = query(engine, """
        SELECT gender,
               COUNT(*) AS customers,
               ROUND(SUM(monetary),2) AS total_revenue
        FROM mart.customer_rfm
        GROUP BY gender
    """)
    ax5 = fig.add_subplot(gs[1, 1])
    ax5.bar(gender_data["gender"], gender_data["total_revenue"],
            color=[PALETTE[0], PALETTE[1]], alpha=0.85)
    ax5.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax5.set_title("Revenue by Gender", fontweight="bold")

    # ── Traffic source ──
    traffic = query(engine, """
        SELECT traffic_source,
               COUNT(*) AS customers,
               ROUND(AVG(monetary),2) AS avg_clv
        FROM mart.customer_rfm
        GROUP BY traffic_source
        ORDER BY avg_clv DESC
    """)
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.bar(traffic["traffic_source"], traffic["avg_clv"],
            color=PALETTE[:len(traffic)], alpha=0.85)
    ax6.set_title("Avg CLV by Traffic Source", fontweight="bold")
    ax6.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax6.tick_params(axis="x", rotation=15)

    fig.suptitle("Customer Segmentation & Behavioral Analysis",
                 fontsize=16, fontweight="bold")
    path = f"{REPORTS_DIR}/03_customer_segmentation.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return seg_summary


# ═══════════════════════════════════════════════════════════════════════════════
# 5. GEOGRAPHIC ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def plot_geographic(engine):
    log.info("[5] Geographic Revenue Analysis …")
    geo = query(engine, """
        SELECT customer_country, total_revenue, unique_customers,
               total_orders, avg_item_price
        FROM mart.geo_revenue
        ORDER BY total_revenue DESC
    """)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Geographic Revenue Distribution", fontsize=16, fontweight="bold")

    # Revenue by country
    ax = axes[0]
    colors_g = PALETTE[:len(geo)]
    bars = ax.bar(geo["customer_country"], geo["total_revenue"],
                  color=colors_g, alpha=0.85)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax.set_title("Revenue by Country", fontweight="bold")
    ax.tick_params(axis="x", rotation=30)

    # Revenue per customer
    ax = axes[1]
    geo["rev_per_cust"] = geo["total_revenue"] / geo["unique_customers"]
    ax.bar(geo["customer_country"], geo["rev_per_cust"],
           color=PALETTE[4], alpha=0.85)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}"))
    ax.set_title("Revenue per Customer by Country", fontweight="bold")
    ax.tick_params(axis="x", rotation=30)

    plt.tight_layout()
    path = f"{REPORTS_DIR}/04_geographic.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return geo


# ═══════════════════════════════════════════════════════════════════════════════
# 6. COHORT RETENTION
# ═══════════════════════════════════════════════════════════════════════════════

def plot_cohort_retention(engine):
    log.info("[6] Cohort Retention Analysis …")
    cohort = query(engine, """
        SELECT
            STRFTIME(cohort_month::DATE, '%Y-%m') AS cohort,
            months_since_first,
            active_customers
        FROM mart.cohort_retention
        WHERE months_since_first <= 11
          AND cohort_month >= '2020-01-01'
          AND cohort_month <= '2023-12-01'
    """)

    if cohort.empty:
        log.warning("   No cohort data, skipping.")
        return None

    # Pivot for heatmap
    cohort_pivot = cohort.pivot_table(
        index="cohort", columns="months_since_first",
        values="active_customers", aggfunc="sum"
    ).fillna(0)

    # Normalize to retention rate
    cohort_sizes = cohort_pivot[0]
    retention    = cohort_pivot.div(cohort_sizes, axis=0).round(3) * 100

    fig, ax = plt.subplots(figsize=(14, max(6, len(retention) * 0.35)))
    mask = retention.isnull()
    sns.heatmap(
        retention,
        annot=True, fmt=".0f", cmap="YlOrRd_r",
        vmin=0, vmax=100,
        linewidths=0.4, linecolor="white",
        ax=ax, cbar_kws={"label": "Retention %"},
        annot_kws={"size": 7.5}, mask=mask
    )
    ax.set_title("Customer Cohort Retention Heatmap (%)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Months Since First Purchase")
    ax.set_ylabel("Cohort (First Purchase Month)")
    ax.tick_params(axis="y", labelsize=8)

    plt.tight_layout()
    path = f"{REPORTS_DIR}/05_cohort_retention.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return retention


# ═══════════════════════════════════════════════════════════════════════════════
# 7. REVENUE BREAKDOWN DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

def plot_revenue_breakdown(engine):
    log.info("[7] Revenue Breakdown Dashboard …")

    # Annual revenue
    annual = query(engine, """
        SELECT year,
               ROUND(SUM(gross_revenue),2) AS gross_revenue,
               ROUND(SUM(net_revenue),2)   AS net_revenue,
               ROUND(SUM(gross_profit),2)  AS gross_profit,
               SUM(total_orders)           AS orders,
               ROUND(AVG(avg_order_value),2) AS avg_aov
        FROM mart.monthly_sales
        GROUP BY year ORDER BY year
    """)

    # Department split
    dept = query(engine, """
        SELECT department,
               ROUND(SUM(sale_price),2) AS revenue,
               COUNT(*) AS items
        FROM fact.FactSales
        GROUP BY department ORDER BY revenue DESC
    """)

    # Price tier
    tier = query(engine, """
        SELECT price_tier,
               COUNT(*) AS units,
               ROUND(SUM(sale_price),2) AS revenue
        FROM fact.FactSales
        GROUP BY price_tier ORDER BY revenue DESC
    """)

    # Brand top 10
    brand = query(engine, """
        SELECT brand,
               ROUND(SUM(sale_price),2) AS revenue,
               COUNT(*) AS units
        FROM fact.FactSales
        GROUP BY brand ORDER BY revenue DESC LIMIT 10
    """)

    fig = plt.figure(figsize=(18, 12))
    gs  = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.35)

    # Annual trend
    ax1 = fig.add_subplot(gs[0, :2])
    x    = np.arange(len(annual))
    w    = 0.3
    ax1.bar(x - w/2, annual["gross_revenue"], w, label="Gross Revenue", color=PALETTE[0], alpha=0.85)
    ax1.bar(x + w/2, annual["net_revenue"],   w, label="Net Revenue",   color=PALETTE[1], alpha=0.85)
    ax1.plot(x, annual["gross_profit"], "o-", color=PALETTE[2], lw=2, label="Gross Profit")
    ax1.set_xticks(x); ax1.set_xticklabels(annual["year"].astype(int))
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v/1e6:.1f}M"))
    ax1.set_title("Annual Revenue & Profit", fontweight="bold")
    ax1.legend()

    # Dept pie
    ax2 = fig.add_subplot(gs[0, 2])
    ax2.pie(dept["revenue"], labels=dept["department"], autopct="%1.0f%%",
            colors=PALETTE[:len(dept)], startangle=90,
            wedgeprops=dict(width=0.55))
    ax2.set_title("Revenue by Department", fontweight="bold")

    # Price tier
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.bar(tier["price_tier"], tier["revenue"],
            color=PALETTE[:len(tier)], alpha=0.85)
    ax3.set_title("Revenue by Price Tier", fontweight="bold")
    ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax3.tick_params(axis="x", rotation=15)

    # Top 10 brands
    ax4 = fig.add_subplot(gs[1, 1:])
    brand_s = brand.sort_values("revenue", ascending=True)
    ax4.barh(brand_s["brand"], brand_s["revenue"],
             color=PALETTE[6], alpha=0.8)
    ax4.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
    ax4.set_title("Top 10 Brands by Revenue", fontweight="bold")

    fig.suptitle("Revenue Breakdown & Performance", fontsize=16, fontweight="bold")
    path = f"{REPORTS_DIR}/06_revenue_breakdown.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    log.info(f"   Saved → {path}")
    return annual


# ═══════════════════════════════════════════════════════════════════════════════
# 8. SAVE JSON INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════

def save_insights(kpi, monthly_df, seg_summary, cat_rev, geo, annual):
    """Serialize key findings to JSON for use in the presentation."""

    insights = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "kpis": {k: float(v) if isinstance(v, (int, float, np.integer, np.floating)) else str(v)
                 for k, v in kpi.items()},
        "top_5_categories": cat_rev.head(5)[["category","total_revenue","avg_margin_pct"]].to_dict("records"),
        "customer_segments": seg_summary.to_dict("records"),
        "top_3_countries":   geo.head(3)[["customer_country","total_revenue","unique_customers"]].to_dict("records"),
        "annual_revenue":    annual.to_dict("records"),
        "peak_month": (
            monthly_df.loc[monthly_df["net_revenue"].idxmax(), "year_month"]
        ),
    }

    path = f"{REPORTS_DIR}/insights.json"
    with open(path, "w") as f:
        json.dump(insights, f, indent=2, default=str)
    log.info(f"   Insights JSON saved → {path}")
    return insights


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("EDA & Data Analysis — TheLook eCommerce")
    log.info("=" * 60)

    engine = get_engine()

    kpi        = kpi_summary(engine)
    monthly_df = plot_monthly_revenue(engine, kpi)
    top_prod, cat_rev = plot_top_products(engine)
    seg_summary= plot_customer_segmentation(engine)
    geo        = plot_geographic(engine)
    _          = plot_cohort_retention(engine)
    annual     = plot_revenue_breakdown(engine)
    insights   = save_insights(kpi, monthly_df, seg_summary, cat_rev, geo, annual)

    log.info("\n✅ Analysis complete! Charts saved to /reports/")
    log.info("\n── Key Findings ──")
    log.info(f"  Total Revenue: ${kpi['gross_revenue']:,.0f}")
    log.info(f"  Net Revenue:   ${kpi['net_revenue']:,.0f}")
    log.info(f"  Gross Margin:  {kpi['overall_margin_pct']}%")
    log.info(f"  Peak Month:    {insights['peak_month']}")
    log.info(f"  YoY Growth:    {kpi['yoy_growth_pct']}%")
    log.info(f"  Avg CLV:       ${kpi['avg_clv']:.2f}")

    return insights

if __name__ == "__main__":
    main()
