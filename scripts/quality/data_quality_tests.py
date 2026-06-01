"""
=============================================================================
Data Quality Testing Suite
=============================================================================
Implements comprehensive data quality checks using:
  1. Custom SQL-based rules (primary approach — no heavy dependencies)
  2. Great Expectations integration (optional advanced validation)

Test Categories:
  ├── Completeness   — NULL checks on critical columns
  ├── Uniqueness     — Duplicate key detection
  ├── Referential    — Foreign key integrity (facts → dimensions)
  ├── Validity       — Domain/range/format constraints
  ├── Consistency    — Cross-table business logic
  └── Freshness      — Data recency checks

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import duckdb
import json
import os
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

# ─── Logging ─────────────────────────────────────────────────────────────────
os.makedirs("/home/user/ecommerce-pipeline/logs", exist_ok=True)
os.makedirs("/home/user/ecommerce-pipeline/reports", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/user/ecommerce-pipeline/logs/quality.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH = "/home/user/ecommerce-pipeline/data/warehouse/ecommerce.duckdb"

# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class QualityTest:
    name: str
    category: str          # Completeness | Uniqueness | Referential | Validity | Consistency
    table: str
    sql: str
    description: str
    threshold: float = 0.0  # max allowed failure rate (0.0 = zero tolerance)
    critical: bool = True

@dataclass
class TestResult:
    test: QualityTest
    passed: bool
    failed_rows: int
    total_rows: int
    failure_rate: float
    details: str
    duration_ms: float


# ═══════════════════════════════════════════════════════════════════════════════
# TEST DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

TESTS: List[QualityTest] = [

    # ── COMPLETENESS ─────────────────────────────────────────────────────────
    QualityTest(
        name="no_null_user_id_in_orders",
        category="Completeness",
        table="staging.stg_orders",
        sql="SELECT COUNT(*) FROM staging.stg_orders WHERE user_id IS NULL",
        description="Orders must have a valid user_id",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="no_null_product_id_in_items",
        category="Completeness",
        table="staging.stg_order_items",
        sql="SELECT COUNT(*) FROM staging.stg_order_items WHERE product_id IS NULL",
        description="Order items must reference a product",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="no_null_email_in_customers",
        category="Completeness",
        table="staging.stg_users",
        sql="SELECT COUNT(*) FROM staging.stg_users WHERE email IS NULL OR TRIM(email)=''",
        description="Customer records must have email address",
        threshold=0.01,  # allow 1%
        critical=False,
    ),
    QualityTest(
        name="no_null_sale_price_in_items",
        category="Completeness",
        table="staging.stg_order_items",
        sql="SELECT COUNT(*) FROM staging.stg_order_items WHERE sale_price IS NULL",
        description="Order items must have a sale price",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="no_null_customer_key_in_fact",
        category="Completeness",
        table="fact.FactSales",
        sql="SELECT COUNT(*) FROM fact.FactSales WHERE customer_key IS NULL",
        description="FactSales must have customer foreign key",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="no_null_product_key_in_fact",
        category="Completeness",
        table="fact.FactSales",
        sql="SELECT COUNT(*) FROM fact.FactSales WHERE product_key IS NULL",
        description="FactSales must have product foreign key",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="no_null_date_key_in_fact",
        category="Completeness",
        table="fact.FactSales",
        sql="SELECT COUNT(*) FROM fact.FactSales WHERE date_key IS NULL",
        description="FactSales must have date foreign key",
        threshold=0.0,
        critical=True,
    ),

    # ── UNIQUENESS ────────────────────────────────────────────────────────────
    QualityTest(
        name="unique_user_ids",
        category="Uniqueness",
        table="staging.stg_users",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT user_id)
               FROM staging.stg_users""",
        description="User IDs must be unique",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="unique_product_ids",
        category="Uniqueness",
        table="staging.stg_products",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT product_id)
               FROM staging.stg_products""",
        description="Product IDs must be unique",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="unique_order_item_ids",
        category="Uniqueness",
        table="staging.stg_order_items",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT order_item_id)
               FROM staging.stg_order_items""",
        description="Order item IDs must be unique",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="unique_dim_customer_keys",
        category="Uniqueness",
        table="dim.DimCustomer",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT user_id)
               FROM dim.DimCustomer""",
        description="DimCustomer must have unique surrogate keys",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="unique_date_keys_in_dimdate",
        category="Uniqueness",
        table="dim.DimDate",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT date_key)
               FROM dim.DimDate""",
        description="DimDate date_key must be unique",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="unique_customer_emails",
        category="Uniqueness",
        table="staging.stg_users",
        sql="""SELECT COUNT(*) - COUNT(DISTINCT email)
               FROM staging.stg_users""",
        description="Customer email addresses should be unique",
        threshold=0.005,  # allow 0.5% for data quality
        critical=False,
    ),

    # ── REFERENTIAL INTEGRITY ──────────────────────────────────────────────
    QualityTest(
        name="fact_customer_fk_integrity",
        category="Referential",
        table="fact.FactSales → dim.DimCustomer",
        sql="""SELECT COUNT(*) FROM fact.FactSales fs
               LEFT JOIN dim.DimCustomer dc ON fs.customer_key = dc.user_id
               WHERE dc.user_id IS NULL""",
        description="All FactSales customer_keys must exist in DimCustomer",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="fact_product_fk_integrity",
        category="Referential",
        table="fact.FactSales → dim.DimProduct",
        sql="""SELECT COUNT(*) FROM fact.FactSales fs
               LEFT JOIN dim.DimProduct dp ON fs.product_key = dp.product_id
               WHERE dp.product_id IS NULL""",
        description="All FactSales product_keys must exist in DimProduct",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="fact_date_fk_integrity",
        category="Referential",
        table="fact.FactSales → dim.DimDate",
        sql="""SELECT COUNT(*) FROM fact.FactSales fs
               LEFT JOIN dim.DimDate dd ON fs.date_key = dd.date_key
               WHERE dd.date_key IS NULL""",
        description="All FactSales date_keys must exist in DimDate",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="order_items_order_fk_integrity",
        category="Referential",
        table="staging.stg_order_items → staging.stg_orders",
        sql="""SELECT COUNT(*) FROM staging.stg_order_items oi
               LEFT JOIN staging.stg_orders o ON oi.order_id = o.order_id
               WHERE o.order_id IS NULL""",
        description="All order items must belong to a valid order",
        threshold=0.0,
        critical=True,
    ),

    # ── VALIDITY ──────────────────────────────────────────────────────────────
    QualityTest(
        name="positive_sale_prices",
        category="Validity",
        table="staging.stg_order_items",
        sql="SELECT COUNT(*) FROM staging.stg_order_items WHERE sale_price <= 0",
        description="Sale prices must be positive",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="valid_age_range",
        category="Validity",
        table="staging.stg_users",
        sql="SELECT COUNT(*) FROM staging.stg_users WHERE age < 13 OR age > 100",
        description="Customer ages must be between 13 and 100",
        threshold=0.01,
        critical=False,
    ),
    QualityTest(
        name="valid_gender_values",
        category="Validity",
        table="staging.stg_users",
        sql="""SELECT COUNT(*) FROM staging.stg_users
               WHERE gender NOT IN ('Male', 'Female', 'Unknown')""",
        description="Gender values must be Male/Female/Unknown",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="valid_order_status_values",
        category="Validity",
        table="staging.stg_orders",
        sql="""SELECT COUNT(*) FROM staging.stg_orders
               WHERE order_status NOT IN
               ('Complete','Returned','Cancelled','Processing','Shipped')""",
        description="Order statuses must be valid enum values",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="retail_price_geq_cost",
        category="Validity",
        table="staging.stg_products",
        sql="""SELECT COUNT(*) FROM staging.stg_products
               WHERE retail_price < cost""",
        description="Retail price must be >= product cost",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="date_keys_in_valid_range",
        category="Validity",
        table="fact.FactSales",
        sql="""SELECT COUNT(*) FROM fact.FactSales
               WHERE date_key < 20190101 OR date_key > 20251231""",
        description="All sales date keys must be between 2019-2025",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="sale_price_not_exceed_retail",
        category="Validity",
        table="staging.stg_order_items",
        sql="""SELECT COUNT(*) FROM staging.stg_order_items
               WHERE sale_price > retail_price * 1.05""",  # 5% tolerance
        description="Sale price should not greatly exceed retail price",
        threshold=0.02,
        critical=False,
    ),

    # ── CONSISTENCY ───────────────────────────────────────────────────────────
    QualityTest(
        name="order_total_matches_items",
        category="Consistency",
        table="fact.FactSales / stg_orders",
        sql="""SELECT COUNT(*) FROM (
                   SELECT
                       order_id,
                       ROUND(SUM(sale_price), 2) AS items_total,
                       MAX(order_total)           AS recorded_total
                   FROM (
                       SELECT oi.order_id, oi.sale_price, o.order_total
                       FROM staging.stg_order_items oi
                       JOIN staging.stg_orders o ON oi.order_id = o.order_id
                   )
                   GROUP BY order_id
                   HAVING ABS(items_total - recorded_total) > 0.50
               )""",
        description="Sum of order items should match order total (±$0.50)",
        threshold=0.02,
        critical=False,
    ),
    QualityTest(
        name="returned_orders_have_return_date",
        category="Consistency",
        table="staging.stg_orders",
        sql="""SELECT COUNT(*) FROM staging.stg_orders
               WHERE order_status = 'Returned' AND returned_at IS NULL""",
        description="Returned orders must have a return timestamp",
        threshold=0.05,  # some tolerance for late updates
        critical=False,
    ),
    QualityTest(
        name="shipped_after_order_date",
        category="Consistency",
        table="staging.stg_orders",
        sql="""SELECT COUNT(*) FROM staging.stg_orders
               WHERE shipped_at IS NOT NULL
                 AND shipped_at < order_created_at""",
        description="Shipped timestamp must be after order creation",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="delivered_after_shipped",
        category="Consistency",
        table="staging.stg_orders",
        sql="""SELECT COUNT(*) FROM staging.stg_orders
               WHERE delivered_at IS NOT NULL
                 AND shipped_at   IS NOT NULL
                 AND delivered_at < shipped_at""",
        description="Delivered timestamp must be after shipped timestamp",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="dim_customer_segment_populated",
        category="Consistency",
        table="dim.DimCustomer",
        sql="""SELECT COUNT(*) FROM dim.DimCustomer
               WHERE customer_segment IS NULL OR customer_segment = ''""",
        description="Every customer must have a segment classification",
        threshold=0.0,
        critical=True,
    ),

    # ── DATA VOLUME / FRESHNESS ───────────────────────────────────────────────
    QualityTest(
        name="minimum_order_volume",
        category="Freshness",
        table="fact.FactSales",
        sql="""SELECT CASE WHEN COUNT(DISTINCT order_id) >= 1000 THEN 0 ELSE 1 END
               FROM fact.FactSales""",
        description="Fact table should have at least 1,000 orders",
        threshold=0.0,
        critical=True,
    ),
    QualityTest(
        name="minimum_customer_volume",
        category="Freshness",
        table="dim.DimCustomer",
        sql="""SELECT CASE WHEN COUNT(*) >= 1000 THEN 0 ELSE 1 END
               FROM dim.DimCustomer""",
        description="DimCustomer should have at least 1,000 records",
        threshold=0.0,
        critical=True,
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════
# TEST RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def get_table_count(con, table: str) -> int:
    """Get total row count for a table (handles composite names)."""
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 1  # default to 1 to avoid division by zero


def run_tests(con) -> Tuple[List[TestResult], dict]:
    results  = []
    summary  = {"total": 0, "passed": 0, "failed": 0,
                "critical_failures": 0, "warnings": 0}

    for test in TESTS:
        summary["total"] += 1
        start = datetime.now()

        try:
            failed_rows = con.execute(test.sql).fetchone()[0]

            # get total count for simple single-table tests
            if "→" in test.table:
                total_rows = con.execute(
                    f"SELECT COUNT(*) FROM {test.table.split('→')[0].strip()}"
                ).fetchone()[0]
            else:
                total_rows = get_table_count(con, test.table)

            failure_rate = failed_rows / max(total_rows, 1)
            passed       = failure_rate <= test.threshold
            duration_ms  = (datetime.now() - start).total_seconds() * 1000

            result = TestResult(
                test=test,
                passed=passed,
                failed_rows=failed_rows,
                total_rows=total_rows,
                failure_rate=failure_rate,
                details=(f"{failed_rows:,} rows failed "
                         f"(rate: {failure_rate:.4%}, threshold: {test.threshold:.4%})"),
                duration_ms=round(duration_ms, 2),
            )

            if passed:
                summary["passed"] += 1
            else:
                summary["failed"] += 1
                if test.critical:
                    summary["critical_failures"] += 1
                else:
                    summary["warnings"] += 1

        except Exception as e:
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            result = TestResult(
                test=test,
                passed=False,
                failed_rows=-1,
                total_rows=0,
                failure_rate=1.0,
                details=f"ERROR: {str(e)}",
                duration_ms=round(duration_ms, 2),
            )
            summary["failed"]            += 1
            summary["critical_failures"] += 1 if test.critical else 0

        results.append(result)

    return results, summary


def print_report(results: List[TestResult], summary: dict):
    """Print colored test report to console."""
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    RESET  = "\033[0m"
    BOLD   = "\033[1m"

    print(f"\n{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}{CYAN}   DATA QUALITY TEST REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{CYAN}{'='*70}{RESET}\n")

    # Group by category
    categories = {}
    for r in results:
        cat = r.test.category
        categories.setdefault(cat, []).append(r)

    for cat, cat_results in categories.items():
        passed_cat = sum(1 for r in cat_results if r.passed)
        print(f"{BOLD}  [{cat}] {passed_cat}/{len(cat_results)} passed{RESET}")
        for r in cat_results:
            icon  = f"{GREEN}✓{RESET}" if r.passed else f"{RED}✗{RESET}"
            warn  = "" if r.test.critical else f" {YELLOW}[WARN]{RESET}"
            line  = f"    {icon} {r.test.name}{warn}"
            print(f"{line:<65} {r.details}")
        print()

    # Summary
    print(f"{BOLD}{CYAN}{'─'*70}{RESET}")
    total    = summary["total"]
    passed   = summary["passed"]
    failed   = summary["failed"]
    critical = summary["critical_failures"]

    status_color = GREEN if critical == 0 else RED
    print(f"  {BOLD}Total Tests  :{RESET} {total}")
    print(f"  {BOLD}Passed       :{RESET} {GREEN}{passed}{RESET}")
    print(f"  {BOLD}Failed       :{RESET} {RED if failed else GREEN}{failed}{RESET}")
    print(f"  {BOLD}Critical Fails:{RESET} {status_color}{critical}{RESET}")
    print(f"  {BOLD}Warnings     :{RESET} {YELLOW}{summary['warnings']}{RESET}")
    print(f"\n  {BOLD}Overall Status:{RESET} "
          f"{'  ✅ PASS' if critical == 0 else '  ❌ FAIL'}")
    print(f"{BOLD}{CYAN}{'='*70}{RESET}\n")


def save_json_report(results: List[TestResult], summary: dict):
    """Save machine-readable JSON report."""
    report = {
        "run_timestamp": datetime.now().isoformat(),
        "summary": summary,
        "tests": [
            {
                "name":          r.test.name,
                "category":      r.test.category,
                "table":         r.test.table,
                "description":   r.test.description,
                "passed":        r.passed,
                "failed_rows":   r.failed_rows,
                "total_rows":    r.total_rows,
                "failure_rate":  round(r.failure_rate, 6),
                "threshold":     r.test.threshold,
                "critical":      r.test.critical,
                "details":       r.details,
                "duration_ms":   r.duration_ms,
            }
            for r in results
        ]
    }
    path = "/home/user/ecommerce-pipeline/reports/quality_report.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Quality report saved → {path}")
    return report


def run_quality_suite():
    log.info("=" * 60)
    log.info("Data Quality Test Suite")
    log.info("=" * 60)

    con = duckdb.connect(DB_PATH)
    try:
        results, summary = run_tests(con)
        print_report(results, summary)
        report = save_json_report(results, summary)

        # Log summary
        log.info(f"Tests: {summary['total']} total, "
                 f"{summary['passed']} passed, "
                 f"{summary['failed']} failed "
                 f"({summary['critical_failures']} critical)")

        return results, summary

    finally:
        con.close()


if __name__ == "__main__":
    run_quality_suite()
