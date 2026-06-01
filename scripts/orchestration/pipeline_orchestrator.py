"""
=============================================================================
Pipeline Orchestrator — TheLook eCommerce
=============================================================================
Orchestrates the complete ELT pipeline with:
  - DAG-style dependency management
  - Step-level retry logic
  - Run history tracking
  - Schedule-based execution (cron-style via `schedule` library)
  - Slack/email alerting hooks (extensible)

Pipeline DAG:
  [Ingestion] → [ELT/Transform] → [Quality Tests] → [Analysis]
                                        ↓
                               [Alert if failures]

Scheduling (cron equivalent):
  - Full pipeline:  daily at 02:00 UTC
  - Quality tests:  every 6 hours
  - Analysis:       weekly on Sunday 04:00 UTC

Author: Data Engineering Team
Date: 2026-06-01
=============================================================================
"""

import subprocess
import sys
import json
import os
import time
import logging
import schedule
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Callable

# ─── Dynamic base directory (works on any machine / any clone location) ───────
BASE_DIR      = Path(__file__).resolve().parents[2]
PIPELINE_BASE = str(BASE_DIR)
RUN_HISTORY   = str(BASE_DIR / "logs" / "run_history.json")

# ─── Logging ─────────────────────────────────────────────────────────────────
os.makedirs(BASE_DIR / "logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(BASE_DIR / "logs" / "orchestrator.log")),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class StepResult:
    step_name:    str
    status:       str       # "success" | "failed" | "skipped"
    start_time:   str
    end_time:     str
    duration_sec: float
    error_msg:    Optional[str] = None
    retries:      int = 0

@dataclass
class PipelineRun:
    run_id:      str
    trigger:     str        # "manual" | "scheduled" | "ci"
    start_time:  str
    end_time:    str = ""
    status:      str = "running"
    steps:       List[StepResult] = field(default_factory=list)
    total_sec:   float = 0.0


# ─── Step Registry ───────────────────────────────────────────────────────────

PIPELINE_STEPS = [
    {
        "name":        "data_ingestion",
        "script":      str(BASE_DIR / "scripts" / "ingestion" / "generate_thelook_data.py"),
        "description": "Ingest raw data from source → DuckDB raw schema",
        "max_retries": 2,
        "critical":    True,
    },
    {
        "name":        "elt_transform",
        "script":      str(BASE_DIR / "scripts" / "transformations" / "elt_pipeline.py"),
        "description": "ELT: raw → staging → dim/fact → mart",
        "max_retries": 3,
        "critical":    True,
    },
    {
        "name":        "quality_tests",
        "script":      str(BASE_DIR / "scripts" / "quality" / "data_quality_tests.py"),
        "description": "Run 31 data quality tests across all layers",
        "max_retries": 1,
        "critical":    True,
    },
    {
        "name":        "analysis",
        "script":      str(BASE_DIR / "scripts" / "analysis" / "eda_analysis.py"),
        "description": "EDA, KPI computation, chart generation",
        "max_retries": 1,
        "critical":    False,
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# STEP EXECUTOR
# ═══════════════════════════════════════════════════════════════════════════════

def run_step(step: dict, dry_run: bool = False) -> StepResult:
    """Execute a single pipeline step with retry logic."""
    name       = step["name"]
    script     = step["script"]
    max_retry  = step.get("max_retries", 1)
    start_time = datetime.now()

    log.info(f"  ▶ Step: {name}")
    log.info(f"    Script: {script}")

    if dry_run:
        log.info(f"    [DRY RUN] Skipping execution")
        return StepResult(
            step_name=name,
            status="skipped",
            start_time=start_time.isoformat(),
            end_time=datetime.now().isoformat(),
            duration_sec=0.0,
        )

    last_error = None
    for attempt in range(1, max_retry + 1):
        try:
            result = subprocess.run(
                [sys.executable, script],
                capture_output=True,
                text=True,
                timeout=600,
            )
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            if result.returncode == 0:
                log.info(f"    ✅ {name} succeeded in {duration:.1f}s")
                return StepResult(
                    step_name=name,
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_sec=round(duration, 2),
                    retries=attempt - 1,
                )
            else:
                last_error = result.stderr[-500:] if result.stderr else "Unknown error"
                log.warning(f"    ⚠ Attempt {attempt}/{max_retry} failed: {last_error[:100]}")
                if attempt < max_retry:
                    time.sleep(5 * attempt)  # exponential backoff

        except subprocess.TimeoutExpired:
            last_error = "Timeout after 600s"
            log.warning(f"    ⚠ Attempt {attempt}/{max_retry} timed out")
        except Exception as e:
            last_error = str(e)
            log.warning(f"    ⚠ Attempt {attempt}/{max_retry} error: {e}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    log.error(f"    ❌ {name} FAILED after {max_retry} attempts: {last_error}")
    return StepResult(
        step_name=name,
        status="failed",
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        duration_sec=round(duration, 2),
        error_msg=last_error,
        retries=max_retry - 1,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    trigger: str = "manual",
    steps: Optional[List[str]] = None,
    dry_run: bool = False,
) -> PipelineRun:
    """
    Execute the full pipeline (or a subset of steps).

    Args:
        trigger:  How the run was triggered (manual/scheduled/ci)
        steps:    Optional list of step names to run (default: all)
        dry_run:  If True, skip actual script execution
    """
    run_id    = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run       = PipelineRun(
        run_id=run_id,
        trigger=trigger,
        start_time=datetime.now().isoformat(),
    )

    log.info("=" * 60)
    log.info(f"Pipeline Run: {run_id}  Trigger: {trigger}")
    log.info("=" * 60)

    selected = [s for s in PIPELINE_STEPS
                if steps is None or s["name"] in steps]

    pipeline_failed = False
    for step in selected:
        if pipeline_failed and step.get("critical"):
            log.warning(f"  ⏭  Skipping {step['name']} (upstream failure)")
            run.steps.append(StepResult(
                step_name=step["name"],
                status="skipped",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
                duration_sec=0.0,
                error_msg="Skipped due to upstream failure",
            ))
            continue

        result = run_step(step, dry_run=dry_run)
        run.steps.append(result)

        if result.status == "failed" and step.get("critical"):
            pipeline_failed = True

    # Finalize run
    run.end_time   = datetime.now().isoformat()
    total_sec      = sum(s.duration_sec for s in run.steps)
    run.total_sec  = round(total_sec, 2)
    run.status     = "failed" if pipeline_failed else "success"

    # Print summary
    log.info("\n── Run Summary ──")
    for s in run.steps:
        icon = {"success": "✅", "failed": "❌", "skipped": "⏭"}.get(s.status, "?")
        log.info(f"  {icon} {s.step_name:<25} {s.status:<10} {s.duration_sec:.1f}s")
    log.info(f"\n  Status: {run.status.upper()}   Total: {run.total_sec:.1f}s")

    # Persist run history
    _save_run_history(run)

    return run


# ═══════════════════════════════════════════════════════════════════════════════
# RUN HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

def _save_run_history(run: PipelineRun):
    history = []
    if os.path.exists(RUN_HISTORY):
        with open(RUN_HISTORY) as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = []

    # Convert dataclass to dict
    run_dict = {
        "run_id":    run.run_id,
        "trigger":   run.trigger,
        "start_time": run.start_time,
        "end_time":  run.end_time,
        "status":    run.status,
        "total_sec": run.total_sec,
        "steps":     [asdict(s) for s in run.steps],
    }
    history.append(run_dict)
    # Keep last 50 runs
    history = history[-50:]

    with open(RUN_HISTORY, "w") as f:
        json.dump(history, f, indent=2)
    log.info(f"  Run history saved → {RUN_HISTORY}")


def get_run_history() -> list:
    if not os.path.exists(RUN_HISTORY):
        return []
    with open(RUN_HISTORY) as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════════

def scheduled_full_pipeline():
    log.info("⏰ Scheduled trigger: FULL PIPELINE")
    run_pipeline(trigger="scheduled")

def scheduled_quality_only():
    log.info("⏰ Scheduled trigger: QUALITY TESTS ONLY")
    run_pipeline(trigger="scheduled", steps=["quality_tests"])

def scheduled_analysis_only():
    log.info("⏰ Scheduled trigger: ANALYSIS ONLY")
    run_pipeline(trigger="scheduled", steps=["analysis"])

def start_scheduler():
    """
    Start the background scheduler.

    Schedule:
      - Full pipeline:   Daily  at 02:00 UTC
      - Quality checks:  Every  6 hours
      - Analysis:        Every  Sunday at 04:00 UTC
    """
    log.info("Starting pipeline scheduler …")
    log.info("  Full pipeline  → daily at 02:00")
    log.info("  Quality tests  → every 6 hours")
    log.info("  Analysis       → every Sunday 04:00")

    schedule.every().day.at("02:00").do(scheduled_full_pipeline)
    schedule.every(6).hours.do(scheduled_quality_only)
    schedule.every().sunday.at("04:00").do(scheduled_analysis_only)

    log.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Orchestrator")
    parser.add_argument("--mode",   choices=["run", "schedule", "dry-run", "history"],
                        default="run")
    parser.add_argument("--steps",  nargs="*",
                        help="Specific steps to run (default: all)")
    args = parser.parse_args()

    if args.mode == "run":
        result = run_pipeline(trigger="manual", steps=args.steps)
        sys.exit(0 if result.status == "success" else 1)

    elif args.mode == "dry-run":
        result = run_pipeline(trigger="manual", steps=args.steps, dry_run=True)
        sys.exit(0)

    elif args.mode == "schedule":
        start_scheduler()

    elif args.mode == "history":
        history = get_run_history()
        if not history:
            print("No run history found.")
        else:
            print(f"\nLast {len(history)} pipeline runs:\n")
            for run in history[-10:]:
                icon = "✅" if run["status"] == "success" else "❌"
                print(f"  {icon} {run['run_id']}  {run['status']:<10}  "
                      f"{run['total_sec']:.0f}s  [{run['trigger']}]")
