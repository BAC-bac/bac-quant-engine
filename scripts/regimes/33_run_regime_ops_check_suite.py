"""
BACQE Script 33
Run Regime Ops Check Suite

Purpose:
- Run the regime operational validation suite in one command
- Executes Scripts 27, 28, 29, 30, 31, and 32 in order
- Captures return codes, elapsed time, and latest status
- Produces a final ops-suite report

This script does not modify parquet production data.
"""

from pathlib import Path
from datetime import datetime
import subprocess
import sys
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger" / "ops_check_suite"

SCRIPTS_TO_RUN = [
    "scripts/regimes/27_incremental_sync_verification_report.py",
    "scripts/regimes/28_incremental_append_design_audit.py",
    "scripts/regimes/29_incremental_append_readiness_monitor.py",
    "scripts/regimes/30_incremental_append_dry_run_planner.py",
    "scripts/regimes/31_regime_engine_operational_health_check.py",
    "scripts/regimes/32_regime_engine_daily_status_reporter.py",
]


def run_script(script_path: str) -> dict:
    full_path = PROJECT_ROOT / script_path
    start = datetime.now()

    result = {
        "script": script_path,
        "script_path": str(full_path),
        "started_at": start.isoformat(timespec="seconds"),
        "finished_at": None,
        "elapsed_seconds": None,
        "return_code": None,
        "status": None,
        "stdout_tail": None,
        "stderr_tail": None,
    }

    if not full_path.exists():
        result["finished_at"] = datetime.now().isoformat(timespec="seconds")
        result["return_code"] = None
        result["status"] = "missing_script"
        result["stderr_tail"] = f"Missing script: {full_path}"
        return result

    completed = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )

    end = datetime.now()

    result["finished_at"] = end.isoformat(timespec="seconds")
    result["elapsed_seconds"] = round((end - start).total_seconds(), 3)
    result["return_code"] = completed.returncode
    result["status"] = "passed" if completed.returncode == 0 else "failed"

    stdout_lines = completed.stdout.splitlines()
    stderr_lines = completed.stderr.splitlines()

    result["stdout_tail"] = "\n".join(stdout_lines[-25:])
    result["stderr_tail"] = "\n".join(stderr_lines[-25:])

    return result


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME OPS CHECK SUITE")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    records = []

    for script in SCRIPTS_TO_RUN:
        print(f"[RUN] {script}")
        record = run_script(script)
        records.append(record)

        print(
            f"      status={record['status']} "
            f"return_code={record['return_code']} "
            f"elapsed={record['elapsed_seconds']}s"
        )

        if record["status"] != "passed":
            print("      [STOP] A script failed or was missing. Stopping suite.")
            break

    results_df = pd.DataFrame(records)

    failed_count = int(results_df["status"].isin(["failed", "missing_script"]).sum())
    passed_count = int(results_df["status"].eq("passed").sum())

    overall_status = "GREEN" if failed_count == 0 and passed_count == len(SCRIPTS_TO_RUN) else "RED"

    overall = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "scripts_expected": len(SCRIPTS_TO_RUN),
        "scripts_run": int(len(results_df)),
        "scripts_passed": passed_count,
        "scripts_failed_or_missing": failed_count,
        "total_elapsed_seconds": round(
            float(pd.to_numeric(results_df["elapsed_seconds"], errors="coerce").fillna(0).sum()),
            3,
        ),
        "next_recommended_step": (
            "If GREEN, regime operational checks are complete. "
            "If RED, inspect the failed script stdout/stderr tail in the suite report."
        ),
    }

    report_latest = OUTPUT_DIR / "regime_ops_check_suite_latest.csv"
    report_ts = OUTPUT_DIR / f"regime_ops_check_suite_{run_ts}.csv"

    overall_latest = OUTPUT_DIR / "regime_ops_check_suite_latest.json"
    overall_ts = OUTPUT_DIR / f"regime_ops_check_suite_{run_ts}.json"

    results_df.to_csv(report_latest, index=False)
    results_df.to_csv(report_ts, index=False)

    with overall_latest.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    with overall_ts.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    print("-" * 90)
    print("[DONE] Regime ops check suite complete.")
    print(f"Overall status: {overall_status}")
    print(f"Scripts passed: {passed_count} / {len(SCRIPTS_TO_RUN)}")
    print(f"Total elapsed seconds: {overall['total_elapsed_seconds']}")
    print(f"Report latest: {report_latest}")
    print(f"Overall JSON:  {overall_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()