"""
BACQE REGIME ENGINE - 50 Build BACQE Operator Cycle

Runs the current BACQE market-intelligence dashboard chain:

    47_build_current_regime_dashboard.py
    48_build_combined_market_intelligence_dashboard.py
    49_build_market_regime_alignment_engine.py

This creates a repeatable one-command refresh cycle for the regime/operator layer.
"""

from pathlib import Path
from datetime import datetime, timezone
import subprocess
import sys
import json


PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SCRIPTS = [
    "scripts/regimes/47_build_current_regime_dashboard.py",
    "scripts/regimes/48_build_combined_market_intelligence_dashboard.py",
    "scripts/regimes/49_build_market_regime_alignment_engine.py",
]

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"
REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_operator_cycle"


def run_script(script_path: Path) -> dict:
    start_time = datetime.now(timezone.utc)

    print("-" * 110)
    print(f"[RUN] {script_path}")
    print("-" * 110)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    end_time = datetime.now(timezone.utc)
    elapsed_seconds = (end_time - start_time).total_seconds()

    status = "success" if result.returncode == 0 else "failed"

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print("[STDERR]")
        print(result.stderr)

    return {
        "script": str(script_path),
        "status": status,
        "returncode": result.returncode,
        "start_time_utc": start_time.isoformat(),
        "end_time_utc": end_time.isoformat(),
        "elapsed_seconds": elapsed_seconds,
        "stdout_tail": result.stdout[-4000:] if result.stdout else "",
        "stderr_tail": result.stderr[-4000:] if result.stderr else "",
    }


def build_text_report(summary: dict) -> str:
    lines = []

    lines.append("=" * 110)
    lines.append("BACQE OPERATOR CYCLE SUMMARY")
    lines.append("=" * 110)
    lines.append(f"Cycle status:          {summary['cycle_status']}")
    lines.append(f"Cycle start UTC:       {summary['cycle_start_utc']}")
    lines.append(f"Cycle end UTC:         {summary['cycle_end_utc']}")
    lines.append(f"Total elapsed seconds: {summary['total_elapsed_seconds']:.2f}")
    lines.append(f"Scripts total:         {summary['scripts_total']}")
    lines.append(f"Scripts successful:    {summary['scripts_successful']}")
    lines.append(f"Scripts failed:        {summary['scripts_failed']}")
    lines.append(f"Scripts missing:       {summary['scripts_missing']}")
    lines.append("-" * 110)

    for record in summary["records"]:
        lines.append(
            f"{record['status'].upper():<8} | "
            f"{record['elapsed_seconds']:.2f}s | "
            f"{record['script']}"
        )

    lines.append("=" * 110)

    return "\n".join(lines)


def main() -> None:
    cycle_start = datetime.now(timezone.utc)

    print("=" * 110)
    print("BACQE REGIME ENGINE - 50 BACQE OPERATOR CYCLE")
    print("=" * 110)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Cycle start:  {cycle_start.isoformat()}")
    print("=" * 110)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    records = []

    for script in SCRIPTS:
        script_path = PROJECT_ROOT / script

        if not script_path.exists():
            record = {
                "script": str(script_path),
                "status": "missing",
                "returncode": None,
                "start_time_utc": datetime.now(timezone.utc).isoformat(),
                "end_time_utc": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": 0,
                "stdout_tail": "",
                "stderr_tail": "Script file not found.",
            }
            records.append(record)
            print(f"[FAIL] Missing script: {script_path}")
            break

        record = run_script(script_path)
        records.append(record)

        if record["status"] != "success":
            print(f"[STOP] Cycle stopped because script failed: {script_path}")
            break

    cycle_end = datetime.now(timezone.utc)
    total_elapsed = (cycle_end - cycle_start).total_seconds()

    successful = sum(1 for r in records if r["status"] == "success")
    failed = sum(1 for r in records if r["status"] == "failed")
    missing = sum(1 for r in records if r["status"] == "missing")

    cycle_status = (
        "success"
        if successful == len(SCRIPTS) and failed == 0 and missing == 0
        else "failed"
    )

    summary = {
        "cycle_status": cycle_status,
        "cycle_start_utc": cycle_start.isoformat(),
        "cycle_end_utc": cycle_end.isoformat(),
        "total_elapsed_seconds": total_elapsed,
        "scripts_total": len(SCRIPTS),
        "scripts_successful": successful,
        "scripts_failed": failed,
        "scripts_missing": missing,
        "records": records,
    }

    timestamp = cycle_end.strftime("%Y%m%d_%H%M%S")

    latest_json = REPORT_DIR / "bacqe_operator_cycle_latest.json"
    latest_txt = REPORT_DIR / "bacqe_operator_cycle_latest.txt"

    archive_json = REPORT_DIR / f"bacqe_operator_cycle_{timestamp}.json"
    archive_txt = REPORT_DIR / f"bacqe_operator_cycle_{timestamp}.txt"

    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    with open(archive_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    text_report = build_text_report(summary)

    latest_txt.write_text(text_report, encoding="utf-8")
    archive_txt.write_text(text_report, encoding="utf-8")

    print(text_report)

    print("[DONE] BACQE operator cycle complete.")
    print(f"Latest JSON:  {latest_json}")
    print(f"Latest TXT:   {latest_txt}")
    print(f"Archive JSON: {archive_json}")
    print(f"Archive TXT:  {archive_txt}")
    print("=" * 110)

    if cycle_status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()