"""
BACQE REGIME ENGINE - 66 BACQE Full Live Refresh Cycle
"""

from pathlib import Path
from datetime import datetime, timezone
import subprocess
import sys
import json


PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SCRIPTS = [
    "scripts/tick_research/33_run_live_microstructure_state_cycle.py",
    "scripts/regimes/50_build_bacqe_operator_cycle.py",
    "scripts/regimes/60_run_bacqe_adaptive_operator_cycle.py",
    "scripts/regimes/61_build_adaptive_strategy_selection_dashboard.py",
    "scripts/regimes/62_build_live_bacqe_status_monitor.py",
]

REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_full_live_refresh_cycle"
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"


def run_script(script_path: Path) -> dict:
    start_time = datetime.now(timezone.utc)

    print("-" * 150)
    print(f"[RUN] {script_path}")
    print("-" * 150)

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
        "stdout_tail": result.stdout[-6000:] if result.stdout else "",
        "stderr_tail": result.stderr[-6000:] if result.stderr else "",
    }


def build_text_report(summary: dict) -> str:
    lines = []

    lines.append("=" * 150)
    lines.append("BACQE ADAPTIVE OPERATOR CYCLE SUMMARY")
    lines.append("=" * 150)
    lines.append(f"Cycle status:          {summary['cycle_status']}")
    lines.append(f"Cycle start UTC:       {summary['cycle_start_utc']}")
    lines.append(f"Cycle end UTC:         {summary['cycle_end_utc']}")
    lines.append(f"Total elapsed seconds: {summary['total_elapsed_seconds']:.2f}")
    lines.append(f"Scripts total:         {summary['scripts_total']}")
    lines.append(f"Scripts successful:    {summary['scripts_successful']}")
    lines.append(f"Scripts failed:        {summary['scripts_failed']}")
    lines.append(f"Scripts missing:       {summary['scripts_missing']}")
    lines.append("-" * 150)

    for record in summary["records"]:
        lines.append(
            f"{record['status'].upper():<8} | "
            f"{record['elapsed_seconds']:.2f}s | "
            f"{record['script']}"
        )

    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    cycle_start = datetime.now(timezone.utc)

    print("=" * 150)
    print("BACQE REGIME ENGINE - 60 BACQE ADAPTIVE OPERATOR CYCLE")
    print("=" * 150)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Cycle start:  {cycle_start.isoformat()}")
    print("=" * 150)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

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

    latest_json = REPORT_DIR / "bacqe_adaptive_operator_cycle_latest.json"
    latest_txt = REPORT_DIR / "bacqe_adaptive_operator_cycle_latest.txt"

    archive_json = REPORT_DIR / f"bacqe_adaptive_operator_cycle_{timestamp}.json"
    archive_txt = REPORT_DIR / f"bacqe_adaptive_operator_cycle_{timestamp}.txt"

    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    with open(archive_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    text_report = build_text_report(summary)

    latest_txt.write_text(text_report, encoding="utf-8")
    archive_txt.write_text(text_report, encoding="utf-8")

    print(text_report)

    print("[DONE] BACQE adaptive operator cycle complete.")
    print(f"Latest JSON:  {latest_json}")
    print(f"Latest TXT:   {latest_txt}")
    print(f"Archive JSON: {archive_json}")
    print(f"Archive TXT:  {archive_txt}")
    print("=" * 150)

    if cycle_status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()