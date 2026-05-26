"""
BACQE TICK RESEARCH - 33 Run Live Microstructure State Cycle

Runs the current microstructure intelligence chain:

    29_build_state_transition_engine.py
    30_build_state_forecast_engine.py
    31_score_current_microstructure_state.py
    32_build_live_state_dashboard.py

This creates a repeatable one-command refresh cycle.
"""

from pathlib import Path
from datetime import datetime, timezone
import subprocess
import sys
import json


PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")

SCRIPTS = [
    "scripts/tick_research/29_build_state_transition_engine.py",
    "scripts/tick_research/30_build_state_forecast_engine.py",
    "scripts/tick_research/31_score_current_microstructure_state.py",
    "scripts/tick_research/32_build_live_state_dashboard.py",
]

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LOG_DIR = PROJECT_ROOT / "logs" / "tick_research"
REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "live_state_cycle"


def run_script(script_path: Path) -> dict:
    start_time = datetime.now(timezone.utc)

    print("-" * 100)
    print(f"[RUN] {script_path}")
    print("-" * 100)

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
        "stdout_tail": result.stdout[-3000:] if result.stdout else "",
        "stderr_tail": result.stderr[-3000:] if result.stderr else "",
    }


def main() -> None:
    cycle_start = datetime.now(timezone.utc)

    print("=" * 100)
    print("BACQE TICK RESEARCH - 33 LIVE MICROSTRUCTURE STATE CYCLE")
    print("=" * 100)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Cycle start:  {cycle_start.isoformat()}")
    print("=" * 100)

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

    cycle_status = "success" if successful == len(SCRIPTS) and failed == 0 and missing == 0 else "failed"

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

    latest_json = REPORT_DIR / "live_microstructure_state_cycle_latest.json"
    latest_txt = REPORT_DIR / "live_microstructure_state_cycle_latest.txt"

    timestamped_json = REPORT_DIR / f"live_microstructure_state_cycle_{timestamp}.json"
    timestamped_txt = REPORT_DIR / f"live_microstructure_state_cycle_{timestamp}.txt"

    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    with open(timestamped_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, default=str)

    lines = []
    lines.append("=" * 100)
    lines.append("BACQE LIVE MICROSTRUCTURE STATE CYCLE SUMMARY")
    lines.append("=" * 100)
    lines.append(f"Cycle status:          {cycle_status}")
    lines.append(f"Cycle start UTC:       {cycle_start.isoformat()}")
    lines.append(f"Cycle end UTC:         {cycle_end.isoformat()}")
    lines.append(f"Total elapsed seconds: {total_elapsed:.2f}")
    lines.append(f"Scripts total:         {len(SCRIPTS)}")
    lines.append(f"Scripts successful:    {successful}")
    lines.append(f"Scripts failed:        {failed}")
    lines.append(f"Scripts missing:       {missing}")
    lines.append("-" * 100)

    for record in records:
        lines.append(
            f"{record['status'].upper():<8} | "
            f"{record['elapsed_seconds']:.2f}s | "
            f"{record['script']}"
        )

    lines.append("=" * 100)

    text_report = "\n".join(lines)

    latest_txt.write_text(text_report, encoding="utf-8")
    timestamped_txt.write_text(text_report, encoding="utf-8")

    print(text_report)

    print("[DONE] Live microstructure state cycle complete.")
    print(f"Latest JSON: {latest_json}")
    print(f"Latest TXT:  {latest_txt}")
    print(f"Archive JSON: {timestamped_json}")
    print(f"Archive TXT:  {timestamped_txt}")
    print("=" * 100)

    if cycle_status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()