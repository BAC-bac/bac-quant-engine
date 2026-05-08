"""
08_run_regime_incremental.py

Runs selected parts of the BACQE Regime Engine by timeframe group.

Modes:
- full
- small
- medium
- large

This script is designed to prepare the Regime Engine for scheduling via:
- Windows Task Scheduler
- Linux cron
- manual command-line execution
"""

import argparse
import subprocess
from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[2]

REGIME_SCRIPTS_DIR = PROJECT_ROOT / "scripts" / "regimes"
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"
LOG_DIR.mkdir(parents=True, exist_ok=True)


PIPELINE_STAGES = [
    "01_download_market_data.py",
    "01b_audit_market_data_lake.py",
    "02_build_regime_features.py",
    "03_classify_regimes.py",
    "03b_summarise_regime_classifications.py",
    "03c_analyse_regime_summary.py",
    "04_regime_transition_matrix.py",
    "05_regime_forecast_engine.py",
    "06_latest_regime_dashboard_table.py",
]


def run_stage(script_name: str, mode: str) -> bool:
    script_path = REGIME_SCRIPTS_DIR / script_name

    if not script_path.exists():
        print(f"[ERROR] Missing script: {script_path}")
        return False

    print("=" * 80)
    print(f"[RUNNING] {script_name} | mode={mode}")
    print("=" * 80)

    result = subprocess.run(
        ["python", str(script_path), "--mode", mode],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        print(f"[FAILED] {script_name}")
        return False

    print(f"[OK] {script_name}")
    return True


def run_pipeline(mode: str) -> None:
    start_time = datetime.now()

    print("=" * 80)
    print("BACQE REGIME INCREMENTAL PIPELINE")
    print("=" * 80)
    print(f"Mode: {mode}")
    print(f"Started: {start_time}")
    print("=" * 80)

    successful = 0
    failed = 0

    for stage in PIPELINE_STAGES:
        ok = run_stage(stage, mode)

        if ok:
            successful += 1
        else:
            failed += 1
            break

    end_time = datetime.now()
    elapsed = end_time - start_time

    print("=" * 80)
    print("BACQE REGIME INCREMENTAL PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Mode: {mode}")
    print(f"Successful stages: {successful}")
    print(f"Failed stages: {failed}")
    print(f"Elapsed: {elapsed}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Pipeline mode to run",
    )

    args = parser.parse_args()

    run_pipeline(args.mode)


if __name__ == "__main__":
    main()