"""
08_run_regime_incremental.py

Runs selected parts of the BACQE Regime Engine by timeframe group.

Modes:
- full
- small
- medium
- large
"""

import argparse
import subprocess
import sys
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


def write_log(log_path: Path, message: str) -> None:
    print(message)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def run_stage(script_name: str, mode: str, log_path: Path) -> bool:
    script_path = REGIME_SCRIPTS_DIR / script_name

    if not script_path.exists():
        write_log(log_path, f"[ERROR] Missing script: {script_path}")
        return False

    stage_start = datetime.now()

    write_log(log_path, "=" * 80)
    write_log(log_path, f"[RUNNING] {script_name} | mode={mode}")
    write_log(log_path, f"Started: {stage_start}")
    write_log(log_path, "=" * 80)

    result = subprocess.run(
        [sys.executable, str(script_path), "--mode", mode],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )

    if result.stdout:
        write_log(log_path, result.stdout)

    if result.stderr:
        write_log(log_path, result.stderr)

    stage_end = datetime.now()
    elapsed = stage_end - stage_start

    if result.returncode != 0:
        write_log(log_path, f"[FAILED] {script_name}")
        write_log(log_path, f"Elapsed: {elapsed}")
        return False

    write_log(log_path, f"[OK] {script_name}")
    write_log(log_path, f"Elapsed: {elapsed}")

    return True


def run_pipeline(mode: str) -> None:
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")

    log_path = LOG_DIR / f"regime_incremental_pipeline_{mode}_{timestamp}.log"

    write_log(log_path, "=" * 80)
    write_log(log_path, "BACQE REGIME INCREMENTAL PIPELINE")
    write_log(log_path, "=" * 80)
    write_log(log_path, f"Mode: {mode}")
    write_log(log_path, f"Started: {start_time}")
    write_log(log_path, f"Project root: {PROJECT_ROOT}")
    write_log(log_path, f"Python executable: {sys.executable}")
    write_log(log_path, f"Pipeline log: {log_path}")
    write_log(log_path, "=" * 80)

    successful = 0
    failed = 0

    for stage in PIPELINE_STAGES:
        ok = run_stage(stage, mode, log_path)

        if ok:
            successful += 1
        else:
            failed += 1
            break

    end_time = datetime.now()
    elapsed = end_time - start_time

    write_log(log_path, "=" * 80)
    write_log(log_path, "BACQE REGIME INCREMENTAL PIPELINE COMPLETE")
    write_log(log_path, "=" * 80)
    write_log(log_path, f"Mode: {mode}")
    write_log(log_path, f"Successful stages: {successful}")
    write_log(log_path, f"Failed stages: {failed}")
    write_log(log_path, f"Finished: {end_time}")
    write_log(log_path, f"Total elapsed: {elapsed}")
    write_log(log_path, "=" * 80)

    if failed > 0:
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BACQE Regime Engine incremental pipeline."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Pipeline mode to run.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(args.mode)


if __name__ == "__main__":
    main()