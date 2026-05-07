"""
07_run_regime_pipeline.py
=========================

BAC Quant Engine - Regime Engine
Stage 07: Master regime pipeline runner.

Purpose:
- Run the full BACQE regime pipeline sequentially
- Centralise execution logic
- Allow future scheduler integration
- Provide one-command regime rebuild/update

Pipeline Stages:
1. Audit market data
2. Build regime features
3. Classify regimes
4. Build summaries
5. Run diagnostics
6. Build transition matrices
7. Build forecast engine
8. Build dashboard tables
"""

from pathlib import Path
from datetime import datetime
import subprocess
import logging
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_path = LOG_DIR / f"run_regime_pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


PIPELINE_STAGES = [
    {
        "name": "01b_audit_market_data_lake",
        "script": "scripts/regimes/01b_audit_market_data_lake.py",
    },
    {
        "name": "02_build_regime_features",
        "script": "scripts/regimes/02_build_regime_features.py",
    },
    {
        "name": "03_classify_regimes",
        "script": "scripts/regimes/03_classify_regimes.py",
    },
    {
        "name": "03b_summarise_regime_classifications",
        "script": "scripts/regimes/03b_summarise_regime_classifications.py",
    },
    {
        "name": "03c_analyse_regime_summary",
        "script": "scripts/regimes/03c_analyse_regime_summary.py",
    },
    {
        "name": "04_regime_transition_matrix",
        "script": "scripts/regimes/04_regime_transition_matrix.py",
    },
    {
        "name": "05_regime_forecast_engine",
        "script": "scripts/regimes/05_regime_forecast_engine.py",
    },
    {
        "name": "06_latest_regime_dashboard_table",
        "script": "scripts/regimes/06_latest_regime_dashboard_table.py",
    },
]


def run_stage(stage_number: int, stage: dict) -> bool:
    name = stage["name"]
    script = stage["script"]

    logger.info("=" * 80)
    logger.info(f"Starting Stage {stage_number}: {name}")
    logger.info(f"Script: {script}")

    start = time.time()

    try:
        result = subprocess.run(
            [sys.executable, script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        elapsed = round(time.time() - start, 2)

        if result.stdout:
            logger.info("STDOUT:")
            logger.info(result.stdout)

        if result.stderr:
            logger.warning("STDERR:")
            logger.warning(result.stderr)

        if result.returncode != 0:
            logger.error(
                f"Stage {stage_number} FAILED "
                f"(return code {result.returncode}) "
                f"after {elapsed}s"
            )
            return False

        logger.info(
            f"Stage {stage_number} completed successfully "
            f"in {elapsed}s"
        )

        return True

    except Exception as exc:
        logger.exception(
            f"Stage {stage_number} crashed with exception: {exc}"
        )
        return False


def main() -> None:
    logger.info("=" * 80)
    logger.info("BACQE REGIME PIPELINE STARTING")
    logger.info("=" * 80)

    pipeline_start = time.time()

    successful = 0
    failed = 0
    failed_stages = []

    for idx, stage in enumerate(PIPELINE_STAGES, start=1):
        ok = run_stage(idx, stage)

        if ok:
            successful += 1
        else:
            failed += 1
            failed_stages.append(stage["name"])

    total_elapsed = round(time.time() - pipeline_start, 2)

    logger.info("=" * 80)
    logger.info("BACQE REGIME PIPELINE COMPLETE")
    logger.info("=" * 80)

    logger.info(f"Total stages: {len(PIPELINE_STAGES)}")
    logger.info(f"Successful stages: {successful}")
    logger.info(f"Failed stages: {failed}")
    logger.info(f"Total elapsed seconds: {total_elapsed}")

    if failed_stages:
        logger.warning("Failed stage list:")
        for stage in failed_stages:
            logger.warning(f"- {stage}")

    else:
        logger.info("All pipeline stages completed successfully")


if __name__ == "__main__":
    main()