"""
BACQE DATA REGISTRY - 05 Run Data Registry Cycle

Runs the full BACQE data registry workflow:

    01_scan_data_lake.py
    02_profile_datasets.py
    03_build_dataset_registry.py
    04_generate_data_quality_report.py

This becomes the one-command health check for the BACQE data lake.
"""

from pathlib import Path
from datetime import datetime, timezone
import subprocess
import sys
import time


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")

SCRIPT_DIR = PROJECT_ROOT / "scripts" / "data_registry"

SCRIPTS = [
    "01_scan_data_lake.py",
    "02_profile_datasets.py",
    "03_build_dataset_registry.py",
    "04_generate_data_quality_report.py",
]

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
LOG_DIR = PROJECT_ROOT / "logs" / "data_registry"


# =============================================================================
# HELPERS
# =============================================================================

def run_stage(script_name: str) -> dict:
    """
    Run a single registry stage.
    """

    script_path = SCRIPT_DIR / script_name

    if not script_path.exists():
        return {
            "script": script_name,
            "status": "failed",
            "elapsed_seconds": 0,
            "return_code": None,
            "error": f"Script not found: {script_path}",
        }

    print("=" * 90)
    print(f"[RUN] {script_name}")
    print("=" * 90)

    start = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        text=True,
    )

    elapsed = round(time.time() - start, 2)

    status = "success" if result.returncode == 0 else "failed"

    return {
        "script": script_name,
        "status": status,
        "elapsed_seconds": elapsed,
        "return_code": result.returncode,
        "error": None if status == "success" else f"Return code: {result.returncode}",
    }


def write_cycle_log(results: list[dict]) -> Path:
    """
    Write a simple cycle log.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"data_registry_cycle_{timestamp}.log"
    latest_log_path = LOG_DIR / "data_registry_cycle_latest.log"

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE DATA REGISTRY CYCLE LOG")
    lines.append("=" * 90)
    lines.append(f"Run time UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Project root: {PROJECT_ROOT}")
    lines.append(f"Data lake:    {DATA_LAKE_ROOT}")
    lines.append("-" * 90)

    for result in results:
        lines.append(
            f"{result['script']} | "
            f"status={result['status']} | "
            f"elapsed={result['elapsed_seconds']}s | "
            f"return_code={result['return_code']} | "
            f"error={result['error']}"
        )

    total_elapsed = round(sum(r["elapsed_seconds"] for r in results), 2)
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")

    lines.append("-" * 90)
    lines.append(f"Total stages:        {len(results)}")
    lines.append(f"Successful stages:   {successful}")
    lines.append(f"Failed stages:       {failed}")
    lines.append(f"Total elapsed sec:   {total_elapsed}")
    lines.append("=" * 90)

    log_text = "\n".join(lines)

    log_path.write_text(log_text, encoding="utf-8")
    latest_log_path.write_text(log_text, encoding="utf-8")

    return log_path


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 90)
    print("BACQE DATA REGISTRY - 05 RUN FULL DATA REGISTRY CYCLE")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Script dir:   {SCRIPT_DIR}")
    print(f"Data lake:    {DATA_LAKE_ROOT}")
    print("-" * 90)

    results = []

    cycle_start = time.time()

    for script in SCRIPTS:
        result = run_stage(script)
        results.append(result)

        if result["status"] != "success":
            print("-" * 90)
            print(f"[FAILED] Stopping registry cycle at: {script}")
            print(f"Reason: {result['error']}")
            print("-" * 90)
            break

    cycle_elapsed = round(time.time() - cycle_start, 2)

    log_path = write_cycle_log(results)

    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")

    print("=" * 90)
    print("BACQE DATA REGISTRY CYCLE COMPLETE")
    print("=" * 90)
    print(f"Total stages attempted: {len(results)}")
    print(f"Successful stages:      {successful}")
    print(f"Failed stages:          {failed}")
    print(f"Total elapsed seconds:  {cycle_elapsed}")
    print(f"Cycle log:              {log_path}")
    print("=" * 90)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()