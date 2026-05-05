from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path


DATA_LAKE = Path("/mnt/quant_lab")
MAX_AGE_HOURS = 30

REQUIRED_OUTPUTS = {
    "rpg_latest_tips": DATA_LAKE / "raw/rpg_tips/latest_rpg_tips.csv",
    "rpg_history": DATA_LAKE / "raw/rpg_tips/rpg_tips_history.csv",
    "results_curated": DATA_LAKE / "curated/results_curated.parquet",
    "race_features": DATA_LAKE / "curated/race_features.parquet",
    "tips_results_merged": DATA_LAKE / "analysis/tips_results_merged.parquet",
    "pipeline_status": DATA_LAKE / "meta/run_logs/pipeline_status.csv",
}


def file_age_hours(path: Path) -> float:
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    return (datetime.now() - modified).total_seconds() / 3600


def main() -> None:
    failures = []

    print("Greyhound live output validation")
    print("=" * 40)

    for name, path in REQUIRED_OUTPUTS.items():
        if not path.exists():
            failures.append(f"{name}: missing file: {path}")
            print(f"[FAIL] {name}: missing")
            continue

        age = file_age_hours(path)
        size = path.stat().st_size

        if size == 0:
            failures.append(f"{name}: empty file: {path}")
            print(f"[FAIL] {name}: empty")
            continue

        if age > MAX_AGE_HOURS:
            failures.append(f"{name}: stale file, age={age:.2f} hours: {path}")
            print(f"[FAIL] {name}: stale ({age:.2f}h)")
            continue

        print(f"[OK] {name}: age={age:.2f}h size={size:,} bytes")

    if failures:
        print("\nValidation failed:")
        for failure in failures:
            print(f"- {failure}")
        raise SystemExit(1)

    print("\nValidation passed.")


if __name__ == "__main__":
    main()
