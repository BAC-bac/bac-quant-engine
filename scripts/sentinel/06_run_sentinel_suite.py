from pathlib import Path
from datetime import datetime
import subprocess
import sys
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXE = sys.executable
QUANT_ROOT = Path("E:/Quant_Lab")

REPORT_DIR = QUANT_ROOT / "data/analysis/sentinel/sentinel_suite"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

SENTINEL_SCRIPTS = [
    "scripts/sentinel/01_check_market_data_health.py",
    "scripts/sentinel/02_check_feature_pipeline_health.py",
    "scripts/sentinel/03_check_regime_classification_health.py",
    "scripts/sentinel/04_check_regime_forecast_health.py",
    "scripts/sentinel/05_generate_sentinel_summary.py",
]


FRESHNESS_CHECKS = [
    ("mt5_ticks", QUANT_ROOT / "data/raw/ticks/mt5"),
    ("mt5_ohlcv_all", QUANT_ROOT / "data/raw/fx/mt5_ohlcv/FTMO"),
    ("mt5_ohlcv_H1", QUANT_ROOT / "data/raw/fx/mt5_ohlcv/FTMO/H1"),
    ("mt5_ohlcv_M15", QUANT_ROOT / "data/raw/fx/mt5_ohlcv/FTMO/M15"),
    ("mt5_ohlcv_D1", QUANT_ROOT / "data/raw/fx/mt5_ohlcv/FTMO/D1"),
    ("information_data", QUANT_ROOT / "data/raw/information_data"),
    ("processed_data", QUANT_ROOT / "data/processed"),
    ("analysis_data", QUANT_ROOT / "data/analysis"),
    ("microstructure", QUANT_ROOT / "data/analysis/microstructure"),
    ("sentinel", QUANT_ROOT / "data/analysis/sentinel"),
    ("data_registry", QUANT_ROOT / "data/analysis/data_registry"),
    ("active_greyhound_results", QUANT_ROOT / "raw/Greyhound Racing"),
    ("active_rpg_tips", QUANT_ROOT / "raw/rpg_tips"),
    ("sports_curated", QUANT_ROOT / "curated"),
    ("sports_analysis", QUANT_ROOT / "analysis"),
    ("sports_staging", QUANT_ROOT / "staging"),
    ("run_logs", QUANT_ROOT / "meta/run_logs"),
    ("legacy_greyhounds", QUANT_ROOT / "greyhounds"),
]


EXPECTED_STALE = {
    "legacy_greyhounds",
    "information_data",  # expected until macro/info pipeline is automated properly
}


def freshness_label(age_hours: float | None, dataset: str) -> str:
    if dataset in EXPECTED_STALE:
        return "expected_stale"

    if age_hours is None:
        return "missing_or_empty"
    if age_hours <= 24:
        return "fresh"
    if age_hours <= 72:
        return "recent"
    if age_hours <= 168:
        return "stale_warning"
    return "stale_critical"


def scan_folder(dataset: str, folder: Path) -> dict:
    now = datetime.now()

    if not folder.exists():
        return {
            "dataset": dataset,
            "folder": str(folder),
            "status": "missing",
            "file_count": 0,
            "latest_file": None,
            "modified_time": None,
            "age_hours": None,
            "freshness_label": "missing",
        }

    files = [p for p in folder.rglob("*") if p.is_file()]

    if not files:
        return {
            "dataset": dataset,
            "folder": str(folder),
            "status": "empty",
            "file_count": 0,
            "latest_file": None,
            "modified_time": None,
            "age_hours": None,
            "freshness_label": "empty",
        }

    latest_file = max(files, key=lambda p: p.stat().st_mtime)
    modified_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
    age_hours = round((now - modified_time).total_seconds() / 3600, 2)

    return {
        "dataset": dataset,
        "folder": str(folder),
        "status": "ok",
        "file_count": len(files),
        "latest_file": str(latest_file),
        "modified_time": modified_time,
        "age_hours": age_hours,
        "freshness_label": freshness_label(age_hours, dataset),
    }


def run_script(script_path: str) -> dict:
    full_path = PROJECT_ROOT / script_path
    started = datetime.now()

    print("-" * 90)
    print(f"[RUN] {script_path}")

    if not full_path.exists():
        print(f"[MISSING] {full_path}")
        return {
            "script": script_path,
            "status": "missing",
            "return_code": None,
            "started": started,
            "finished": datetime.now(),
            "duration_seconds": None,
        }

    result = subprocess.run(
        [PYTHON_EXE, str(full_path)],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )

    finished = datetime.now()
    duration = round((finished - started).total_seconds(), 2)

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print("[STDERR]")
        print(result.stderr)

    status = "success" if result.returncode == 0 else "failed"

    print(f"[{status.upper()}] {script_path} return_code={result.returncode} duration={duration}s")

    return {
        "script": script_path,
        "status": status,
        "return_code": result.returncode,
        "started": started,
        "finished": finished,
        "duration_seconds": duration,
    }


def run_freshness_audit() -> pd.DataFrame:
    print("-" * 90)
    print("[RUN] Data lake freshness audit")

    rows = [scan_folder(dataset, folder) for dataset, folder in FRESHNESS_CHECKS]
    df = pd.DataFrame(rows)

    latest_csv = REPORT_DIR / "data_lake_freshness_latest.csv"
    latest_json = REPORT_DIR / "data_lake_freshness_latest.json"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_csv = REPORT_DIR / f"data_lake_freshness_{timestamp}.csv"

    df.to_csv(latest_csv, index=False)
    df.to_csv(timestamped_csv, index=False)
    df.to_json(latest_json, orient="records", indent=2, date_format="iso")

    print(df[[
        "dataset",
        "status",
        "file_count",
        "modified_time",
        "age_hours",
        "freshness_label",
    ]])

    print(f"[SAVED] {latest_csv}")
    return df


def overall_health(script_df: pd.DataFrame, freshness_df: pd.DataFrame) -> str:
    failed_scripts = script_df[script_df["status"].isin(["failed", "missing"])]

    critical_freshness = freshness_df[
        freshness_df["freshness_label"].isin(["missing", "empty", "stale_critical"])
    ]

    if len(failed_scripts) > 0 or len(critical_freshness) > 0:
        return "FAIL"

    warnings = freshness_df[freshness_df["freshness_label"].isin(["stale_warning", "recent"])]

    if len(warnings) > 0:
        return "WARNING"

    return "PASS"


def main() -> None:
    print("=" * 90)
    print("BACQE SENTINEL 06 - FULL SENTINEL SUITE")
    print("=" * 90)

    started = datetime.now()
    print(f"Started: {started}")

    script_results = [run_script(script) for script in SENTINEL_SCRIPTS]
    script_df = pd.DataFrame(script_results)

    freshness_df = run_freshness_audit()

    health = overall_health(script_df, freshness_df)

    finished = datetime.now()
    duration = round((finished - started).total_seconds(), 2)

    suite_summary = {
        "started": started,
        "finished": finished,
        "duration_seconds": duration,
        "overall_health": health,
        "scripts_total": len(script_df),
        "scripts_success": int((script_df["status"] == "success").sum()),
        "scripts_failed": int((script_df["status"] == "failed").sum()),
        "scripts_missing": int((script_df["status"] == "missing").sum()),
        "freshness_checks_total": len(freshness_df),
        "freshness_fresh": int((freshness_df["freshness_label"] == "fresh").sum()),
        "freshness_recent": int((freshness_df["freshness_label"] == "recent").sum()),
        "freshness_warning": int((freshness_df["freshness_label"] == "stale_warning").sum()),
        "freshness_critical": int((freshness_df["freshness_label"] == "stale_critical").sum()),
        "freshness_expected_stale": int((freshness_df["freshness_label"] == "expected_stale").sum()),
    }

    summary_df = pd.DataFrame([suite_summary])

    summary_csv = REPORT_DIR / "sentinel_suite_latest.csv"
    summary_json = REPORT_DIR / "sentinel_suite_latest.json"

    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2, date_format="iso")

    print("-" * 90)
    print("SENTINEL SUITE SUMMARY")
    print("-" * 90)
    print(summary_df.T)
    print("-" * 90)
    print(f"[SAVED] {summary_csv}")
    print(f"[SAVED] {summary_json}")
    print("=" * 90)


if __name__ == "__main__":
    main()