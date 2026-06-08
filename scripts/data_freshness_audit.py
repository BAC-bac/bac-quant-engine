from pathlib import Path
from datetime import datetime
import pandas as pd


ROOT = Path("E:/Quant_Lab")
OUTPUT_FILE = ROOT / "data_freshness_audit.csv"


CHECKS = [
    # BACQE market data
    ("mt5_ticks", ROOT / "data/raw/ticks/mt5"),
    ("mt5_ohlcv_all", ROOT / "data/raw/fx/mt5_ohlcv/FTMO"),
    ("mt5_ohlcv_H1", ROOT / "data/raw/fx/mt5_ohlcv/FTMO/H1"),
    ("mt5_ohlcv_M15", ROOT / "data/raw/fx/mt5_ohlcv/FTMO/M15"),
    ("mt5_ohlcv_D1", ROOT / "data/raw/fx/mt5_ohlcv/FTMO/D1"),

    # Information data
    ("information_data", ROOT / "data/raw/information_data"),

    # BACQE outputs
    ("processed_data", ROOT / "data/processed"),
    ("analysis_data", ROOT / "data/analysis"),
    ("microstructure", ROOT / "data/analysis/microstructure"),
    ("sentinel", ROOT / "data/analysis/sentinel"),
    ("data_registry", ROOT / "data/analysis/data_registry"),

    # Active sports / greyhound pipeline
    ("active_greyhound_results", ROOT / "raw/Greyhound Racing"),
    ("active_rpg_tips", ROOT / "raw/rpg_tips"),
    ("sports_curated", ROOT / "curated"),
    ("sports_analysis", ROOT / "analysis"),
    ("sports_staging", ROOT / "staging"),
    ("run_logs", ROOT / "meta/run_logs"),

    # Legacy folder
    ("legacy_greyhounds", ROOT / "greyhounds"),
]


def freshness_label(age_hours: float | None) -> str:
    if age_hours is None:
        return "missing_or_empty"
    if age_hours <= 24:
        return "fresh"
    if age_hours <= 72:
        return "recent"
    if age_hours <= 168:
        return "stale_warning"
    return "stale_critical"


def scan_folder(name: str, folder: Path) -> dict:
    now = datetime.now()

    if not folder.exists():
        return {
            "dataset": name,
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
            "dataset": name,
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
        "dataset": name,
        "folder": str(folder),
        "status": "ok",
        "file_count": len(files),
        "latest_file": str(latest_file),
        "modified_time": modified_time,
        "age_hours": age_hours,
        "freshness_label": freshness_label(age_hours),
    }


def main() -> None:
    print("=" * 90)
    print("BACQE DATA FRESHNESS AUDIT")
    print("=" * 90)

    rows = [scan_folder(name, folder) for name, folder in CHECKS]
    df = pd.DataFrame(rows)

    df = df.sort_values(
        by=["freshness_label", "age_hours"],
        ascending=[True, False],
        na_position="last",
    )

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 180)

    print(df[[
        "dataset",
        "status",
        "file_count",
        "modified_time",
        "age_hours",
        "freshness_label",
        "latest_file",
    ]])

    df.to_csv(OUTPUT_FILE, index=False)

    print("-" * 90)
    print(f"[SAVED] {OUTPUT_FILE}")
    print("=" * 90)


if __name__ == "__main__":
    main()