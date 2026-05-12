"""
BACQE Script 16
Regime Incremental Update Ledger

Purpose:
- Build a state ledger of regime pipeline files
- Track stage, broker, timeframe, symbol, file size, row count, latest timestamp
- Save timestamped and latest ledger files
- Provide the foundation for change detection in Script 17

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

REGIME_PROCESSED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes"
REGIME_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis"

LEDGER_DIR = REGIME_ANALYSIS_DIR / "regime_incremental_ledger"

WATCH_FOLDERS = {
    "features": REGIME_PROCESSED_DIR / "features",
    "classified": REGIME_PROCESSED_DIR / "classified",
    "recent_features": REGIME_PROCESSED_DIR / "recent" / "features",
    "recent_classified": REGIME_PROCESSED_DIR / "recent" / "classified",
    "signal_router": REGIME_ANALYSIS_DIR / "regime_signal_router",
    "strategy_mapping": REGIME_ANALYSIS_DIR / "regime_strategy_mapping",
    "strategy_performance": REGIME_ANALYSIS_DIR / "regime_strategy_performance",
    "router_validation": REGIME_ANALYSIS_DIR / "regime_router_validation",
}

SUPPORTED_EXTENSIONS = {".parquet", ".csv"}


# ============================================================
# HELPERS
# ============================================================

def bytes_to_mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 4)


def get_modified_time(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def infer_parts(stage: str, path: Path, base_folder: Path) -> dict:
    """
    Attempts to infer broker, timeframe, and symbol from paths like:

    E:/Quant_Lab/data/processed/regimes/features/FTMO/M15/GBPUSD_M15_features.parquet

    Expected relative parts:
    FTMO / M15 / filename
    """
    rel_parts = path.relative_to(base_folder).parts

    broker = None
    timeframe = None
    symbol = None

    if len(rel_parts) >= 3:
        broker = rel_parts[0]
        timeframe = rel_parts[1]

    filename_stem = path.stem

    if timeframe and "_" in filename_stem:
        possible_symbol = filename_stem.split("_")[0]
        symbol = possible_symbol
    else:
        symbol = filename_stem

    return {
        "broker": broker,
        "timeframe": timeframe,
        "symbol": symbol,
        "relative_parts": "/".join(rel_parts),
    }


def count_parquet_rows_and_latest_timestamp(path: Path):
    """
    Counts parquet rows using pyarrow metadata when possible.
    Also attempts to find latest timestamp/date from common datetime columns.
    """
    rows = None
    latest_timestamp = None
    timestamp_column = None

    try:
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(path)
        rows = parquet_file.metadata.num_rows
    except Exception:
        rows = None

    try:
        sample = pd.read_parquet(path)

        if rows is None:
            rows = len(sample)

        datetime_candidates = [
            "timestamp",
            "time",
            "datetime",
            "date",
            "bar_time",
            "event_dt",
            "open_time",
        ]

        for col in datetime_candidates:
            if col in sample.columns:
                converted = pd.to_datetime(sample[col], errors="coerce")
                if converted.notna().any():
                    latest_timestamp = converted.max().isoformat()
                    timestamp_column = col
                    break

        if latest_timestamp is None and isinstance(sample.index, pd.DatetimeIndex):
            latest_timestamp = sample.index.max().isoformat()
            timestamp_column = "index"

    except Exception:
        pass

    return rows, latest_timestamp, timestamp_column


def count_csv_rows_and_latest_timestamp(path: Path):
    rows = None
    latest_timestamp = None
    timestamp_column = None

    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            rows = max(sum(1 for _ in f) - 1, 0)
    except Exception:
        rows = None

    try:
        sample = pd.read_csv(path)

        datetime_candidates = [
            "timestamp",
            "time",
            "datetime",
            "date",
            "bar_time",
            "event_dt",
            "open_time",
        ]

        for col in datetime_candidates:
            if col in sample.columns:
                converted = pd.to_datetime(sample[col], errors="coerce")
                if converted.notna().any():
                    latest_timestamp = converted.max().isoformat()
                    timestamp_column = col
                    break

    except Exception:
        pass

    return rows, latest_timestamp, timestamp_column


def inspect_file(stage: str, path: Path, base_folder: Path) -> dict:
    file_size_bytes = path.stat().st_size
    ext = path.suffix.lower()

    inferred = infer_parts(stage, path, base_folder)

    rows = None
    latest_timestamp = None
    timestamp_column = None

    if ext == ".parquet":
        rows, latest_timestamp, timestamp_column = count_parquet_rows_and_latest_timestamp(path)
    elif ext == ".csv":
        rows, latest_timestamp, timestamp_column = count_csv_rows_and_latest_timestamp(path)

    return {
        "scan_time": datetime.now().isoformat(timespec="seconds"),
        "stage": stage,
        "broker": inferred["broker"],
        "timeframe": inferred["timeframe"],
        "symbol": inferred["symbol"],
        "file_name": path.name,
        "file_path": str(path),
        "relative_parts": inferred["relative_parts"],
        "extension": ext,
        "size_mb": bytes_to_mb(file_size_bytes),
        "size_bytes": file_size_bytes,
        "rows": rows,
        "latest_timestamp": latest_timestamp,
        "timestamp_column": timestamp_column,
        "modified_time": get_modified_time(path),
    }


def scan_stage(stage: str, folder: Path) -> list[dict]:
    records = []

    if not folder.exists():
        records.append({
            "scan_time": datetime.now().isoformat(timespec="seconds"),
            "stage": stage,
            "broker": None,
            "timeframe": None,
            "symbol": None,
            "file_name": None,
            "file_path": str(folder),
            "relative_parts": None,
            "extension": None,
            "size_mb": 0,
            "size_bytes": 0,
            "rows": None,
            "latest_timestamp": None,
            "timestamp_column": None,
            "modified_time": None,
            "status": "missing_folder",
        })
        return records

    files = [
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    if not files:
        records.append({
            "scan_time": datetime.now().isoformat(timespec="seconds"),
            "stage": stage,
            "broker": None,
            "timeframe": None,
            "symbol": None,
            "file_name": None,
            "file_path": str(folder),
            "relative_parts": None,
            "extension": None,
            "size_mb": 0,
            "size_bytes": 0,
            "rows": None,
            "latest_timestamp": None,
            "timestamp_column": None,
            "modified_time": None,
            "status": "empty_folder",
        })
        return records

    for path in files:
        try:
            record = inspect_file(stage, path, folder)
            record["status"] = "ok"
        except Exception as exc:
            record = {
                "scan_time": datetime.now().isoformat(timespec="seconds"),
                "stage": stage,
                "broker": None,
                "timeframe": None,
                "symbol": path.stem,
                "file_name": path.name,
                "file_path": str(path),
                "relative_parts": None,
                "extension": path.suffix.lower(),
                "size_mb": None,
                "size_bytes": None,
                "rows": None,
                "latest_timestamp": None,
                "timestamp_column": None,
                "modified_time": None,
                "status": f"error: {exc}",
            }

        records.append(record)

    return records


def build_summary(ledger_df: pd.DataFrame) -> pd.DataFrame:
    valid = ledger_df[ledger_df["status"].eq("ok")].copy()

    if valid.empty:
        return pd.DataFrame()

    summary = (
        valid.groupby(["stage", "broker", "timeframe"], dropna=False)
        .agg(
            file_count=("file_name", "count"),
            total_size_mb=("size_mb", "sum"),
            total_rows=("rows", "sum"),
            latest_modified=("modified_time", "max"),
            latest_data_timestamp=("latest_timestamp", "max"),
        )
        .reset_index()
    )

    summary["total_size_mb"] = summary["total_size_mb"].round(4)

    return summary.sort_values(
        by=["stage", "broker", "timeframe"],
        ascending=True,
    )


def main():
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME INCREMENTAL UPDATE LEDGER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Data lake root: {DATA_LAKE_ROOT}")
    print(f"Ledger dir: {LEDGER_DIR}")
    print("-" * 90)

    all_records = []

    for stage, folder in WATCH_FOLDERS.items():
        print(f"[SCAN] {stage}: {folder}")
        all_records.extend(scan_stage(stage, folder))

    ledger_df = pd.DataFrame(all_records)
    summary_df = build_summary(ledger_df)

    ledger_timestamped_csv = LEDGER_DIR / f"regime_incremental_ledger_{run_timestamp}.csv"
    ledger_latest_csv = LEDGER_DIR / "regime_incremental_ledger_latest.csv"

    summary_timestamped_csv = LEDGER_DIR / f"regime_incremental_summary_{run_timestamp}.csv"
    summary_latest_csv = LEDGER_DIR / "regime_incremental_summary_latest.csv"

    ledger_json = LEDGER_DIR / f"regime_incremental_ledger_{run_timestamp}.json"

    ledger_df.to_csv(ledger_timestamped_csv, index=False)
    ledger_df.to_csv(ledger_latest_csv, index=False)

    if not summary_df.empty:
        summary_df.to_csv(summary_timestamped_csv, index=False)
        summary_df.to_csv(summary_latest_csv, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(PROJECT_ROOT),
        "data_lake_root": str(DATA_LAKE_ROOT),
        "watch_folders": {k: str(v) for k, v in WATCH_FOLDERS.items()},
        "ledger_timestamped_csv": str(ledger_timestamped_csv),
        "ledger_latest_csv": str(ledger_latest_csv),
        "summary_timestamped_csv": str(summary_timestamped_csv),
        "summary_latest_csv": str(summary_latest_csv),
        "record_count": int(len(ledger_df)),
        "ok_count": int(ledger_df["status"].eq("ok").sum()) if "status" in ledger_df.columns else 0,
        "error_count": int(ledger_df["status"].astype(str).str.startswith("error").sum()) if "status" in ledger_df.columns else 0,
        "missing_or_empty_count": int(ledger_df["status"].isin(["missing_folder", "empty_folder"]).sum()) if "status" in ledger_df.columns else 0,
        "next_recommended_step": (
            "Build Script 17 to compare latest ledger against previous timestamped ledger "
            "and detect changed symbol/timeframe/stage files."
        ),
    }

    with ledger_json.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Incremental ledger created.")
    print(f"Ledger timestamped CSV: {ledger_timestamped_csv}")
    print(f"Ledger latest CSV:      {ledger_latest_csv}")
    print(f"Summary latest CSV:     {summary_latest_csv}")
    print(f"Ledger JSON:            {ledger_json}")

    print("-" * 90)
    print(f"Total records: {len(ledger_df)}")

    if "status" in ledger_df.columns:
        print("\nStatus counts:")
        print(ledger_df["status"].value_counts(dropna=False).to_string())

    if not summary_df.empty:
        print("\nSummary preview:")
        print(summary_df.head(30).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()