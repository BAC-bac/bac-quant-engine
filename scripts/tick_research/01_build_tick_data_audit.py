"""
BACQE TICK RESEARCH - 01 Build Tick Data Audit

Audits collected MT5 tick parquet files.

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_data_audit_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_data_audit_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5"

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

REQUIRED_COLUMNS = {
    "time",
    "bid",
    "ask",
    "spread",
    "mid",
    "symbol",
    "broker",
    "capture_time_utc",
    "time_msc_dt",
}


def infer_partition_value(file_path: Path, key: str) -> str | None:
    for part in file_path.parts:
        part_lower = part.lower()
        if part_lower.startswith(f"{key.lower()}="):
            return part.split("=", 1)[1]
    return None


def profile_tick_file(file_path: Path) -> dict:
    try:
        df = pd.read_parquet(file_path)

        missing_cols = sorted(REQUIRED_COLUMNS - set(df.columns))

        symbol = infer_partition_value(file_path, "symbol")
        broker = infer_partition_value(file_path, "broker")

        time_col = "time_msc_dt" if "time_msc_dt" in df.columns else "time"

        tick_time = pd.to_datetime(df[time_col], errors="coerce", utc=True)

        row_count = len(df)
        duplicate_rows = int(df.duplicated().sum()) if row_count else 0

        bid_missing = df["bid"].isna().sum() if "bid" in df.columns else None
        ask_missing = df["ask"].isna().sum() if "ask" in df.columns else None
        spread_missing = df["spread"].isna().sum() if "spread" in df.columns else None

        avg_spread = df["spread"].mean() if "spread" in df.columns else None
        max_spread = df["spread"].max() if "spread" in df.columns else None
        min_spread = df["spread"].min() if "spread" in df.columns else None

        bad_spread_count = None
        if "spread" in df.columns:
            bad_spread_count = int((df["spread"] < 0).sum())

        min_time = tick_time.min().isoformat() if tick_time.notna().any() else None
        max_time = tick_time.max().isoformat() if tick_time.notna().any() else None

        duration_seconds = None
        if tick_time.notna().any():
            duration_seconds = round((tick_time.max() - tick_time.min()).total_seconds(), 2)

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "symbol": symbol,
            "broker": broker,
            "row_count": row_count,
            "column_count": len(df.columns),
            "missing_required_columns": "|".join(missing_cols),
            "read_status": "success",
            "min_tick_time": min_time,
            "max_tick_time": max_time,
            "duration_seconds": duration_seconds,
            "duplicate_rows": duplicate_rows,
            "bid_missing": bid_missing,
            "ask_missing": ask_missing,
            "spread_missing": spread_missing,
            "avg_spread": round(avg_spread, 6) if pd.notna(avg_spread) else None,
            "min_spread": round(min_spread, 6) if pd.notna(min_spread) else None,
            "max_spread": round(max_spread, 6) if pd.notna(max_spread) else None,
            "bad_spread_count": bad_spread_count,
            "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 4),
            "modified_time_utc": datetime.fromtimestamp(
                file_path.stat().st_mtime,
                tz=timezone.utc,
            ).isoformat(),
            "error_message": None,
        }

    except Exception as exc:
        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "symbol": infer_partition_value(file_path, "symbol"),
            "broker": infer_partition_value(file_path, "broker"),
            "row_count": None,
            "column_count": None,
            "missing_required_columns": None,
            "read_status": "failed",
            "min_tick_time": None,
            "max_tick_time": None,
            "duration_seconds": None,
            "duplicate_rows": None,
            "bid_missing": None,
            "ask_missing": None,
            "spread_missing": None,
            "avg_spread": None,
            "min_spread": None,
            "max_spread": None,
            "bad_spread_count": None,
            "file_size_mb": None,
            "modified_time_utc": None,
            "error_message": str(exc)[:500],
        }


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 01 BUILD TICK DATA AUDIT")
    print("=" * 90)
    print(f"Tick root:   {TICK_ROOT}")
    print(f"Output dir:  {OUTPUT_DIR}")
    print("-" * 90)

    if not TICK_ROOT.exists():
        raise FileNotFoundError(f"Tick root does not exist: {TICK_ROOT}")

    tick_files = sorted(TICK_ROOT.rglob("*.parquet"))

    print(f"Tick parquet files found: {len(tick_files):,}")
    print("-" * 90)

    records = []

    for i, file_path in enumerate(tick_files, start=1):
        records.append(profile_tick_file(file_path))

        if i % 500 == 0:
            print(f"[INFO] Audited {i:,}/{len(tick_files):,} files")

    audit = pd.DataFrame(records)

    audit["audit_time_utc"] = datetime.now(timezone.utc).isoformat()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "tick_data_audit_latest.csv"
    parquet_path = OUTPUT_DIR / "tick_data_audit_latest.parquet"

    audit.to_csv(csv_path, index=False)
    audit.to_parquet(parquet_path, index=False)

    print("-" * 90)
    print("[DONE] Tick data audit created.")
    print(f"Rows:      {len(audit):,}")
    print(f"CSV:       {csv_path}")
    print(f"Parquet:   {parquet_path}")
    print("-" * 90)

    print("\nRead status summary:")
    print(audit["read_status"].value_counts(dropna=False).to_string())

    print("\nSymbol summary:")
    print(audit["symbol"].value_counts(dropna=False).to_string())

    print("\nRows by symbol:")
    print(audit.groupby("symbol", dropna=False)["row_count"].sum().sort_values(ascending=False).to_string())


if __name__ == "__main__":
    main()