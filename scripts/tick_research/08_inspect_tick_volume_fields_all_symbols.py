"""
BACQE TICK RESEARCH - 08 Inspect Tick Volume Fields All Symbols

Checks all captured MT5 tick parquet files to see whether volume, volume_real,
and last fields contain usable information.

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_volume_field_inspection_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_volume_field_inspection_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5" / "broker=FTMO"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

SYMBOLS = ["GBPUSD", "EURUSD", "GBPJPY", "EURGBP", "USDJPY", "XAUUSD"]

MAX_FILES_PER_SYMBOL = None
# For quick testing:
# MAX_FILES_PER_SYMBOL = 200


def inspect_symbol(symbol: str) -> dict:
    symbol_dir = TICK_ROOT / f"symbol={symbol}"

    if not symbol_dir.exists():
        return {
            "symbol": symbol,
            "status": "missing_symbol_dir",
            "file_count": 0,
            "row_count": 0,
            "volume_present": False,
            "volume_real_present": False,
            "last_present": False,
            "volume_positive_count": 0,
            "volume_real_positive_count": 0,
            "last_positive_count": 0,
            "volume_sum": 0.0,
            "volume_real_sum": 0.0,
            "last_nonzero_sum": 0.0,
            "volume_unique_values_sample": None,
            "volume_real_unique_values_sample": None,
            "last_unique_values_sample": None,
            "first_tick_time": None,
            "last_tick_time": None,
            "error_message": "Symbol directory not found",
        }

    files = sorted(symbol_dir.rglob("*.parquet"))

    if MAX_FILES_PER_SYMBOL is not None:
        files = files[:MAX_FILES_PER_SYMBOL]

    total_rows = 0

    volume_present = False
    volume_real_present = False
    last_present = False

    volume_positive_count = 0
    volume_real_positive_count = 0
    last_positive_count = 0

    volume_sum = 0.0
    volume_real_sum = 0.0
    last_nonzero_sum = 0.0

    volume_unique_values = set()
    volume_real_unique_values = set()
    last_unique_values = set()

    first_tick_time = None
    last_tick_time = None

    failed_files = 0
    error_samples = []

    for i, file_path in enumerate(files, start=1):
        try:
            df = pd.read_parquet(file_path)

            total_rows += len(df)

            if "time_msc_dt" in df.columns:
                times = pd.to_datetime(df["time_msc_dt"], errors="coerce", utc=True)

                if times.notna().any():
                    file_first = times.min()
                    file_last = times.max()

                    first_tick_time = file_first if first_tick_time is None else min(first_tick_time, file_first)
                    last_tick_time = file_last if last_tick_time is None else max(last_tick_time, file_last)

            if "volume" in df.columns:
                volume_present = True
                vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
                volume_positive_count += int((vol > 0).sum())
                volume_sum += float(vol.sum())

                for value in vol.dropna().unique()[:20]:
                    volume_unique_values.add(float(value))

            if "volume_real" in df.columns:
                volume_real_present = True
                vol_real = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)
                volume_real_positive_count += int((vol_real > 0).sum())
                volume_real_sum += float(vol_real.sum())

                for value in vol_real.dropna().unique()[:20]:
                    volume_real_unique_values.add(float(value))

            if "last" in df.columns:
                last_present = True
                last = pd.to_numeric(df["last"], errors="coerce").fillna(0)
                last_positive_count += int((last > 0).sum())
                last_nonzero_sum += float(last[last > 0].sum())

                for value in last.dropna().unique()[:20]:
                    last_unique_values.add(float(value))

        except Exception as exc:
            failed_files += 1

            if len(error_samples) < 5:
                error_samples.append(f"{file_path.name}: {exc}")

        if i % 500 == 0:
            print(f"[INFO] {symbol}: inspected {i:,}/{len(files):,} files")

    return {
        "symbol": symbol,
        "status": "success" if failed_files == 0 else "partial_success",
        "file_count": len(files),
        "failed_files": failed_files,
        "row_count": total_rows,
        "volume_present": volume_present,
        "volume_real_present": volume_real_present,
        "last_present": last_present,
        "volume_positive_count": volume_positive_count,
        "volume_real_positive_count": volume_real_positive_count,
        "last_positive_count": last_positive_count,
        "volume_positive_pct": round((volume_positive_count / total_rows) * 100, 6) if total_rows else 0,
        "volume_real_positive_pct": round((volume_real_positive_count / total_rows) * 100, 6) if total_rows else 0,
        "last_positive_pct": round((last_positive_count / total_rows) * 100, 6) if total_rows else 0,
        "volume_sum": volume_sum,
        "volume_real_sum": volume_real_sum,
        "last_nonzero_sum": last_nonzero_sum,
        "volume_unique_values_sample": "|".join(map(str, sorted(volume_unique_values)[:20])),
        "volume_real_unique_values_sample": "|".join(map(str, sorted(volume_real_unique_values)[:20])),
        "last_unique_values_sample": "|".join(map(str, sorted(last_unique_values)[:20])),
        "first_tick_time": first_tick_time.isoformat() if first_tick_time is not None else None,
        "last_tick_time": last_tick_time.isoformat() if last_tick_time is not None else None,
        "error_message": " | ".join(error_samples) if error_samples else None,
        "inspection_time_utc": datetime.now(timezone.utc).isoformat(),
    }


def classify_usability(row: pd.Series) -> str:
    if row["status"] not in {"success", "partial_success"}:
        return "unusable"

    if row["volume_real_positive_count"] > 0:
        return "volume_real_usable"

    if row["volume_positive_count"] > 0:
        return "volume_usable"

    if row["last_positive_count"] > 0:
        return "last_price_usable"

    return "tick_only"


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 08 INSPECT TICK VOLUME FIELDS ALL SYMBOLS")
    print("=" * 90)
    print(f"Tick root:  {TICK_ROOT}")
    print(f"Output dir: {OUTPUT_DIR}")
    print("-" * 90)

    records = []

    for symbol in SYMBOLS:
        print(f"[RUN] Inspecting {symbol}")
        records.append(inspect_symbol(symbol))

    inspection = pd.DataFrame(records)

    inspection["volume_usability"] = inspection.apply(classify_usability, axis=1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "tick_volume_field_inspection_latest.csv"
    parquet_path = OUTPUT_DIR / "tick_volume_field_inspection_latest.parquet"

    inspection.to_csv(csv_path, index=False)
    inspection.to_parquet(parquet_path, index=False)

    print("-" * 90)
    print("[DONE] Tick volume field inspection complete.")
    print(f"Rows:      {len(inspection):,}")
    print(f"CSV:       {csv_path}")
    print(f"Parquet:   {parquet_path}")
    print("-" * 90)

    display_cols = [
        "symbol",
        "file_count",
        "row_count",
        "volume_positive_count",
        "volume_real_positive_count",
        "last_positive_count",
        "volume_usability",
    ]

    print(inspection[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()