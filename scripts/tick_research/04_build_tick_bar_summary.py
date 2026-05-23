"""
BACQE TICK RESEARCH - 04 Build Tick Bar Summary

Compares fixed-size tick bars for one symbol.

Input:
    E:/Quant_Lab/data/processed/tick_research/tick_bars/symbol=GBPUSD/tick_size=*/...

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_bar_summary_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_bar_summary_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"

INPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars" / f"symbol={SYMBOL}"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

OUTPUT_CSV = OUTPUT_DIR / "tick_bar_summary_latest.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "tick_bar_summary_latest.parquet"


def summarise_tick_bars(file_path: Path) -> dict:
    bars = pd.read_parquet(file_path)

    tick_size = int(bars["tick_size"].iloc[0]) if "tick_size" in bars.columns and len(bars) else None

    direction_counts = bars["direction"].value_counts(dropna=False).to_dict() if "direction" in bars.columns else {}

    up_bars = direction_counts.get(1, 0)
    down_bars = direction_counts.get(-1, 0)
    flat_bars = direction_counts.get(0, 0)

    bar_count = len(bars)

    return {
        "symbol": SYMBOL,
        "tick_size": tick_size,
        "bar_count": bar_count,
        "first_bar_time": bars["bar_start_time"].min() if "bar_start_time" in bars.columns else None,
        "last_bar_time": bars["bar_end_time"].max() if "bar_end_time" in bars.columns else None,
        "avg_duration_seconds": bars["duration_seconds"].mean(),
        "median_duration_seconds": bars["duration_seconds"].median(),
        "min_duration_seconds": bars["duration_seconds"].min(),
        "max_duration_seconds": bars["duration_seconds"].max(),
        "avg_range": bars["range"].mean(),
        "median_range": bars["range"].median(),
        "max_range": bars["range"].max(),
        "avg_spread": bars["avg_spread"].mean(),
        "max_spread": bars["max_spread"].max(),
        "return_std": bars["return"].std(),
        "log_return_std": bars["log_return"].std(),
        "absolute_return_mean": bars["return"].abs().mean(),
        "absolute_log_return_mean": bars["log_return"].abs().mean(),
        "up_bars": up_bars,
        "down_bars": down_bars,
        "flat_bars": flat_bars,
        "up_pct": round((up_bars / bar_count) * 100, 2) if bar_count else None,
        "down_pct": round((down_bars / bar_count) * 100, 2) if bar_count else None,
        "flat_pct": round((flat_bars / bar_count) * 100, 2) if bar_count else None,
        "source_file": str(file_path),
    }


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 04 BUILD TICK BAR SUMMARY")
    print("=" * 90)
    print(f"Symbol:     {SYMBOL}")
    print(f"Input root: {INPUT_ROOT}")
    print("-" * 90)

    if not INPUT_ROOT.exists():
        raise FileNotFoundError(f"Input root not found: {INPUT_ROOT}")

    files = sorted(INPUT_ROOT.rglob(f"{SYMBOL}_tick_bars_*_latest.parquet"))

    if not files:
        raise FileNotFoundError(f"No tick bar parquet files found under: {INPUT_ROOT}")

    print(f"Tick bar files found: {len(files)}")

    records = []

    for file_path in files:
        print(f"[LOAD] {file_path}")
        records.append(summarise_tick_bars(file_path))

    summary = pd.DataFrame(records)

    numeric_cols = [
        "avg_duration_seconds",
        "median_duration_seconds",
        "min_duration_seconds",
        "max_duration_seconds",
        "avg_range",
        "median_range",
        "max_range",
        "avg_spread",
        "max_spread",
        "return_std",
        "log_return_std",
        "absolute_return_mean",
        "absolute_log_return_mean",
    ]

    for col in numeric_cols:
        if col in summary.columns:
            summary[col] = summary[col].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = summary.sort_values("tick_size").reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary.to_csv(OUTPUT_CSV, index=False)
    summary.to_parquet(OUTPUT_PARQUET, index=False)

    print("-" * 90)
    print("[DONE] Tick bar summary created.")
    print(f"Rows:      {len(summary):,}")
    print(f"CSV:       {OUTPUT_CSV}")
    print(f"Parquet:   {OUTPUT_PARQUET}")
    print("-" * 90)

    display_cols = [
        "symbol",
        "tick_size",
        "bar_count",
        "avg_duration_seconds",
        "median_duration_seconds",
        "max_duration_seconds",
        "avg_range",
        "return_std",
        "up_pct",
        "down_pct",
        "flat_pct",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()