"""
BACQE TICK RESEARCH - 11 Compare Tick Bars vs Tick Imbalance Bars - Multi Symbol

Compares fixed tick bars against fixed-threshold tick imbalance bars.

Inputs:
    data/processed/tick_research/tick_bars/symbol=<SYMBOL>
    data/processed/tick_research/tick_imbalance_bars/symbol=<SYMBOL>

Outputs:
    Per-symbol comparison files:
        data/analysis/tick_research/tick_vs_imbalance/symbol=<SYMBOL>/

    Master comparison files:
        data/analysis/tick_research/tick_vs_imbalance/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

BROKER = "FTMO"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]

TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars"
IMBALANCE_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars"

OUTPUT_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "tick_vs_imbalance"

# ==========================================================
# HELPERS
# ==========================================================

def normalise_bar_columns(bars: pd.DataFrame) -> pd.DataFrame:
    bars = bars.copy()

    # Standardise time column names
    if "bar_start_time" not in bars.columns and "start_time" in bars.columns:
        bars["bar_start_time"] = bars["start_time"]

    if "bar_end_time" not in bars.columns and "end_time" in bars.columns:
        bars["bar_end_time"] = bars["end_time"]

    # Standardise OHLC column names
    if "open" not in bars.columns and "open_mid" in bars.columns:
        bars["open"] = bars["open_mid"]

    if "high" not in bars.columns and "high_mid" in bars.columns:
        bars["high"] = bars["high_mid"]

    if "low" not in bars.columns and "low_mid" in bars.columns:
        bars["low"] = bars["low_mid"]

    if "close" not in bars.columns and "close_mid" in bars.columns:
        bars["close"] = bars["close_mid"]

    # Standardise spread column names
    if "avg_spread" not in bars.columns and "mean_spread" in bars.columns:
        bars["avg_spread"] = bars["mean_spread"]

    # Build duration if missing
    if "duration_seconds" not in bars.columns:
        if "bar_start_time" in bars.columns and "bar_end_time" in bars.columns:
            start = pd.to_datetime(bars["bar_start_time"], errors="coerce", utc=True)
            end = pd.to_datetime(bars["bar_end_time"], errors="coerce", utc=True)
            bars["duration_seconds"] = (end - start).dt.total_seconds()

    # Build missing derived columns
    if "range" not in bars.columns:
        bars["range"] = bars["high"] - bars["low"]

    if "return" not in bars.columns:
        bars["return"] = bars["close"].pct_change()

    if "log_return" not in bars.columns:
        bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))

    if "direction" not in bars.columns:
        bars["direction"] = 0
        bars.loc[bars["close"] > bars["open"], "direction"] = 1
        bars.loc[bars["close"] < bars["open"], "direction"] = -1

    return bars


def lag1_autocorr(series: pd.Series) -> float | None:
    clean = series.replace([np.inf, -np.inf], np.nan).dropna()

    if len(clean) < 3:
        return None

    return clean.autocorr(lag=1)


def load_tick_bars(symbol: str, tick_size: int) -> pd.DataFrame:
    path = (
        TICK_BAR_ROOT
        / f"symbol={symbol}"
        / f"tick_size={tick_size}"
        / f"{symbol}_tick_bars_{tick_size}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: tick bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)

    bars = normalise_bar_columns(bars)

    return bars


def load_imbalance_bars(symbol: str, threshold: int) -> pd.DataFrame:
    path = (
        IMBALANCE_BAR_ROOT
        / f"symbol={symbol}"
        / f"imbalance_threshold={threshold}"
        / f"{symbol}_tick_imbalance_bars_{threshold}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: imbalance bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)

    bars = normalise_bar_columns(bars)

    return bars


def summarise_bars(symbol: str, bars: pd.DataFrame) -> dict:
    returns = bars["return"].replace([np.inf, -np.inf], np.nan).dropna()
    log_returns = bars["log_return"].replace([np.inf, -np.inf], np.nan).dropna()

    direction_counts = bars["direction"].value_counts(dropna=False).to_dict()

    up_bars = direction_counts.get(1, 0)
    down_bars = direction_counts.get(-1, 0)
    flat_bars = direction_counts.get(0, 0)

    bar_count = len(bars)

    summary = {
        "symbol": symbol,
        "broker": BROKER,
        "bar_family": bars["bar_family"].iloc[0],
        "bar_type": bars["bar_type"].iloc[0],
        "bar_parameter": bars["bar_parameter"].iloc[0],
        "bar_count": bar_count,
        "first_bar_time": bars["bar_start_time"].min(),
        "last_bar_time": bars["bar_end_time"].max(),
        "avg_duration_seconds": bars["duration_seconds"].mean(),
        "median_duration_seconds": bars["duration_seconds"].median(),
        "min_duration_seconds": bars["duration_seconds"].min(),
        "max_duration_seconds": bars["duration_seconds"].max(),
        "avg_tick_count": bars["tick_count"].mean() if "tick_count" in bars.columns else None,
        "median_tick_count": bars["tick_count"].median() if "tick_count" in bars.columns else None,
        "min_tick_count": bars["tick_count"].min() if "tick_count" in bars.columns else None,
        "max_tick_count": bars["tick_count"].max() if "tick_count" in bars.columns else None,
        "avg_range": bars["range"].mean(),
        "median_range": bars["range"].median(),
        "max_range": bars["range"].max(),
        "avg_spread": bars["avg_spread"].mean(),
        "max_spread": bars["max_spread"].max(),
        "return_mean": returns.mean(),
        "return_std": returns.std(),
        "return_skew": returns.skew(),
        "return_kurtosis": returns.kurtosis(),
        "abs_return_mean": returns.abs().mean(),
        "log_return_mean": log_returns.mean(),
        "log_return_std": log_returns.std(),
        "log_return_skew": log_returns.skew(),
        "log_return_kurtosis": log_returns.kurtosis(),
        "lag1_return_autocorr": lag1_autocorr(returns),
        "up_bars": up_bars,
        "down_bars": down_bars,
        "flat_bars": flat_bars,
        "up_pct": round((up_bars / bar_count) * 100, 2) if bar_count else None,
        "down_pct": round((down_bars / bar_count) * 100, 2) if bar_count else None,
        "flat_pct": round((flat_bars / bar_count) * 100, 2) if bar_count else None,
        "comparison_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    if "imbalance_sum" in bars.columns:
        summary.update(
            {
                "avg_imbalance_sum": bars["imbalance_sum"].mean(),
                "median_imbalance_sum": bars["imbalance_sum"].median(),
                "avg_imbalance_abs": bars["imbalance_abs"].mean(),
                "median_imbalance_abs": bars["imbalance_abs"].median(),
                "avg_imbalance_ratio": bars["imbalance_ratio"].mean(),
                "median_imbalance_ratio": bars["imbalance_ratio"].median(),
                "positive_imbalance_pct": round((bars["imbalance_sum"] > 0).mean() * 100, 2),
                "negative_imbalance_pct": round((bars["imbalance_sum"] < 0).mean() * 100, 2),
            }
        )
    else:
        summary.update(
            {
                "avg_imbalance_sum": None,
                "median_imbalance_sum": None,
                "avg_imbalance_abs": None,
                "median_imbalance_abs": None,
                "avg_imbalance_ratio": None,
                "median_imbalance_ratio": None,
                "positive_imbalance_pct": None,
                "negative_imbalance_pct": None,
            }
        )

    return summary


def sort_comparison(comparison: pd.DataFrame) -> pd.DataFrame:
    order = {
        "tick_100": 1,
        "tick_250": 2,
        "tick_500": 3,
        "tick_1000": 4,
        "imbalance_25": 5,
        "imbalance_50": 6,
        "imbalance_100": 7,
        "imbalance_200": 8,
    }

    comparison["sort_order"] = comparison["bar_type"].map(order).fillna(999)

    return (
        comparison
        .sort_values(["symbol", "sort_order"])
        .drop(columns=["sort_order"])
        .reset_index(drop=True)
    )


def save_symbol_comparison(symbol: str, comparison: pd.DataFrame) -> None:
    output_dir = OUTPUT_ROOT / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{symbol}_tick_vs_imbalance_bar_comparison_latest.csv"
    parquet_path = output_dir / f"{symbol}_tick_vs_imbalance_bar_comparison_latest.parquet"

    comparison.to_csv(csv_path, index=False)
    comparison.to_parquet(parquet_path, index=False)

    print(f"[DONE] {symbol}: comparison saved.")
    print(f"       Rows:    {len(comparison):,}")
    print(f"       CSV:     {csv_path}")
    print(f"       Parquet: {parquet_path}")


def process_symbol(symbol: str) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    records = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(symbol, tick_size)

        if bars.empty:
            continue

        records.append(summarise_bars(symbol, bars))
        print(f"[DONE] {symbol}: loaded tick bars {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(symbol, threshold)

        if bars.empty:
            continue

        records.append(summarise_bars(symbol, bars))
        print(f"[DONE] {symbol}: loaded imbalance bars {threshold} | bars={len(bars):,}")

    if not records:
        print(f"[WARN] {symbol}: no comparison records created.")
        return pd.DataFrame()

    comparison = pd.DataFrame(records)

    numeric_cols = comparison.select_dtypes(include=["float", "int"]).columns
    comparison[numeric_cols] = comparison[numeric_cols].round(8)

    comparison = sort_comparison(comparison)

    save_symbol_comparison(symbol, comparison)

    display_cols = [
        "symbol",
        "bar_type",
        "bar_count",
        "avg_duration_seconds",
        "median_duration_seconds",
        "avg_tick_count",
        "median_tick_count",
        "avg_range",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "avg_imbalance_ratio",
        "positive_imbalance_pct",
        "negative_imbalance_pct",
    ]

    print(comparison[display_cols].to_string(index=False))

    return comparison


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 11 COMPARE TICK VS IMBALANCE BARS - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:              {BROKER}")
    print(f"Tick bar root:        {TICK_BAR_ROOT}")
    print(f"Imbalance bar root:   {IMBALANCE_BAR_ROOT}")
    print(f"Output root:          {OUTPUT_ROOT}")
    print(f"Symbols:              {SYMBOLS}")
    print(f"Tick sizes:           {TICK_SIZES}")
    print(f"Imbalance thresholds: {IMBALANCE_THRESHOLDS}")
    print("=" * 90)

    all_comparisons = []

    for symbol in SYMBOLS:
        comparison = process_symbol(symbol)

        if not comparison.empty:
            all_comparisons.append(comparison)

    if not all_comparisons:
        print("[WARN] No comparison files were created.")
        return

    master = pd.concat(all_comparisons, ignore_index=True)
    master = sort_comparison(master)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    master_csv = OUTPUT_ROOT / "tick_vs_imbalance_bar_comparison_latest.csv"
    master_parquet = OUTPUT_ROOT / "tick_vs_imbalance_bar_comparison_latest.parquet"

    master.to_csv(master_csv, index=False)
    master.to_parquet(master_parquet, index=False)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol tick vs imbalance comparison created.")
    print(f"Symbols compared: {master['symbol'].nunique()}")
    print(f"Rows:             {len(master):,}")
    print(f"CSV:              {master_csv}")
    print(f"Parquet:          {master_parquet}")
    print("=" * 90)


if __name__ == "__main__":
    main()