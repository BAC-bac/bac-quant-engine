"""
BACQE TICK RESEARCH - 11 Compare Tick Bars vs Tick Imbalance Bars

Compares:
    tick_1000
    imbalance_25
    imbalance_50
    imbalance_100
    imbalance_200

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_imbalance_bar_comparison_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_imbalance_bar_comparison_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_BAR_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "tick_bars"
    / f"symbol={SYMBOL}"
)

IMBALANCE_BAR_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "tick_imbalance_bars"
    / f"symbol={SYMBOL}"
)

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]


def lag1_autocorr(series: pd.Series) -> float | None:
    clean = series.replace([np.inf, -np.inf], np.nan).dropna()

    if len(clean) < 3:
        return None

    return clean.autocorr(lag=1)


def load_tick_bars(tick_size: int) -> pd.DataFrame:
    path = (
        TICK_BAR_ROOT
        / f"tick_size={tick_size}"
        / f"{SYMBOL}_tick_bars_{tick_size}_latest.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(f"Tick bar file not found: {path}")

    bars = pd.read_parquet(path)

    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)

    return bars


def load_imbalance_bars(threshold: int) -> pd.DataFrame:
    path = (
        IMBALANCE_BAR_ROOT
        / f"imbalance_threshold={threshold}"
        / f"{SYMBOL}_tick_imbalance_bars_{threshold}_latest.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(f"Imbalance bar file not found: {path}")

    bars = pd.read_parquet(path)

    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)

    return bars


def summarise_bars(bars: pd.DataFrame) -> dict:
    returns = bars["return"].replace([np.inf, -np.inf], np.nan).dropna()
    log_returns = bars["log_return"].replace([np.inf, -np.inf], np.nan).dropna()

    direction_counts = bars["direction"].value_counts(dropna=False).to_dict()

    up_bars = direction_counts.get(1, 0)
    down_bars = direction_counts.get(-1, 0)
    flat_bars = direction_counts.get(0, 0)

    bar_count = len(bars)

    summary = {
        "symbol": SYMBOL,
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


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 11 COMPARE TICK VS IMBALANCE BARS")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Broker: {BROKER}")
    print("-" * 90)

    records = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(tick_size)
        records.append(summarise_bars(bars))
        print(f"[DONE] Loaded tick bars: {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(threshold)
        records.append(summarise_bars(bars))
        print(f"[DONE] Loaded imbalance bars: {threshold} | bars={len(bars):,}")

    comparison = pd.DataFrame(records)

    numeric_cols = comparison.select_dtypes(include=["float", "int"]).columns
    comparison[numeric_cols] = comparison[numeric_cols].round(8)

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

    comparison = comparison.sort_values("sort_order").drop(columns=["sort_order"]).reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "tick_vs_imbalance_bar_comparison_latest.csv"
    parquet_path = OUTPUT_DIR / "tick_vs_imbalance_bar_comparison_latest.parquet"

    comparison.to_csv(csv_path, index=False)
    comparison.to_parquet(parquet_path, index=False)

    print("-" * 90)
    print("[DONE] Tick vs imbalance bar comparison created.")
    print(f"Rows:      {len(comparison):,}")
    print(f"CSV:       {csv_path}")
    print(f"Parquet:   {parquet_path}")
    print("-" * 90)

    display_cols = [
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


if __name__ == "__main__":
    main()