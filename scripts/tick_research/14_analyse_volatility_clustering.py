"""
BACQE TICK RESEARCH - 14 Analyse Volatility Clustering

Compares volatility clustering across fixed tick bars and tick imbalance bars.

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/volatility_clustering_analysis_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/volatility_clustering_analysis_latest.parquet
    E:/Quant_Lab/reports/tick_research/volatility_clustering/volatility_clustering_report_latest.txt
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

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "volatility_clustering"

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]

ROLLING_WINDOWS = [10, 25, 50]


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


def safe_autocorr(series: pd.Series, lag: int = 1) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()

    if len(clean) <= lag + 2:
        return None

    return clean.autocorr(lag=lag)


def calculate_clustering_metrics(bars: pd.DataFrame) -> dict:
    bars = bars.copy()

    returns = pd.to_numeric(bars["return"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    abs_returns = returns.abs()
    squared_returns = returns ** 2

    bar_count = len(bars)

    metrics = {
        "symbol": SYMBOL,
        "broker": BROKER,
        "bar_family": bars["bar_family"].iloc[0],
        "bar_type": bars["bar_type"].iloc[0],
        "bar_parameter": bars["bar_parameter"].iloc[0],
        "bar_count": bar_count,
        "first_bar_time": bars["bar_start_time"].min(),
        "last_bar_time": bars["bar_end_time"].max(),
        "return_std": returns.std(),
        "abs_return_mean": abs_returns.mean(),
        "squared_return_mean": squared_returns.mean(),
        "abs_return_autocorr_lag1": safe_autocorr(abs_returns, lag=1),
        "abs_return_autocorr_lag5": safe_autocorr(abs_returns, lag=5),
        "squared_return_autocorr_lag1": safe_autocorr(squared_returns, lag=1),
        "squared_return_autocorr_lag5": safe_autocorr(squared_returns, lag=5),
        "return_abs_to_std_ratio": abs_returns.mean() / returns.std() if returns.std() and returns.std() != 0 else np.nan,
        "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    for window in ROLLING_WINDOWS:
        rolling_vol = returns.rolling(window=window).std()
        rolling_abs = abs_returns.rolling(window=window).mean()

        metrics[f"rolling_vol_{window}_mean"] = rolling_vol.mean()
        metrics[f"rolling_vol_{window}_std"] = rolling_vol.std()
        metrics[f"rolling_vol_{window}_cv"] = (
            rolling_vol.std() / rolling_vol.mean()
            if rolling_vol.mean() and rolling_vol.mean() != 0
            else np.nan
        )

        metrics[f"rolling_abs_{window}_mean"] = rolling_abs.mean()
        metrics[f"rolling_abs_{window}_std"] = rolling_abs.std()
        metrics[f"rolling_abs_{window}_cv"] = (
            rolling_abs.std() / rolling_abs.mean()
            if rolling_abs.mean() and rolling_abs.mean() != 0
            else np.nan
        )

        metrics[f"rolling_vol_{window}_autocorr_lag1"] = safe_autocorr(rolling_vol, lag=1)
        metrics[f"rolling_abs_{window}_autocorr_lag1"] = safe_autocorr(rolling_abs, lag=1)

    return metrics


def build_report(analysis: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    ranking = analysis.copy()

    ranking["abs_return_cluster_score"] = pd.to_numeric(
        ranking["abs_return_autocorr_lag1"],
        errors="coerce",
    )

    ranking = ranking.sort_values(
        "abs_return_cluster_score",
        ascending=False,
        na_position="last",
    )

    display_cols = [
        "bar_type",
        "bar_family",
        "bar_count",
        "return_std",
        "abs_return_autocorr_lag1",
        "abs_return_autocorr_lag5",
        "squared_return_autocorr_lag1",
        "squared_return_autocorr_lag5",
        "rolling_vol_25_cv",
        "rolling_vol_25_autocorr_lag1",
        "rolling_abs_25_autocorr_lag1",
    ]

    available_cols = [col for col in display_cols if col in ranking.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - VOLATILITY CLUSTERING REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Symbol:          {SYMBOL}")
    lines.append(f"Broker:          {BROKER}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("VOLATILITY CLUSTERING RANKING")
    lines.append("-" * 90)
    lines.append(ranking[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("abs_return_autocorr_lag1 measures whether large absolute returns follow large absolute returns.")
    lines.append("squared_return_autocorr_lag1 is a classic volatility clustering proxy.")
    lines.append("rolling_vol_cv measures how unstable rolling volatility is relative to its mean.")
    lines.append("Higher autocorrelation suggests stronger volatility clustering.")
    lines.append("Lower rolling volatility CV suggests smoother volatility estimation.")
    lines.append("")
    lines.append("This report is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 14 ANALYSE VOLATILITY CLUSTERING")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Broker: {BROKER}")
    print("-" * 90)

    records = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(tick_size)
        records.append(calculate_clustering_metrics(bars))
        print(f"[DONE] Analysed tick bars: {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(threshold)
        records.append(calculate_clustering_metrics(bars))
        print(f"[DONE] Analysed imbalance bars: {threshold} | bars={len(bars):,}")

    analysis = pd.DataFrame(records)

    numeric_cols = analysis.select_dtypes(include=["float", "int"]).columns
    analysis[numeric_cols] = analysis[numeric_cols].round(8)

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

    analysis["sort_order"] = analysis["bar_type"].map(order).fillna(999)
    analysis = analysis.sort_values("sort_order").drop(columns=["sort_order"]).reset_index(drop=True)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "volatility_clustering_analysis_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "volatility_clustering_analysis_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "volatility_clustering_report_latest.txt"

    analysis.to_csv(csv_path, index=False)
    analysis.to_parquet(parquet_path, index=False)

    report = build_report(analysis)
    report_path.write_text(report, encoding="utf-8")

    print("-" * 90)
    print("[DONE] Volatility clustering analysis created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = [
        "bar_type",
        "bar_count",
        "return_std",
        "abs_return_autocorr_lag1",
        "squared_return_autocorr_lag1",
        "rolling_vol_25_cv",
        "rolling_vol_25_autocorr_lag1",
    ]

    print(analysis[display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()