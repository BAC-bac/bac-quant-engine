"""
03c_analyse_regime_summary.py
=============================

BAC Quant Engine - Regime Engine
Stage 03c: Analyse regime summary outputs.

Purpose:
- Read latest regime summary/distribution files
- Diagnose classifier balance
- Identify dominant regime bias
- Rank most trending/ranging/volatile assets
- Save analysis reports
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SUMMARY_PATH = Path("E:/Quant_Lab/data/analysis/regime_summaries/FTMO/regime_summary_latest.parquet")
DISTRIBUTION_PATH = Path("E:/Quant_Lab/data/analysis/regime_summaries/FTMO/regime_distribution_latest.parquet")

REPORT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_diagnostics/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REPORT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"analyse_regime_summary_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def save_report(df: pd.DataFrame, name: str, timestamp: str) -> None:
    csv_path = REPORT_ROOT / f"{name}_{timestamp}.csv"
    parquet_path = REPORT_ROOT / f"{name}_{timestamp}.parquet"

    latest_csv = REPORT_ROOT / f"{name}_latest.csv"
    latest_parquet = REPORT_ROOT / f"{name}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def main() -> None:
    logger.info("Starting regime summary diagnostics")

    summary_df = pd.read_parquet(SUMMARY_PATH)
    distribution_df = pd.read_parquet(DISTRIBUTION_PATH)

    logger.info(f"Summary rows: {len(summary_df)}")
    logger.info(f"Distribution rows: {len(distribution_df)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    dominant_counts = (
        summary_df["dominant_composite_regime"]
        .value_counts()
        .reset_index()
    )
    dominant_counts.columns = ["dominant_composite_regime", "file_count"]
    dominant_counts["file_pct"] = round(
        dominant_counts["file_count"] / dominant_counts["file_count"].sum() * 100,
        2,
    )

    save_report(dominant_counts, "dominant_regime_counts", timestamp)

    timeframe_balance = (
        summary_df.groupby("timeframe")
        .agg(
            files=("symbol", "count"),
            avg_confidence=("avg_regime_confidence", "mean"),
            avg_bull_trend_pct=("bull_trend_pct", "mean"),
            avg_bear_trend_pct=("bear_trend_pct", "mean"),
            avg_range_or_transition_pct=("range_or_transition_pct", "mean"),
            avg_high_volatility_pct=("high_volatility_pct", "mean"),
            avg_normal_volatility_pct=("normal_volatility_pct", "mean"),
            avg_low_volatility_pct=("low_volatility_pct", "mean"),
            avg_strong_trend_pct=("strong_trend_pct", "mean"),
            avg_weak_trend_pct=("weak_trend_pct", "mean"),
        )
        .reset_index()
    )

    save_report(timeframe_balance, "timeframe_regime_balance", timestamp)

    most_bullish = summary_df.sort_values("bull_trend_pct", ascending=False).head(50)
    most_bearish = summary_df.sort_values("bear_trend_pct", ascending=False).head(50)
    most_ranging = summary_df.sort_values("range_or_transition_pct", ascending=False).head(50)
    most_volatile = summary_df.sort_values("high_volatility_pct", ascending=False).head(50)
    strongest_trend = summary_df.sort_values("strong_trend_pct", ascending=False).head(50)
    weakest_trend = summary_df.sort_values("weak_trend_pct", ascending=False).head(50)
    highest_confidence = summary_df.sort_values("avg_regime_confidence", ascending=False).head(50)

    save_report(most_bullish, "top_most_bullish", timestamp)
    save_report(most_bearish, "top_most_bearish", timestamp)
    save_report(most_ranging, "top_most_ranging", timestamp)
    save_report(most_volatile, "top_most_volatile", timestamp)
    save_report(strongest_trend, "top_strongest_trend", timestamp)
    save_report(weakest_trend, "top_weakest_trend", timestamp)
    save_report(highest_confidence, "top_highest_confidence", timestamp)

    regime_global_distribution = (
        distribution_df.groupby("composite_regime")
        .agg(
            total_count=("count", "sum"),
            avg_pct_across_files=("pct", "mean"),
            median_pct_across_files=("pct", "median"),
            files_present=("symbol", "count"),
        )
        .reset_index()
        .sort_values("total_count", ascending=False)
    )

    save_report(regime_global_distribution, "global_composite_regime_distribution", timestamp)

    logger.info("Diagnostics completed successfully")

    logger.info("Dominant regime counts:")
    logger.info(dominant_counts.to_string(index=False))

    logger.info("Timeframe regime balance:")
    logger.info(
        timeframe_balance[
            [
                "timeframe",
                "files",
                "avg_confidence",
                "avg_bull_trend_pct",
                "avg_bear_trend_pct",
                "avg_range_or_transition_pct",
                "avg_high_volatility_pct",
            ]
        ].to_string(index=False)
    )

    logger.info("Top 20 most ranging files:")
    logger.info(
        most_ranging[
            [
                "symbol",
                "timeframe",
                "range_or_transition_pct",
                "dominant_composite_regime",
                "avg_regime_confidence",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()