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
import argparse

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


TIMEFRAME_GROUPS = {
    "small": ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"],
    "medium": ["M20", "M30", "H1", "H2", "H3", "H4"],
    "large": ["H6", "H8", "H12", "D1", "W1", "MN1"],
    "full": None,
}


def get_allowed_timeframes(mode: str) -> set[str] | None:
    allowed_timeframes = TIMEFRAME_GROUPS.get(mode)

    if allowed_timeframes is None:
        return None

    return set(allowed_timeframes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyse BACQE regime summaries by timeframe group."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Choose which timeframe group to analyse.",
    )

    return parser.parse_args()


def get_summary_paths(mode: str) -> tuple[Path, Path]:
    if mode == "full":
        return SUMMARY_PATH, DISTRIBUTION_PATH

    summary_path = SUMMARY_PATH.parent / f"regime_summary_{mode}_latest.parquet"
    distribution_path = DISTRIBUTION_PATH.parent / f"regime_distribution_{mode}_latest.parquet"

    if not summary_path.exists() or not distribution_path.exists():
        logger.warning(
            f"Mode-specific summary files not found for mode={mode}. "
            "Falling back to full latest summary files."
        )
        return SUMMARY_PATH, DISTRIBUTION_PATH

    return summary_path, distribution_path


def save_report(df: pd.DataFrame, name: str, timestamp: str, mode: str = "full") -> None:
    suffix = "" if mode == "full" else f"_{mode}"

    csv_path = REPORT_ROOT / f"{name}{suffix}_{timestamp}.csv"
    parquet_path = REPORT_ROOT / f"{name}{suffix}_{timestamp}.parquet"

    latest_csv = REPORT_ROOT / f"{name}{suffix}_latest.csv"
    latest_parquet = REPORT_ROOT / f"{name}{suffix}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def main(mode: str = "full") -> None:
    logger.info("=" * 80)
    logger.info("Starting regime summary diagnostics")
    logger.info(f"Mode: {mode}")
    logger.info("=" * 80)

    summary_path, distribution_path = get_summary_paths(mode)

    if not summary_path.exists():
        raise FileNotFoundError(f"Missing summary file: {summary_path}")

    if not distribution_path.exists():
        raise FileNotFoundError(f"Missing distribution file: {distribution_path}")

    logger.info(f"Summary path: {summary_path}")
    logger.info(f"Distribution path: {distribution_path}")

    summary_df = pd.read_parquet(summary_path)
    distribution_df = pd.read_parquet(distribution_path)

    allowed_timeframes = get_allowed_timeframes(mode)

    if allowed_timeframes is not None:
        summary_df = summary_df[summary_df["timeframe"].isin(allowed_timeframes)].copy()
        distribution_df = distribution_df[distribution_df["timeframe"].isin(allowed_timeframes)].copy()

    logger.info(f"Summary rows after mode filter: {len(summary_df)}")
    logger.info(f"Distribution rows after mode filter: {len(distribution_df)}")

    if summary_df.empty or distribution_df.empty:
        logger.warning("No summary/distribution rows available for this mode")
        return

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

    save_report(dominant_counts, "dominant_regime_counts", timestamp, mode=mode)

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

    save_report(timeframe_balance, "timeframe_regime_balance", timestamp, mode=mode)

    most_bullish = summary_df.sort_values("bull_trend_pct", ascending=False).head(50)
    most_bearish = summary_df.sort_values("bear_trend_pct", ascending=False).head(50)
    most_ranging = summary_df.sort_values("range_or_transition_pct", ascending=False).head(50)
    most_volatile = summary_df.sort_values("high_volatility_pct", ascending=False).head(50)
    strongest_trend = summary_df.sort_values("strong_trend_pct", ascending=False).head(50)
    weakest_trend = summary_df.sort_values("weak_trend_pct", ascending=False).head(50)
    highest_confidence = summary_df.sort_values("avg_regime_confidence", ascending=False).head(50)

    save_report(most_bullish, "top_most_bullish", timestamp, mode=mode)
    save_report(most_bearish, "top_most_bearish", timestamp, mode=mode)
    save_report(most_ranging, "top_most_ranging", timestamp, mode=mode)
    save_report(most_volatile, "top_most_volatile", timestamp, mode=mode)
    save_report(strongest_trend, "top_strongest_trend", timestamp, mode=mode)
    save_report(weakest_trend, "top_weakest_trend", timestamp, mode=mode)
    save_report(highest_confidence, "top_highest_confidence", timestamp, mode=mode)

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

    save_report(regime_global_distribution, "global_composite_regime_distribution", timestamp, mode=mode)

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
    args = parse_args()
    main(mode=args.mode)