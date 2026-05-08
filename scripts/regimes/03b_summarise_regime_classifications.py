"""
03b_summarise_regime_classifications.py
=======================================

BAC Quant Engine - Regime Engine
Stage 03b: Summarise classified regime files.

Purpose:
- Scan all classified regime parquet files
- Summarise regime distributions by symbol/timeframe
- Calculate dominant regimes, confidence, trend/volatility/momentum breakdowns
- Save summary reports for analysis
"""

from pathlib import Path
from datetime import datetime
import logging
import sys
import argparse

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

REGIME_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")
REPORT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_summaries/FTMO")

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REPORT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"summarise_regimes_{datetime.now():%Y%m%d_%H%M%S}.log"

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
        description="Summarise BACQE regime classifications by timeframe group."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Choose which timeframe group to summarise.",
    )

    return parser.parse_args()


MIN_ROWS = 100


def extract_symbol_timeframe(path: Path) -> tuple[str, str]:
    timeframe = path.parent.name
    suffix = f"_{timeframe}_regimes"
    symbol = path.stem.replace(suffix, "")
    return symbol, timeframe


def summarise_file(path: Path) -> dict | None:
    symbol, timeframe = extract_symbol_timeframe(path)

    df = pd.read_parquet(path)

    if df.empty or len(df) < MIN_ROWS:
        logger.warning(f"{symbol} {timeframe}: skipped, not enough rows")
        return None

    required_cols = {
        "time",
        "trend_state",
        "volatility_state",
        "momentum_state",
        "trend_strength_state",
        "composite_regime",
        "regime_confidence",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} {timeframe}: missing columns {missing}")

    df["time"] = pd.to_datetime(df["time"], utc=True)

    composite_counts = df["composite_regime"].value_counts(normalize=True)
    trend_counts = df["trend_state"].value_counts(normalize=True)
    volatility_counts = df["volatility_state"].value_counts(normalize=True)
    momentum_counts = df["momentum_state"].value_counts(normalize=True)
    strength_counts = df["trend_strength_state"].value_counts(normalize=True)

    dominant_composite = composite_counts.index[0]
    dominant_trend = trend_counts.index[0]
    dominant_volatility = volatility_counts.index[0]
    dominant_momentum = momentum_counts.index[0]
    dominant_strength = strength_counts.index[0]

    summary = {
        "symbol": symbol,
        "timeframe": timeframe,
        "rows": len(df),
        "start_time": df["time"].min(),
        "end_time": df["time"].max(),
        "date_range_days": (df["time"].max() - df["time"].min()).days,
        "avg_regime_confidence": round(df["regime_confidence"].mean(), 4),
        "median_regime_confidence": round(df["regime_confidence"].median(), 4),
        "dominant_composite_regime": dominant_composite,
        "dominant_composite_pct": round(composite_counts.iloc[0] * 100, 2),
        "dominant_trend_state": dominant_trend,
        "dominant_trend_pct": round(trend_counts.iloc[0] * 100, 2),
        "dominant_volatility_state": dominant_volatility,
        "dominant_volatility_pct": round(volatility_counts.iloc[0] * 100, 2),
        "dominant_momentum_state": dominant_momentum,
        "dominant_momentum_pct": round(momentum_counts.iloc[0] * 100, 2),
        "dominant_trend_strength_state": dominant_strength,
        "dominant_trend_strength_pct": round(strength_counts.iloc[0] * 100, 2),
        "bull_trend_pct": round(trend_counts.get("bull_trend", 0) * 100, 2),
        "bear_trend_pct": round(trend_counts.get("bear_trend", 0) * 100, 2),
        "range_or_transition_pct": round(trend_counts.get("range_or_transition", 0) * 100, 2),
        "high_volatility_pct": round(volatility_counts.get("high_volatility", 0) * 100, 2),
        "normal_volatility_pct": round(volatility_counts.get("normal_volatility", 0) * 100, 2),
        "low_volatility_pct": round(volatility_counts.get("low_volatility", 0) * 100, 2),
        "strong_trend_pct": round(strength_counts.get("strong_trend", 0) * 100, 2),
        "moderate_trend_pct": round(strength_counts.get("moderate_trend", 0) * 100, 2),
        "weak_trend_pct": round(strength_counts.get("weak_trend", 0) * 100, 2),
        "bullish_momentum_pct": round(momentum_counts.get("bullish_momentum", 0) * 100, 2),
        "bearish_momentum_pct": round(momentum_counts.get("bearish_momentum", 0) * 100, 2),
        "neutral_momentum_pct": round(momentum_counts.get("neutral_momentum", 0) * 100, 2),
    }

    return summary


def build_detailed_distribution(files: list[Path]) -> pd.DataFrame:
    rows = []

    for path in files:
        symbol, timeframe = extract_symbol_timeframe(path)
        df = pd.read_parquet(path)

        if df.empty or "composite_regime" not in df.columns:
            continue

        counts = df["composite_regime"].value_counts()
        total = counts.sum()

        for regime, count in counts.items():
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "composite_regime": regime,
                    "count": int(count),
                    "pct": round((count / total) * 100, 2),
                }
            )

    return pd.DataFrame(rows)


def main(mode: str = "full") -> None:
    logger.info("=" * 80)
    logger.info("Starting regime summary build")
    logger.info(f"Mode: {mode}")
    logger.info(f"Regime root: {REGIME_ROOT}")
    logger.info(f"Report root: {REPORT_ROOT}")
    logger.info("=" * 80)

    files = sorted(REGIME_ROOT.rglob("*_regimes.parquet"))

    allowed_timeframes = get_allowed_timeframes(mode)

    if allowed_timeframes is not None:
        files = [
            path for path in files
            if path.parent.name in allowed_timeframes
        ]

    logger.info(f"Discovered {len(files)} classified regime files after mode filter")

    if allowed_timeframes is not None:
        logger.info(f"Allowed timeframes: {sorted(allowed_timeframes)}")
    else:
        logger.info("Allowed timeframes: all")

    if not files:
        logger.warning("No classified regime files found")
        return

    summaries = []
    fail_count = 0

    for idx, path in enumerate(files, start=1):
        logger.info(f"[{idx}/{len(files)}] Summarising {path.name}")

        try:
            result = summarise_file(path)

            if result is not None:
                summaries.append(result)

        except Exception as exc:
            logger.error(f"Failed to summarise {path}: {exc}")
            fail_count += 1

    summary_df = pd.DataFrame(summaries)

    if summary_df.empty:
        logger.warning("No summaries created")
        return

    summary_df = summary_df.sort_values(
        by=["timeframe", "avg_regime_confidence", "rows"],
        ascending=[True, False, False],
    )

    distribution_df = build_detailed_distribution(files)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    suffix = "" if mode == "full" else f"_{mode}"

    summary_csv = REPORT_ROOT / f"regime_summary{suffix}_{timestamp}.csv"
    summary_parquet = REPORT_ROOT / f"regime_summary{suffix}_{timestamp}.parquet"
    latest_summary_csv = REPORT_ROOT / f"regime_summary{suffix}_latest.csv"
    latest_summary_parquet = REPORT_ROOT / f"regime_summary{suffix}_latest.parquet"

    distribution_csv = REPORT_ROOT / f"regime_distribution{suffix}_{timestamp}.csv"
    distribution_parquet = REPORT_ROOT / f"regime_distribution{suffix}_{timestamp}.parquet"
    latest_distribution_csv = REPORT_ROOT / f"regime_distribution{suffix}_latest.csv"
    latest_distribution_parquet = REPORT_ROOT / f"regime_distribution{suffix}_latest.parquet"

    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_parquet(summary_parquet, index=False)
    summary_df.to_csv(latest_summary_csv, index=False)
    summary_df.to_parquet(latest_summary_parquet, index=False)

    distribution_df.to_csv(distribution_csv, index=False)
    distribution_df.to_parquet(distribution_parquet, index=False)
    distribution_df.to_csv(latest_distribution_csv, index=False)
    distribution_df.to_parquet(latest_distribution_parquet, index=False)

    logger.info("Regime summary build completed successfully")
    logger.info(f"Summary rows: {len(summary_df)}")
    logger.info(f"Distribution rows: {len(distribution_df)}")
    logger.info(f"Failures: {fail_count}")

    logger.info(f"Saved summary: {latest_summary_csv}")
    logger.info(f"Saved distribution: {latest_distribution_csv}")

    logger.info("Top 20 highest-confidence symbol/timeframe regimes:")
    cols = [
        "symbol",
        "timeframe",
        "rows",
        "avg_regime_confidence",
        "dominant_composite_regime",
        "dominant_composite_pct",
    ]

    logger.info(
        summary_df.sort_values(
            "avg_regime_confidence",
            ascending=False,
        )[cols].head(20).to_string(index=False)
    )

    logger.info("Dominant composite regime counts:")
    logger.info(
        summary_df["dominant_composite_regime"]
        .value_counts()
        .to_string()
    )


if __name__ == "__main__":
    args = parse_args()
    main(mode=args.mode)