"""
03_classify_regimes.py
======================

BAC Quant Engine - Regime Engine
Stage 03: Classify market regimes from feature parquet files.

Classifier logic: v2

Purpose:
- Read regime feature files
- Classify trend, volatility, momentum and composite regimes
- Save regime-labelled parquet files
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import numpy as np
import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FEATURE_ROOT = Path("E:/Quant_Lab/data/processed/regimes/features/FTMO")
REGIME_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REGIME_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"classify_regimes_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ============================================================
# REGIME CLASSIFICATION
# ============================================================

def classify_trend(row: pd.Series) -> str:
    if (
        row["price_above_sma_slow"]
        and row["sma_fast_above_sma_slow"]
        and row["sma_fast_slope_10"] > 0
    ):
        return "bull_trend"

    if (
        not row["price_above_sma_slow"]
        and not row["sma_fast_above_sma_slow"]
        and row["sma_fast_slope_10"] < 0
    ):
        return "bear_trend"

    return "range_or_transition"


def classify_volatility(row: pd.Series) -> str:
    if row["atr_ratio_14_50"] >= 1.25:
        return "high_volatility"

    if row["atr_ratio_14_50"] <= 0.80:
        return "low_volatility"

    return "normal_volatility"


def classify_momentum(row: pd.Series) -> str:
    rsi = row["rsi_14"]

    if rsi >= 65:
        return "bullish_momentum"

    if rsi <= 35:
        return "bearish_momentum"

    return "neutral_momentum"


def classify_trend_strength(row: pd.Series) -> str:
    adx = row["adx_14"]

    if adx >= 30:
        return "strong_trend"

    if adx >= 20:
        return "moderate_trend"

    return "weak_trend"


def classify_composite(row: pd.Series) -> str:
    trend = row["trend_state"]
    volatility = row["volatility_state"]
    momentum = row["momentum_state"]
    strength = row["trend_strength_state"]

    # 1. Weak-trend environments should be classified as range/transition first.
    # This prevents the composite label from becoming too trend-dominant.
    if strength == "weak_trend":
        if volatility == "low_volatility":
            return "quiet_range"

        if volatility == "high_volatility":
            return "volatile_range"

        return "range"

    # 2. Moderate trends with conflicting momentum are transitional.
    if strength == "moderate_trend":
        if trend == "bull_trend" and momentum == "bearish_momentum":
            return "transition"

        if trend == "bear_trend" and momentum == "bullish_momentum":
            return "transition"

        if trend == "range_or_transition":
            if volatility == "high_volatility":
                return "volatile_transition"

            return "transition"

    # 3. Strong aligned trends are the cleanest trend regimes.
    if trend == "bull_trend":
        if volatility == "high_volatility":
            return "bull_trend_high_vol"

        if volatility == "low_volatility":
            return "bull_trend_low_vol"

        return "bull_trend_normal_vol"

    if trend == "bear_trend":
        if volatility == "high_volatility":
            return "bear_trend_high_vol"

        if volatility == "low_volatility":
            return "bear_trend_low_vol"

        return "bear_trend_normal_vol"

    # 4. Remaining range/transition cases.
    if trend == "range_or_transition":
        if volatility == "high_volatility":
            return "volatile_transition"

        if volatility == "low_volatility":
            return "quiet_range"

        return "transition"

    return "transition"


def calculate_regime_score(df: pd.DataFrame) -> pd.Series:
    """
    Confidence score from 0 to 1.

    Higher score = cleaner regime conditions.
    """

    strength_score = np.where(
        df["trend_strength_state"] == "strong_trend",
        0.35,
        np.where(
            df["trend_strength_state"] == "moderate_trend",
            0.22,
            0.12,
        ),
    )

    volatility_score = np.where(
        df["volatility_state"] == "normal_volatility",
        0.25,
        np.where(
            df["volatility_state"] == "high_volatility",
            0.20,
            0.15,
        ),
    )

    momentum_alignment_score = np.where(
        (
            (df["trend_state"] == "bull_trend")
            & (df["momentum_state"] == "bullish_momentum")
        )
        | (
            (df["trend_state"] == "bear_trend")
            & (df["momentum_state"] == "bearish_momentum")
        ),
        0.25,
        np.where(
            df["trend_state"] == "range_or_transition",
            0.18,
            0.10,
        ),
    )

    structure_score = np.where(
        df["sma_fast_above_sma_slow"] == df["price_above_sma_slow"],
        0.15,
        0.05,
    )

    score = (
        strength_score
        + volatility_score
        + momentum_alignment_score
        + structure_score
    )

    return np.clip(score, 0, 1)


def classify_regimes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_cols = {
        "time",
        "close",
        "sma_fast_slope_10",
        "price_above_sma_slow",
        "sma_fast_above_sma_slow",
        "atr_ratio_14_50",
        "rsi_14",
        "adx_14",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")

    df = df.dropna(
        subset=[
            "sma_fast_slope_10",
            "atr_ratio_14_50",
            "rsi_14",
            "adx_14",
        ]
    ).copy()

    df["trend_state"] = df.apply(classify_trend, axis=1)
    df["volatility_state"] = df.apply(classify_volatility, axis=1)
    df["momentum_state"] = df.apply(classify_momentum, axis=1)
    df["trend_strength_state"] = df.apply(classify_trend_strength, axis=1)

    df["composite_regime"] = df.apply(classify_composite, axis=1)
    df["regime_confidence"] = calculate_regime_score(df)

    return df


# ============================================================
# FILE PROCESSING
# ============================================================

def process_file(feature_path: Path) -> bool:
    timeframe = feature_path.parent.name
    symbol = feature_path.stem.replace(f"_{timeframe}_features", "")

    output_dir = REGIME_ROOT / timeframe
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{symbol}_{timeframe}_regimes.parquet"

    if output_path.exists():
        logger.info(f"{symbol} {timeframe}: already classified, skipping")
        return True

    df = pd.read_parquet(feature_path)

    classified = classify_regimes(df)

    classified.to_parquet(output_path, index=False)

    logger.info(
        f"{symbol} {timeframe}: saved {len(classified)} classified rows -> {output_path}"
    )

    return True


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    logger.info("Starting regime classification")
    logger.info(f"Feature root: {FEATURE_ROOT}")
    logger.info(f"Regime root: {REGIME_ROOT}")

    feature_files = sorted(FEATURE_ROOT.rglob("*_features.parquet"))

    logger.info(f"Discovered {len(feature_files)} feature files")

    if not feature_files:
        logger.warning("No feature files found")
        return

    success_count = 0
    fail_count = 0

    for idx, feature_path in enumerate(feature_files, start=1):
        logger.info(f"[{idx}/{len(feature_files)}] Processing {feature_path.name}")

        try:
            process_file(feature_path)
            success_count += 1

        except Exception as exc:
            logger.error(f"Failed to classify {feature_path}: {exc}")
            fail_count += 1

    logger.info("Regime classification completed")
    logger.info(f"Successful files: {success_count}")
    logger.info(f"Failed files: {fail_count}")


if __name__ == "__main__":
    main()