"""
02_build_regime_features.py
===========================

BAC Quant Engine - Regime Engine
Stage 02: Build regime features from OHLCV parquet data.

Purpose:
- Read audited, indicator-ready OHLCV files
- Build core regime features
- Save feature parquet files for later regime classification
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

DATA_ROOT = Path("E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO")
AUDIT_PATH = Path("E:/Quant_Lab/data/analysis/regime_audits/market_data_audit_latest.parquet")
FEATURE_ROOT = Path("E:/Quant_Lab/data/processed/regimes/features/FTMO")

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

FEATURE_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"build_regime_features_{datetime.now():%Y%m%d_%H%M%S}.log"

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
# SETTINGS
# ============================================================

MIN_ROWS = 500

SMA_FAST = 20
SMA_SLOW = 50
VOL_WINDOW = 20
ATR_WINDOW = 14
RSI_WINDOW = 14
BB_WINDOW = 20
BB_STD = 2
ADX_WINDOW = 14
SLOPE_WINDOW = 10


# ============================================================
# FEATURE HELPERS
# ============================================================

def calculate_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)

    tr_1 = high - low
    tr_2 = (high - prev_close).abs()
    tr_3 = (low - prev_close).abs()

    true_range = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)

    atr = true_range.ewm(alpha=1 / window, adjust=False).mean()

    return atr


def calculate_adx(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
    minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

    atr = calculate_atr(df, window)

    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean() / atr

    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100

    adx = dx.ewm(alpha=1 / window, adjust=False).mean()

    return adx


def calculate_slope(series: pd.Series, window: int = 10) -> pd.Series:
    return series.diff(window) / window


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_cols = {"time", "open", "high", "low", "close"}

    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

    close = df["close"]

    # Returns
    df["return_1"] = close.pct_change()
    df["log_return_1"] = np.log(close / close.shift(1))

    # Rolling volatility
    df["rolling_vol_20"] = df["log_return_1"].rolling(VOL_WINDOW).std()
    df["rolling_vol_50"] = df["log_return_1"].rolling(50).std()

    # Moving averages
    df["sma_fast_20"] = close.rolling(SMA_FAST).mean()
    df["sma_slow_50"] = close.rolling(SMA_SLOW).mean()
    df["sma_ratio_20_50"] = df["sma_fast_20"] / df["sma_slow_50"]

    # Trend slope
    df["sma_fast_slope_10"] = calculate_slope(df["sma_fast_20"], SLOPE_WINDOW)
    df["close_slope_10"] = calculate_slope(close, SLOPE_WINDOW)

    # ATR and volatility structure
    df["atr_14"] = calculate_atr(df, ATR_WINDOW)
    df["atr_pct_14"] = df["atr_14"] / close
    df["atr_ma_50"] = df["atr_14"].rolling(50).mean()
    df["atr_ratio_14_50"] = df["atr_14"] / df["atr_ma_50"]

    # RSI
    df["rsi_14"] = calculate_rsi(close, RSI_WINDOW)

    # Bollinger Band width
    bb_mid = close.rolling(BB_WINDOW).mean()
    bb_std = close.rolling(BB_WINDOW).std()

    bb_upper = bb_mid + (BB_STD * bb_std)
    bb_lower = bb_mid - (BB_STD * bb_std)

    df["bb_mid_20"] = bb_mid
    df["bb_upper_20_2"] = bb_upper
    df["bb_lower_20_2"] = bb_lower
    df["bb_width_20_2"] = (bb_upper - bb_lower) / bb_mid

    # ADX-style trend strength
    df["adx_14"] = calculate_adx(df, ADX_WINDOW)

    # Regime helper booleans
    df["price_above_sma_fast"] = close > df["sma_fast_20"]
    df["price_above_sma_slow"] = close > df["sma_slow_50"]
    df["sma_fast_above_sma_slow"] = df["sma_fast_20"] > df["sma_slow_50"]

    df["high_volatility_flag"] = df["atr_ratio_14_50"] > 1.25
    df["low_volatility_flag"] = df["atr_ratio_14_50"] < 0.80
    df["trend_strength_flag"] = df["adx_14"] > 25

    return df


# ============================================================
# FILE PROCESSING
# ============================================================

def get_indicator_ready_files() -> pd.DataFrame:
    if not AUDIT_PATH.exists():
        raise FileNotFoundError(f"Missing audit file: {AUDIT_PATH}")

    audit_df = pd.read_parquet(AUDIT_PATH)

    ready_df = audit_df[
        (audit_df["indicator_ready"] == True) &
        (audit_df["rows"] >= MIN_ROWS) &
        (audit_df["error"].isna())
    ].copy()

    return ready_df


def process_file(symbol: str, timeframe: str) -> bool:
    input_path = DATA_ROOT / timeframe / f"{symbol}_{timeframe}.parquet"

    output_dir = FEATURE_ROOT / timeframe
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{symbol}_{timeframe}_features.parquet"

    if not input_path.exists():
        logger.warning(f"Missing input file: {input_path}")
        return False

    df = pd.read_parquet(input_path)

    features = build_features(df)

    features.to_parquet(output_path, index=False)

    logger.info(
        f"{symbol} {timeframe}: saved {len(features)} rows -> {output_path}"
    )

    return True


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    logger.info("Starting regime feature build")
    logger.info(f"Data root: {DATA_ROOT}")
    logger.info(f"Audit path: {AUDIT_PATH}")
    logger.info(f"Feature root: {FEATURE_ROOT}")

    ready_df = get_indicator_ready_files()

    logger.info(f"Indicator-ready files to process: {len(ready_df)}")

    success_count = 0
    fail_count = 0

    for idx, row in ready_df.iterrows():
        symbol = row["symbol"]
        timeframe = row["timeframe"]

        try:
            logger.info(f"Processing {symbol} {timeframe}")
            ok = process_file(symbol, timeframe)

            if ok:
                success_count += 1
            else:
                fail_count += 1

        except Exception as exc:
            logger.error(f"{symbol} {timeframe}: failed with error: {exc}")
            fail_count += 1

    logger.info("Regime feature build completed")
    logger.info(f"Successful files: {success_count}")
    logger.info(f"Failed files: {fail_count}")


if __name__ == "__main__":
    main()