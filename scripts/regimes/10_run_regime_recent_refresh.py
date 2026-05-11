"""
10_run_regime_recent_refresh.py
===============================

BAC Quant Engine - Regime Engine
Stage 10: Fast recent regime refresh.

Purpose:
- Avoid full historical rebuilds for trading/monitoring use
- Read only the most recent bars from raw OHLCV files
- Build recent regime features
- Classify recent regimes
- Generate latest regime forecast/dashboard rows
- Save lightweight recent outputs

This does NOT replace the full research pipeline.
It is a fast operational refresh layer.
"""

from pathlib import Path
from datetime import datetime, timezone
import argparse
import logging
import sys

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_ROOT = Path("E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO")
TRANSITION_ROOT = Path("E:/Quant_Lab/data/analysis/regime_transitions/FTMO")

RECENT_FEATURE_ROOT = Path("E:/Quant_Lab/data/processed/regimes/recent/features/FTMO")
RECENT_CLASSIFIED_ROOT = Path("E:/Quant_Lab/data/processed/regimes/recent/classified/FTMO")
RECENT_OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_recent/FTMO")

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

for path in [RECENT_FEATURE_ROOT, RECENT_CLASSIFIED_ROOT, RECENT_OUTPUT_ROOT, LOG_DIR]:
    path.mkdir(parents=True, exist_ok=True)


TIMEFRAME_GROUPS = {
    "small": ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"],
    "medium": ["M20", "M30", "H1", "H2", "H3", "H4"],
    "large": ["H6", "H8", "H12", "D1", "W1", "MN1"],
    "full": None,
}


TIMEFRAME_ORDER = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M4": 4,
    "M5": 5,
    "M6": 6,
    "M10": 10,
    "M12": 12,
    "M15": 15,
    "M20": 20,
    "M30": 30,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
    "H6": 360,
    "H8": 480,
    "H12": 720,
    "D1": 1440,
    "W1": 10080,
    "MN1": 43200,
}


VOLATILE_REGIMES = {
    "bull_trend_high_vol",
    "bear_trend_high_vol",
    "volatile_range",
    "volatile_transition",
    "volatile_range_or_transition",
}

TREND_REGIMES = {
    "bull_trend_normal_vol",
    "bull_trend_high_vol",
    "bull_trend_low_vol",
    "bear_trend_normal_vol",
    "bear_trend_high_vol",
    "bear_trend_low_vol",
}

RANGE_REGIMES = {
    "range",
    "quiet_range",
    "volatile_range",
    "volatile_range_or_transition",
}

BULL_REGIMES = {
    "bull_trend_normal_vol",
    "bull_trend_high_vol",
    "bull_trend_low_vol",
}

BEAR_REGIMES = {
    "bear_trend_normal_vol",
    "bear_trend_high_vol",
    "bear_trend_low_vol",
}

TRANSITION_REGIMES = {
    "transition",
    "volatile_transition",
}


log_path = LOG_DIR / f"recent_regime_refresh_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def market_is_open_utc() -> bool:
    """
    FX market-hours guard using UTC.

    Open:
    - Sunday from 22:00 UTC
    - Monday to Thursday all day
    - Friday until 22:00 UTC
    """

    now = datetime.now(timezone.utc)

    weekday = now.weekday()  # Monday=0, Sunday=6
    hour = now.hour

    return (
        (weekday == 6 and hour >= 22) or
        (weekday in [0, 1, 2, 3]) or
        (weekday == 4 and hour < 22)
    )

def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def get_allowed_timeframes(mode: str) -> set[str] | None:
    allowed = TIMEFRAME_GROUPS.get(mode)
    return None if allowed is None else set(allowed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fast BACQE recent regime refresh."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="small",
        help="Timeframe group to refresh.",
    )

    parser.add_argument(
        "--lookback-bars",
        type=int,
        default=1000,
        help="Number of recent bars to process per symbol/timeframe.",
    )

    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol whitelist, e.g. GBPUSD EURUSD XAUUSD.",
    )

    return parser.parse_args()


def extract_symbol_timeframe(path: Path) -> tuple[str, str]:
    timeframe = path.parent.name
    suffix = f"_{timeframe}"
    symbol = path.stem.replace(suffix, "")
    return symbol, timeframe


def calculate_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)

    tr_1 = high - low
    tr_2 = (high - prev_close).abs()
    tr_3 = (low - prev_close).abs()

    true_range = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)

    return true_range.ewm(alpha=1 / window, adjust=False).mean()


def calculate_adx(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
    minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

    atr = calculate_atr(df, window)

    plus_di = (
        100
        * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean()
        / atr
    )

    minus_di = (
        100
        * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean()
        / atr
    )

    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100

    return dx.ewm(alpha=1 / window, adjust=False).mean()


def calculate_slope(series: pd.Series, window: int = 10) -> pd.Series:
    return series.diff(window) / window


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

    close = df["close"]

    # Returns
    df["return_1"] = close.pct_change()

    price_ratio = close / close.shift(1)
    price_ratio = price_ratio.replace([np.inf, -np.inf], np.nan)
    price_ratio = price_ratio.where(price_ratio > 0)

    df["log_return_1"] = np.log(price_ratio)

    df["rolling_vol_20"] = df["log_return_1"].rolling(20).std()
    df["rolling_vol_50"] = df["log_return_1"].rolling(50).std()

    df["sma_fast_20"] = close.rolling(20).mean()
    df["sma_slow_50"] = close.rolling(50).mean()
    df["sma_ratio_20_50"] = df["sma_fast_20"] / df["sma_slow_50"]

    df["sma_fast_slope_10"] = calculate_slope(df["sma_fast_20"], 10)
    df["close_slope_10"] = calculate_slope(close, 10)

    df["atr_14"] = calculate_atr(df, 14)
    df["atr_pct_14"] = df["atr_14"] / close
    df["atr_ma_50"] = df["atr_14"].rolling(50).mean()
    df["atr_ratio_14_50"] = df["atr_14"] / df["atr_ma_50"]

    df["rsi_14"] = calculate_rsi(close, 14)

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()

    bb_upper = bb_mid + (2 * bb_std)
    bb_lower = bb_mid - (2 * bb_std)

    df["bb_mid_20"] = bb_mid
    df["bb_upper_20_2"] = bb_upper
    df["bb_lower_20_2"] = bb_lower
    df["bb_width_20_2"] = (bb_upper - bb_lower) / bb_mid

    df["adx_14"] = calculate_adx(df, 14)

    df["price_above_sma_fast"] = close > df["sma_fast_20"]
    df["price_above_sma_slow"] = close > df["sma_slow_50"]
    df["sma_fast_above_sma_slow"] = df["sma_fast_20"] > df["sma_slow_50"]

    df["high_volatility_flag"] = df["atr_ratio_14_50"] > 1.25
    df["low_volatility_flag"] = df["atr_ratio_14_50"] < 0.80
    df["trend_strength_flag"] = df["adx_14"] > 25

    return df


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

    if strength == "weak_trend":
        if volatility == "low_volatility":
            return "quiet_range"

        if volatility == "high_volatility":
            return "volatile_range"

        return "range"

    if strength == "moderate_trend":
        if trend == "bull_trend" and momentum == "bearish_momentum":
            return "transition"

        if trend == "bear_trend" and momentum == "bullish_momentum":
            return "transition"

        if trend == "range_or_transition":
            if volatility == "high_volatility":
                return "volatile_transition"

            return "transition"

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

    if trend == "range_or_transition":
        if volatility == "high_volatility":
            return "volatile_transition"

        if volatility == "low_volatility":
            return "quiet_range"

        return "transition"

    return "transition"


def calculate_regime_score(df: pd.DataFrame) -> pd.Series:
    strength_score = np.where(
        df["trend_strength_state"] == "strong_trend",
        0.35,
        np.where(df["trend_strength_state"] == "moderate_trend", 0.22, 0.12),
    )

    volatility_score = np.where(
        df["volatility_state"] == "normal_volatility",
        0.25,
        np.where(df["volatility_state"] == "high_volatility", 0.20, 0.15),
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
        np.where(df["trend_state"] == "range_or_transition", 0.18, 0.10),
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


def get_transition_paths(mode: str) -> tuple[Path, Path]:
    suffix = get_mode_suffix(mode)

    detail_path = TRANSITION_ROOT / f"regime_transition_detail{suffix}_latest.parquet"
    global_path = TRANSITION_ROOT / f"regime_transition_global{suffix}_latest.parquet"

    return detail_path, global_path


def get_transition_probabilities(
    symbol: str,
    timeframe: str,
    current_regime: str,
    transition_df: pd.DataFrame,
    global_transition_df: pd.DataFrame,
) -> pd.DataFrame:
    local = transition_df[
        (transition_df["symbol"] == symbol)
        & (transition_df["timeframe"] == timeframe)
        & (transition_df["from_regime"] == current_regime)
    ].copy()

    if not local.empty:
        local["source"] = "local"
        return local.sort_values("transition_probability", ascending=False)

    global_rows = global_transition_df[
        global_transition_df["from_regime"] == current_regime
    ].copy()

    if global_rows.empty:
        return pd.DataFrame()

    global_rows["symbol"] = symbol
    global_rows["timeframe"] = timeframe
    global_rows["source"] = "global_fallback"

    return global_rows.sort_values("transition_probability", ascending=False)


def classify_forecast_signal(
    current_regime: str,
    persistence_prob: float,
    volatility_prob: float,
    breakout_prob: float,
    bullish_prob: float,
    bearish_prob: float,
    range_prob: float,
) -> str:
    if persistence_prob >= 0.85:
        if current_regime in TREND_REGIMES:
            return "regime_persistence_trend"
        if current_regime in RANGE_REGIMES:
            return "regime_persistence_range"
        return "regime_persistence"

    if current_regime in RANGE_REGIMES and breakout_prob >= 0.15:
        return "range_breakout_risk"

    if volatility_prob >= 0.15:
        return "volatility_expansion_risk"

    if bullish_prob >= 0.20 and bullish_prob > bearish_prob * 1.5:
        return "bullish_resolution_bias"

    if bearish_prob >= 0.20 and bearish_prob > bullish_prob * 1.5:
        return "bearish_resolution_bias"

    if range_prob >= 0.70:
        return "range_continuation_bias"

    return "mixed_or_uncertain"


def build_forecast_row(
    latest_state: pd.Series,
    symbol: str,
    timeframe: str,
    probs: pd.DataFrame,
) -> dict:
    current_regime = latest_state["composite_regime"]

    base = {
        "symbol": symbol,
        "timeframe": timeframe,
        "latest_time": latest_state["time"],
        "current_regime": current_regime,
        "trend_state": latest_state["trend_state"],
        "volatility_state": latest_state["volatility_state"],
        "momentum_state": latest_state["momentum_state"],
        "trend_strength_state": latest_state["trend_strength_state"],
        "current_confidence": round(float(latest_state["regime_confidence"]), 4),
    }

    if probs.empty:
        return {
            **base,
            "most_likely_next_regime": None,
            "most_likely_next_probability_pct": None,
            "persistence_probability_pct": None,
            "transition_probability_pct": None,
            "breakout_probability_pct": None,
            "volatility_expansion_probability_pct": None,
            "bullish_probability_pct": None,
            "bearish_probability_pct": None,
            "range_probability_pct": None,
            "forecast_source": "none",
            "forecast_signal": "no_transition_data",
        }

    top = probs.iloc[0]

    persistence_prob = probs.loc[
        probs["to_regime"] == current_regime,
        "transition_probability",
    ].sum()

    volatility_prob = probs.loc[
        probs["to_regime"].isin(VOLATILE_REGIMES),
        "transition_probability",
    ].sum()

    trend_prob = probs.loc[
        probs["to_regime"].isin(TREND_REGIMES),
        "transition_probability",
    ].sum()

    range_prob = probs.loc[
        probs["to_regime"].isin(RANGE_REGIMES),
        "transition_probability",
    ].sum()

    bullish_prob = probs.loc[
        probs["to_regime"].isin(BULL_REGIMES),
        "transition_probability",
    ].sum()

    bearish_prob = probs.loc[
        probs["to_regime"].isin(BEAR_REGIMES),
        "transition_probability",
    ].sum()

    transition_prob = probs.loc[
        probs["to_regime"].isin(TRANSITION_REGIMES),
        "transition_probability",
    ].sum()

    breakout_prob = trend_prob if current_regime in RANGE_REGIMES else transition_prob

    forecast_signal = classify_forecast_signal(
        current_regime=current_regime,
        persistence_prob=persistence_prob,
        volatility_prob=volatility_prob,
        breakout_prob=breakout_prob,
        bullish_prob=bullish_prob,
        bearish_prob=bearish_prob,
        range_prob=range_prob,
    )

    return {
        **base,
        "most_likely_next_regime": top["to_regime"],
        "most_likely_next_probability_pct": round(top["transition_probability"] * 100, 2),
        "persistence_probability_pct": round(persistence_prob * 100, 2),
        "transition_probability_pct": round(transition_prob * 100, 2),
        "breakout_probability_pct": round(breakout_prob * 100, 2),
        "volatility_expansion_probability_pct": round(volatility_prob * 100, 2),
        "bullish_probability_pct": round(bullish_prob * 100, 2),
        "bearish_probability_pct": round(bearish_prob * 100, 2),
        "range_probability_pct": round(range_prob * 100, 2),
        "forecast_source": top.get("source", "unknown"),
        "forecast_signal": forecast_signal,
    }


def classify_dashboard_bucket(row: pd.Series) -> str:
    signal = row["forecast_signal"]
    current_regime = row["current_regime"]

    if signal in {"volatility_expansion_risk", "range_breakout_risk"}:
        return "watchlist_risk_event"

    if signal in {"bullish_resolution_bias", "bearish_resolution_bias"}:
        return "directional_resolution_watch"

    if signal == "regime_persistence_trend":
        return "persistent_trend"

    if signal in {"regime_persistence_range", "range_continuation_bias"}:
        return "persistent_range"

    if "volatile" in str(current_regime):
        return "volatile_environment"

    return "mixed_or_uncertain"


def classify_bias(row: pd.Series) -> str:
    bull = row.get("bullish_probability_pct", 0)
    bear = row.get("bearish_probability_pct", 0)
    current = str(row.get("current_regime", ""))

    if bull > bear * 1.5 and bull >= 20:
        return "bullish"

    if bear > bull * 1.5 and bear >= 20:
        return "bearish"

    if "bull" in current:
        return "bullish_current"

    if "bear" in current:
        return "bearish_current"

    return "neutral"


def calculate_priority_score(row: pd.Series) -> float:
    score = 0.0

    score += float(row.get("current_confidence", 0)) * 20
    score += float(row.get("volatility_expansion_probability_pct", 0) or 0) * 0.35
    score += float(row.get("breakout_probability_pct", 0) or 0) * 0.25
    score += float(row.get("persistence_probability_pct", 0) or 0) * 0.10

    signal = row.get("forecast_signal")

    if signal == "volatility_expansion_risk":
        score += 20

    elif signal == "range_breakout_risk":
        score += 18

    elif signal in {"bullish_resolution_bias", "bearish_resolution_bias"}:
        score += 15

    elif signal == "regime_persistence_trend":
        score += 10

    elif signal == "mixed_or_uncertain":
        score += 3

    return round(score, 2)


def build_dashboard(forecast_df: pd.DataFrame) -> pd.DataFrame:
    df = forecast_df.copy()

    df["latest_time"] = pd.to_datetime(df["latest_time"], utc=True)
    df["timeframe_rank"] = df["timeframe"].map(TIMEFRAME_ORDER).fillna(999999)

    df["dashboard_bucket"] = df.apply(classify_dashboard_bucket, axis=1)
    df["directional_bias"] = df.apply(classify_bias, axis=1)
    df["priority_score"] = df.apply(calculate_priority_score, axis=1)

    return df.sort_values(
        by=["priority_score", "timeframe_rank", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def save_outputs(df: pd.DataFrame, name: str, timestamp: str, mode: str) -> None:
    suffix = get_mode_suffix(mode)

    csv_path = RECENT_OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.csv"
    parquet_path = RECENT_OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.parquet"

    latest_csv = RECENT_OUTPUT_ROOT / f"{name}{suffix}_latest.csv"
    latest_parquet = RECENT_OUTPUT_ROOT / f"{name}{suffix}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def process_raw_file(
    path: Path,
    lookback_bars: int,
    transition_df: pd.DataFrame,
    global_transition_df: pd.DataFrame,
) -> dict | None:
    symbol, timeframe = extract_symbol_timeframe(path)

    df = pd.read_parquet(path)

    if df.empty:
        return None

    df = df.tail(lookback_bars).copy()

    features = build_features(df)
    classified = classify_regimes(features)

    if classified.empty:
        return None

    feature_dir = RECENT_FEATURE_ROOT / timeframe
    classified_dir = RECENT_CLASSIFIED_ROOT / timeframe

    feature_dir.mkdir(parents=True, exist_ok=True)
    classified_dir.mkdir(parents=True, exist_ok=True)

    feature_path = feature_dir / f"{symbol}_{timeframe}_recent_features.parquet"
    classified_path = classified_dir / f"{symbol}_{timeframe}_recent_regimes.parquet"

    features.to_parquet(feature_path, index=False)
    classified.to_parquet(classified_path, index=False)

    latest_state = classified.iloc[-1]

    probs = get_transition_probabilities(
        symbol=symbol,
        timeframe=timeframe,
        current_regime=latest_state["composite_regime"],
        transition_df=transition_df,
        global_transition_df=global_transition_df,
    )

    forecast_row = build_forecast_row(
        latest_state=latest_state,
        symbol=symbol,
        timeframe=timeframe,
        probs=probs,
    )

    forecast_row["recent_rows_processed"] = len(df)
    forecast_row["recent_features_rows"] = len(features)
    forecast_row["recent_classified_rows"] = len(classified)

    return forecast_row


def main(mode: str, lookback_bars: int, symbols: list[str] | None) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE recent regime refresh")
    logger.info(f"Mode: {mode}")
    logger.info(f"Lookback bars: {lookback_bars}")
    logger.info(f"Symbols: {symbols if symbols else 'all'}")
    logger.info("=" * 80)

    if not market_is_open_utc():
        logger.info("Market is closed according to UTC FX session guard. Skipping refresh.")
        return

    allowed_timeframes = get_allowed_timeframes(mode)
    suffix = get_mode_suffix(mode)

    transition_path, global_transition_path = get_transition_paths(mode)

    logger.info(f"Transition detail path: {transition_path}")
    logger.info(f"Global transition path: {global_transition_path}")

    if not transition_path.exists():
        raise FileNotFoundError(f"Missing transition detail file: {transition_path}")

    if not global_transition_path.exists():
        raise FileNotFoundError(f"Missing global transition file: {global_transition_path}")

    transition_df = pd.read_parquet(transition_path)
    global_transition_df = pd.read_parquet(global_transition_path)

    files = sorted(RAW_ROOT.rglob("*.parquet"))

    if allowed_timeframes is not None:
        files = [
            path for path in files
            if path.parent.name in allowed_timeframes
        ]

    if symbols:
        symbol_set = set(symbols)
        files = [
            path for path in files
            if extract_symbol_timeframe(path)[0] in symbol_set
        ]

    logger.info(f"Raw files selected: {len(files)}")

    if not files:
        logger.warning("No raw files found after filtering")
        return

    forecasts = []
    failures = 0

    for idx, path in enumerate(files, start=1):
        symbol, timeframe = extract_symbol_timeframe(path)
        logger.info(f"[{idx}/{len(files)}] Recent refresh: {symbol} {timeframe}")

        try:
            row = process_raw_file(
                path=path,
                lookback_bars=lookback_bars,
                transition_df=transition_df,
                global_transition_df=global_transition_df,
            )

            if row is not None:
                forecasts.append(row)

        except Exception as exc:
            logger.error(f"{symbol} {timeframe}: failed with error: {exc}")
            failures += 1

    forecast_df = pd.DataFrame(forecasts)

    if forecast_df.empty:
        logger.warning("No forecast rows created")
        return

    dashboard_df = build_dashboard(forecast_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_outputs(forecast_df, "recent_regime_forecast", timestamp, mode)
    save_outputs(dashboard_df, "recent_regime_dashboard", timestamp, mode)

    watchlist = dashboard_df[
        dashboard_df["dashboard_bucket"].isin(
            [
                "watchlist_risk_event",
                "directional_resolution_watch",
                "volatile_environment",
            ]
        )
    ].copy()

    persistent_trends = dashboard_df[
        dashboard_df["dashboard_bucket"] == "persistent_trend"
    ].copy()

    persistent_ranges = dashboard_df[
        dashboard_df["dashboard_bucket"] == "persistent_range"
    ].copy()

    save_outputs(watchlist, "recent_regime_watchlist", timestamp, mode)
    save_outputs(persistent_trends, "recent_persistent_trends", timestamp, mode)
    save_outputs(persistent_ranges, "recent_persistent_ranges", timestamp, mode)

    logger.info("=" * 80)
    logger.info("Recent regime refresh completed")
    logger.info(f"Mode: {mode}")
    logger.info(f"Forecast rows: {len(forecast_df)}")
    logger.info(f"Dashboard rows: {len(dashboard_df)}")
    logger.info(f"Failures: {failures}")
    logger.info("=" * 80)

    logger.info("Dashboard bucket counts:")
    logger.info(dashboard_df["dashboard_bucket"].value_counts().to_string())

    logger.info("Top 30 recent dashboard priorities:")
    display_cols = [
        "symbol",
        "timeframe",
        "latest_time",
        "current_regime",
        "forecast_signal",
        "dashboard_bucket",
        "directional_bias",
        "priority_score",
        "persistence_probability_pct",
        "breakout_probability_pct",
        "volatility_expansion_probability_pct",
    ]

    logger.info(
        dashboard_df[display_cols]
        .head(30)
        .to_string(index=False)
    )


if __name__ == "__main__":
    args = parse_args()
    main(
        mode=args.mode,
        lookback_bars=args.lookback_bars,
        symbols=args.symbols,
    )