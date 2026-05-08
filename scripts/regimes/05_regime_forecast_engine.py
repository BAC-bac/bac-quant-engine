"""
05_regime_forecast_engine.py
============================

BAC Quant Engine - Regime Engine
Stage 05: Regime forecast engine.

Purpose:
- Read latest classified regime files
- Read regime transition probabilities
- Estimate likely next regime for each symbol/timeframe
- Estimate persistence, transition, breakout and volatility risk
- Save forecast reports
- Support timeframe-group processing using --mode full/small/medium/large
"""

from pathlib import Path
from datetime import datetime
import logging
import sys
import argparse

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

REGIME_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")
TRANSITION_DIR = Path("E:/Quant_Lab/data/analysis/regime_transitions/FTMO")

REPORT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_forecasts/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REPORT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# TIMEFRAME GROUPS
# ============================================================

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


def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BACQE regime forecast engine by timeframe group."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Choose which timeframe group to forecast.",
    )

    return parser.parse_args()


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"regime_forecast_engine_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


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


def extract_symbol_timeframe(path: Path) -> tuple[str, str]:
    timeframe = path.parent.name
    suffix = f"_{timeframe}_regimes"
    symbol = path.stem.replace(suffix, "")
    return symbol, timeframe


def get_transition_paths(mode: str) -> tuple[Path, Path]:
    suffix = get_mode_suffix(mode)

    transition_path = TRANSITION_DIR / f"regime_transition_detail{suffix}_latest.parquet"
    global_transition_path = TRANSITION_DIR / f"regime_transition_global{suffix}_latest.parquet"

    return transition_path, global_transition_path


def get_latest_state(path: Path) -> dict | None:
    symbol, timeframe = extract_symbol_timeframe(path)

    df = pd.read_parquet(path)

    if df.empty:
        return None

    required_cols = {
        "time",
        "composite_regime",
        "trend_state",
        "volatility_state",
        "momentum_state",
        "trend_strength_state",
        "regime_confidence",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} {timeframe}: missing columns {missing}")

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").reset_index(drop=True)

    latest = df.iloc[-1]

    current_regime = latest["composite_regime"]

    run_length = 1
    for regime in reversed(df["composite_regime"].iloc[:-1].tolist()):
        if regime == current_regime:
            run_length += 1
        else:
            break

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "latest_time": latest["time"],
        "current_regime": current_regime,
        "trend_state": latest["trend_state"],
        "volatility_state": latest["volatility_state"],
        "momentum_state": latest["momentum_state"],
        "trend_strength_state": latest["trend_strength_state"],
        "current_confidence": round(float(latest["regime_confidence"]), 4),
        "current_regime_run_length": run_length,
    }


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


def summarise_forecast(
    latest_state: dict,
    probs: pd.DataFrame,
) -> dict:
    current_regime = latest_state["current_regime"]

    if probs.empty:
        return {
            **latest_state,
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
        **latest_state,
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


def main(mode: str = "full") -> None:
    logger.info("=" * 80)
    logger.info("Starting regime forecast engine")
    logger.info(f"Mode: {mode}")
    logger.info("=" * 80)

    suffix = get_mode_suffix(mode)
    allowed_timeframes = get_allowed_timeframes(mode)

    transition_path, global_transition_path = get_transition_paths(mode)

    logger.info(f"Transition detail path: {transition_path}")
    logger.info(f"Global transition path: {global_transition_path}")

    if not transition_path.exists():
        raise FileNotFoundError(f"Missing transition detail file: {transition_path}")

    if not global_transition_path.exists():
        raise FileNotFoundError(f"Missing global transition file: {global_transition_path}")

    transition_df = pd.read_parquet(transition_path)
    global_transition_df = pd.read_parquet(global_transition_path)

    regime_files = sorted(REGIME_ROOT.rglob("*_regimes.parquet"))

    if allowed_timeframes is not None:
        regime_files = [
            path for path in regime_files
            if path.parent.name in allowed_timeframes
        ]

    logger.info(f"Regime files discovered after mode filter: {len(regime_files)}")

    if allowed_timeframes is not None:
        logger.info(f"Allowed timeframes: {sorted(allowed_timeframes)}")
    else:
        logger.info("Allowed timeframes: all")

    logger.info(f"Transition rows loaded: {len(transition_df)}")
    logger.info(f"Global transition rows loaded: {len(global_transition_df)}")

    if not regime_files:
        logger.warning("No regime files found")
        return

    forecasts = []
    failures = 0

    for idx, path in enumerate(regime_files, start=1):
        logger.info(f"[{idx}/{len(regime_files)}] Forecasting {path.name}")

        try:
            latest_state = get_latest_state(path)

            if latest_state is None:
                continue

            probs = get_transition_probabilities(
                symbol=latest_state["symbol"],
                timeframe=latest_state["timeframe"],
                current_regime=latest_state["current_regime"],
                transition_df=transition_df,
                global_transition_df=global_transition_df,
            )

            forecast = summarise_forecast(latest_state, probs)
            forecasts.append(forecast)

        except Exception as exc:
            logger.error(f"Failed to forecast {path}: {exc}")
            failures += 1

    forecast_df = pd.DataFrame(forecasts)

    if forecast_df.empty:
        logger.warning("No forecasts created")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = REPORT_ROOT / f"regime_forecast{suffix}_{timestamp}.csv"
    parquet_path = REPORT_ROOT / f"regime_forecast{suffix}_{timestamp}.parquet"

    latest_csv = REPORT_ROOT / f"regime_forecast{suffix}_latest.csv"
    latest_parquet = REPORT_ROOT / f"regime_forecast{suffix}_latest.parquet"

    forecast_df.to_csv(csv_path, index=False)
    forecast_df.to_parquet(parquet_path, index=False)

    forecast_df.to_csv(latest_csv, index=False)
    forecast_df.to_parquet(latest_parquet, index=False)

    logger.info("=" * 80)
    logger.info("Regime forecast engine completed")
    logger.info(f"Mode: {mode}")
    logger.info(f"Forecast rows: {len(forecast_df)}")
    logger.info(f"Failures: {failures}")
    logger.info(f"Saved latest forecast: {latest_csv}")
    logger.info("=" * 80)

    logger.info("Forecast signal counts:")
    logger.info(forecast_df["forecast_signal"].value_counts().to_string())

    logger.info("Top 30 highest breakout probability forecasts:")
    cols = [
        "symbol",
        "timeframe",
        "latest_time",
        "current_regime",
        "most_likely_next_regime",
        "persistence_probability_pct",
        "breakout_probability_pct",
        "volatility_expansion_probability_pct",
        "bullish_probability_pct",
        "bearish_probability_pct",
        "forecast_signal",
    ]

    logger.info(
        forecast_df.sort_values(
            "breakout_probability_pct",
            ascending=False,
        )[cols].head(30).to_string(index=False)
    )

    logger.info("Top 30 highest volatility expansion probability forecasts:")
    logger.info(
        forecast_df.sort_values(
            "volatility_expansion_probability_pct",
            ascending=False,
        )[cols].head(30).to_string(index=False)
    )


if __name__ == "__main__":
    args = parse_args()
    main(mode=args.mode)