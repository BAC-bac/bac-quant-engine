"""
13_strategy_performance_by_regime.py
====================================

BAC Quant Engine - Regime Engine
Stage 13: Strategy performance by regime.

Purpose:
- Read classified regime files
- Create simple proxy strategy returns by regime
- Estimate which strategy families perform best under each regime
- Save performance tables for research and future strategy selection

This is research only.
It does NOT place trades.
"""

from pathlib import Path
from datetime import datetime
import argparse
import logging
import sys

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CLASSIFIED_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")
OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_strategy_performance/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


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


log_path = LOG_DIR / f"strategy_performance_by_regime_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def get_allowed_timeframes(mode: str) -> set[str] | None:
    allowed = TIMEFRAME_GROUPS.get(mode)
    return None if allowed is None else set(allowed)


def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyse proxy strategy performance by regime."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="small",
        help="Timeframe group to analyse.",
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit for testing.",
    )

    parser.add_argument(
        "--holding-bars",
        type=int,
        default=1,
        help="Forward return horizon in bars.",
    )

    return parser.parse_args()


def extract_symbol_timeframe(path: Path) -> tuple[str, str]:
    timeframe = path.parent.name
    suffix = f"_{timeframe}_regimes"
    symbol = path.stem.replace(suffix, "")
    return symbol, timeframe


def safe_profit_factor(returns: pd.Series) -> float | None:
    gains = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        if gains > 0:
            return np.inf
        return None

    return round(float(gains / abs(losses)), 4)


def safe_sharpe(returns: pd.Series) -> float | None:
    returns = returns.dropna()

    if len(returns) < 30:
        return None

    std = returns.std()

    if std == 0 or pd.isna(std):
        return None

    return round(float(returns.mean() / std), 4)


def add_proxy_strategy_returns(df: pd.DataFrame, holding_bars: int) -> pd.DataFrame:
    df = df.copy()

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").reset_index(drop=True)

    df["future_return"] = df["close"].pct_change(periods=holding_bars).shift(-holding_bars)

    df["trend_long_return"] = np.where(
        df["trend_state"] == "bull_trend",
        df["future_return"],
        np.nan,
    )

    df["trend_short_return"] = np.where(
        df["trend_state"] == "bear_trend",
        -df["future_return"],
        np.nan,
    )

    df["trend_following_return"] = np.where(
        df["trend_state"] == "bull_trend",
        df["future_return"],
        np.where(
            df["trend_state"] == "bear_trend",
            -df["future_return"],
            np.nan,
        ),
    )

    df["mean_reversion_return"] = np.where(
        df["composite_regime"].isin(["range", "quiet_range", "volatile_range"]),
        -np.sign(df["return_1"]) * df["future_return"],
        np.nan,
    )

    df["breakout_return"] = np.where(
        df["composite_regime"].isin(["transition", "volatile_transition"]),
        np.sign(df["close_slope_10"]) * df["future_return"],
        np.nan,
    )

    df["risk_off_return"] = 0.0

    return df


def summarise_strategy_returns(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
    strategy_col: str,
    strategy_name: str,
) -> list[dict]:
    rows = []

    grouped = df.dropna(subset=[strategy_col]).groupby("composite_regime")

    for regime, group in grouped:
        returns = group[strategy_col].dropna()

        if returns.empty:
            continue

        win_rate = float((returns > 0).mean())
        avg_return = float(returns.mean())
        median_return = float(returns.median())

        rows.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "timeframe_rank": TIMEFRAME_ORDER.get(timeframe, 999999),
                "composite_regime": regime,
                "strategy_name": strategy_name,
                "observations": len(returns),
                "avg_return": round(avg_return, 8),
                "median_return": round(median_return, 8),
                "total_return_proxy": round(float(returns.sum()), 8),
                "win_rate_pct": round(win_rate * 100, 2),
                "profit_factor": safe_profit_factor(returns),
                "sharpe_proxy": safe_sharpe(returns),
                "max_single_loss": round(float(returns.min()), 8),
                "max_single_gain": round(float(returns.max()), 8),
            }
        )

    return rows


def process_file(path: Path, holding_bars: int) -> list[dict]:
    symbol, timeframe = extract_symbol_timeframe(path)

    df = pd.read_parquet(path)

    required_cols = {
        "time",
        "close",
        "return_1",
        "close_slope_10",
        "trend_state",
        "composite_regime",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} {timeframe}: missing columns {missing}")

    df = add_proxy_strategy_returns(df, holding_bars=holding_bars)

    strategy_cols = {
        "trend_long_return": "trend_following_long_proxy",
        "trend_short_return": "trend_following_short_proxy",
        "trend_following_return": "trend_following_two_way_proxy",
        "mean_reversion_return": "mean_reversion_range_proxy",
        "breakout_return": "breakout_transition_proxy",
        "risk_off_return": "risk_off_proxy",
    }

    rows = []

    for col, strategy_name in strategy_cols.items():
        rows.extend(
            summarise_strategy_returns(
                df=df,
                symbol=symbol,
                timeframe=timeframe,
                strategy_col=col,
                strategy_name=strategy_name,
            )
        )

    return rows


def build_global_summary(performance_df: pd.DataFrame) -> pd.DataFrame:
    if performance_df.empty:
        return performance_df

    grouped = (
        performance_df.groupby(["timeframe", "composite_regime", "strategy_name"])
        .agg(
            symbol_count=("symbol", "nunique"),
            total_observations=("observations", "sum"),
            avg_return_mean=("avg_return", "mean"),
            median_return_mean=("median_return", "mean"),
            win_rate_mean_pct=("win_rate_pct", "mean"),
            profit_factor_median=("profit_factor", "median"),
            sharpe_proxy_median=("sharpe_proxy", "median"),
            total_return_proxy_sum=("total_return_proxy", "sum"),
        )
        .reset_index()
    )

    grouped["timeframe_rank"] = grouped["timeframe"].map(TIMEFRAME_ORDER).fillna(999999)

    grouped = grouped.sort_values(
        by=[
            "timeframe_rank",
            "composite_regime",
            "win_rate_mean_pct",
            "profit_factor_median",
        ],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)

    return grouped


def build_best_strategy_by_regime(global_df: pd.DataFrame) -> pd.DataFrame:
    if global_df.empty:
        return global_df

    filtered = global_df[
        global_df["strategy_name"] != "risk_off_proxy"
    ].copy()

    if filtered.empty:
        return filtered

    best = (
        filtered.sort_values(
            by=["win_rate_mean_pct", "profit_factor_median", "total_observations"],
            ascending=[False, False, False],
        )
        .groupby(["timeframe", "composite_regime"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    return best.sort_values(
        by=["timeframe_rank", "composite_regime"],
        ascending=[True, True],
    ).reset_index(drop=True)


def save_outputs(df: pd.DataFrame, name: str, timestamp: str, mode: str) -> None:
    suffix = get_mode_suffix(mode)

    csv_path = OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.csv"
    parquet_path = OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.parquet"

    latest_csv = OUTPUT_ROOT / f"{name}{suffix}_latest.csv"
    latest_parquet = OUTPUT_ROOT / f"{name}{suffix}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def main(mode: str, max_files: int | None, holding_bars: int) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE Strategy Performance by Regime")
    logger.info(f"Mode: {mode}")
    logger.info(f"Max files: {max_files}")
    logger.info(f"Holding bars: {holding_bars}")
    logger.info("=" * 80)

    allowed_timeframes = get_allowed_timeframes(mode)

    files = sorted(CLASSIFIED_ROOT.rglob("*_regimes.parquet"))

    if allowed_timeframes is not None:
        files = [
            path for path in files
            if path.parent.name in allowed_timeframes
        ]

    if max_files is not None:
        files = files[:max_files]

    logger.info(f"Classified files selected: {len(files)}")

    if not files:
        logger.warning("No files selected")
        return

    all_rows = []
    failures = 0

    for idx, path in enumerate(files, start=1):
        symbol, timeframe = extract_symbol_timeframe(path)
        logger.info(f"[{idx}/{len(files)}] Analysing {symbol} {timeframe}")

        try:
            rows = process_file(path, holding_bars=holding_bars)
            all_rows.extend(rows)

        except Exception as exc:
            logger.error(f"{symbol} {timeframe}: failed with error: {exc}")
            failures += 1

    performance_df = pd.DataFrame(all_rows)

    if performance_df.empty:
        logger.warning("No performance rows created")
        return

    global_df = build_global_summary(performance_df)
    best_df = build_best_strategy_by_regime(global_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_outputs(performance_df, "strategy_performance_by_symbol_regime", timestamp, mode)
    save_outputs(global_df, "strategy_performance_global_by_regime", timestamp, mode)
    save_outputs(best_df, "strategy_performance_best_by_regime", timestamp, mode)

    logger.info("=" * 80)
    logger.info("Strategy Performance by Regime completed")
    logger.info(f"Files analysed: {len(files)}")
    logger.info(f"Performance rows: {len(performance_df)}")
    logger.info(f"Global rows: {len(global_df)}")
    logger.info(f"Best rows: {len(best_df)}")
    logger.info(f"Failures: {failures}")
    logger.info("=" * 80)

    logger.info("Best strategy by regime preview:")
    display_cols = [
        "timeframe",
        "composite_regime",
        "strategy_name",
        "symbol_count",
        "total_observations",
        "win_rate_mean_pct",
        "profit_factor_median",
        "sharpe_proxy_median",
    ]

    logger.info(best_df[display_cols].head(50).to_string(index=False))


if __name__ == "__main__":
    args = parse_args()
    main(
        mode=args.mode,
        max_files=args.max_files,
        holding_bars=args.holding_bars,
    )