"""
04_regime_transition_matrix.py
==============================

BAC Quant Engine - Regime Engine
Stage 04: Regime transition matrix.

Purpose:
- Read classified regime parquet files
- Calculate regime-to-regime transition counts and probabilities
- Measure regime persistence
- Save transition reports
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

REGIME_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")
REPORT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_transitions/FTMO")

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REPORT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"regime_transitions_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


MIN_ROWS = 100


def extract_symbol_timeframe(path: Path) -> tuple[str, str]:
    timeframe = path.parent.name
    suffix = f"_{timeframe}_regimes"
    symbol = path.stem.replace(suffix, "")
    return symbol, timeframe


def analyse_transitions(path: Path) -> tuple[pd.DataFrame, dict] | tuple[None, None]:
    symbol, timeframe = extract_symbol_timeframe(path)

    df = pd.read_parquet(path)

    if df.empty or len(df) < MIN_ROWS:
        return None, None

    required_cols = {"time", "composite_regime", "regime_confidence"}
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} {timeframe}: missing columns {missing}")

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").reset_index(drop=True)

    df["from_regime"] = df["composite_regime"]
    df["to_regime"] = df["composite_regime"].shift(-1)

    transitions = df.dropna(subset=["to_regime"]).copy()

    if transitions.empty:
        return None, None

    counts = (
        transitions.groupby(["from_regime", "to_regime"])
        .size()
        .reset_index(name="transition_count")
    )

    total_from = (
        counts.groupby("from_regime")["transition_count"]
        .sum()
        .reset_index(name="from_total")
    )

    counts = counts.merge(total_from, on="from_regime", how="left")
    counts["transition_probability"] = counts["transition_count"] / counts["from_total"]

    counts["transition_probability_pct"] = round(
        counts["transition_probability"] * 100,
        2,
    )

    counts["symbol"] = symbol
    counts["timeframe"] = timeframe

    counts = counts[
        [
            "symbol",
            "timeframe",
            "from_regime",
            "to_regime",
            "transition_count",
            "from_total",
            "transition_probability",
            "transition_probability_pct",
        ]
    ]

    same_regime = transitions["from_regime"] == transitions["to_regime"]

    persistence_rate = same_regime.mean()

    current_regime = df["composite_regime"].iloc[-1]
    previous_regime = df["composite_regime"].iloc[-2] if len(df) > 1 else None

    latest_confidence = df["regime_confidence"].iloc[-1]

    current_run_length = 1

    for regime in reversed(df["composite_regime"].iloc[:-1].tolist()):
        if regime == current_regime:
            current_run_length += 1
        else:
            break

    summary = {
        "symbol": symbol,
        "timeframe": timeframe,
        "rows": len(df),
        "start_time": df["time"].min(),
        "end_time": df["time"].max(),
        "unique_regimes": df["composite_regime"].nunique(),
        "persistence_rate": round(persistence_rate, 4),
        "persistence_pct": round(persistence_rate * 100, 2),
        "current_regime": current_regime,
        "previous_regime": previous_regime,
        "latest_confidence": round(float(latest_confidence), 4),
        "current_regime_run_length": current_run_length,
    }

    return counts, summary


def main() -> None:
    logger.info("Starting regime transition analysis")
    logger.info(f"Regime root: {REGIME_ROOT}")
    logger.info(f"Report root: {REPORT_ROOT}")

    files = sorted(REGIME_ROOT.rglob("*_regimes.parquet"))

    logger.info(f"Discovered {len(files)} classified regime files")

    all_transitions = []
    summaries = []
    failures = 0

    for idx, path in enumerate(files, start=1):
        logger.info(f"[{idx}/{len(files)}] Analysing {path.name}")

        try:
            transitions, summary = analyse_transitions(path)

            if transitions is not None:
                all_transitions.append(transitions)

            if summary is not None:
                summaries.append(summary)

        except Exception as exc:
            logger.error(f"Failed {path}: {exc}")
            failures += 1

    if not all_transitions:
        logger.warning("No transitions created")
        return

    transition_df = pd.concat(all_transitions, ignore_index=True)
    summary_df = pd.DataFrame(summaries)

    global_transition = (
        transition_df.groupby(["from_regime", "to_regime"])
        .agg(transition_count=("transition_count", "sum"))
        .reset_index()
    )

    global_totals = (
        global_transition.groupby("from_regime")["transition_count"]
        .sum()
        .reset_index(name="from_total")
    )

    global_transition = global_transition.merge(global_totals, on="from_regime", how="left")
    global_transition["transition_probability"] = (
        global_transition["transition_count"] / global_transition["from_total"]
    )
    global_transition["transition_probability_pct"] = round(
        global_transition["transition_probability"] * 100,
        2,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    reports = {
        "regime_transition_detail": transition_df,
        "regime_transition_summary": summary_df,
        "regime_transition_global": global_transition,
    }

    for name, df in reports.items():
        csv_path = REPORT_ROOT / f"{name}_{timestamp}.csv"
        parquet_path = REPORT_ROOT / f"{name}_{timestamp}.parquet"

        latest_csv = REPORT_ROOT / f"{name}_latest.csv"
        latest_parquet = REPORT_ROOT / f"{name}_latest.parquet"

        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)

        df.to_csv(latest_csv, index=False)
        df.to_parquet(latest_parquet, index=False)

        logger.info(f"Saved {name}: {latest_csv}")

    logger.info("Regime transition analysis completed")
    logger.info(f"Transition rows: {len(transition_df)}")
    logger.info(f"Summary rows: {len(summary_df)}")
    logger.info(f"Failures: {failures}")

    logger.info("Global transition probabilities:")
    logger.info(
        global_transition.sort_values(
            ["from_regime", "transition_probability"],
            ascending=[True, False],
        ).to_string(index=False)
    )

    logger.info("Top 20 most persistent symbol/timeframes:")
    logger.info(
        summary_df.sort_values(
            "persistence_rate",
            ascending=False,
        )[
            [
                "symbol",
                "timeframe",
                "persistence_pct",
                "current_regime",
                "latest_confidence",
                "current_regime_run_length",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()