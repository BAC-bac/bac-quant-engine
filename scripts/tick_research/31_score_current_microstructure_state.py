"""
BACQE TICK RESEARCH - 31 Score Current Microstructure State

Scores the latest available microstructure / multi-layer state against the
state forecast engine.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/multi_layer_states/GBPUSD_multi_layer_state_model_latest.parquet
    E:/Quant_Lab/data/analysis/tick_research/state_forecast_engine_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/current_microstructure_state_score_latest.csv
    E:/Quant_Lab/reports/tick_research/current_state/current_microstructure_state_score_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
SYMBOL = "GBPUSD"

STATE_MODEL_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "multi_layer_states"
    / f"{SYMBOL}_multi_layer_state_model_latest.parquet"
)

FORECAST_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "state_forecast_engine_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "current_state"

STATE_COLUMNS = [
    "compact_multi_layer_state",
    "trend_micro_state",
    "vol_micro_state",
    "microstructure_regime",
    "m15_composite_regime",
]


def classify_live_bias(row: pd.Series) -> str:
    edge = row.get("expected_persistence_edge", np.nan)
    quality = row.get("forecast_quality", "unknown")

    if quality not in {"usable", "stronger"}:
        return "insufficient_evidence"

    if pd.isna(edge):
        return "unclear"

    if edge >= 20:
        return "strong_momentum_bias"

    if edge >= 10:
        return "mild_momentum_bias"

    if edge <= -20:
        return "strong_mean_reversion_bias"

    if edge <= -10:
        return "mild_mean_reversion_bias"

    return "neutral_or_unclear"


def classify_actionability(row: pd.Series) -> str:
    quality = row.get("forecast_quality", "unknown")
    transitions = row.get("total_transitions", 0)
    probability = row.get("most_likely_transition_probability", np.nan)

    if quality == "stronger" and transitions >= 50 and pd.notna(probability) and probability >= 0.60:
        return "research_watchlist"

    if quality in {"usable", "stronger"} and transitions >= 10:
        return "diagnostic_only"

    return "too_early"


def load_latest_state() -> pd.Series:
    if not STATE_MODEL_PATH.exists():
        raise FileNotFoundError(f"State model file not found: {STATE_MODEL_PATH}")

    states = pd.read_parquet(STATE_MODEL_PATH)

    states["bar_start_time"] = pd.to_datetime(states["bar_start_time"], errors="coerce", utc=True)
    states = states.dropna(subset=["bar_start_time"]).sort_values("bar_start_time")

    if states.empty:
        raise ValueError("State model is empty after timestamp cleaning.")

    return states.iloc[-1]


def score_current_state(latest: pd.Series, forecasts: pd.DataFrame) -> pd.DataFrame:
    records = []

    for state_col in STATE_COLUMNS:
        if state_col not in latest.index:
            continue

        current_state = str(latest[state_col])
        current_bar_type = str(latest["bar_type"])

        match = forecasts[
            (forecasts["state_column"] == state_col)
            & (forecasts["bar_type"] == current_bar_type)
            & (forecasts["from_state"] == current_state)
        ].copy()

        if match.empty:
            records.append(
                {
                    "symbol": SYMBOL,
                    "bar_type": current_bar_type,
                    "state_column": state_col,
                    "current_state": current_state,
                    "forecast_found": False,
                    "live_bias": "no_forecast_available",
                    "actionability": "too_early",
                    "score_time_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue

        row = match.iloc[0].to_dict()

        record = {
            "symbol": SYMBOL,
            "latest_bar_start_time": latest.get("bar_start_time"),
            "latest_bar_end_time": latest.get("bar_end_time"),
            "bar_type": current_bar_type,
            "bar_family": latest.get("bar_family"),
            "state_column": state_col,
            "current_state": current_state,
            "forecast_found": True,
            "microstructure_regime": latest.get("microstructure_regime"),
            "m15_composite_regime": latest.get("m15_composite_regime"),
            "m15_trend_state": latest.get("m15_trend_state"),
            "m15_volatility_state": latest.get("m15_volatility_state"),
            "m15_momentum_state": latest.get("m15_momentum_state"),
            "latest_return": latest.get("return"),
            "latest_abs_return": latest.get("abs_return"),
            "latest_duration_seconds": latest.get("duration_seconds"),
            "latest_tick_count": latest.get("tick_count"),
            "latest_ticks_per_second": latest.get("ticks_per_second"),
            "latest_m15_regime_confidence": latest.get("m15_regime_confidence"),
            **row,
            "live_bias": classify_live_bias(pd.Series(row)),
            "actionability": classify_actionability(pd.Series(row)),
            "score_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        records.append(record)

    scored = pd.DataFrame(records)

    numeric_cols = scored.select_dtypes(include=["float", "int"]).columns
    scored[numeric_cols] = scored[numeric_cols].round(8)

    return scored


def build_report(scored: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "state_column",
        "bar_type",
        "current_state",
        "most_likely_next_state",
        "most_likely_transition_probability",
        "self_transition_probability",
        "state_stability_label",
        "expected_persist_pct",
        "expected_flip_pct",
        "expected_persistence_edge",
        "expected_behaviour_label",
        "forecast_quality",
        "live_bias",
        "actionability",
    ]

    available_cols = [col for col in display_cols if col in scored.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - CURRENT MICROSTRUCTURE STATE SCORE")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Symbol:          {SYMBOL}")
    lines.append(f"State model:     {STATE_MODEL_PATH}")
    lines.append(f"Forecast engine: {FORECAST_PATH}")
    lines.append("-" * 90)

    if scored.empty:
        lines.append("No current state scores were produced.")
    else:
        first = scored.iloc[0]

        lines.append(f"Latest bar start: {first.get('latest_bar_start_time')}")
        lines.append(f"Latest bar end:   {first.get('latest_bar_end_time')}")
        lines.append(f"Bar type:         {first.get('bar_type')}")
        lines.append(f"Micro regime:     {first.get('microstructure_regime')}")
        lines.append(f"M15 regime:       {first.get('m15_composite_regime')}")
        lines.append(f"M15 trend:        {first.get('m15_trend_state')}")
        lines.append(f"M15 volatility:   {first.get('m15_volatility_state')}")
        lines.append("-" * 90)

        lines.append("")
        lines.append("STATE FORECAST SCORES")
        lines.append("-" * 90)
        lines.append(scored[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is a state-intelligence report, not a trading instruction.")
    lines.append("Forecasts are based on historical transition behaviour.")
    lines.append("research_watchlist means the state is interesting enough to monitor, not trade blindly.")
    lines.append("insufficient_evidence means more live data is needed before interpreting the state.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 31 SCORE CURRENT MICROSTRUCTURE STATE")
    print("=" * 90)
    print(f"State model:     {STATE_MODEL_PATH}")
    print(f"Forecast engine: {FORECAST_PATH}")
    print("-" * 90)

    if not FORECAST_PATH.exists():
        raise FileNotFoundError(f"Forecast engine file not found: {FORECAST_PATH}")

    latest = load_latest_state()
    forecasts = pd.read_csv(FORECAST_PATH, low_memory=False)

    scored = score_current_state(latest, forecasts)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    score_csv = OUTPUT_ANALYSIS_DIR / "current_microstructure_state_score_latest.csv"
    score_parquet = OUTPUT_ANALYSIS_DIR / "current_microstructure_state_score_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "current_microstructure_state_score_latest.txt"

    scored.to_csv(score_csv, index=False)
    scored.to_parquet(score_parquet, index=False)

    report = build_report(scored)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Current microstructure state score created.")
    print(f"Score CSV:     {score_csv}")
    print(f"Score Parquet: {score_parquet}")
    print(f"Report:        {report_path}")
    print("-" * 90)

    display_cols = [
        "state_column",
        "bar_type",
        "current_state",
        "most_likely_next_state",
        "most_likely_transition_probability",
        "expected_persistence_edge",
        "expected_behaviour_label",
        "forecast_quality",
        "live_bias",
        "actionability",
    ]

    available_cols = [col for col in display_cols if col in scored.columns]

    print(scored[available_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()