"""
BACQE TICK RESEARCH - 31 Score Current Microstructure State - Multi Symbol

Scores the latest available microstructure / multi-layer state for each symbol
against the multi-symbol state forecast engine produced by Script 30.
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

STATE_MODEL_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "multi_layer_states"
)

FORECAST_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "state_forecasts"
    / "_master"
    / "master_state_forecast_engine_latest.csv"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "current_state"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "current_state"
)

STATE_COLUMNS = [
    "compact_multi_layer_state",
    "multi_layer_state",
    "trend_micro_state",
    "vol_micro_state",
    "momentum_micro_state",
    "trend_strength_micro_state",
    "microstructure_regime",
]


def detect_selected_timeframe(latest: pd.Series) -> str:
    value = latest.get("selected_timeframe", None)

    if pd.notna(value):
        return str(value)

    value = latest.get("selected_regime_timeframe", None)

    if pd.notna(value):
        return str(value)

    return "unknown"


def get_regime_cols(timeframe: str) -> dict:
    prefix = str(timeframe).lower()

    return {
        "trend": f"{prefix}_trend_state",
        "volatility": f"{prefix}_volatility_state",
        "momentum": f"{prefix}_momentum_state",
        "trend_strength": f"{prefix}_trend_strength_state",
        "composite": f"{prefix}_composite_regime",
        "confidence": f"{prefix}_regime_confidence",
    }


def classify_live_bias(row: pd.Series) -> str:
    edge = row.get("expected_persistence_edge", np.nan)
    quality = row.get("forecast_quality", "unknown")

    if quality not in {"usable", "stronger", "high_confidence"}:
        return "insufficient_evidence"

    if pd.isna(edge):
        return "unclear"

    if edge >= 10:
        return "strong_momentum_bias"

    if edge >= 5:
        return "mild_momentum_bias"

    if edge <= -10:
        return "strong_mean_reversion_bias"

    if edge <= -5:
        return "mild_mean_reversion_bias"

    return "neutral_or_unclear"


def classify_actionability(row: pd.Series) -> str:
    quality = row.get("forecast_quality", "unknown")
    transitions = row.get("total_transitions", 0)
    probability = row.get("most_likely_transition_probability", np.nan)
    is_unknown_state = bool(row.get("is_unknown_state", False))

    if is_unknown_state:
        return "ignore_unknown_state"

    if (
        quality == "high_confidence"
        and transitions >= 500
        and pd.notna(probability)
        and probability >= 0.60
    ):
        return "research_watchlist_high_confidence"

    if (
        quality == "stronger"
        and transitions >= 100
        and pd.notna(probability)
        and probability >= 0.50
    ):
        return "research_watchlist"

    if quality in {"usable", "stronger", "high_confidence"} and transitions >= 10:
        return "diagnostic_only"

    return "too_early"


def get_state_model_path(symbol: str) -> Path:
    return (
        STATE_MODEL_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_multi_layer_state_model_latest.parquet"
    )


def load_latest_state(symbol: str) -> pd.Series:
    path = get_state_model_path(symbol)

    if not path.exists():
        raise FileNotFoundError(f"{symbol}: state model file not found: {path}")

    states = pd.read_parquet(path)

    states["bar_start_time"] = pd.to_datetime(
        states["bar_start_time"],
        errors="coerce",
        utc=True,
    )

    states = (
        states.dropna(subset=["bar_start_time"])
        .sort_values("bar_start_time")
        .reset_index(drop=True)
    )

    if states.empty:
        raise ValueError(f"{symbol}: state model is empty after timestamp cleaning.")

    return states.iloc[-1]


def score_symbol_current_state(
    symbol: str,
    latest: pd.Series,
    forecasts: pd.DataFrame,
) -> pd.DataFrame:
    records = []

    selected_timeframe = detect_selected_timeframe(latest)
    regime_cols = get_regime_cols(selected_timeframe)

    current_bar_type = str(latest.get("bar_type", "unknown_bar_type"))

    symbol_forecasts = forecasts[
        forecasts["symbol"].astype(str) == symbol
    ].copy()

    for state_col in STATE_COLUMNS:
        if state_col not in latest.index:
            continue

        current_state = str(latest[state_col])

        match = symbol_forecasts[
            (symbol_forecasts["state_column"].astype(str) == state_col)
            & (symbol_forecasts["bar_type"].astype(str) == current_bar_type)
            & (symbol_forecasts["from_state"].astype(str) == current_state)
        ].copy()

        base_record = {
            "symbol": symbol,
            "selected_timeframe": selected_timeframe,
            "latest_bar_start_time": latest.get("bar_start_time"),
            "latest_bar_end_time": latest.get("bar_end_time"),
            "bar_type": current_bar_type,
            "bar_family": latest.get("bar_family"),
            "state_column": state_col,
            "current_state": current_state,
            "microstructure_regime": latest.get("microstructure_regime"),
            "regime_join_status": latest.get("regime_join_status"),
            "has_selected_regime": latest.get("has_selected_regime"),
            "composite_regime": latest.get(regime_cols["composite"]),
            "trend_state": latest.get(regime_cols["trend"]),
            "volatility_state": latest.get(regime_cols["volatility"]),
            "momentum_state": latest.get(regime_cols["momentum"]),
            "trend_strength_state": latest.get(regime_cols["trend_strength"]),
            "regime_confidence": latest.get(regime_cols["confidence"]),
            "latest_return": latest.get("return"),
            "latest_abs_return": latest.get("abs_return"),
            "latest_duration_seconds": latest.get("duration_seconds"),
            "latest_tick_count": latest.get("tick_count"),
            "latest_ticks_per_second": latest.get("ticks_per_second"),
            "score_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        if match.empty:
            records.append(
                {
                    **base_record,
                    "forecast_found": False,
                    "live_bias": "no_forecast_available",
                    "actionability": "too_early",
                }
            )
            continue

        row = match.iloc[0].to_dict()

        record = {
            **base_record,
            "forecast_found": True,
            **row,
        }

        record["live_bias"] = classify_live_bias(pd.Series(record))
        record["actionability"] = classify_actionability(pd.Series(record))

        records.append(record)

    scored = pd.DataFrame(records)

    numeric_cols = scored.select_dtypes(include=["float", "int"]).columns
    scored[numeric_cols] = scored[numeric_cols].round(8)

    return scored


def build_symbol_report(symbol: str, scored: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "state_column",
        "bar_type",
        "current_state",
        "forecast_found",
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
    lines.append(f"BACQE TICK RESEARCH - CURRENT MICROSTRUCTURE STATE SCORE - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"State model:     {get_state_model_path(symbol)}")
    lines.append(f"Forecast engine: {FORECAST_PATH}")
    lines.append("-" * 90)

    if scored.empty:
        lines.append("No current state scores were produced.")
    else:
        first = scored.iloc[0]

        lines.append(f"Latest bar start:      {first.get('latest_bar_start_time')}")
        lines.append(f"Latest bar end:        {first.get('latest_bar_end_time')}")
        lines.append(f"Bar type:              {first.get('bar_type')}")
        lines.append(f"Selected timeframe:    {first.get('selected_timeframe')}")
        lines.append(f"Regime join status:    {first.get('regime_join_status')}")
        lines.append(f"Micro regime:          {first.get('microstructure_regime')}")
        lines.append(f"Composite regime:      {first.get('composite_regime')}")
        lines.append(f"Trend state:           {first.get('trend_state')}")
        lines.append(f"Volatility state:      {first.get('volatility_state')}")
        lines.append(f"Momentum state:        {first.get('momentum_state')}")
        lines.append(f"Regime confidence:     {first.get('regime_confidence')}")
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
    lines.append("research_watchlist_high_confidence means the state is interesting enough to monitor.")
    lines.append("Unknown states are retained but marked as ignore_unknown_state.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str, forecasts: pd.DataFrame) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    try:
        latest = load_latest_state(symbol)
    except Exception as exc:
        print(f"[ERROR] {symbol}: {exc}")
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "forecast_found": False,
                    "live_bias": "state_load_failed",
                    "actionability": "too_early",
                    "error_message": str(exc),
                    "score_time_utc": datetime.now(timezone.utc).isoformat(),
                }
            ]
        )

    scored = score_symbol_current_state(symbol, latest, forecasts)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    score_csv = analysis_dir / f"{symbol}_current_microstructure_state_score_latest.csv"
    score_parquet = analysis_dir / f"{symbol}_current_microstructure_state_score_latest.parquet"
    report_path = report_dir / f"{symbol}_current_microstructure_state_score_latest.txt"

    scored.to_csv(score_csv, index=False)
    scored.to_parquet(score_parquet, index=False)

    report = build_symbol_report(symbol, scored)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: Score CSV: {score_csv}")
    print(f"[DONE] {symbol}: Report:    {report_path}")

    return scored


def save_master_outputs(score_frames: list[pd.DataFrame]) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_scores = pd.concat(score_frames, ignore_index=True)

    score_csv = master_analysis_dir / "master_current_microstructure_state_score_latest.csv"
    score_parquet = master_analysis_dir / "master_current_microstructure_state_score_latest.parquet"
    report_path = master_report_dir / "master_current_microstructure_state_score_latest.txt"

    master_scores.to_csv(score_csv, index=False)
    master_scores.to_parquet(score_parquet, index=False)

    display_cols = [
        "symbol",
        "selected_timeframe",
        "bar_type",
        "state_column",
        "current_state",
        "forecast_found",
        "most_likely_next_state",
        "most_likely_transition_probability",
        "self_transition_probability",
        "expected_persistence_edge",
        "forecast_quality",
        "live_bias",
        "actionability",
    ]

    available_cols = [col for col in display_cols if col in master_scores.columns]

    watchlist = master_scores[
        master_scores["actionability"].isin(
            ["research_watchlist", "research_watchlist_high_confidence"]
        )
    ].copy()

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - MASTER CURRENT MICROSTRUCTURE STATE SCORE")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Forecast engine: {FORECAST_PATH}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("RESEARCH WATCHLIST STATES")
    lines.append("-" * 90)
    if watchlist.empty:
        lines.append("No current states met research watchlist criteria.")
    else:
        lines.append(watchlist[available_cols].to_string(index=False))

    lines.append("")
    lines.append("ALL CURRENT STATE SCORES")
    lines.append("-" * 90)
    lines.append(master_scores[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This report scores current states against historical transition forecasts.")
    lines.append("It is for research monitoring only, not automated execution.")
    lines.append("=" * 90)

    report_path.write_text("\n".join(lines), encoding="utf-8")

    print("-" * 90)
    print("[DONE] Master current state score created.")
    print(f"Master Score CSV:     {score_csv}")
    print(f"Master Score Parquet: {score_parquet}")
    print(f"Master Report:        {report_path}")
    print("-" * 90)
    print("MASTER CURRENT STATE PREVIEW")
    print(master_scores[available_cols].to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 31 SCORE CURRENT MICROSTRUCTURE STATE - MULTI SYMBOL")
    print("=" * 90)
    print(f"State model root: {STATE_MODEL_ROOT}")
    print(f"Forecast engine:  {FORECAST_PATH}")
    print(f"Output analysis:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:   {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:          {SYMBOLS}")
    print("-" * 90)

    if not FORECAST_PATH.exists():
        raise FileNotFoundError(f"Forecast engine file not found: {FORECAST_PATH}")

    forecasts = pd.read_csv(FORECAST_PATH, low_memory=False)

    score_frames = []

    for symbol in SYMBOLS:
        scored = process_symbol(symbol, forecasts)

        if not scored.empty:
            score_frames.append(scored)

    if not score_frames:
        print("[WARN] No current state scores created.")
        return

    save_master_outputs(score_frames)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol current microstructure state scoring complete.")
    print(f"Symbols scored: {len(score_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()