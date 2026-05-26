"""
BACQE TICK RESEARCH - 30 Build State Forecast Engine

Builds a transparent state-forecast layer from the state transition matrix.

Input:
    E:/Quant_Lab/data/analysis/tick_research/state_transition_matrix_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/state_transition_summary_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/state_forecast_engine_latest.csv
    E:/Quant_Lab/reports/tick_research/state_forecasts/state_forecast_engine_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TRANSITION_MATRIX_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "state_transition_matrix_latest.csv"
)

TRANSITION_SUMMARY_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "state_transition_summary_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "state_forecasts"

MIN_TRANSITIONS_FOR_USABLE = 10
MIN_TRANSITIONS_FOR_STRONGER = 50


def classify_stability(self_prob: float) -> str:
    if pd.isna(self_prob):
        return "unknown"

    if self_prob >= 0.60:
        return "highly_persistent"

    if self_prob >= 0.40:
        return "moderately_persistent"

    if self_prob >= 0.20:
        return "unstable"

    return "highly_unstable"


def classify_forecast_quality(total_transitions: int, top_prob: float) -> str:
    if total_transitions < MIN_TRANSITIONS_FOR_USABLE:
        return "low_sample"

    if total_transitions >= MIN_TRANSITIONS_FOR_STRONGER and top_prob >= 0.40:
        return "stronger"

    if total_transitions >= MIN_TRANSITIONS_FOR_USABLE and top_prob >= 0.25:
        return "usable"

    return "weak"


def classify_expected_behaviour(row: pd.Series) -> str:
    persist = row.get("expected_persist_pct", np.nan)
    flip = row.get("expected_flip_pct", np.nan)
    abs_ret = row.get("expected_next_abs_return", np.nan)

    if pd.notna(persist) and pd.notna(flip):
        edge = persist - flip

        if edge >= 15:
            return "momentum_bias"

        if edge <= -15:
            return "mean_reversion_bias"

    if pd.notna(abs_ret):
        if abs_ret >= row.get("global_abs_return_75pct", np.inf):
            return "high_activity_expected"

    return "neutral_or_unclear"


def build_forecasts(matrix: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    records = []

    grouped = matrix.groupby(["state_column", "bar_type", "from_state"], dropna=False)

    global_abs_75 = matrix["avg_next_abs_return"].quantile(0.75)

    for keys, group in grouped:
        state_column, bar_type, from_state = keys

        group = group.sort_values("transition_probability", ascending=False)

        top = group.iloc[0]

        total_transitions = int(group["transition_count"].sum())

        self_row = group[group["to_state"] == from_state]
        self_prob = (
            float(self_row["transition_probability"].iloc[0])
            if not self_row.empty
            else 0.0
        )

        weighted_next_abs_return = np.average(
            group["avg_next_abs_return"].fillna(0),
            weights=group["transition_count"],
        )

        weighted_persist_pct = np.average(
            group["persist_target_pct"].fillna(0),
            weights=group["transition_count"],
        )

        weighted_flip_pct = np.average(
            group["flip_target_pct"].fillna(0),
            weights=group["transition_count"],
        )

        weighted_up_pct = np.average(
            group["up_target_pct"].fillna(0),
            weights=group["transition_count"],
        )

        forecast = {
            "state_column": state_column,
            "bar_type": bar_type,
            "from_state": from_state,
            "total_transitions": total_transitions,
            "unique_next_states": group["to_state"].nunique(),
            "most_likely_next_state": top["to_state"],
            "most_likely_transition_probability": top["transition_probability"],
            "self_transition_probability": self_prob,
            "state_stability_label": classify_stability(self_prob),
            "expected_next_abs_return": weighted_next_abs_return,
            "expected_persist_pct": weighted_persist_pct,
            "expected_flip_pct": weighted_flip_pct,
            "expected_up_pct": weighted_up_pct,
            "expected_persistence_edge": weighted_persist_pct - weighted_flip_pct,
            "global_abs_return_75pct": global_abs_75,
            "forecast_quality": classify_forecast_quality(
                total_transitions,
                top["transition_probability"],
            ),
            "forecast_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        forecast["expected_behaviour_label"] = classify_expected_behaviour(pd.Series(forecast))

        records.append(forecast)

    forecasts = pd.DataFrame(records)

    numeric_cols = forecasts.select_dtypes(include=["float", "int"]).columns
    forecasts[numeric_cols] = forecasts[numeric_cols].round(8)

    forecasts = forecasts.sort_values(
        [
            "forecast_quality",
            "expected_persistence_edge",
            "total_transitions",
        ],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return forecasts


def build_report(forecasts: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = forecasts[forecasts["forecast_quality"].isin(["usable", "stronger"])].copy()

    momentum = usable.sort_values(
        "expected_persistence_edge",
        ascending=False,
        na_position="last",
    ).head(40)

    mean_reversion = usable.sort_values(
        "expected_persistence_edge",
        ascending=True,
        na_position="last",
    ).head(40)

    persistent = usable.sort_values(
        "self_transition_probability",
        ascending=False,
        na_position="last",
    ).head(40)

    display_cols = [
        "state_column",
        "bar_type",
        "from_state",
        "total_transitions",
        "most_likely_next_state",
        "most_likely_transition_probability",
        "self_transition_probability",
        "state_stability_label",
        "expected_persist_pct",
        "expected_flip_pct",
        "expected_persistence_edge",
        "expected_next_abs_return",
        "expected_behaviour_label",
        "forecast_quality",
    ]

    available_cols = [col for col in display_cols if col in forecasts.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - STATE FORECAST ENGINE REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Transition matrix:  {TRANSITION_MATRIX_PATH}")
    lines.append(f"Transition summary: {TRANSITION_SUMMARY_PATH}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MOMENTUM-BIASED STATE FORECASTS")
    lines.append("-" * 90)

    if momentum.empty:
        lines.append("No usable momentum-biased forecasts found.")
    else:
        lines.append(momentum[available_cols].to_string(index=False))

    lines.append("")
    lines.append("MEAN-REVERSION-BIASED STATE FORECASTS")
    lines.append("-" * 90)

    if mean_reversion.empty:
        lines.append("No usable mean-reversion-biased forecasts found.")
    else:
        lines.append(mean_reversion[available_cols].to_string(index=False))

    lines.append("")
    lines.append("MOST SELF-PERSISTENT STATES")
    lines.append("-" * 90)

    if persistent.empty:
        lines.append("No usable persistent states found.")
    else:
        lines.append(persistent[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is a transparent transition-based forecast engine, not a trading model.")
    lines.append("Forecasts are conditional on historical observed state transitions.")
    lines.append("forecast_quality depends on sample size and transition concentration.")
    lines.append("expected_persistence_edge = expected_persist_pct - expected_flip_pct.")
    lines.append("Small samples remain hypotheses only.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 30 BUILD STATE FORECAST ENGINE")
    print("=" * 90)
    print(f"Transition matrix:  {TRANSITION_MATRIX_PATH}")
    print(f"Transition summary: {TRANSITION_SUMMARY_PATH}")
    print("-" * 90)

    if not TRANSITION_MATRIX_PATH.exists():
        raise FileNotFoundError(f"Transition matrix not found: {TRANSITION_MATRIX_PATH}")

    if not TRANSITION_SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Transition summary not found: {TRANSITION_SUMMARY_PATH}")

    matrix = pd.read_csv(TRANSITION_MATRIX_PATH, low_memory=False)
    summary = pd.read_csv(TRANSITION_SUMMARY_PATH, low_memory=False)

    print(f"Matrix rows:  {len(matrix):,}")
    print(f"Summary rows: {len(summary):,}")

    forecasts = build_forecasts(matrix, summary)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    forecast_csv = OUTPUT_ANALYSIS_DIR / "state_forecast_engine_latest.csv"
    forecast_parquet = OUTPUT_ANALYSIS_DIR / "state_forecast_engine_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "state_forecast_engine_report_latest.txt"

    forecasts.to_csv(forecast_csv, index=False)
    forecasts.to_parquet(forecast_parquet, index=False)

    report = build_report(forecasts)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] State forecast engine created.")
    print(f"Forecast CSV:     {forecast_csv}")
    print(f"Forecast Parquet: {forecast_parquet}")
    print(f"Report:           {report_path}")
    print("-" * 90)

    display_cols = [
        "state_column",
        "bar_type",
        "from_state",
        "total_transitions",
        "most_likely_next_state",
        "most_likely_transition_probability",
        "self_transition_probability",
        "expected_persistence_edge",
        "expected_behaviour_label",
        "forecast_quality",
    ]

    print(
        forecasts[
            forecasts["forecast_quality"].isin(["usable", "stronger"])
        ][display_cols]
        .head(50)
        .to_string(index=False)
    )

    print("=" * 90)


if __name__ == "__main__":
    main()