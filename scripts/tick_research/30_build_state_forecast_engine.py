"""
BACQE TICK RESEARCH - 30 Build State Forecast Engine - Multi Symbol
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TRANSITION_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "state_transitions"
OUTPUT_ANALYSIS_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "state_forecasts"
OUTPUT_REPORT_ROOT = DATA_LAKE_ROOT / "reports" / "tick_research" / "state_forecasts"

MASTER_MATRIX_PATH = TRANSITION_ROOT / "_master" / "master_state_transition_matrix_latest.csv"
MASTER_SUMMARY_PATH = TRANSITION_ROOT / "_master" / "master_state_transition_summary_latest.csv"

MIN_TRANSITIONS_FOR_USABLE = 10
MIN_TRANSITIONS_FOR_STRONGER = 100
MIN_TRANSITIONS_FOR_HIGH_CONFIDENCE = 500


def classify_stability(self_prob: float) -> str:
    if pd.isna(self_prob):
        return "unknown"
    if self_prob >= 0.80:
        return "very_highly_persistent"
    if self_prob >= 0.60:
        return "highly_persistent"
    if self_prob >= 0.40:
        return "moderately_persistent"
    if self_prob >= 0.20:
        return "unstable"
    return "highly_unstable"


def classify_forecast_quality(total_transitions: int, top_prob: float) -> str:
    if total_transitions >= MIN_TRANSITIONS_FOR_HIGH_CONFIDENCE and top_prob >= 0.40:
        return "high_confidence"
    if total_transitions >= MIN_TRANSITIONS_FOR_STRONGER and top_prob >= 0.35:
        return "stronger"
    if total_transitions >= MIN_TRANSITIONS_FOR_USABLE and top_prob >= 0.25:
        return "usable"
    if total_transitions >= MIN_TRANSITIONS_FOR_USABLE:
        return "weak"
    return "low_sample"


def classify_expected_behaviour(row: pd.Series) -> str:
    edge = row.get("expected_persistence_edge", np.nan)
    abs_ret = row.get("expected_next_abs_return", np.nan)
    global_abs_75 = row.get("global_abs_return_75pct", np.nan)

    if pd.notna(edge):
        if edge >= 10:
            return "momentum_bias"
        if edge <= -10:
            return "mean_reversion_bias"

    if pd.notna(abs_ret) and pd.notna(global_abs_75):
        if abs_ret >= global_abs_75:
            return "high_activity_expected"

    return "neutral_or_unclear"


def build_forecasts(matrix: pd.DataFrame) -> pd.DataFrame:
    records = []

    matrix = matrix.copy()

    numeric_cols = [
        "transition_count",
        "transition_probability",
        "persist_target_pct",
        "flip_target_pct",
        "up_target_pct",
        "avg_next_abs_return",
        "avg_ticks_per_second",
    ]

    for col in numeric_cols:
        if col in matrix.columns:
            matrix[col] = pd.to_numeric(matrix[col], errors="coerce")

    group_cols = [
        "symbol",
        "selected_timeframe",
        "state_column",
        "bar_type",
        "from_state",
    ]

    global_abs_75 = matrix["avg_next_abs_return"].quantile(0.75)

    for keys, group in matrix.groupby(group_cols, dropna=False):
        symbol, selected_timeframe, state_column, bar_type, from_state = keys

        group = group.sort_values(
            ["transition_probability", "transition_count"],
            ascending=[False, False],
        )

        top = group.iloc[0]
        total_transitions = int(group["transition_count"].sum())

        self_row = group[group["to_state"].astype(str) == str(from_state)]
        self_prob = (
            float(self_row["transition_probability"].iloc[0])
            if not self_row.empty
            else 0.0
        )

        weights = group["transition_count"].fillna(0)

        expected_next_abs_return = np.average(
            group["avg_next_abs_return"].fillna(0),
            weights=weights,
        )

        expected_persist_pct = np.average(
            group["persist_target_pct"].fillna(0),
            weights=weights,
        )

        expected_flip_pct = np.average(
            group["flip_target_pct"].fillna(0),
            weights=weights,
        )

        expected_up_pct = np.average(
            group["up_target_pct"].fillna(0),
            weights=weights,
        )

        expected_ticks_per_second = (
            np.average(group["avg_ticks_per_second"].fillna(0), weights=weights)
            if "avg_ticks_per_second" in group.columns
            else np.nan
        )

        forecast = {
            "symbol": symbol,
            "selected_timeframe": selected_timeframe,
            "state_column": state_column,
            "bar_type": bar_type,
            "from_state": from_state,
            "is_unknown_state": "unknown" in str(from_state).lower(),
            "total_transitions": total_transitions,
            "unique_next_states": group["to_state"].nunique(),
            "most_likely_next_state": top["to_state"],
            "most_likely_transition_probability": top["transition_probability"],
            "self_transition_probability": self_prob,
            "state_stability_label": classify_stability(self_prob),
            "expected_next_abs_return": expected_next_abs_return,
            "expected_persist_pct": expected_persist_pct,
            "expected_flip_pct": expected_flip_pct,
            "expected_up_pct": expected_up_pct,
            "expected_ticks_per_second": expected_ticks_per_second,
            "expected_persistence_edge": expected_persist_pct - expected_flip_pct,
            "global_abs_return_75pct": global_abs_75,
            "forecast_quality": classify_forecast_quality(
                total_transitions,
                top["transition_probability"],
            ),
            "forecast_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        forecast["expected_behaviour_label"] = classify_expected_behaviour(
            pd.Series(forecast)
        )

        records.append(forecast)

    forecasts = pd.DataFrame(records)

    numeric_cols = forecasts.select_dtypes(include=["float", "int"]).columns
    forecasts[numeric_cols] = forecasts[numeric_cols].round(8)

    return forecasts.sort_values(
        [
            "forecast_quality",
            "expected_persistence_edge",
            "total_transitions",
        ],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def build_report(title: str, forecasts: pd.DataFrame) -> str:
    usable = forecasts[
        forecasts["forecast_quality"].isin(["usable", "stronger", "high_confidence"])
        & (~forecasts["is_unknown_state"])
    ].copy()

    momentum = usable.sort_values(
        "expected_persistence_edge",
        ascending=False,
        na_position="last",
    ).head(50)

    mean_reversion = usable.sort_values(
        "expected_persistence_edge",
        ascending=True,
        na_position="last",
    ).head(50)

    persistent = usable.sort_values(
        "self_transition_probability",
        ascending=False,
        na_position="last",
    ).head(50)

    display_cols = [
        "symbol",
        "selected_timeframe",
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
    lines.append(title)
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Master transition matrix:  {MASTER_MATRIX_PATH}")
    lines.append(f"Master transition summary: {MASTER_SUMMARY_PATH}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MOMENTUM-BIASED STATE FORECASTS")
    lines.append("-" * 90)
    lines.append(momentum[available_cols].to_string(index=False) if not momentum.empty else "No usable momentum-biased forecasts found.")

    lines.append("")
    lines.append("MEAN-REVERSION-BIASED STATE FORECASTS")
    lines.append("-" * 90)
    lines.append(mean_reversion[available_cols].to_string(index=False) if not mean_reversion.empty else "No usable mean-reversion-biased forecasts found.")

    lines.append("")
    lines.append("MOST SELF-PERSISTENT STATES")
    lines.append("-" * 90)
    lines.append(persistent[available_cols].to_string(index=False) if not persistent.empty else "No usable persistent states found.")

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is a transparent transition-based forecast engine, not a trading model.")
    lines.append("Forecasts are conditional on historically observed state transitions.")
    lines.append("Unknown states are excluded from report ranking tables but retained in output files.")
    lines.append("expected_persistence_edge = expected_persist_pct - expected_flip_pct.")
    lines.append("Small samples remain hypotheses only.")
    lines.append("=" * 90)

    return "\n".join(lines)


def save_symbol_outputs(forecasts: pd.DataFrame) -> list[pd.DataFrame]:
    saved_frames = []

    for symbol, symbol_df in forecasts.groupby("symbol", dropna=False):
        symbol = str(symbol)

        analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
        report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

        analysis_dir.mkdir(parents=True, exist_ok=True)
        report_dir.mkdir(parents=True, exist_ok=True)

        csv_path = analysis_dir / f"{symbol}_state_forecast_engine_latest.csv"
        parquet_path = analysis_dir / f"{symbol}_state_forecast_engine_latest.parquet"
        report_path = report_dir / f"{symbol}_state_forecast_engine_report_latest.txt"

        symbol_df.to_csv(csv_path, index=False)
        symbol_df.to_parquet(parquet_path, index=False)

        report = build_report(
            f"BACQE TICK RESEARCH - STATE FORECAST ENGINE REPORT - {symbol}",
            symbol_df,
        )
        report_path.write_text(report, encoding="utf-8")

        print(f"[DONE] {symbol}: Forecast CSV: {csv_path}")
        print(f"[DONE] {symbol}: Report:       {report_path}")

        saved_frames.append(symbol_df)

    return saved_frames


def save_master_outputs(forecasts: pd.DataFrame) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    csv_path = master_analysis_dir / "master_state_forecast_engine_latest.csv"
    parquet_path = master_analysis_dir / "master_state_forecast_engine_latest.parquet"
    report_path = master_report_dir / "master_state_forecast_engine_report_latest.txt"

    forecasts.to_csv(csv_path, index=False)
    forecasts.to_parquet(parquet_path, index=False)

    report = build_report(
        "BACQE TICK RESEARCH - MASTER STATE FORECAST ENGINE REPORT",
        forecasts,
    )
    report_path.write_text(report, encoding="utf-8")

    print("-" * 90)
    print("[DONE] Master state forecast engine created.")
    print(f"Master Forecast CSV:     {csv_path}")
    print(f"Master Forecast Parquet: {parquet_path}")
    print(f"Master Report:           {report_path}")

    display_cols = [
        "symbol",
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

    preview = forecasts[
        forecasts["forecast_quality"].isin(["usable", "stronger", "high_confidence"])
        & (~forecasts["is_unknown_state"])
    ][display_cols].head(50)

    print("-" * 90)
    print("MASTER FORECAST PREVIEW")
    print(preview.to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 30 BUILD STATE FORECAST ENGINE - MULTI SYMBOL")
    print("=" * 90)
    print(f"Master transition matrix:  {MASTER_MATRIX_PATH}")
    print(f"Master transition summary: {MASTER_SUMMARY_PATH}")
    print(f"Output analysis root:      {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:        {OUTPUT_REPORT_ROOT}")
    print("-" * 90)

    if not MASTER_MATRIX_PATH.exists():
        raise FileNotFoundError(f"Master transition matrix not found: {MASTER_MATRIX_PATH}")

    if not MASTER_SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Master transition summary not found: {MASTER_SUMMARY_PATH}")

    matrix = pd.read_csv(MASTER_MATRIX_PATH, low_memory=False)
    summary = pd.read_csv(MASTER_SUMMARY_PATH, low_memory=False)

    print(f"Matrix rows:  {len(matrix):,}")
    print(f"Summary rows: {len(summary):,}")

    forecasts = build_forecasts(matrix)

    save_symbol_outputs(forecasts)
    save_master_outputs(forecasts)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol state forecast engine complete.")
    print(f"Forecast rows: {len(forecasts):,}")
    print("=" * 90)


if __name__ == "__main__":
    main()