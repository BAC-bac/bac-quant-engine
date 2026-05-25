"""
BACQE TICK RESEARCH - 29 Build State Transition Engine

Builds transition matrices for BACQE multi-layer states.

Input:
    E:/Quant_Lab/data/processed/tick_research/multi_layer_states/GBPUSD_multi_layer_state_model_latest.parquet

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/state_transition_matrix_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/state_transition_summary_latest.csv
    E:/Quant_Lab/reports/tick_research/state_transitions/state_transition_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
SYMBOL = "GBPUSD"

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "multi_layer_states"
    / f"{SYMBOL}_multi_layer_state_model_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "state_transitions"

STATE_COLUMNS = [
    "compact_multi_layer_state",
    "trend_micro_state",
    "vol_micro_state",
    "microstructure_regime",
    "m15_composite_regime",
]

MIN_TRANSITIONS = 10


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(data["bar_start_time"], errors="coerce", utc=True)
    data = data.dropna(subset=["bar_start_time"]).copy()

    data = data.sort_values(["bar_type", "bar_start_time"]).reset_index(drop=True)

    numeric_cols = [
        "return",
        "future_return_h1",
        "future_abs_return_h1",
        "target_direction_persist_h1",
        "target_direction_flip_h1",
        "target_up_h1",
        "abs_return",
        "duration_seconds",
        "tick_count",
        "ticks_per_second",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    return data


def build_transition_matrix(data: pd.DataFrame, state_col: str) -> pd.DataFrame:
    records = []

    for bar_type, group in data.groupby("bar_type", dropna=False):
        group = group.sort_values("bar_start_time").copy()

        group["from_state"] = group[state_col].astype(str)
        group["to_state"] = group[state_col].astype(str).shift(-1)

        transitions = group.dropna(subset=["to_state"]).copy()

        if transitions.empty:
            continue

        counts = (
            transitions.groupby(["from_state", "to_state"], dropna=False)
            .agg(
                transition_count=("to_state", "count"),
                avg_current_return=("return", "mean"),
                avg_next_return=("future_return_h1", "mean"),
                avg_next_abs_return=("future_abs_return_h1", "mean"),
                persist_target_pct=("target_direction_persist_h1", "mean"),
                flip_target_pct=("target_direction_flip_h1", "mean"),
                up_target_pct=("target_up_h1", "mean"),
                avg_duration_seconds=("duration_seconds", "mean"),
                avg_tick_count=("tick_count", "mean"),
                avg_ticks_per_second=("ticks_per_second", "mean"),
            )
            .reset_index()
        )

        total_from = counts.groupby("from_state")["transition_count"].transform("sum")

        counts["transition_probability"] = counts["transition_count"] / total_from
        counts["bar_type"] = bar_type
        counts["state_column"] = state_col
        counts["transition_time_utc"] = datetime.now(timezone.utc).isoformat()

        for col in ["persist_target_pct", "flip_target_pct", "up_target_pct"]:
            counts[col] = counts[col] * 100

        records.append(counts)

    if not records:
        return pd.DataFrame()

    matrix = pd.concat(records, ignore_index=True)

    numeric_cols = matrix.select_dtypes(include=["float", "int"]).columns
    matrix[numeric_cols] = matrix[numeric_cols].round(8)

    return matrix.sort_values(
        ["state_column", "bar_type", "from_state", "transition_probability"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)


def build_state_summary(matrix: pd.DataFrame) -> pd.DataFrame:
    if matrix.empty:
        return pd.DataFrame()

    records = []

    grouped = matrix.groupby(["state_column", "bar_type", "from_state"], dropna=False)

    for keys, group in grouped:
        state_column, bar_type, from_state = keys

        total_transitions = group["transition_count"].sum()

        self_transition = group[group["from_state"] == group["to_state"]]
        self_transition_count = int(self_transition["transition_count"].sum()) if not self_transition.empty else 0
        self_transition_probability = (
            self_transition_count / total_transitions if total_transitions else np.nan
        )

        top_row = group.sort_values("transition_probability", ascending=False).iloc[0]

        records.append(
            {
                "state_column": state_column,
                "bar_type": bar_type,
                "from_state": from_state,
                "total_transitions": total_transitions,
                "unique_next_states": group["to_state"].nunique(),
                "top_next_state": top_row["to_state"],
                "top_transition_probability": top_row["transition_probability"],
                "self_transition_count": self_transition_count,
                "self_transition_probability": self_transition_probability,
                "avg_persist_target_pct": group["persist_target_pct"].mean(),
                "avg_flip_target_pct": group["flip_target_pct"].mean(),
                "avg_up_target_pct": group["up_target_pct"].mean(),
                "avg_next_abs_return": group["avg_next_abs_return"].mean(),
                "sample_quality": (
                    "stronger"
                    if total_transitions >= 100
                    else "usable"
                    if total_transitions >= MIN_TRANSITIONS
                    else "low_sample"
                ),
                "summary_time_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    summary = pd.DataFrame(records)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary.sort_values(
        ["sample_quality", "self_transition_probability", "total_transitions"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def build_report(matrix: pd.DataFrame, summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable_summary = summary[summary["sample_quality"].isin(["usable", "stronger"])].copy()

    strongest_self = usable_summary.sort_values(
        "self_transition_probability",
        ascending=False,
        na_position="last",
    ).head(40)

    most_unstable = usable_summary.sort_values(
        "self_transition_probability",
        ascending=True,
        na_position="last",
    ).head(40)

    strongest_transition = matrix[
        matrix["transition_count"] >= MIN_TRANSITIONS
    ].sort_values(
        "transition_probability",
        ascending=False,
        na_position="last",
    ).head(40)

    summary_cols = [
        "state_column",
        "bar_type",
        "from_state",
        "total_transitions",
        "unique_next_states",
        "top_next_state",
        "top_transition_probability",
        "self_transition_probability",
        "sample_quality",
    ]

    matrix_cols = [
        "state_column",
        "bar_type",
        "from_state",
        "to_state",
        "transition_count",
        "transition_probability",
        "persist_target_pct",
        "flip_target_pct",
        "avg_next_abs_return",
    ]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - STATE TRANSITION ENGINE REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Min transitions: {MIN_TRANSITIONS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("STRONGEST SELF-PERSISTING STATES")
    lines.append("-" * 90)
    lines.append(strongest_self[summary_cols].to_string(index=False))

    lines.append("")
    lines.append("MOST UNSTABLE STATES")
    lines.append("-" * 90)
    lines.append(most_unstable[summary_cols].to_string(index=False))

    lines.append("")
    lines.append("STRONGEST TRANSITIONS")
    lines.append("-" * 90)
    lines.append(strongest_transition[matrix_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("Self-transition probability measures state persistence.")
    lines.append("Low self-transition probability suggests unstable or transient states.")
    lines.append("Transition probability is conditional on the from_state.")
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 29 BUILD STATE TRANSITION ENGINE")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Multi-layer state model not found: {INPUT_PATH}")

    raw = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(raw):,}")
    print(f"Columns loaded: {len(raw.columns):,}")

    data = prepare_data(raw)

    matrices = []

    for state_col in STATE_COLUMNS:
        if state_col not in data.columns:
            print(f"[WARN] Missing state column, skipping: {state_col}")
            continue

        print(f"[RUN] Building transitions for: {state_col}")
        matrix = build_transition_matrix(data, state_col)

        if not matrix.empty:
            matrices.append(matrix)

    if matrices:
        transition_matrix = pd.concat(matrices, ignore_index=True)
    else:
        transition_matrix = pd.DataFrame()

    state_summary = build_state_summary(transition_matrix)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    matrix_csv = OUTPUT_ANALYSIS_DIR / "state_transition_matrix_latest.csv"
    matrix_parquet = OUTPUT_ANALYSIS_DIR / "state_transition_matrix_latest.parquet"

    summary_csv = OUTPUT_ANALYSIS_DIR / "state_transition_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "state_transition_summary_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "state_transition_report_latest.txt"

    transition_matrix.to_csv(matrix_csv, index=False)
    transition_matrix.to_parquet(matrix_parquet, index=False)

    state_summary.to_csv(summary_csv, index=False)
    state_summary.to_parquet(summary_parquet, index=False)

    report = build_report(transition_matrix, state_summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] State transition engine created.")
    print(f"Matrix CSV:      {matrix_csv}")
    print(f"Matrix Parquet:  {matrix_parquet}")
    print(f"Summary CSV:     {summary_csv}")
    print(f"Summary Parquet: {summary_parquet}")
    print(f"Report:          {report_path}")
    print("-" * 90)

    display_cols = [
        "state_column",
        "bar_type",
        "from_state",
        "total_transitions",
        "top_next_state",
        "top_transition_probability",
        "self_transition_probability",
        "sample_quality",
    ]

    print(state_summary[display_cols].head(50).to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()