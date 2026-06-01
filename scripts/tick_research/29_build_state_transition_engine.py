"""
BACQE TICK RESEARCH - 29 Build State Transition Engine - Multi Symbol
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOLS = ["GBPUSD", "EURUSD", "USDJPY", "EURGBP", "GBPJPY", "XAUUSD"]

INPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "multi_layer_states"
OUTPUT_ANALYSIS_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "state_transitions"
OUTPUT_REPORT_ROOT = DATA_LAKE_ROOT / "reports" / "tick_research" / "state_transitions"

STATE_COLUMNS = [
    "compact_multi_layer_state",
    "multi_layer_state",
    "trend_micro_state",
    "vol_micro_state",
    "momentum_micro_state",
    "trend_strength_micro_state",
    "microstructure_regime",
]

MIN_TRANSITIONS = 10
STRONGER_TRANSITIONS = 100
HIGH_CONFIDENCE_TRANSITIONS = 500


def assign_sample_quality(total_transitions: int) -> str:
    if total_transitions >= HIGH_CONFIDENCE_TRANSITIONS:
        return "high_confidence"
    if total_transitions >= STRONGER_TRANSITIONS:
        return "stronger"
    if total_transitions >= MIN_TRANSITIONS:
        return "usable"
    return "low_sample"


def get_input_path(symbol: str) -> Path:
    return (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_multi_layer_state_model_latest.parquet"
    )


def prepare_data(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    data = df.copy()
    data["symbol"] = symbol

    data["bar_start_time"] = pd.to_datetime(
        data["bar_start_time"],
        errors="coerce",
        utc=True,
    )

    data = data.dropna(subset=["bar_start_time"]).copy()

    sort_cols = ["symbol", "bar_type", "bar_start_time"]
    data = data.sort_values(sort_cols).reset_index(drop=True)

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
        "regime_alignment_gap_minutes",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "selected_timeframe" not in data.columns:
        data["selected_timeframe"] = "unknown"

    return data


def build_transition_matrix(data: pd.DataFrame, state_col: str) -> pd.DataFrame:
    records = []

    group_cols = ["symbol", "selected_timeframe", "bar_type"]

    for keys, group in data.groupby(group_cols, dropna=False):
        symbol, selected_timeframe, bar_type = keys

        group = group.sort_values("bar_start_time").copy()

        group["from_state"] = group[state_col].fillna("unknown").astype(str)
        group["to_state"] = group[state_col].fillna("unknown").astype(str).shift(-1)

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
                avg_abs_return=("abs_return", "mean"),
                avg_duration_seconds=("duration_seconds", "mean"),
                avg_tick_count=("tick_count", "mean"),
                avg_ticks_per_second=("ticks_per_second", "mean"),
                avg_alignment_gap_minutes=("regime_alignment_gap_minutes", "mean"),
            )
            .reset_index()
        )

        total_from = counts.groupby("from_state")["transition_count"].transform("sum")

        counts["transition_probability"] = counts["transition_count"] / total_from
        counts["symbol"] = symbol
        counts["selected_timeframe"] = selected_timeframe
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
        ["symbol", "state_column", "bar_type", "from_state", "transition_probability"],
        ascending=[True, True, True, True, False],
    ).reset_index(drop=True)


def build_state_summary(matrix: pd.DataFrame) -> pd.DataFrame:
    if matrix.empty:
        return pd.DataFrame()

    records = []

    grouped = matrix.groupby(
        ["symbol", "selected_timeframe", "state_column", "bar_type", "from_state"],
        dropna=False,
    )

    for keys, group in grouped:
        symbol, selected_timeframe, state_column, bar_type, from_state = keys

        total_transitions = int(group["transition_count"].sum())

        self_transition = group[group["from_state"] == group["to_state"]]
        self_transition_count = (
            int(self_transition["transition_count"].sum())
            if not self_transition.empty
            else 0
        )

        self_transition_probability = (
            self_transition_count / total_transitions
            if total_transitions
            else np.nan
        )

        top_row = group.sort_values(
            ["transition_probability", "transition_count"],
            ascending=[False, False],
        ).iloc[0]

        records.append(
            {
                "symbol": symbol,
                "selected_timeframe": selected_timeframe,
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
                "avg_ticks_per_second": group["avg_ticks_per_second"].mean(),
                "sample_quality": assign_sample_quality(total_transitions),
                "is_unknown_state": ("unknown" in str(from_state).lower()),
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


def build_report(symbol: str, matrix: pd.DataFrame, summary: pd.DataFrame, input_path: Path) -> str:
    usable_summary = summary[summary["sample_quality"].isin(["usable", "stronger", "high_confidence"])].copy()

    # Remove unknown states from ranking tables
    usable_summary = usable_summary[~usable_summary["is_unknown_state"]].copy()

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
        "symbol",
        "selected_timeframe",
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
        "symbol",
        "selected_timeframe",
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
    lines.append(f"BACQE TICK RESEARCH - STATE TRANSITION ENGINE REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Input:           {input_path}")
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


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = get_input_path(symbol)

    if not input_path.exists():
        print(f"[WARN] {symbol}: multi-layer state model not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    raw = pd.read_parquet(input_path)

    print(f"[INFO] Rows loaded:    {len(raw):,}")
    print(f"[INFO] Columns loaded: {len(raw.columns):,}")

    data = prepare_data(raw, symbol)

    matrices = []

    for state_col in STATE_COLUMNS:
        if state_col not in data.columns:
            print(f"[WARN] {symbol}: missing state column, skipping: {state_col}")
            continue

        print(f"[RUN] {symbol}: transitions for {state_col}")
        matrix = build_transition_matrix(data, state_col)

        if not matrix.empty:
            matrices.append(matrix)

    transition_matrix = (
        pd.concat(matrices, ignore_index=True)
        if matrices
        else pd.DataFrame()
    )

    state_summary = build_state_summary(transition_matrix)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    matrix_csv = analysis_dir / f"{symbol}_state_transition_matrix_latest.csv"
    matrix_parquet = analysis_dir / f"{symbol}_state_transition_matrix_latest.parquet"

    summary_csv = analysis_dir / f"{symbol}_state_transition_summary_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_state_transition_summary_latest.parquet"

    report_path = report_dir / f"{symbol}_state_transition_report_latest.txt"

    transition_matrix.to_csv(matrix_csv, index=False)
    transition_matrix.to_parquet(matrix_parquet, index=False)

    state_summary.to_csv(summary_csv, index=False)
    state_summary.to_parquet(summary_parquet, index=False)

    report = build_report(symbol, transition_matrix, state_summary, input_path)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: Matrix CSV:  {matrix_csv}")
    print(f"[DONE] {symbol}: Summary CSV: {summary_csv}")
    print(f"[DONE] {symbol}: Report:      {report_path}")

    return transition_matrix, state_summary


def save_master_outputs(
    matrix_frames: list[pd.DataFrame],
    summary_frames: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_matrix = pd.concat(matrix_frames, ignore_index=True)
    master_summary = pd.concat(summary_frames, ignore_index=True)

    matrix_csv = master_analysis_dir / "master_state_transition_matrix_latest.csv"
    matrix_parquet = master_analysis_dir / "master_state_transition_matrix_latest.parquet"

    summary_csv = master_analysis_dir / "master_state_transition_summary_latest.csv"
    summary_parquet = master_analysis_dir / "master_state_transition_summary_latest.parquet"

    report_path = master_report_dir / "master_state_transition_report_latest.txt"

    master_matrix.to_csv(matrix_csv, index=False)
    master_matrix.to_parquet(matrix_parquet, index=False)

    master_summary.to_csv(summary_csv, index=False)
    master_summary.to_parquet(summary_parquet, index=False)

    usable_summary = master_summary[
        master_summary["sample_quality"].isin(["usable", "stronger", "high_confidence"])].copy()

    # Remove unknown states from ranking tables
    usable_summary = usable_summary[~usable_summary["is_unknown_state"]].copy()

    strongest_self = usable_summary.sort_values(
        "self_transition_probability",
        ascending=False,
        na_position="last",
    ).head(80)

    strongest_transition = master_matrix[
        master_matrix["transition_count"] >= MIN_TRANSITIONS
    ].sort_values(
        "transition_probability",
        ascending=False,
        na_position="last",
    ).head(80)

    summary_cols = [
        "symbol",
        "selected_timeframe",
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
        "symbol",
        "selected_timeframe",
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

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER STATE TRANSITION ENGINE REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "STRONGEST SELF-PERSISTING STATES",
                "-" * 90,
                strongest_self[summary_cols].to_string(index=False),
                "",
                "STRONGEST TRANSITIONS",
                "-" * 90,
                strongest_transition[matrix_cols].to_string(index=False),
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "Self-transition probability measures state persistence.",
                "Transition probability is conditional on the from_state.",
                "This is diagnostic research, not a trading signal.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master state transition engine created.")
    print(f"Master Matrix CSV:  {matrix_csv}")
    print(f"Master Summary CSV: {summary_csv}")
    print(f"Master Report:      {report_path}")
    print("-" * 90)

    print("MASTER STATE SUMMARY PREVIEW")
    print(strongest_self[summary_cols].head(50).to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 29 BUILD STATE TRANSITION ENGINE - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:      {INPUT_ROOT}")
    print(f"Output analysis: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:  {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:         {SYMBOLS}")
    print("-" * 90)

    matrix_frames = []
    summary_frames = []

    for symbol in SYMBOLS:
        matrix, summary = process_symbol(symbol)

        if not matrix.empty:
            matrix_frames.append(matrix)

        if not summary.empty:
            summary_frames.append(summary)

    if not matrix_frames or not summary_frames:
        print("[WARN] No transition outputs created.")
        return

    save_master_outputs(matrix_frames, summary_frames)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol state transition engine complete.")
    print(f"Symbols analysed: {len(summary_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()