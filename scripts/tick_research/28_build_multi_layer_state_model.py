"""
BACQE TICK RESEARCH - 28 Build Multi-Layer State Model

Builds combined market-state labels from:

    M15 regime state
    + microstructure regime
    + activity regime
    + persistence regime

Input:
    E:/Quant_Lab/data/processed/tick_research/regime_fusion/GBPUSD_microstructure_m15_regime_fusion_latest.parquet
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
    / "regime_fusion"
    / f"{SYMBOL}_microstructure_m15_regime_fusion_latest.parquet"
)

OUTPUT_PROCESSED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "multi_layer_states"
OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "multi_layer_states"

MIN_ROWS = 20


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    numeric_cols = [
        "return",
        "abs_return",
        "future_return_h1",
        "future_abs_return_h1",
        "target_up_h1",
        "target_direction_persist_h1",
        "target_direction_flip_h1",
        "duration_seconds",
        "tick_count",
        "ticks_per_second",
        "range",
        "range_per_tick",
        "volatility_per_tick",
        "imbalance_ratio",
        "abs_imbalance_ratio",
        "m15_regime_confidence",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "abs_imbalance_ratio" not in data.columns and "imbalance_ratio" in data.columns:
        data["abs_imbalance_ratio"] = data["imbalance_ratio"].abs()

    data["ticks_per_second"] = data["ticks_per_second"].replace([np.inf, -np.inf], np.nan)

    return data


def add_relative_regimes(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    df["activity_regime"] = "normal_activity_speed"

    for bar_type, group_index in df.groupby("bar_type").groups.items():
        idx = list(group_index)

        q_low = df.loc[idx, "ticks_per_second"].quantile(0.33)
        q_high = df.loc[idx, "ticks_per_second"].quantile(0.67)

        df.loc[idx, "activity_regime"] = np.where(
            df.loc[idx, "ticks_per_second"] <= q_low,
            "slow_activity_speed",
            np.where(
                df.loc[idx, "ticks_per_second"] >= q_high,
                "fast_activity_speed",
                "normal_activity_speed",
            ),
        )

    df["volatility_micro_regime"] = "normal_micro_volatility"

    for bar_type, group_index in df.groupby("bar_type").groups.items():
        idx = list(group_index)

        q_low = df.loc[idx, "abs_return"].quantile(0.33)
        q_high = df.loc[idx, "abs_return"].quantile(0.67)

        df.loc[idx, "volatility_micro_regime"] = np.where(
            df.loc[idx, "abs_return"] <= q_low,
            "low_micro_volatility",
            np.where(
                df.loc[idx, "abs_return"] >= q_high,
                "high_micro_volatility",
                "normal_micro_volatility",
            ),
        )

    df["persistence_regime"] = "unknown_persistence"

    df.loc[
        (df["target_direction_persist_h1"] == 1),
        "persistence_regime",
    ] = "persisted_next_bar"

    df.loc[
        (df["target_direction_flip_h1"] == 1),
        "persistence_regime",
    ] = "flipped_next_bar"

    df.loc[
        (df["target_direction_persist_h1"] == 0)
        & (df["target_direction_flip_h1"] == 0),
        "persistence_regime",
    ] = "neutral_next_bar"

    return df


def add_multi_layer_state_labels(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    for col in [
        "m15_composite_regime",
        "m15_trend_state",
        "m15_volatility_state",
        "microstructure_regime",
        "activity_regime",
        "volatility_micro_regime",
    ]:
        df[col] = df[col].fillna("unknown").astype(str)

    df["multi_layer_state"] = (
        df["m15_composite_regime"]
        + "__"
        + df["microstructure_regime"]
        + "__"
        + df["activity_regime"]
        + "__"
        + df["volatility_micro_regime"]
    )

    df["compact_multi_layer_state"] = (
        df["m15_composite_regime"]
        + "__"
        + df["microstructure_regime"]
    )

    df["trend_micro_state"] = (
        df["m15_trend_state"]
        + "__"
        + df["microstructure_regime"]
    )

    df["vol_micro_state"] = (
        df["m15_volatility_state"]
        + "__"
        + df["microstructure_regime"]
    )

    df["multi_layer_state_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return df


def summarise_states(df: pd.DataFrame, state_col: str, summary_level: str) -> pd.DataFrame:
    summary = (
        df.groupby(["bar_type", "bar_family", state_col], dropna=False)
        .agg(
            rows=(state_col, "count"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_future_return_h1=("future_return_h1", "mean"),
            avg_future_abs_return_h1=("future_abs_return_h1", "mean"),
            target_up_h1_pct=("target_up_h1", "mean"),
            target_direction_persist_h1_pct=("target_direction_persist_h1", "mean"),
            target_direction_flip_h1_pct=("target_direction_flip_h1", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
            avg_ticks_per_second=("ticks_per_second", "mean"),
            avg_abs_imbalance_ratio=("abs_imbalance_ratio", "mean"),
            avg_m15_regime_confidence=("m15_regime_confidence", "mean"),
        )
        .reset_index()
    )

    for col in [
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]:
        summary[col] = summary[col] * 100

    summary["persistence_edge_h1"] = (
        summary["target_direction_persist_h1_pct"]
        - summary["target_direction_flip_h1_pct"]
    )

    summary["sample_quality"] = "low_sample"
    summary.loc[summary["rows"] >= MIN_ROWS, "sample_quality"] = "usable"
    summary.loc[summary["rows"] >= 100, "sample_quality"] = "stronger"

    summary["summary_level"] = summary_level
    summary["state_column"] = state_col
    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary.sort_values(
        ["sample_quality", "persistence_edge_h1", "rows"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def build_report(summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = summary[summary["sample_quality"].isin(["usable", "stronger"])].copy()

    strongest_persistence = usable.sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(40)

    strongest_activity = usable.sort_values(
        "avg_ticks_per_second",
        ascending=False,
        na_position="last",
    ).head(40)

    strongest_volatility = usable.sort_values(
        "avg_abs_return",
        ascending=False,
        na_position="last",
    ).head(40)

    display_cols = [
        "summary_level",
        "bar_type",
        "state_column",
        "compact_multi_layer_state",
        "multi_layer_state",
        "trend_micro_state",
        "vol_micro_state",
        "rows",
        "sample_quality",
        "avg_abs_return",
        "avg_ticks_per_second",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in summary.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - MULTI-LAYER STATE MODEL REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("STRONGEST MULTI-LAYER PERSISTENCE STATES")
    lines.append("-" * 90)
    lines.append(strongest_persistence[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST ACTIVITY MULTI-LAYER STATES")
    lines.append("-" * 90)
    lines.append(strongest_activity[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST VOLATILITY MULTI-LAYER STATES")
    lines.append("-" * 90)
    lines.append(strongest_volatility[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is a research state model, not a trading system.")
    lines.append("Multi-layer states combine M15 regime context with event-time microstructure behaviour.")
    lines.append("Persistence edge = target_direction_persist_h1_pct - target_direction_flip_h1_pct.")
    lines.append("Small-sample states should be treated only as hypotheses.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 28 BUILD MULTI-LAYER STATE MODEL")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Regime fusion file not found: {INPUT_PATH}")

    fused = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(fused):,}")
    print(f"Columns loaded: {len(fused.columns):,}")

    data = prepare_data(fused)
    data = add_relative_regimes(data)
    state_model = add_multi_layer_state_labels(data)

    summaries = []

    summaries.append(
        summarise_states(
            state_model,
            "compact_multi_layer_state",
            "compact_multi_layer_state",
        )
    )

    summaries.append(
        summarise_states(
            state_model,
            "multi_layer_state",
            "full_multi_layer_state",
        )
    )

    summaries.append(
        summarise_states(
            state_model,
            "trend_micro_state",
            "trend_micro_state",
        )
    )

    summaries.append(
        summarise_states(
            state_model,
            "vol_micro_state",
            "vol_micro_state",
        )
    )

    summary = pd.concat(summaries, ignore_index=True)

    OUTPUT_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    state_parquet = OUTPUT_PROCESSED_DIR / f"{SYMBOL}_multi_layer_state_model_latest.parquet"
    state_csv = OUTPUT_PROCESSED_DIR / f"{SYMBOL}_multi_layer_state_model_latest.csv"

    summary_csv = OUTPUT_ANALYSIS_DIR / "multi_layer_state_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "multi_layer_state_summary_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "multi_layer_state_model_report_latest.txt"

    state_model.to_parquet(state_parquet, index=False)
    state_model.to_csv(state_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Multi-layer state model created.")
    print(f"State Parquet:   {state_parquet}")
    print(f"State CSV:       {state_csv}")
    print(f"Summary CSV:     {summary_csv}")
    print(f"Summary Parquet: {summary_parquet}")
    print(f"Report:          {report_path}")
    print("-" * 90)

    display_cols = [
        "summary_level",
        "bar_type",
        "state_column",
        "rows",
        "sample_quality",
        "avg_abs_return",
        "avg_ticks_per_second",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    preview = summary[
        summary["sample_quality"].isin(["usable", "stronger"])
    ].sort_values("persistence_edge_h1", ascending=False)

    print(preview[display_cols].head(50).to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()