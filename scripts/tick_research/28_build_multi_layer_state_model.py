"""
BACQE TICK RESEARCH - 28 Build Multi-Layer State Model - Multi Symbol

Builds combined market-state labels from:

    selected BACQE regime state
    + microstructure regime
    + activity regime
    + micro volatility regime
    + persistence regime

Inputs:
    E:/Quant_Lab/data/processed/tick_research/regime_fusion/symbol=<SYMBOL>/
        <SYMBOL>_microstructure_regime_fusion_latest.parquet
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

INPUT_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "regime_fusion"
)

OUTPUT_PROCESSED_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "multi_layer_states"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "multi_layer_states"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "multi_layer_states"
)

MIN_ROWS = 20
STRONGER_ROWS = 100
HIGH_CONFIDENCE_ROWS = 500


def detect_selected_timeframe(data: pd.DataFrame) -> str:
    if "selected_regime_timeframe" in data.columns:
        values = data["selected_regime_timeframe"].dropna().astype(str).unique()
        if len(values) > 0:
            return values[0]

    for col in data.columns:
        if col.startswith("regime_") and col.endswith("_time"):
            return col.replace("regime_", "").replace("_time", "").upper()

    return "UNKNOWN"


def get_regime_cols(timeframe: str) -> dict:
    prefix = timeframe.lower()

    return {
        "time": f"regime_{prefix}_time",
        "trend": f"{prefix}_trend_state",
        "volatility": f"{prefix}_volatility_state",
        "momentum": f"{prefix}_momentum_state",
        "trend_strength": f"{prefix}_trend_strength_state",
        "composite": f"{prefix}_composite_regime",
        "confidence": f"{prefix}_regime_confidence",
    }


def prepare_data(df: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame, str, dict]:
    data = df.copy()
    data["symbol"] = symbol

    timeframe = detect_selected_timeframe(data)
    regime_cols = get_regime_cols(timeframe)

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
        regime_cols["confidence"],
        "regime_alignment_gap_minutes",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "abs_imbalance_ratio" not in data.columns and "imbalance_ratio" in data.columns:
        data["abs_imbalance_ratio"] = data["imbalance_ratio"].abs()

    if "ticks_per_second" in data.columns:
        data["ticks_per_second"] = data["ticks_per_second"].replace([np.inf, -np.inf], np.nan)

    for key, col in regime_cols.items():
        if key == "time":
            continue
        if col not in data.columns:
            data[col] = "unknown"

    if "microstructure_regime" not in data.columns:
        data["microstructure_regime"] = "unknown_microstructure_regime"

    if "bar_type" not in data.columns:
        data["bar_type"] = "unknown_bar_type"

    if "bar_family" not in data.columns:
        data["bar_family"] = "unknown_bar_family"

    if "has_selected_regime" not in data.columns:
        data["has_selected_regime"] = False

    data["has_selected_regime"] = data["has_selected_regime"].fillna(False).astype(bool)

    return data, timeframe, regime_cols


def add_relative_regimes(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    df["activity_regime"] = "normal_activity_speed"

    if "ticks_per_second" in df.columns:
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

    if "abs_return" in df.columns:
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

    if "target_direction_persist_h1" in df.columns:
        df.loc[df["target_direction_persist_h1"] == 1, "persistence_regime"] = "persisted_next_bar"

    if "target_direction_flip_h1" in df.columns:
        df.loc[df["target_direction_flip_h1"] == 1, "persistence_regime"] = "flipped_next_bar"

    if {"target_direction_persist_h1", "target_direction_flip_h1"}.issubset(df.columns):
        df.loc[
            (df["target_direction_persist_h1"] == 0)
            & (df["target_direction_flip_h1"] == 0),
            "persistence_regime",
        ] = "neutral_next_bar"

    return df


def add_multi_layer_state_labels(
    data: pd.DataFrame,
    timeframe: str,
    regime_cols: dict,
) -> pd.DataFrame:
    df = data.copy()

    composite_col = regime_cols["composite"]
    trend_col = regime_cols["trend"]
    volatility_col = regime_cols["volatility"]
    momentum_col = regime_cols["momentum"]
    trend_strength_col = regime_cols["trend_strength"]

    required_state_cols = [
        composite_col,
        trend_col,
        volatility_col,
        momentum_col,
        trend_strength_col,
        "microstructure_regime",
        "activity_regime",
        "volatility_micro_regime",
    ]

    for col in required_state_cols:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].fillna("unknown").astype(str)

    prefix = timeframe.lower()

    df["multi_layer_state"] = (
        df[composite_col]
        + "__"
        + df["microstructure_regime"]
        + "__"
        + df["activity_regime"]
        + "__"
        + df["volatility_micro_regime"]
    )

    df["compact_multi_layer_state"] = (
        df[composite_col]
        + "__"
        + df["microstructure_regime"]
    )

    df["trend_micro_state"] = (
        df[trend_col]
        + "__"
        + df["microstructure_regime"]
    )

    df["vol_micro_state"] = (
        df[volatility_col]
        + "__"
        + df["microstructure_regime"]
    )

    df["momentum_micro_state"] = (
        df[momentum_col]
        + "__"
        + df["microstructure_regime"]
    )

    df["trend_strength_micro_state"] = (
        df[trend_strength_col]
        + "__"
        + df["microstructure_regime"]
    )

    df["selected_timeframe"] = timeframe
    df["selected_timeframe_prefix"] = prefix
    df["multi_layer_state_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return df


def assign_sample_quality(rows: int) -> str:
    if rows >= HIGH_CONFIDENCE_ROWS:
        return "high_confidence"
    if rows >= STRONGER_ROWS:
        return "stronger"
    if rows >= MIN_ROWS:
        return "usable"
    return "low_sample"


def summarise_states(
    df: pd.DataFrame,
    state_col: str,
    summary_level: str,
    timeframe: str,
    regime_cols: dict,
) -> pd.DataFrame:
    if state_col not in df.columns:
        return pd.DataFrame()

    group_cols = ["symbol", "selected_timeframe", "bar_type", "bar_family", state_col]

    summary = (
        df.groupby(group_cols, dropna=False)
        .agg(
            rows=(state_col, "count"),
            matched_regime_pct=("has_selected_regime", "mean"),
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
            avg_alignment_gap_minutes=("regime_alignment_gap_minutes", "mean"),
        )
        .reset_index()
    )

    confidence_col = regime_cols["confidence"]

    if confidence_col in df.columns:
        confidence_summary = (
            df.groupby(group_cols, dropna=False)
            .agg(avg_regime_confidence=(confidence_col, "mean"))
            .reset_index()
        )
        summary = summary.merge(confidence_summary, on=group_cols, how="left")
    else:
        summary["avg_regime_confidence"] = np.nan

    for col in [
        "matched_regime_pct",
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]:
        if col in summary.columns:
            summary[col] = summary[col] * 100

    summary["persistence_edge_h1"] = (
        summary["target_direction_persist_h1_pct"]
        - summary["target_direction_flip_h1_pct"]
    )

    summary["sample_quality"] = summary["rows"].apply(assign_sample_quality)
    summary["summary_level"] = summary_level
    summary["state_column"] = state_col
    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary.sort_values(
        ["sample_quality", "persistence_edge_h1", "rows"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def build_symbol_report(
    symbol: str,
    summary: pd.DataFrame,
    input_path: Path,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = summary[
        summary["sample_quality"].isin(["usable", "stronger", "high_confidence"])
    ].copy()

    strongest_persistence = usable.sort_values(
        ["sample_quality", "persistence_edge_h1", "rows"],
        ascending=[True, False, False],
        na_position="last",
    ).head(50)

    high_confidence = usable[
        usable["sample_quality"] == "high_confidence"
    ].sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(50)

    strongest_activity = usable.sort_values(
        "avg_ticks_per_second",
        ascending=False,
        na_position="last",
    ).head(50)

    strongest_volatility = usable.sort_values(
        "avg_abs_return",
        ascending=False,
        na_position="last",
    ).head(50)

    display_cols = [
        "symbol",
        "selected_timeframe",
        "summary_level",
        "bar_type",
        "state_column",
        "rows",
        "matched_regime_pct",
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
    lines.append(f"BACQE TICK RESEARCH - MULTI-LAYER STATE MODEL REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {input_path}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("HIGH CONFIDENCE STATES")
    lines.append("-" * 90)
    lines.append(high_confidence[available_cols].to_string(index=False) if not high_confidence.empty else "No high-confidence states yet.")

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
    lines.append("Multi-layer states combine selected regime context with event-time microstructure behaviour.")
    lines.append("Persistence edge = target_direction_persist_h1_pct - target_direction_flip_h1_pct.")
    lines.append("High-confidence states require at least 500 rows.")
    lines.append("Small-sample states should be treated only as hypotheses.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regime_fusion_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: regime fusion file not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    fused = pd.read_parquet(input_path)

    print(f"[INFO] Rows loaded:    {len(fused):,}")
    print(f"[INFO] Columns loaded: {len(fused.columns):,}")

    data, timeframe, regime_cols = prepare_data(fused, symbol)
    data = add_relative_regimes(data)
    state_model = add_multi_layer_state_labels(data, timeframe, regime_cols)

    summaries = []

    for state_col, summary_level in [
        ("compact_multi_layer_state", "compact_multi_layer_state"),
        ("multi_layer_state", "full_multi_layer_state"),
        ("trend_micro_state", "trend_micro_state"),
        ("vol_micro_state", "vol_micro_state"),
        ("momentum_micro_state", "momentum_micro_state"),
        ("trend_strength_micro_state", "trend_strength_micro_state"),
    ]:
        summary_part = summarise_states(
            state_model,
            state_col,
            summary_level,
            timeframe,
            regime_cols,
        )

        if not summary_part.empty:
            summaries.append(summary_part)

    if not summaries:
        print(f"[WARN] {symbol}: no state summaries generated.")
        return state_model, pd.DataFrame()

    summary = pd.concat(summaries, ignore_index=True)

    processed_dir = OUTPUT_PROCESSED_ROOT / f"symbol={symbol}"
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    processed_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    state_parquet = processed_dir / f"{symbol}_multi_layer_state_model_latest.parquet"
    state_csv = processed_dir / f"{symbol}_multi_layer_state_model_latest.csv"

    summary_csv = analysis_dir / f"{symbol}_multi_layer_state_summary_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_multi_layer_state_summary_latest.parquet"

    report_path = report_dir / f"{symbol}_multi_layer_state_model_report_latest.txt"

    state_model.to_parquet(state_parquet, index=False)
    state_model.to_csv(state_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_symbol_report(symbol, summary, input_path)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: State Parquet:   {state_parquet}")
    print(f"[DONE] {symbol}: Summary CSV:     {summary_csv}")
    print(f"[DONE] {symbol}: Report:          {report_path}")

    return state_model, summary


def save_master_outputs(
    state_frames: list[pd.DataFrame],
    summary_frames: list[pd.DataFrame],
) -> None:
    master_processed_dir = OUTPUT_PROCESSED_ROOT / "_master"
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_processed_dir.mkdir(parents=True, exist_ok=True)
    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_state_model = pd.concat(state_frames, ignore_index=True)
    master_summary = pd.concat(summary_frames, ignore_index=True)

    state_parquet = master_processed_dir / "master_multi_layer_state_model_latest.parquet"
    state_csv = master_processed_dir / "master_multi_layer_state_model_latest.csv"

    summary_csv = master_analysis_dir / "master_multi_layer_state_summary_latest.csv"
    summary_parquet = master_analysis_dir / "master_multi_layer_state_summary_latest.parquet"

    report_path = master_report_dir / "master_multi_layer_state_model_report_latest.txt"

    master_state_model.to_parquet(state_parquet, index=False)
    master_state_model.to_csv(state_csv, index=False)

    master_summary.to_csv(summary_csv, index=False)
    master_summary.to_parquet(summary_parquet, index=False)

    usable = master_summary[
        master_summary["sample_quality"].isin(["usable", "stronger", "high_confidence"])
    ].copy()

    high_confidence = usable[
        usable["sample_quality"] == "high_confidence"
    ].sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(100)

    strongest_persistence = usable.sort_values(
        ["sample_quality", "persistence_edge_h1", "rows"],
        ascending=[True, False, False],
        na_position="last",
    ).head(100)

    display_cols = [
        "symbol",
        "selected_timeframe",
        "summary_level",
        "bar_type",
        "state_column",
        "rows",
        "matched_regime_pct",
        "sample_quality",
        "avg_abs_return",
        "avg_ticks_per_second",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in master_summary.columns]

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER MULTI-LAYER STATE MODEL REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "HIGH CONFIDENCE STATES",
                "-" * 90,
                high_confidence[available_cols].to_string(index=False)
                if not high_confidence.empty
                else "No high-confidence states yet.",
                "",
                "STRONGEST MULTI-LAYER PERSISTENCE STATES",
                "-" * 90,
                strongest_persistence[available_cols].to_string(index=False),
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "This is a research state model, not a trading system.",
                "Multi-layer states combine selected regime context with event-time microstructure behaviour.",
                "Persistence edge = target_direction_persist_h1_pct - target_direction_flip_h1_pct.",
                "High-confidence states require at least 500 rows.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master multi-layer state model created.")
    print(f"Master State Parquet:   {state_parquet}")
    print(f"Master Summary CSV:     {summary_csv}")
    print(f"Master Report:          {report_path}")
    print("-" * 90)

    if not high_confidence.empty:
        print("HIGH CONFIDENCE STATES")
        print(high_confidence[available_cols].head(50).to_string(index=False))
        print("-" * 90)

    print("STRONGEST STATES PREVIEW")
    print(strongest_persistence[available_cols].head(50).to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 28 BUILD MULTI-LAYER STATE MODEL - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:            {INPUT_ROOT}")
    print(f"Output processed root: {OUTPUT_PROCESSED_ROOT}")
    print(f"Output analysis root:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:    {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:               {SYMBOLS}")
    print("-" * 90)

    state_frames = []
    summary_frames = []

    for symbol in SYMBOLS:
        state_model, summary = process_symbol(symbol)

        if not state_model.empty:
            state_frames.append(state_model)

        if not summary.empty:
            summary_frames.append(summary)

    if not state_frames or not summary_frames:
        print("[WARN] No multi-layer state models created.")
        return

    save_master_outputs(
        state_frames=state_frames,
        summary_frames=summary_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol multi-layer state model complete.")
    print(f"Symbols analysed: {len(summary_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()