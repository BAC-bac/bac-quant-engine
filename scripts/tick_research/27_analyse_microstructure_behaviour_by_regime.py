"""
BACQE TICK RESEARCH - 27 Analyse Microstructure Behaviour By Regime

Analyses how microstructure behaviour changes across BACQE M15 regime states.

Input:
    E:/Quant_Lab/data/processed/tick_research/regime_fusion/GBPUSD_microstructure_m15_regime_fusion_latest.parquet

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/microstructure_behaviour_by_regime_latest.csv
    E:/Quant_Lab/reports/tick_research/microstructure_by_regime/microstructure_behaviour_by_regime_report_latest.txt
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

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "microstructure_by_regime"

MIN_ROWS = 20


def safe_autocorr(series: pd.Series, lag: int = 1) -> float:
    clean = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()

    if len(clean) <= lag + 2:
        return np.nan

    return clean.autocorr(lag=lag)


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    numeric_cols = [
        "return",
        "abs_return",
        "range",
        "duration_seconds",
        "tick_count",
        "ticks_per_second",
        "range_per_tick",
        "volatility_per_tick",
        "imbalance_ratio",
        "abs_imbalance_ratio",
        "target_up_h1",
        "target_direction_persist_h1",
        "target_direction_flip_h1",
        "future_return_h1",
        "future_abs_return_h1",
        "m15_regime_confidence",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "abs_imbalance_ratio" not in data.columns and "imbalance_ratio" in data.columns:
        data["abs_imbalance_ratio"] = data["imbalance_ratio"].abs()

    data["is_directional_imbalance_regime"] = (
        data["microstructure_regime"].astype(str).str.contains("directional_imbalance")
    ).astype(int)

    data["is_volatility_expansion_micro"] = (
        data["microstructure_regime"].astype(str).str.contains("volatility_expansion")
    ).astype(int)

    data["is_compressed_micro"] = (
        data["microstructure_regime"].astype(str).eq("compressed_low_vol")
    ).astype(int)

    return data


def summarise_by_group(data: pd.DataFrame, group_cols: list[str], summary_level: str) -> pd.DataFrame:
    records = []

    for keys, group in data.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        base = {
            "summary_level": summary_level,
            "rows": len(group),
            "sample_quality": "low_sample",
            "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        for col, value in zip(group_cols, keys):
            base[col] = value

        if len(group) >= MIN_ROWS:
            base["sample_quality"] = "usable"

        if len(group) >= 100:
            base["sample_quality"] = "stronger"

        base.update(
            {
                "avg_return": group["return"].mean(),
                "avg_abs_return": group["abs_return"].mean(),
                "return_std": group["return"].std(),
                "avg_future_return_h1": group["future_return_h1"].mean(),
                "avg_future_abs_return_h1": group["future_abs_return_h1"].mean(),
                "target_up_h1_pct": group["target_up_h1"].mean() * 100,
                "target_direction_persist_h1_pct": group["target_direction_persist_h1"].mean() * 100,
                "target_direction_flip_h1_pct": group["target_direction_flip_h1"].mean() * 100,
                "avg_range": group["range"].mean(),
                "avg_duration_seconds": group["duration_seconds"].mean(),
                "median_duration_seconds": group["duration_seconds"].median(),
                "avg_tick_count": group["tick_count"].mean(),
                "median_tick_count": group["tick_count"].median(),
                "avg_ticks_per_second": group["ticks_per_second"].mean(),
                "avg_range_per_tick": group["range_per_tick"].mean(),
                "avg_volatility_per_tick": group["volatility_per_tick"].mean(),
                "avg_abs_imbalance_ratio": group["abs_imbalance_ratio"].mean(),
                "directional_imbalance_micro_pct": group["is_directional_imbalance_regime"].mean() * 100,
                "volatility_expansion_micro_pct": group["is_volatility_expansion_micro"].mean() * 100,
                "compressed_micro_pct": group["is_compressed_micro"].mean() * 100,
                "avg_m15_regime_confidence": group["m15_regime_confidence"].mean(),
                "abs_return_autocorr_lag1": safe_autocorr(group["abs_return"], lag=1),
                "return_autocorr_lag1": safe_autocorr(group["return"], lag=1),
            }
        )

        base["persistence_edge_h1"] = (
            base["target_direction_persist_h1_pct"] - base["target_direction_flip_h1_pct"]
        )

        records.append(base)

    summary = pd.DataFrame(records)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary


def build_report(summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = summary[summary["sample_quality"].isin(["usable", "stronger"])].copy()

    strongest_persistence = usable.sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(30)

    strongest_activity = usable.sort_values(
        "avg_ticks_per_second",
        ascending=False,
        na_position="last",
    ).head(30)

    strongest_volatility = usable.sort_values(
        "avg_abs_return",
        ascending=False,
        na_position="last",
    ).head(30)

    display_cols = [
        "summary_level",
        "bar_type",
        "m15_composite_regime",
        "m15_trend_state",
        "m15_volatility_state",
        "rows",
        "sample_quality",
        "avg_abs_return",
        "return_std",
        "avg_ticks_per_second",
        "directional_imbalance_micro_pct",
        "volatility_expansion_micro_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in summary.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - MICROSTRUCTURE BEHAVIOUR BY REGIME")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Minimum rows:    {MIN_ROWS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("STRONGEST H1 PERSISTENCE EDGES")
    lines.append("-" * 90)
    lines.append(strongest_persistence[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST ACTIVITY STATES")
    lines.append("-" * 90)
    lines.append(strongest_activity[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST VOLATILITY STATES")
    lines.append("-" * 90)
    lines.append(strongest_volatility[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is diagnostic research, not a trading system.")
    lines.append("Persistence edge = persistence % - flip %.")
    lines.append("High activity states may reflect faster information arrival.")
    lines.append("High volatility states may reflect larger movement but not necessarily directional edge.")
    lines.append("Small samples should be treated as hypotheses only.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 27 ANALYSE MICROSTRUCTURE BEHAVIOUR BY REGIME")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Fusion file not found: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    data = prepare_data(df)

    summaries = []

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", "m15_composite_regime"],
            "bar_type_composite_regime",
        )
    )

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", "m15_trend_state", "m15_volatility_state"],
            "bar_type_trend_volatility",
        )
    )

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", "m15_composite_regime", "microstructure_regime"],
            "bar_type_composite_micro_regime",
        )
    )

    summary = pd.concat(summaries, ignore_index=True)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "microstructure_behaviour_by_regime_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "microstructure_behaviour_by_regime_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "microstructure_behaviour_by_regime_report_latest.txt"

    summary.to_csv(csv_path, index=False)
    summary.to_parquet(parquet_path, index=False)

    report = build_report(summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Microstructure behaviour by regime analysis created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = [
        "summary_level",
        "bar_type",
        "m15_composite_regime",
        "m15_trend_state",
        "m15_volatility_state",
        "rows",
        "sample_quality",
        "avg_abs_return",
        "avg_ticks_per_second",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in summary.columns]

    preview = summary[
        summary["sample_quality"].isin(["usable", "stronger"])
    ].sort_values("persistence_edge_h1", ascending=False)

    print(preview[available_cols].head(40).to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()