"""
BACQE TICK RESEARCH - 17 Compare Bar Predictability

Tests simple next-bar predictability across microstructure regimes.

Input:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/GBPUSD_microstructure_regimes_latest.parquet

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/bar_predictability_analysis_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/bar_predictability_analysis_latest.parquet
    E:/Quant_Lab/reports/tick_research/bar_predictability/bar_predictability_report_latest.txt
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
    / "microstructure_regimes"
    / f"{SYMBOL}_microstructure_regimes_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "bar_predictability"

MIN_OBSERVATIONS = 30


def add_forward_labels(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(data["bar_start_time"], errors="coerce", utc=True)
    data = data.sort_values(["bar_type", "bar_start_time"]).reset_index(drop=True)

    data["next_return"] = data.groupby("bar_type")["return"].shift(-1)
    data["next_abs_return"] = data["next_return"].abs()

    data["next_direction"] = 0
    data.loc[data["next_return"] > 0, "next_direction"] = 1
    data.loc[data["next_return"] < 0, "next_direction"] = -1

    data["current_direction"] = pd.to_numeric(data["direction"], errors="coerce").fillna(0).astype(int)

    data["direction_persisted"] = (
        (data["current_direction"] != 0)
        & (data["next_direction"] == data["current_direction"])
    )

    data["direction_flipped"] = (
        (data["current_direction"] != 0)
        & (data["next_direction"] == -data["current_direction"])
    )

    data["next_positive"] = data["next_return"] > 0
    data["next_negative"] = data["next_return"] < 0

    return data


def summarise_predictability(labelled: pd.DataFrame) -> pd.DataFrame:
    clean = labelled.dropna(subset=["next_return"]).copy()

    group_cols = ["bar_type", "bar_family", "microstructure_regime"]

    summary = (
        clean.groupby(group_cols, dropna=False)
        .agg(
            observations=("next_return", "count"),
            current_avg_return=("return", "mean"),
            next_avg_return=("next_return", "mean"),
            next_median_return=("next_return", "median"),
            next_avg_abs_return=("next_abs_return", "mean"),
            next_return_std=("next_return", "std"),
            next_positive_pct=("next_positive", "mean"),
            next_negative_pct=("next_negative", "mean"),
            direction_persistence_pct=("direction_persisted", "mean"),
            direction_flip_pct=("direction_flipped", "mean"),
            avg_current_range=("range", "mean"),
            avg_current_duration_seconds=("duration_seconds", "mean"),
            avg_current_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["next_positive_pct"] *= 100
    summary["next_negative_pct"] *= 100
    summary["direction_persistence_pct"] *= 100
    summary["direction_flip_pct"] *= 100

    summary["edge_proxy"] = summary["next_avg_return"] / summary["next_return_std"].replace(0, np.nan)
    summary["activity_adjusted_abs_return"] = (
        summary["next_avg_abs_return"] / summary["avg_current_duration_seconds"].replace(0, np.nan)
    )

    summary["sample_quality"] = "low_sample"
    summary.loc[summary["observations"] >= MIN_OBSERVATIONS, "sample_quality"] = "usable"
    summary.loc[summary["observations"] >= 100, "sample_quality"] = "stronger"

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(
        ["bar_type", "sample_quality", "observations"],
        ascending=[True, True, False],
    ).reset_index(drop=True)


def build_bar_level_summary(labelled: pd.DataFrame) -> pd.DataFrame:
    clean = labelled.dropna(subset=["next_return"]).copy()

    summary = (
        clean.groupby(["bar_type", "bar_family"], dropna=False)
        .agg(
            observations=("next_return", "count"),
            next_avg_return=("next_return", "mean"),
            next_avg_abs_return=("next_abs_return", "mean"),
            next_return_std=("next_return", "std"),
            next_positive_pct=("next_positive", "mean"),
            direction_persistence_pct=("direction_persisted", "mean"),
            direction_flip_pct=("direction_flipped", "mean"),
        )
        .reset_index()
    )

    summary["next_positive_pct"] *= 100
    summary["direction_persistence_pct"] *= 100
    summary["direction_flip_pct"] *= 100
    summary["edge_proxy"] = summary["next_avg_return"] / summary["next_return_std"].replace(0, np.nan)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary


def build_report(regime_summary: pd.DataFrame, bar_summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = regime_summary[regime_summary["observations"] >= MIN_OBSERVATIONS].copy()
    usable = usable.sort_values("edge_proxy", ascending=False, na_position="last")

    display_cols = [
        "bar_type",
        "bar_family",
        "microstructure_regime",
        "observations",
        "sample_quality",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
    ]

    bar_cols = [
        "bar_type",
        "bar_family",
        "observations",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
    ]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - BAR PREDICTABILITY REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Minimum obs:     {MIN_OBSERVATIONS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("BAR-LEVEL PREDICTABILITY")
    lines.append("-" * 90)
    lines.append(bar_summary[bar_cols].to_string(index=False))

    lines.append("")
    lines.append("REGIME-LEVEL PREDICTABILITY - USABLE SAMPLES")
    lines.append("-" * 90)

    if usable.empty:
        lines.append("No regimes met the minimum observation threshold.")
    else:
        lines.append(usable[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("edge_proxy is next_avg_return divided by next_return_std.")
    lines.append("Positive edge_proxy suggests positive next-bar drift, but sample size matters.")
    lines.append("Direction persistence measures whether current bar direction continues next bar.")
    lines.append("Small datasets can produce unstable results; treat these as hypotheses.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 17 COMPARE BAR PREDICTABILITY")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Microstructure regimes file not found: {INPUT_PATH}")

    regimes = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded: {len(regimes):,}")

    labelled = add_forward_labels(regimes)

    regime_summary = summarise_predictability(labelled)
    bar_summary = build_bar_level_summary(labelled)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    regime_csv = OUTPUT_ANALYSIS_DIR / "bar_predictability_analysis_latest.csv"
    regime_parquet = OUTPUT_ANALYSIS_DIR / "bar_predictability_analysis_latest.parquet"
    bar_csv = OUTPUT_ANALYSIS_DIR / "bar_predictability_bar_level_latest.csv"
    bar_parquet = OUTPUT_ANALYSIS_DIR / "bar_predictability_bar_level_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "bar_predictability_report_latest.txt"

    regime_summary.to_csv(regime_csv, index=False)
    regime_summary.to_parquet(regime_parquet, index=False)

    bar_summary.to_csv(bar_csv, index=False)
    bar_summary.to_parquet(bar_parquet, index=False)

    report = build_report(regime_summary, bar_summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Bar predictability analysis created.")
    print(f"Regime CSV:     {regime_csv}")
    print(f"Regime Parquet: {regime_parquet}")
    print(f"Bar CSV:        {bar_csv}")
    print(f"Bar Parquet:    {bar_parquet}")
    print(f"Report:         {report_path}")
    print("-" * 90)

    display_cols = [
        "bar_type",
        "bar_family",
        "observations",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
    ]

    print("BAR-LEVEL SUMMARY")
    print(bar_summary[display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()