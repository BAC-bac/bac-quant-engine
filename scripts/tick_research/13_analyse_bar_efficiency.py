"""
BACQE TICK RESEARCH - 13 Analyse Bar Efficiency

Compares fixed tick bars and tick imbalance bars using simple structural
efficiency metrics.

Inputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_imbalance_bar_comparison_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/bar_efficiency_analysis_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/bar_efficiency_analysis_latest.parquet
    E:/Quant_Lab/reports/tick_research/bar_efficiency/bar_efficiency_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "tick_vs_imbalance_bar_comparison_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "bar_efficiency"


def min_max_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    min_value = values.min()
    max_value = values.max()

    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series(50.0, index=series.index)

    score = ((values - min_value) / (max_value - min_value)) * 100

    if not higher_is_better:
        score = 100 - score

    return score.round(4)


def build_efficiency_scores(df: pd.DataFrame) -> pd.DataFrame:
    analysis = df.copy()

    numeric_cols = [
        "bar_count",
        "avg_duration_seconds",
        "median_duration_seconds",
        "avg_tick_count",
        "median_tick_count",
        "avg_range",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "avg_imbalance_ratio",
        "positive_imbalance_pct",
        "negative_imbalance_pct",
    ]

    for col in numeric_cols:
        if col in analysis.columns:
            analysis[col] = pd.to_numeric(analysis[col], errors="coerce")

    analysis["abs_lag1_autocorr"] = analysis["lag1_return_autocorr"].abs()
    analysis["abs_imbalance_ratio"] = analysis["avg_imbalance_ratio"].abs()

    analysis["range_per_tick"] = analysis["avg_range"] / analysis["avg_tick_count"].replace(0, np.nan)
    analysis["volatility_per_tick"] = analysis["return_std"] / analysis["avg_tick_count"].replace(0, np.nan)
    analysis["range_per_second"] = analysis["avg_range"] / analysis["avg_duration_seconds"].replace(0, np.nan)
    analysis["volatility_per_second"] = analysis["return_std"] / analysis["avg_duration_seconds"].replace(0, np.nan)

    analysis["tail_efficiency_score"] = min_max_score(
        analysis["return_kurtosis"],
        higher_is_better=False,
    )

    analysis["noise_reduction_score"] = min_max_score(
        analysis["abs_lag1_autocorr"],
        higher_is_better=False,
    )

    analysis["information_density_score"] = min_max_score(
        analysis["range_per_tick"],
        higher_is_better=True,
    )

    analysis["volatility_density_score"] = min_max_score(
        analysis["volatility_per_tick"],
        higher_is_better=True,
    )

    analysis["sample_size_score"] = min_max_score(
        analysis["bar_count"],
        higher_is_better=True,
    )

    analysis["stability_score"] = min_max_score(
        analysis["return_std"],
        higher_is_better=False,
    )

    analysis["structural_efficiency_score"] = (
        analysis["tail_efficiency_score"] * 0.25
        + analysis["noise_reduction_score"] * 0.20
        + analysis["information_density_score"] * 0.20
        + analysis["volatility_density_score"] * 0.15
        + analysis["sample_size_score"] * 0.10
        + analysis["stability_score"] * 0.10
    ).round(4)

    analysis["efficiency_rank"] = (
        analysis["structural_efficiency_score"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    analysis["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    analysis = analysis.sort_values(
        ["efficiency_rank", "structural_efficiency_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    return analysis


def build_report(analysis: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    top = analysis.iloc[0]

    display_cols = [
        "efficiency_rank",
        "bar_type",
        "bar_family",
        "bar_count",
        "avg_tick_count",
        "avg_duration_seconds",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "range_per_tick",
        "volatility_per_tick",
        "tail_efficiency_score",
        "noise_reduction_score",
        "information_density_score",
        "volatility_density_score",
        "sample_size_score",
        "stability_score",
        "structural_efficiency_score",
    ]

    available_cols = [col for col in display_cols if col in analysis.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - BAR EFFICIENCY REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input file:       {INPUT_PATH}")
    lines.append("-" * 90)
    lines.append(f"Best ranked bar type: {top['bar_type']}")
    lines.append(f"Best score:           {top['structural_efficiency_score']}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("EFFICIENCY RANKING")
    lines.append("-" * 90)
    lines.append(analysis[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("tail_efficiency_score rewards lower kurtosis.")
    lines.append("noise_reduction_score rewards lower absolute lag-1 autocorrelation.")
    lines.append("information_density_score rewards more price range per tick.")
    lines.append("volatility_density_score rewards more return movement per tick.")
    lines.append("sample_size_score rewards more observations.")
    lines.append("stability_score rewards lower return volatility.")
    lines.append("")
    lines.append("This is a v1 research score, not a trading signal.")
    lines.append("The score is designed to rank sampling methods for structural research quality.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 13 ANALYSE BAR EFFICIENCY")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input comparison file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    print(f"Rows loaded: {len(df):,}")

    analysis = build_efficiency_scores(df)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "bar_efficiency_analysis_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "bar_efficiency_analysis_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "bar_efficiency_report_latest.txt"

    analysis.to_csv(csv_path, index=False)
    analysis.to_parquet(parquet_path, index=False)

    report = build_report(analysis)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Bar efficiency analysis created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = [
        "efficiency_rank",
        "bar_type",
        "bar_family",
        "bar_count",
        "return_kurtosis",
        "lag1_return_autocorr",
        "range_per_tick",
        "volatility_per_tick",
        "structural_efficiency_score",
    ]

    print(analysis[display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()