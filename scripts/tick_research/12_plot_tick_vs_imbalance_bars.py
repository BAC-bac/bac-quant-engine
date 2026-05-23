"""
BACQE TICK RESEARCH - 12 Plot Tick vs Imbalance Bars

Creates charts from:
    tick_vs_imbalance_bar_comparison_latest.csv

Outputs:
    E:/Quant_Lab/reports/tick_research/tick_vs_imbalance_bars/
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "tick_vs_imbalance_bar_comparison_latest.csv"

OUTPUT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "tick_vs_imbalance_bars"


def load_comparison() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Comparison file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    order = {
        "tick_100": 1,
        "tick_250": 2,
        "tick_500": 3,
        "tick_1000": 4,
        "imbalance_25": 5,
        "imbalance_50": 6,
        "imbalance_100": 7,
        "imbalance_200": 8,
    }

    df["sort_order"] = df["bar_type"].map(order).fillna(999)
    df = df.sort_values("sort_order").reset_index(drop=True)

    return df


def plot_metric(df: pd.DataFrame, metric: str, title: str, ylabel: str, output_name: str) -> None:
    if metric not in df.columns:
        print(f"[WARN] Metric missing, skipping: {metric}")
        return

    plot_df = df.dropna(subset=[metric]).copy()

    if plot_df.empty:
        print(f"[WARN] No values for metric, skipping: {metric}")
        return

    plt.figure(figsize=(12, 6))
    plt.bar(plot_df["bar_type"], plot_df[metric])
    plt.title(title)
    plt.xlabel("Bar Type")
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    output_path = OUTPUT_DIR / output_name
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"[DONE] Saved chart: {output_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 12 PLOT TICK VS IMBALANCE BARS")
    print("=" * 90)
    print(f"Input:      {INPUT_PATH}")
    print(f"Output dir: {OUTPUT_DIR}")
    print("-" * 90)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_comparison()

    print(f"Rows loaded: {len(df):,}")
    print("-" * 90)

    plot_metric(
        df,
        metric="return_std",
        title="Return Volatility: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Return Standard Deviation",
        output_name="01_return_volatility.png",
    )

    plot_metric(
        df,
        metric="return_kurtosis",
        title="Return Kurtosis: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Kurtosis",
        output_name="02_return_kurtosis.png",
    )

    plot_metric(
        df,
        metric="avg_duration_seconds",
        title="Average Duration: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Average Duration (Seconds)",
        output_name="03_average_duration.png",
    )

    plot_metric(
        df,
        metric="median_duration_seconds",
        title="Median Duration: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Median Duration (Seconds)",
        output_name="04_median_duration.png",
    )

    plot_metric(
        df,
        metric="avg_tick_count",
        title="Average Tick Count: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Average Tick Count",
        output_name="05_average_tick_count.png",
    )

    plot_metric(
        df,
        metric="median_tick_count",
        title="Median Tick Count: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Median Tick Count",
        output_name="06_median_tick_count.png",
    )

    plot_metric(
        df,
        metric="lag1_return_autocorr",
        title="Lag-1 Return Autocorrelation: Fixed Tick Bars vs Tick Imbalance Bars",
        ylabel="Lag-1 Return Autocorrelation",
        output_name="07_lag1_autocorrelation.png",
    )

    plot_metric(
        df,
        metric="avg_imbalance_ratio",
        title="Average Imbalance Ratio: Tick Imbalance Bars",
        ylabel="Average Imbalance Ratio",
        output_name="08_average_imbalance_ratio.png",
    )

    plot_metric(
        df,
        metric="positive_imbalance_pct",
        title="Positive Imbalance Percentage",
        ylabel="Positive Imbalance (%)",
        output_name="09_positive_imbalance_percentage.png",
    )

    plot_metric(
        df,
        metric="negative_imbalance_pct",
        title="Negative Imbalance Percentage",
        ylabel="Negative Imbalance (%)",
        output_name="10_negative_imbalance_percentage.png",
    )

    summary_path = OUTPUT_DIR / "chart_run_summary.txt"

    summary_cols = [
        "bar_type",
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

    available_cols = [col for col in summary_cols if col in df.columns]

    summary_text = "\n".join(
        [
            "=" * 90,
            "BACQE TICK VS IMBALANCE BAR CHART SUMMARY",
            "=" * 90,
            f"Run time UTC: {datetime.now(timezone.utc).isoformat()}",
            f"Input:        {INPUT_PATH}",
            f"Output dir:   {OUTPUT_DIR}",
            f"Rows loaded:  {len(df):,}",
            "-" * 90,
            df[available_cols].to_string(index=False),
        ]
    )

    summary_path.write_text(summary_text, encoding="utf-8")

    print("-" * 90)
    print("[COMPLETE] Tick vs imbalance charts created.")
    print(f"Summary: {summary_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()