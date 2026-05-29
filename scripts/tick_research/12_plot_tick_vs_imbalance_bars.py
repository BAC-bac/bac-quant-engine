"""
BACQE TICK RESEARCH - 12 Plot Tick vs Imbalance Bars - Multi Symbol

Creates charts from the multi-symbol comparison output produced by Script 11.

Input:
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_imbalance/
        tick_vs_imbalance_bar_comparison_latest.csv

Outputs:
    Per-symbol charts:
        E:/Quant_Lab/reports/tick_research/tick_vs_imbalance_bars/symbol=<SYMBOL>/

    Master cross-symbol charts:
        E:/Quant_Lab/reports/tick_research/tick_vs_imbalance_bars/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "tick_vs_imbalance"
    / "tick_vs_imbalance_bar_comparison_latest.csv"
)

OUTPUT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "tick_vs_imbalance_bars"
)

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

BAR_ORDER = {
    "tick_100": 1,
    "tick_250": 2,
    "tick_500": 3,
    "tick_1000": 4,
    "imbalance_25": 5,
    "imbalance_50": 6,
    "imbalance_100": 7,
    "imbalance_200": 8,
}


def load_comparison() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Comparison file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if "symbol" not in df.columns:
        raise ValueError("Input comparison file does not contain a 'symbol' column.")

    df["sort_order"] = df["bar_type"].map(BAR_ORDER).fillna(999)
    df = df.sort_values(["symbol", "sort_order"]).reset_index(drop=True)

    return df


def plot_metric(
    df: pd.DataFrame,
    metric: str,
    title: str,
    ylabel: str,
    output_path: Path,
) -> None:
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"[DONE] Saved chart: {output_path}")


def plot_symbol_charts(symbol: str, df: pd.DataFrame) -> None:
    symbol_df = df[df["symbol"] == symbol].copy()

    if symbol_df.empty:
        print(f"[WARN] No rows found for symbol: {symbol}")
        return

    output_dir = OUTPUT_ROOT / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("-" * 90)
    print(f"[SYMBOL] {symbol}")
    print(f"Rows: {len(symbol_df):,}")

    chart_specs = [
        (
            "return_std",
            f"{symbol} Return Volatility: Fixed Tick Bars vs Tick Imbalance Bars",
            "Return Standard Deviation",
            "01_return_volatility.png",
        ),
        (
            "return_kurtosis",
            f"{symbol} Return Kurtosis: Fixed Tick Bars vs Tick Imbalance Bars",
            "Kurtosis",
            "02_return_kurtosis.png",
        ),
        (
            "avg_duration_seconds",
            f"{symbol} Average Duration: Fixed Tick Bars vs Tick Imbalance Bars",
            "Average Duration (Seconds)",
            "03_average_duration.png",
        ),
        (
            "median_duration_seconds",
            f"{symbol} Median Duration: Fixed Tick Bars vs Tick Imbalance Bars",
            "Median Duration (Seconds)",
            "04_median_duration.png",
        ),
        (
            "avg_tick_count",
            f"{symbol} Average Tick Count: Fixed Tick Bars vs Tick Imbalance Bars",
            "Average Tick Count",
            "05_average_tick_count.png",
        ),
        (
            "median_tick_count",
            f"{symbol} Median Tick Count: Fixed Tick Bars vs Tick Imbalance Bars",
            "Median Tick Count",
            "06_median_tick_count.png",
        ),
        (
            "lag1_return_autocorr",
            f"{symbol} Lag-1 Return Autocorrelation",
            "Lag-1 Return Autocorrelation",
            "07_lag1_autocorrelation.png",
        ),
        (
            "avg_imbalance_ratio",
            f"{symbol} Average Imbalance Ratio",
            "Average Imbalance Ratio",
            "08_average_imbalance_ratio.png",
        ),
        (
            "positive_imbalance_pct",
            f"{symbol} Positive Imbalance Percentage",
            "Positive Imbalance (%)",
            "09_positive_imbalance_percentage.png",
        ),
        (
            "negative_imbalance_pct",
            f"{symbol} Negative Imbalance Percentage",
            "Negative Imbalance (%)",
            "10_negative_imbalance_percentage.png",
        ),
    ]

    for metric, title, ylabel, output_name in chart_specs:
        plot_metric(
            symbol_df,
            metric=metric,
            title=title,
            ylabel=ylabel,
            output_path=output_dir / output_name,
        )

    write_symbol_summary(symbol, symbol_df, output_dir)


def plot_master_metric(
    df: pd.DataFrame,
    metric: str,
    bar_type: str,
    title: str,
    ylabel: str,
    output_name: str,
) -> None:
    if metric not in df.columns:
        print(f"[WARN] Master metric missing, skipping: {metric}")
        return

    plot_df = df[df["bar_type"] == bar_type].dropna(subset=[metric]).copy()

    if plot_df.empty:
        print(f"[WARN] No master values for {metric} / {bar_type}")
        return

    plot_df = plot_df.sort_values(metric, ascending=False)

    plt.figure(figsize=(12, 6))
    plt.bar(plot_df["symbol"], plot_df[metric])
    plt.title(title)
    plt.xlabel("Symbol")
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    output_dir = OUTPUT_ROOT / "_master"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / output_name
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"[DONE] Saved master chart: {output_path}")


def write_symbol_summary(symbol: str, df: pd.DataFrame, output_dir: Path) -> None:
    summary_path = output_dir / f"{symbol}_chart_run_summary.txt"

    summary_cols = [
        "symbol",
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
            f"BACQE TICK VS IMBALANCE BAR CHART SUMMARY - {symbol}",
            "=" * 90,
            f"Run time UTC: {datetime.now(timezone.utc).isoformat()}",
            f"Input:        {INPUT_PATH}",
            f"Output dir:   {output_dir}",
            f"Rows loaded:  {len(df):,}",
            "-" * 90,
            df[available_cols].to_string(index=False),
        ]
    )

    summary_path.write_text(summary_text, encoding="utf-8")
    print(f"[DONE] Saved summary: {summary_path}")


def write_master_summary(df: pd.DataFrame) -> None:
    output_dir = OUTPUT_ROOT / "_master"
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = output_dir / "master_chart_run_summary.txt"

    summary_cols = [
        "symbol",
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
            "BACQE TICK VS IMBALANCE BAR MASTER CHART SUMMARY",
            "=" * 90,
            f"Run time UTC: {datetime.now(timezone.utc).isoformat()}",
            f"Input:        {INPUT_PATH}",
            f"Output root:  {OUTPUT_ROOT}",
            f"Rows loaded:  {len(df):,}",
            f"Symbols:      {sorted(df['symbol'].dropna().unique().tolist())}",
            "-" * 90,
            df[available_cols].to_string(index=False),
        ]
    )

    summary_path.write_text(summary_text, encoding="utf-8")
    print(f"[DONE] Saved master summary: {summary_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 12 PLOT TICK VS IMBALANCE BARS - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input:       {INPUT_PATH}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    df = load_comparison()

    print(f"Rows loaded: {len(df):,}")
    print(f"Symbols:     {sorted(df['symbol'].dropna().unique().tolist())}")

    for symbol in SYMBOLS:
        plot_symbol_charts(symbol, df)

    print("-" * 90)
    print("[MASTER] Creating cross-symbol charts")

    plot_master_metric(
        df,
        metric="bar_count",
        bar_type="imbalance_25",
        title="Cross-Symbol Imbalance Bar Count - Threshold 25",
        ylabel="Bar Count",
        output_name="01_cross_symbol_imbalance_25_bar_count.png",
    )

    plot_master_metric(
        df,
        metric="avg_duration_seconds",
        bar_type="imbalance_25",
        title="Cross-Symbol Average Duration - Imbalance 25",
        ylabel="Average Duration (Seconds)",
        output_name="02_cross_symbol_imbalance_25_average_duration.png",
    )

    plot_master_metric(
        df,
        metric="return_std",
        bar_type="imbalance_25",
        title="Cross-Symbol Return Volatility - Imbalance 25",
        ylabel="Return Standard Deviation",
        output_name="03_cross_symbol_imbalance_25_return_volatility.png",
    )

    plot_master_metric(
        df,
        metric="positive_imbalance_pct",
        bar_type="imbalance_200",
        title="Cross-Symbol Positive Imbalance Percentage - Threshold 200",
        ylabel="Positive Imbalance (%)",
        output_name="04_cross_symbol_imbalance_200_positive_imbalance_pct.png",
    )

    write_master_summary(df)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol tick vs imbalance charts created.")
    print(f"Output root: {OUTPUT_ROOT}")
    print("=" * 90)


if __name__ == "__main__":
    main()