"""
BACQE DUKASCOPY 26 - SIGNAL FORENSICS ENGINE

Purpose:
    Analyse validated signal candidates by robustness:
    - feature
    - target
    - side
    - year
    - month
    - positive day rate
    - profit factor stability
    - return consistency
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

TOP_N = 50


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def build_input_path(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_signal_validation"
        / f"symbol={symbol}"
        / "signal_results"
        / "signal_validation_daily_latest.csv"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_signal_forensics"
        / f"symbol={symbol}"
    )


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "signal_forensics",
        output_root / "top_robust_signals",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def extract_date_from_dataset(dataset: str) -> pd.Timestamp:
    parts = dataset.split("_")

    for part in parts:
        try:
            return pd.to_datetime(part, errors="raise")
        except Exception:
            continue

    return pd.NaT


def classify_signal(row: pd.Series) -> str:
    if (
        row["positive_day_rate"] >= 0.80
        and row["mean_profit_factor"] >= 1.25
        and row["year_consistency"] >= 0.67
    ):
        return "robust_candidate"

    if (
        row["positive_day_rate"] >= 0.70
        and row["mean_profit_factor"] >= 1.10
    ):
        return "research_candidate"

    if row["mean_profit_factor"] >= 1.00:
        return "weak_candidate"

    return "reject"


def run_signal_forensics(symbol: str = DEFAULT_SYMBOL) -> None:
    symbol = symbol.upper().strip()

    input_path = build_input_path(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 26 - SIGNAL FORENSICS ENGINE")

    ensure_dirs(output_root)

    print(f"Symbol:      {symbol}")
    print(f"Input path:  {input_path}")
    print(f"Output root: {output_root}")
    print("-" * 90)

    if not input_path.exists():
        print("[STOP] Missing Script 25 daily validation file.")
        return

    df = pd.read_csv(input_path)

    print(f"Loaded rows: {len(df):,}")

    required_cols = {
        "dataset",
        "feature",
        "target",
        "side",
        "signal_count",
        "win_rate",
        "mean_return",
        "total_return",
        "profit_factor",
        "sharpe_like",
    }

    missing = required_cols - set(df.columns)

    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    df["dataset_date"] = df["dataset"].apply(extract_date_from_dataset)
    df["year"] = df["dataset_date"].dt.year
    df["month"] = df["dataset_date"].dt.to_period("M").astype(str)

    df = df.dropna(subset=["dataset_date"])

    numeric_cols = [
        "signal_count",
        "win_rate",
        "mean_return",
        "total_return",
        "profit_factor",
        "sharpe_like",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)

    grouped = (
        df.groupby(["feature", "target", "side"], as_index=False)
        .agg(
            days_tested=("dataset", "nunique"),
            months_tested=("month", "nunique"),
            years_tested=("year", "nunique"),
            total_signals=("signal_count", "sum"),
            avg_daily_signals=("signal_count", "mean"),
            mean_win_rate=("win_rate", "mean"),
            median_win_rate=("win_rate", "median"),
            mean_return=("mean_return", "mean"),
            median_return=("mean_return", "median"),
            total_return=("total_return", "sum"),
            mean_profit_factor=("profit_factor", "mean"),
            median_profit_factor=("profit_factor", "median"),
            min_profit_factor=("profit_factor", "min"),
            max_profit_factor=("profit_factor", "max"),
            mean_sharpe_like=("sharpe_like", "mean"),
            positive_day_rate=("mean_return", lambda x: (x > 0).mean()),
            negative_day_rate=("mean_return", lambda x: (x < 0).mean()),
        )
    )

    yearly = (
        df.groupby(["feature", "target", "side", "year"], as_index=False)
        .agg(
            year_total_return=("total_return", "sum"),
            year_mean_return=("mean_return", "mean"),
            year_mean_profit_factor=("profit_factor", "mean"),
            year_positive_day_rate=("mean_return", lambda x: (x > 0).mean()),
        )
    )

    yearly_summary = (
        yearly.groupby(["feature", "target", "side"], as_index=False)
        .agg(
            profitable_years=("year_total_return", lambda x: (x > 0).sum()),
            tested_years=("year", "nunique"),
            min_year_return=("year_total_return", "min"),
            max_year_return=("year_total_return", "max"),
            mean_year_return=("year_total_return", "mean"),
        )
    )

    yearly_summary["year_consistency"] = (
        yearly_summary["profitable_years"] / yearly_summary["tested_years"]
    )

    grouped = grouped.merge(
        yearly_summary,
        on=["feature", "target", "side"],
        how="left",
    )

    monthly = (
        df.groupby(["feature", "target", "side", "month"], as_index=False)
        .agg(
            month_total_return=("total_return", "sum"),
            month_mean_return=("mean_return", "mean"),
            month_mean_profit_factor=("profit_factor", "mean"),
        )
    )

    monthly_summary = (
        monthly.groupby(["feature", "target", "side"], as_index=False)
        .agg(
            profitable_months=("month_total_return", lambda x: (x > 0).sum()),
            tested_months=("month", "nunique"),
            min_month_return=("month_total_return", "min"),
            max_month_return=("month_total_return", "max"),
            mean_month_return=("month_total_return", "mean"),
        )
    )

    monthly_summary["month_consistency"] = (
        monthly_summary["profitable_months"] / monthly_summary["tested_months"]
    )

    grouped = grouped.merge(
        monthly_summary,
        on=["feature", "target", "side"],
        how="left",
    )

    grouped["profit_factor_score"] = grouped["mean_profit_factor"].clip(0, 3) / 3
    grouped["positive_day_score"] = grouped["positive_day_rate"].fillna(0)
    grouped["month_score"] = grouped["month_consistency"].fillna(0)
    grouped["year_score"] = grouped["year_consistency"].fillna(0)

    grouped["return_score"] = grouped["mean_return"].clip(lower=0)
    max_return = grouped["return_score"].max()

    if pd.notna(max_return) and max_return != 0:
        grouped["return_score"] = grouped["return_score"] / max_return
    else:
        grouped["return_score"] = 0

    grouped["forensic_score"] = (
        grouped["profit_factor_score"] * 0.25
        + grouped["positive_day_score"] * 0.25
        + grouped["month_score"] * 0.20
        + grouped["year_score"] * 0.20
        + grouped["return_score"] * 0.10
    )

    grouped["forensic_label"] = grouped.apply(classify_signal, axis=1)

    grouped = grouped.sort_values("forensic_score", ascending=False)
    grouped.insert(0, "forensic_rank", range(1, len(grouped) + 1))

    top = grouped.head(TOP_N)

    output_all = output_root / "signal_forensics" / "signal_forensics_latest.csv"
    output_top = output_root / "top_robust_signals" / "top_robust_signals_latest.csv"
    output_report = output_root / "reports" / "signal_forensics_report_latest.txt"

    grouped.to_csv(output_all, index=False)
    top.to_csv(output_top, index=False)

    with open(output_report, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY SIGNAL FORENSICS REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input rows: {len(df):,}\n")
        f.write(f"Signals analysed: {len(grouped):,}\n\n")

        f.write("Forensic Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(grouped["forensic_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Robust Signal Candidates\n")
        f.write("-" * 80 + "\n")

        f.write(
            top[
                [
                    "forensic_rank",
                    "feature",
                    "target",
                    "side",
                    "days_tested",
                    "total_signals",
                    "mean_win_rate",
                    "mean_return",
                    "mean_profit_factor",
                    "positive_day_rate",
                    "month_consistency",
                    "year_consistency",
                    "forensic_label",
                    "forensic_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"All: {output_all}\n")
        f.write(f"Top: {output_top}\n")

    print("=" * 90)
    print("[DONE] Signal forensics complete.")
    print(f"All:    {output_all}")
    print(f"Top:    {output_top}")
    print(f"Report: {output_report}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy signal forensics."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_signal_forensics(symbol=args.symbol)


if __name__ == "__main__":
    main()