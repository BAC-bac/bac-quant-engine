"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 03 - EXTENDED HORIZON STABILITY ENGINE

Purpose:
    Evaluate whether feature/target relationships from Script 02 are stable
    across files/days, rather than being caused by isolated outliers.

Input:
    Script 02 raw discovery report.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"

BASE_DIR = Path("E:/Quant_Lab")

DISCOVERY_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "feature_discovery"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "feature_stability"
)

MIN_FILES_TESTED = 20
MIN_TOTAL_SIDE_COUNT = 1_000


def print_header(symbol: str) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 03 - EXTENDED HORIZON STABILITY ENGINE")
    print("=" * 90)
    print(f"Symbol:         {symbol}")
    print(f"Discovery root: {DISCOVERY_ROOT}")
    print(f"Report root:    {REPORT_ROOT}")
    print("-" * 90)


def load_discovery_raw(symbol: str) -> pd.DataFrame:
    path = DISCOVERY_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_raw_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 02 raw discovery report: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 02 raw discovery report is empty.")

    return df


def add_file_date_if_possible(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "filename" not in df.columns:
        df["file_date"] = "unknown"
        return df

    extracted = df["filename"].astype(str).str.extract(r"(\d{4}[-_]\d{2}[-_]\d{2})")[0]

    df["file_date"] = (
        extracted
        .fillna("unknown")
        .astype(str)
        .str.replace("_", "-", regex=False)
    )

    return df


def safe_positive_rate(series: pd.Series) -> float:
    valid = series.dropna()

    if len(valid) == 0:
        return np.nan

    return float((valid > 0).mean())


def calculate_stability(df: pd.DataFrame) -> pd.DataFrame:
    df = add_file_date_if_possible(df)

    required_cols = [
        "target",
        "feature",
        "correlation",
        "abs_correlation",
        "long_win_rate",
        "short_win_rate",
        "long_avg_return",
        "short_avg_return",
        "long_count",
        "short_count",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns from Script 02 report: {missing}")

    grouped = (
        df.groupby(["target", "feature"], dropna=False)
        .agg(
            files_tested=("file", "nunique"),
            dates_tested=("file_date", "nunique"),
            total_rows=("rows", "sum"),
            total_valid_target_rows=("valid_target_rows", "sum"),
            mean_correlation=("correlation", "mean"),
            median_correlation=("correlation", "median"),
            std_correlation=("correlation", "std"),
            mean_abs_correlation=("abs_correlation", "mean"),
            median_abs_correlation=("abs_correlation", "median"),
            max_abs_correlation=("abs_correlation", "max"),
            positive_corr_rate=("correlation", safe_positive_rate),
            mean_long_win_rate=("long_win_rate", "mean"),
            median_long_win_rate=("long_win_rate", "median"),
            mean_short_win_rate=("short_win_rate", "mean"),
            median_short_win_rate=("short_win_rate", "median"),
            mean_long_return=("long_avg_return", "mean"),
            median_long_return=("long_avg_return", "median"),
            mean_short_return=("short_avg_return", "mean"),
            median_short_return=("short_avg_return", "median"),
            total_long_count=("long_count", "sum"),
            total_short_count=("short_count", "sum"),
        )
        .reset_index()
    )

    grouped["corr_direction_consistency"] = np.maximum(
        grouped["positive_corr_rate"],
        1.0 - grouped["positive_corr_rate"],
    )

    grouped["best_side"] = np.where(
        grouped["mean_long_return"] >= grouped["mean_short_return"],
        "long",
        "short",
    )

    grouped["best_mean_return"] = grouped[["mean_long_return", "mean_short_return"]].max(axis=1)
    grouped["best_median_return"] = grouped[["median_long_return", "median_short_return"]].max(axis=1)
    grouped["best_mean_win_rate"] = grouped[["mean_long_win_rate", "mean_short_win_rate"]].max(axis=1)
    grouped["best_median_win_rate"] = grouped[["median_long_win_rate", "median_short_win_rate"]].max(axis=1)
    grouped["best_total_count"] = np.where(
        grouped["best_side"] == "long",
        grouped["total_long_count"],
        grouped["total_short_count"],
    )

    grouped["return_stability_pass"] = grouped["best_median_return"] > 0
    grouped["win_rate_stability_pass"] = grouped["best_median_win_rate"] > 0.5
    grouped["sample_size_pass"] = (
        (grouped["files_tested"] >= MIN_FILES_TESTED)
        & (grouped["best_total_count"] >= MIN_TOTAL_SIDE_COUNT)
    )

    grouped["stability_score"] = (
        grouped["mean_abs_correlation"].fillna(0) * 10000
        + grouped["corr_direction_consistency"].fillna(0.5) * 10
        + (grouped["best_mean_win_rate"].fillna(0.5) - 0.5) * 20
        + grouped["best_mean_return"].fillna(0) * 100000
        + grouped["return_stability_pass"].astype(int) * 5
        + grouped["win_rate_stability_pass"].astype(int) * 5
        + grouped["sample_size_pass"].astype(int) * 5
    )

    grouped["stability_status"] = np.select(
        [
            grouped["sample_size_pass"]
            & grouped["return_stability_pass"]
            & grouped["win_rate_stability_pass"]
            & (grouped["corr_direction_consistency"] >= 0.60),

            grouped["sample_size_pass"]
            & (
                grouped["return_stability_pass"]
                | grouped["win_rate_stability_pass"]
            ),

            ~grouped["sample_size_pass"],
        ],
        [
            "stable_candidate",
            "watchlist_candidate",
            "insufficient_sample",
        ],
        default="weak_or_unstable",
    )

    grouped = grouped.sort_values(
        by=[
            "stability_status",
            "stability_score",
            "best_mean_return",
            "best_mean_win_rate",
            "mean_abs_correlation",
        ],
        ascending=[True, False, False, False, False],
    )

    return grouped


def write_report(symbol: str, stability: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    full_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_stability_latest.csv"
    candidates_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_stable_candidates_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_stability_report_latest.txt"

    stability.to_csv(full_path, index=False)

    candidates = stability[
        stability["stability_status"].isin(
            ["stable_candidate", "watchlist_candidate"]
        )
    ].copy()

    candidates.to_csv(candidates_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 03 - EXTENDED HORIZON STABILITY REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Total feature/target pairs: {len(stability)}\n")
        f.write(f"Stable/watchlist candidates: {len(candidates)}\n\n")

        f.write("STATUS COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(stability["stability_status"].value_counts().to_string())
        f.write("\n\n")

        f.write("TOP 50 STABILITY CANDIDATES\n")
        f.write("-" * 90 + "\n")

        display_cols = [
            "target",
            "feature",
            "best_side",
            "stability_status",
            "stability_score",
            "mean_abs_correlation",
            "corr_direction_consistency",
            "best_mean_win_rate",
            "best_median_win_rate",
            "best_mean_return",
            "best_median_return",
            "files_tested",
            "best_total_count",
        ]

        if stability.empty:
            f.write("No stability results produced.\n")
        else:
            f.write(stability[display_cols].head(50).to_string(index=False))

    print(f"Full stability report: {full_path}")
    print(f"Candidate report:      {candidates_path}")
    print(f"Text report:           {txt_path}")


def main(symbol: str) -> None:
    print_header(symbol)

    raw = load_discovery_raw(symbol)

    print(f"Raw discovery rows loaded: {len(raw):,}")
    print("-" * 90)

    stability = calculate_stability(raw)

    print(f"Feature/target pairs analysed: {len(stability):,}")
    print("Status counts:")
    print(stability["stability_status"].value_counts())
    print("-" * 90)

    write_report(symbol, stability)

    print("-" * 90)
    print("[DONE] Extended horizon stability engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol to process, e.g. EURJPY",
    )

    args = parser.parse_args()

    main(symbol=args.symbol.upper())