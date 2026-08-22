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
import sys
import numpy as np
import pandas as pd

DUKASCOPY_TICKS_DIR = Path(__file__).resolve().parents[1] / "dukascopy_ticks"
if str(DUKASCOPY_TICKS_DIR) not in sys.path:
    sys.path.insert(0, str(DUKASCOPY_TICKS_DIR))

from dukascopy_feature_contract import (  # noqa: E402
    FEATURE_ROLE_CONTRACT_VERSION,
    TARGET_CONTRACT_VERSION,
    feature_contract_fingerprint,
    require_predictor,
    require_target,
)
from dukascopy_contract import (  # noqa: E402
    SYMBOL_METADATA_SCHEMA_VERSION,
    registry_fingerprint,
)
from extended_horizons_e2_contract import candidate_contract_id  # noqa: E402


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
STABILITY_METHODOLOGY_VERSION = "extended_horizon_stability_integrity_e1_v1"
SELECTED_SIDE_METHOD = "higher_file_balanced_mean_directional_return_v1"
RANKING_INTERPRETATION = "exploratory_stability_ranking_index_not_independent_validation"


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
    coverage_path = DISCOVERY_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_coverage_latest.csv"

    if not coverage_path.exists():
        raise FileNotFoundError(f"Missing Script 02 coverage ledger: {coverage_path}")
    validate_discovery_coverage(pd.read_csv(coverage_path))

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 02 raw discovery report: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 02 raw discovery report is empty.")

    return df


def add_file_date_if_possible(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "file_date" in df.columns:
        values = df["file_date"].astype("string")
    elif "filename" in df.columns:
        values = df["filename"].astype(str).str.extract(r"(\d{4}[-_]\d{2}[-_]\d{2})")[0]
    else:
        raise ValueError("Temporal stability requires file_date or a dated filename")

    values = values.astype("string").str.replace("_", "-", regex=False)
    parsed = pd.to_datetime(values, format="%Y-%m-%d", errors="coerce")
    if parsed.isna().any():
        invalid = sorted(set(values[parsed.isna()].fillna("<missing>").astype(str)))
        raise ValueError(f"Unknown/missing file dates cannot be temporal evidence: {invalid}")
    df["file_date"] = parsed.dt.strftime("%Y-%m-%d")

    return df


def validate_discovery_coverage(coverage: pd.DataFrame) -> None:
    required = {"file", "status", "reason"}
    missing = sorted(required - set(coverage.columns))
    if missing:
        raise ValueError(f"Script 02 coverage ledger missing columns: {missing}")
    if coverage.empty:
        raise ValueError("Script 02 coverage ledger is empty")
    incomplete = coverage[coverage["status"] != "success"]
    if not incomplete.empty:
        details = incomplete[["file", "status", "reason"]].to_dict("records")
        raise ValueError(f"EH03 refuses incomplete Script 02 coverage: {details}")
    if coverage["file"].nunique() != len(coverage):
        raise ValueError("Script 02 coverage ledger contains duplicate file records")


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
        "long_positive_count",
        "short_positive_count",
        "long_return_sum",
        "short_return_sum",
        "long_count",
        "short_count",
        "expected_files",
        "input_coverage_status",
        "input_dataset_fingerprint",
        "lower_threshold",
        "upper_threshold",
        "threshold_learning_method",
        "discovery_methodology_version",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns from Script 02 report: {missing}")

    if not (df["input_coverage_status"] == "complete").all():
        raise ValueError("EH03 refuses incomplete Script 02 discovery evidence")

    for feature in df["feature"].dropna().unique():
        require_predictor(str(feature))
    targets = [str(value) for value in df["target"].dropna().unique()]
    for target in targets:
        require_target(target, approved_extra_targets=targets)

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
            file_balanced_mean_long_win_rate=("long_win_rate", "mean"),
            file_balanced_median_long_win_rate=("long_win_rate", "median"),
            file_balanced_mean_short_win_rate=("short_win_rate", "mean"),
            file_balanced_median_short_win_rate=("short_win_rate", "median"),
            file_balanced_mean_long_return=("long_avg_return", "mean"),
            file_balanced_median_long_return=("long_avg_return", "median"),
            file_balanced_mean_short_return=("short_avg_return", "mean"),
            file_balanced_median_short_return=("short_avg_return", "median"),
            total_long_count=("long_count", "sum"),
            total_short_count=("short_count", "sum"),
            total_long_positive_count=("long_positive_count", "sum"),
            total_short_positive_count=("short_positive_count", "sum"),
            total_long_return_sum=("long_return_sum", "sum"),
            total_short_return_sum=("short_return_sum", "sum"),
            expected_files=("expected_files", "max"),
            input_dataset_fingerprint=("input_dataset_fingerprint", "first"),
            median_lower_threshold=("lower_threshold", "median"),
            median_upper_threshold=("upper_threshold", "median"),
            threshold_learning_method=("threshold_learning_method", "first"),
            discovery_methodology_version=("discovery_methodology_version", "first"),
            discovery_interval_start=("file_date", "min"),
            discovery_interval_end=("file_date", "max"),
        )
        .reset_index()
    )

    if not (grouped["files_tested"] == grouped["expected_files"]).all():
        raise ValueError("EH03 input does not contain complete per-candidate file coverage")

    grouped["row_weighted_long_win_rate"] = (
        grouped["total_long_positive_count"] / grouped["total_long_count"].replace(0, np.nan)
    )
    grouped["row_weighted_short_win_rate"] = (
        grouped["total_short_positive_count"] / grouped["total_short_count"].replace(0, np.nan)
    )
    grouped["row_weighted_long_avg_return"] = (
        grouped["total_long_return_sum"] / grouped["total_long_count"].replace(0, np.nan)
    )
    grouped["row_weighted_short_avg_return"] = (
        grouped["total_short_return_sum"] / grouped["total_short_count"].replace(0, np.nan)
    )

    grouped["corr_direction_consistency"] = np.maximum(
        grouped["positive_corr_rate"],
        1.0 - grouped["positive_corr_rate"],
    )

    grouped["selected_side"] = np.where(
        grouped["file_balanced_mean_long_return"]
        >= grouped["file_balanced_mean_short_return"],
        "long",
        "short",
    )
    is_long = grouped["selected_side"] == "long"
    grouped["selected_file_balanced_mean_return"] = np.where(
        is_long, grouped["file_balanced_mean_long_return"], grouped["file_balanced_mean_short_return"]
    )
    grouped["selected_file_balanced_median_return"] = np.where(
        is_long, grouped["file_balanced_median_long_return"], grouped["file_balanced_median_short_return"]
    )
    grouped["selected_file_balanced_mean_win_rate"] = np.where(
        is_long, grouped["file_balanced_mean_long_win_rate"], grouped["file_balanced_mean_short_win_rate"]
    )
    grouped["selected_file_balanced_median_win_rate"] = np.where(
        is_long, grouped["file_balanced_median_long_win_rate"], grouped["file_balanced_median_short_win_rate"]
    )
    grouped["selected_row_weighted_avg_return"] = np.where(
        is_long, grouped["row_weighted_long_avg_return"], grouped["row_weighted_short_avg_return"]
    )
    grouped["selected_row_weighted_win_rate"] = np.where(
        is_long, grouped["row_weighted_long_win_rate"], grouped["row_weighted_short_win_rate"]
    )
    grouped["selected_count"] = np.where(is_long, grouped["total_long_count"], grouped["total_short_count"])

    # E2 freezes the numerical E1 rule. Each file-level Q25/Q75 was learned
    # from predictor values only; the median boundary is a deterministic
    # corpus-level rule and is never recomputed by EH04 evaluation data.
    grouped["threshold_quantile"] = np.where(is_long, 0.75, 0.25)
    grouped["threshold_side"] = np.where(is_long, "upper", "lower")
    grouped["threshold_operator"] = np.where(is_long, ">=", "<=")
    grouped["learned_threshold_value"] = np.where(
        is_long, grouped["median_upper_threshold"], grouped["median_lower_threshold"]
    )
    grouped["threshold_value"] = grouped["learned_threshold_value"]
    grouped["threshold_value_unit"] = "feature_native_numeric_units"
    grouped["threshold_provenance"] = "median_of_e1_file_feature_only_q25_q75"

    # EH04 compatibility aliases, now guaranteed to describe selected_side.
    grouped["best_side"] = grouped["selected_side"]
    grouped["best_mean_return"] = grouped["selected_file_balanced_mean_return"]
    grouped["best_median_return"] = grouped["selected_file_balanced_median_return"]
    grouped["best_mean_win_rate"] = grouped["selected_file_balanced_mean_win_rate"]
    grouped["best_median_win_rate"] = grouped["selected_file_balanced_median_win_rate"]
    grouped["best_total_count"] = grouped["selected_count"]
    grouped["mean_long_return"] = grouped["file_balanced_mean_long_return"]
    grouped["mean_short_return"] = grouped["file_balanced_mean_short_return"]
    grouped["mean_long_win_rate"] = grouped["file_balanced_mean_long_win_rate"]
    grouped["mean_short_win_rate"] = grouped["file_balanced_mean_short_win_rate"]

    grouped["return_stability_pass"] = grouped["best_median_return"] > 0
    grouped["win_rate_stability_pass"] = grouped["best_median_win_rate"] > 0.5
    grouped["sample_size_pass"] = (
        (grouped["files_tested"] >= MIN_FILES_TESTED)
        & (grouped["best_total_count"] >= MIN_TOTAL_SIDE_COUNT)
    )

    grouped["stability_ranking_index"] = (
        grouped["mean_abs_correlation"].fillna(0) * 10000
        + grouped["corr_direction_consistency"].fillna(0.5) * 10
        + (grouped["best_mean_win_rate"].fillna(0.5) - 0.5) * 20
        + grouped["best_mean_return"].fillna(0) * 100000
        + grouped["return_stability_pass"].astype(int) * 5
        + grouped["win_rate_stability_pass"].astype(int) * 5
        + grouped["sample_size_pass"].astype(int) * 5
    )
    grouped["stability_score"] = grouped["stability_ranking_index"]
    grouped["ranking_interpretation"] = RANKING_INTERPRETATION
    grouped["stability_methodology_version"] = STABILITY_METHODOLOGY_VERSION
    grouped["selected_side_method"] = SELECTED_SIDE_METHOD
    grouped["feature_role_contract_version"] = FEATURE_ROLE_CONTRACT_VERSION
    grouped["target_contract_version"] = TARGET_CONTRACT_VERSION
    grouped["feature_contract_fingerprint"] = feature_contract_fingerprint()
    grouped["input_coverage_status"] = "complete"
    grouped["symbol_metadata_schema_version"] = SYMBOL_METADATA_SCHEMA_VERSION
    grouped["symbol_registry_fingerprint"] = registry_fingerprint()
    grouped["candidate_contract_id"] = grouped.apply(
        lambda row: candidate_contract_id(row.to_dict()), axis=1
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
            "target",
            "feature",
        ],
        ascending=[True, False, False, False, False, True, True],
        kind="mergesort",
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
        f.write(f"Stability methodology: {STABILITY_METHODOLOGY_VERSION}\n")
        f.write(f"Selected side: {SELECTED_SIDE_METHOD}\n")
        f.write(f"Ranking interpretation: {RANKING_INTERPRETATION}\n")
        f.write("Status interpretation: exploratory discovery ranking class, not independent validation evidence\n\n")

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
