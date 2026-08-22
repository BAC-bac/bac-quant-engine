"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 02 - EXTENDED HORIZON FEATURE DISCOVERY

Purpose:
    Analyse extended horizon feature files and rank feature relationships
    against longer forward-return targets.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
from hashlib import sha256
import re
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
    predictor_columns,
    require_target,
)


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TARGETS = [
    "future_return_2500",
    "future_return_5000",
    "future_return_10000",
    "future_return_20000",
]

BASE_DIR = Path("E:/Quant_Lab")

INPUT_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"
REPORT_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "feature_discovery"

DISCOVERY_METHODOLOGY_VERSION = "extended_horizon_discovery_integrity_e1_v1"
THRESHOLD_LEARNING_METHOD = "per_file_predictor_only_q25_q75_v1"
SELECTED_SIDE_METHOD = "higher_file_balanced_mean_directional_return_v1"
RANKING_INTERPRETATION = "exploratory_discovery_ranking_index_not_independent_validation"
DATE_PATTERN = re.compile(r"(?<!\d)(\d{4}[-_]\d{2}[-_]\d{2})(?!\d)")

EXCLUDE_COLUMNS = {
    "time",
    "timestamp",
    "datetime",
    "date",
    "open",
    "high",
    "low",
    "close",
    "bid",
    "ask",
    "mid",
    "mid_price",
    "price",
    "volume",
    "tick_volume",
}


def print_header(symbol: str, targets: list[str]) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 02 - EXTENDED HORIZON FEATURE DISCOVERY")
    print("=" * 90)
    print(f"Symbol:      {symbol}")
    print(f"Targets:     {targets}")
    print(f"Input root:  {INPUT_ROOT}")
    print(f"Report root: {REPORT_ROOT}")
    print("-" * 90)


def find_files(symbol: str) -> list[Path]:
    symbol_root = INPUT_ROOT / f"symbol={symbol}"

    if not symbol_root.exists():
        raise FileNotFoundError(f"Input folder not found: {symbol_root}")

    files = sorted(symbol_root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found under: {symbol_root}")

    return files


def get_numeric_feature_columns(df: pd.DataFrame, targets: list[str]) -> list[str]:
    for target in targets:
        require_target(target, approved_extra_targets=targets)
    return predictor_columns(
        df,
        fail_on_unknown_numeric=True,
        approved_extra_targets=targets,
    )


def safe_corr(x: pd.Series, y: pd.Series) -> float:
    valid = x.notna() & y.notna()

    if valid.sum() < 100:
        return np.nan

    x_valid = x[valid]
    y_valid = y[valid]

    if x_valid.nunique(dropna=True) <= 1:
        return np.nan

    if y_valid.nunique(dropna=True) <= 1:
        return np.nan

    return float(x_valid.corr(y_valid))


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_file_date(path: Path) -> str:
    matches = sorted(set(DATE_PATTERN.findall(path.name)))
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one file date in {path.name!r}, found {matches}")
    value = matches[0].replace("_", "-")
    parsed = pd.to_datetime(value, format="%Y-%m-%d", errors="raise")
    return parsed.strftime("%Y-%m-%d")


def directional_stats(feature: pd.Series, target: pd.Series, min_evaluation_rows: int = 100) -> dict:
    eligible_feature = feature.replace([np.inf, -np.inf], np.nan).dropna()
    if eligible_feature.nunique(dropna=True) <= 1:
        return {
            "lower_threshold": np.nan,
            "upper_threshold": np.nan,
            "long_count": 0,
            "short_count": 0,
            "long_positive_count": 0,
            "short_positive_count": 0,
            "long_return_sum": 0.0,
            "short_return_sum": 0.0,
            "long_win_rate": np.nan,
            "short_win_rate": np.nan,
            "long_avg_return": np.nan,
            "short_avg_return": np.nan,
            "long_median_return": np.nan,
            "short_median_return": np.nan,
        }

    # E1 invariant: thresholds use the predictor population only. Target values
    # and target-null locations cannot influence these numerical definitions.
    upper = float(eligible_feature.quantile(0.75))
    lower = float(eligible_feature.quantile(0.25))
    evaluable = feature.notna() & target.notna() & np.isfinite(feature) & np.isfinite(target)
    long_returns = target[evaluable & (feature >= upper)]
    short_returns = -target[evaluable & (feature <= lower)]

    if int(evaluable.sum()) < min_evaluation_rows:
        long_returns = long_returns.iloc[0:0]
        short_returns = short_returns.iloc[0:0]

    return {
        "lower_threshold": lower,
        "upper_threshold": upper,
        "long_count": int(len(long_returns)),
        "short_count": int(len(short_returns)),
        "long_positive_count": int((long_returns > 0).sum()),
        "short_positive_count": int((short_returns > 0).sum()),
        "long_return_sum": float(long_returns.sum()),
        "short_return_sum": float(short_returns.sum()),
        "long_win_rate": float((long_returns > 0).mean()) if len(long_returns) else np.nan,
        "short_win_rate": float((short_returns > 0).mean()) if len(short_returns) else np.nan,
        "long_avg_return": float(long_returns.mean()) if len(long_returns) else np.nan,
        "short_avg_return": float(short_returns.mean()) if len(short_returns) else np.nan,
        "long_median_return": float(long_returns.median()) if len(long_returns) else np.nan,
        "short_median_return": float(short_returns.median()) if len(short_returns) else np.nan,
    }


def analyse_file(path: Path, targets: list[str]) -> list[dict]:
    df = pd.read_parquet(path)
    file_date = extract_file_date(path)
    fingerprint = file_sha256(path)

    missing_targets = [target for target in targets if target not in df.columns]
    if missing_targets:
        raise ValueError(f"Missing approved target columns: {missing_targets}")
    for target in targets:
        require_target(target, approved_extra_targets=targets)

    features = get_numeric_feature_columns(df, targets)
    if not features:
        raise ValueError("No registered predictor_causal columns available")

    rows = []

    for target in targets:
        target_series = df[target]

        for feature in features:
            feature_series = df[feature]

            corr = safe_corr(feature_series, target_series)
            stats = directional_stats(feature_series, target_series)

            rows.append(
                {
                    "file": str(path),
                    "filename": path.name,
                    "file_date": file_date,
                    "input_file_sha256": fingerprint,
                    "target": target,
                    "feature": feature,
                    "rows": len(df),
                    "valid_target_rows": int(target_series.notna().sum()),
                    "correlation": corr,
                    "abs_correlation": abs(corr) if pd.notna(corr) else np.nan,
                    "threshold_learning_method": THRESHOLD_LEARNING_METHOD,
                    "selected_side_method": SELECTED_SIDE_METHOD,
                    "discovery_methodology_version": DISCOVERY_METHODOLOGY_VERSION,
                    "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
                    "target_contract_version": TARGET_CONTRACT_VERSION,
                    "feature_contract_fingerprint": feature_contract_fingerprint(),
                    **stats,
                }
            )

    return rows


def rank_results(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results

    grouped = (
        results.groupby(["target", "feature"], dropna=False)
        .agg(
            files_tested=("file", "nunique"),
            total_rows=("rows", "sum"),
            total_valid_target_rows=("valid_target_rows", "sum"),
            mean_correlation=("correlation", "mean"),
            median_correlation=("correlation", "median"),
            mean_abs_correlation=("abs_correlation", "mean"),
            max_abs_correlation=("abs_correlation", "max"),
            file_balanced_mean_long_win_rate=("long_win_rate", "mean"),
            file_balanced_mean_short_win_rate=("short_win_rate", "mean"),
            file_balanced_mean_long_return=("long_avg_return", "mean"),
            file_balanced_mean_short_return=("short_avg_return", "mean"),
            file_balanced_median_long_return=("long_avg_return", "median"),
            file_balanced_median_short_return=("short_avg_return", "median"),
            total_long_count=("long_count", "sum"),
            total_short_count=("short_count", "sum"),
            total_long_positive_count=("long_positive_count", "sum"),
            total_short_positive_count=("short_positive_count", "sum"),
            total_long_return_sum=("long_return_sum", "sum"),
            total_short_return_sum=("short_return_sum", "sum"),
            expected_files=("expected_files", "max"),
        )
        .reset_index()
    )

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

    grouped["selected_side"] = np.where(
        grouped["file_balanced_mean_long_return"]
        >= grouped["file_balanced_mean_short_return"],
        "long",
        "short",
    )
    is_long = grouped["selected_side"] == "long"
    grouped["selected_count"] = np.where(is_long, grouped["total_long_count"], grouped["total_short_count"])
    grouped["selected_file_balanced_mean_return"] = np.where(
        is_long, grouped["file_balanced_mean_long_return"], grouped["file_balanced_mean_short_return"]
    )
    grouped["selected_file_balanced_median_return"] = np.where(
        is_long, grouped["file_balanced_median_long_return"], grouped["file_balanced_median_short_return"]
    )
    grouped["selected_file_balanced_mean_win_rate"] = np.where(
        is_long, grouped["file_balanced_mean_long_win_rate"], grouped["file_balanced_mean_short_win_rate"]
    )
    grouped["selected_row_weighted_avg_return"] = np.where(
        is_long, grouped["row_weighted_long_avg_return"], grouped["row_weighted_short_avg_return"]
    )
    grouped["selected_row_weighted_win_rate"] = np.where(
        is_long, grouped["row_weighted_long_win_rate"], grouped["row_weighted_short_win_rate"]
    )

    # Backward-compatible aliases consumed by EH03/EH04. They are selected-side
    # values, never independent maxima across long and short.
    grouped["best_side"] = grouped["selected_side"]
    grouped["best_avg_return"] = grouped["selected_file_balanced_mean_return"]
    grouped["best_win_rate"] = grouped["selected_file_balanced_mean_win_rate"]
    grouped["mean_long_return"] = grouped["file_balanced_mean_long_return"]
    grouped["mean_short_return"] = grouped["file_balanced_mean_short_return"]
    grouped["mean_long_win_rate"] = grouped["file_balanced_mean_long_win_rate"]
    grouped["mean_short_win_rate"] = grouped["file_balanced_mean_short_win_rate"]

    grouped["discovery_ranking_index"] = (
        grouped["mean_abs_correlation"].fillna(0) * 10000
        + grouped["best_avg_return"].fillna(0) * 100000
        + (grouped["best_win_rate"].fillna(0.5) - 0.5) * 10
    )
    grouped["discovery_score"] = grouped["discovery_ranking_index"]
    grouped["ranking_interpretation"] = RANKING_INTERPRETATION
    grouped["discovery_methodology_version"] = DISCOVERY_METHODOLOGY_VERSION
    grouped["threshold_learning_method"] = THRESHOLD_LEARNING_METHOD
    grouped["selected_side_method"] = SELECTED_SIDE_METHOD
    grouped["input_coverage_status"] = "complete"

    grouped = grouped.sort_values(
        by=["discovery_ranking_index", "mean_abs_correlation", "best_avg_return", "target", "feature"],
        ascending=[False, False, False, True, True],
        kind="mergesort",
    )

    return grouped


def process_files(files: list[Path], targets: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_rows: list[dict] = []
    coverage_rows: list[dict] = []

    for idx, path in enumerate(files, start=1):
        try:
            rows = analyse_file(path, targets)
            if not rows:
                raise ValueError("No discovery rows produced")
            all_rows.extend(rows)
            coverage_rows.append({
                "file": str(path),
                "filename": path.name,
                "file_date": extract_file_date(path),
                "status": "success",
                "reason": "",
                "rows_added": len(rows),
            })
            print(f"[OK] {idx:>4}/{len(files)} rows_added={len(rows):>8} file={path.name}")
        except Exception as exc:
            coverage_rows.append({
                "file": str(path),
                "filename": path.name,
                "file_date": "",
                "status": "failed",
                "reason": str(exc),
                "rows_added": 0,
            })
            print(f"[ERROR] {idx:>4}/{len(files)} {path.name} :: {exc}")

    results = pd.DataFrame(all_rows)
    if not results.empty:
        fingerprints = sorted(results["input_file_sha256"].dropna().astype(str).unique())
        dataset_fingerprint = sha256("\n".join(fingerprints).encode("utf-8")).hexdigest()
        results["input_dataset_fingerprint"] = dataset_fingerprint
        results["expected_files"] = len(files)
        results["attempted_files"] = len(coverage_rows)
        results["successfully_processed_files"] = sum(
            row["status"] == "success" for row in coverage_rows
        )
        results["failed_files"] = sum(row["status"] == "failed" for row in coverage_rows)
        results["skipped_files"] = sum(row["status"] == "skipped" for row in coverage_rows)
        results["input_coverage_status"] = np.where(
            results["successfully_processed_files"] == len(files), "complete", "incomplete"
        )
    return results, pd.DataFrame(coverage_rows)


def main(symbol: str, targets: list[str]) -> dict:
    print_header(symbol, targets)

    files = find_files(symbol)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"Files found: {len(files)}")
    print("-" * 90)

    results, coverage = process_files(files, targets)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_ranked_latest.csv"
    top_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_top_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_report_latest.txt"
    coverage_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_coverage_latest.csv"

    coverage.to_csv(coverage_path, index=False)
    success_count = int((coverage["status"] == "success").sum())
    failed_count = int((coverage["status"] == "failed").sum())
    skipped_count = int((coverage["status"] == "skipped").sum())
    coverage_complete = success_count == len(files) and failed_count == skipped_count == 0

    if not coverage_complete:
        diagnostic_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_raw_incomplete_latest.csv"
        results.to_csv(diagnostic_path, index=False)
        print("[STOP] Incomplete coverage; normal discovery/ranking outputs were not written.")
        return {
            "status": "incomplete_coverage",
            "expected_files": len(files),
            "attempted_files": len(coverage),
            "successfully_processed_files": success_count,
            "failed_files": failed_count,
            "skipped_files": skipped_count,
            "coverage_rate": success_count / len(files),
            "coverage_path": str(coverage_path),
            "diagnostic_path": str(diagnostic_path),
        }

    results.to_csv(raw_path, index=False)

    ranked = rank_results(results)
    ranked.to_csv(ranked_path, index=False)

    top = ranked.head(250)
    top.to_csv(top_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 02 - EXTENDED HORIZON FEATURE DISCOVERY REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Expected files: {len(files)}\n")
        f.write(f"Successfully processed files: {success_count}\n")
        f.write("Input coverage status: complete\n")
        f.write(f"Discovery methodology: {DISCOVERY_METHODOLOGY_VERSION}\n")
        f.write(f"Threshold learning: {THRESHOLD_LEARNING_METHOD}\n")
        f.write(f"Selected side: {SELECTED_SIDE_METHOD}\n")
        f.write(f"Ranking interpretation: {RANKING_INTERPRETATION}\n")
        f.write(f"Raw rows: {len(results)}\n")
        f.write(f"Ranked feature-target pairs: {len(ranked)}\n\n")

        f.write("TOP 50 FEATURE / TARGET PAIRS\n")
        f.write("-" * 90 + "\n")

        if ranked.empty:
            f.write("No ranked results produced.\n")
        else:
            cols = [
                "target",
                "feature",
                "best_side",
                "discovery_score",
                "mean_abs_correlation",
                "best_win_rate",
                "best_avg_return",
                "files_tested",
            ]
            f.write(ranked[cols].head(50).to_string(index=False))

    print("-" * 90)
    print("[DONE] Extended horizon feature discovery complete")
    print(f"Raw report:    {raw_path}")
    print(f"Ranked report: {ranked_path}")
    print(f"Top report:    {top_path}")
    print(f"Text report:   {txt_path}")
    print("=" * 90)
    return {
        "status": "ok",
        "expected_files": len(files),
        "attempted_files": len(coverage),
        "successfully_processed_files": success_count,
        "failed_files": 0,
        "skipped_files": 0,
        "coverage_rate": 1.0,
        "earliest_processed_date": coverage["file_date"].min(),
        "latest_processed_date": coverage["file_date"].max(),
        "raw_path": str(raw_path),
        "ranked_path": str(ranked_path),
        "top_path": str(top_path),
        "coverage_path": str(coverage_path),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol to process, e.g. EURJPY",
    )

    parser.add_argument(
        "--targets",
        nargs="+",
        default=DEFAULT_TARGETS,
        help="Target columns to analyse",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol.upper(),
        targets=args.targets,
    )
