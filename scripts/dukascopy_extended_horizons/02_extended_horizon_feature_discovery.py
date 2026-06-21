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
import numpy as np
import pandas as pd


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
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    features = []
    for col in numeric_cols:
        col_lower = col.lower()

        if col in targets:
            continue

        if col_lower in EXCLUDE_COLUMNS:
            continue

        if col_lower.startswith("future_return_"):
            continue

        features.append(col)

    return features


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


def directional_stats(feature: pd.Series, target: pd.Series) -> dict:
    valid = feature.notna() & target.notna()

    if valid.sum() < 100:
        return {
            "long_count": 0,
            "short_count": 0,
            "long_win_rate": np.nan,
            "short_win_rate": np.nan,
            "long_avg_return": np.nan,
            "short_avg_return": np.nan,
        }

    f = feature[valid]
    t = target[valid]

    upper = f.quantile(0.75)
    lower = f.quantile(0.25)

    long_mask = f >= upper
    short_mask = f <= lower

    long_returns = t[long_mask]
    short_returns = -t[short_mask]

    return {
        "long_count": int(long_mask.sum()),
        "short_count": int(short_mask.sum()),
        "long_win_rate": float((long_returns > 0).mean()) if len(long_returns) else np.nan,
        "short_win_rate": float((short_returns > 0).mean()) if len(short_returns) else np.nan,
        "long_avg_return": float(long_returns.mean()) if len(long_returns) else np.nan,
        "short_avg_return": float(short_returns.mean()) if len(short_returns) else np.nan,
    }


def analyse_file(path: Path, targets: list[str]) -> list[dict]:
    df = pd.read_parquet(path)

    available_targets = [target for target in targets if target in df.columns]

    if not available_targets:
        return []

    features = get_numeric_feature_columns(df, available_targets)

    rows = []

    for target in available_targets:
        target_series = df[target]

        for feature in features:
            feature_series = df[feature]

            corr = safe_corr(feature_series, target_series)
            stats = directional_stats(feature_series, target_series)

            rows.append(
                {
                    "file": str(path),
                    "filename": path.name,
                    "target": target,
                    "feature": feature,
                    "rows": len(df),
                    "valid_target_rows": int(target_series.notna().sum()),
                    "correlation": corr,
                    "abs_correlation": abs(corr) if pd.notna(corr) else np.nan,
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
            mean_long_win_rate=("long_win_rate", "mean"),
            mean_short_win_rate=("short_win_rate", "mean"),
            mean_long_return=("long_avg_return", "mean"),
            mean_short_return=("short_avg_return", "mean"),
            total_long_count=("long_count", "sum"),
            total_short_count=("short_count", "sum"),
        )
        .reset_index()
    )

    grouped["best_side"] = np.where(
        grouped["mean_long_return"] >= grouped["mean_short_return"],
        "long",
        "short",
    )

    grouped["best_avg_return"] = grouped[["mean_long_return", "mean_short_return"]].max(axis=1)

    grouped["best_win_rate"] = grouped[["mean_long_win_rate", "mean_short_win_rate"]].max(axis=1)

    grouped["discovery_score"] = (
        grouped["mean_abs_correlation"].fillna(0) * 10000
        + grouped["best_avg_return"].fillna(0) * 100000
        + (grouped["best_win_rate"].fillna(0.5) - 0.5) * 10
    )

    grouped = grouped.sort_values(
        by=["discovery_score", "mean_abs_correlation", "best_avg_return"],
        ascending=False,
    )

    return grouped


def main(symbol: str, targets: list[str]) -> None:
    print_header(symbol, targets)

    files = find_files(symbol)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"Files found: {len(files)}")
    print("-" * 90)

    all_rows = []

    for idx, path in enumerate(files, start=1):
        try:
            rows = analyse_file(path, targets)
            all_rows.extend(rows)

            print(
                f"[OK] {idx:>4}/{len(files)} "
                f"rows_added={len(rows):>8} "
                f"file={path.name}"
            )

        except Exception as exc:
            print(f"[ERROR] {idx:>4}/{len(files)} {path.name} :: {exc}")

    results = pd.DataFrame(all_rows)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_ranked_latest.csv"
    top_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_top_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_feature_discovery_report_latest.txt"

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
        f.write(f"Files processed: {len(files)}\n")
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