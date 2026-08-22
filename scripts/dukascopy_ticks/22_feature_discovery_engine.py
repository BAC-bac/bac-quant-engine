"""
BACQE DUKASCOPY 22 - FEATURE DISCOVERY ENGINE

Purpose:
    Discover which Dukascopy-derived features have predictive relationships
    with future returns for a selected symbol.

Refactor note:
    This script can still be run standalone, but now also exposes
    run_feature_discovery() so other BACQE scripts can call it for any symbol.
"""

from pathlib import Path
import argparse
import warnings

import numpy as np
import pandas as pd

from dukascopy_feature_contract import (
    APPROVED_TARGET_PRICE_BASIS,
    FeatureContractError,
    column_spec,
    predictor_columns,
    require_target,
)

warnings.filterwarnings("ignore")

try:
    from scipy.stats import spearmanr
except ImportError:
    spearmanr = None

try:
    from sklearn.feature_selection import mutual_info_regression
except ImportError:
    mutual_info_regression = None


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

FORWARD_WINDOWS = [1, 3, 5, 10, 20]
RUN_MUTUAL_INFO = False
MAX_ROWS_PER_DATASET = 10_000
MIN_VALID_ROWS = 1_000

EXCLUDE_COLS = {
    "timestamp",
    "timestamp_utc",
    "datetime",
    "date",
    "time",
    "symbol",
    "source",
    "bid",
    "ask",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "tick_volume",
    "real_volume",
    "spread",
}


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def build_paths(symbol: str) -> tuple[Path, Path]:
    symbol = symbol.upper().strip()

    input_root = (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_engineered_features"
        / f"symbol={symbol}"
    )

    report_root = (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_feature_discovery"
        / f"symbol={symbol}"
    )

    return input_root, report_root


def ensure_dirs(report_root: Path) -> None:
    for folder in [
        report_root,
        report_root / "feature_inventory",
        report_root / "feature_scores",
        report_root / "feature_rankings",
        report_root / "feature_stability",
        report_root / "reports",
        report_root / "top_features",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_files(input_root: Path) -> list[Path]:
    if not input_root.exists():
        print(f"[MISSING INPUT ROOT] {input_root}")
        return []

    files = sorted(input_root.rglob("*.parquet"))

    print(f"Input root: {input_root}")
    print(f"Engineered parquet files found: {len(files)}")

    return files


def load_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")

    if len(df) > MAX_ROWS_PER_DATASET:
        df = df.sample(MAX_ROWS_PER_DATASET, random_state=42).sort_index()

    return df


def find_timestamp_col(df: pd.DataFrame) -> str | None:
    candidates = ["timestamp_utc", "timestamp", "datetime", "date_time", "time"]

    for col in candidates:
        if col in df.columns:
            return col

    for col in df.columns:
        if "time" in col.lower() or "date" in col.lower():
            return col

    return None


def find_price_col(df: pd.DataFrame) -> str | None:
    if (
        APPROVED_TARGET_PRICE_BASIS in df.columns
        and pd.api.types.is_numeric_dtype(df[APPROVED_TARGET_PRICE_BASIS])
    ):
        return APPROVED_TARGET_PRICE_BASIS
    return None


def create_forward_returns(df: pd.DataFrame, price_col: str) -> pd.DataFrame:
    df = df.copy()

    for window in FORWARD_WINDOWS:
        target = f"future_return_{window}"
        require_target(target, approved_extra_targets=[target])
        df[target] = (
            df[price_col].shift(-window) / df[price_col] - 1
        )

    return df


def build_feature_inventory(df: pd.DataFrame, dataset_name: str, symbol: str) -> pd.DataFrame:
    rows = []

    for col in df.columns:
        try:
            role = column_spec(
                col,
                approved_extra_targets=[f"future_return_{window}" for window in FORWARD_WINDOWS],
            ).role
        except FeatureContractError:
            role = "unregistered"
        rows.append({
            "symbol": symbol,
            "dataset": dataset_name,
            "feature": col,
            "dtype": str(df[col].dtype),
            "rows": len(df),
            "missing_pct": round(df[col].isna().mean() * 100, 4),
            "unique_values": df[col].nunique(dropna=True),
            "is_numeric": pd.api.types.is_numeric_dtype(df[col]),
            "feature_role": role,
        })

    return pd.DataFrame(rows)


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    registered = predictor_columns(
        df,
        fail_on_unknown_numeric=True,
        approved_extra_targets=[f"future_return_{window}" for window in FORWARD_WINDOWS],
    )
    return [col for col in registered if df[col].nunique(dropna=True) >= 5]


def calculate_spearman(x: pd.Series, y: pd.Series) -> tuple[float, float]:
    valid = pd.concat([x, y], axis=1).dropna()

    if len(valid) < MIN_VALID_ROWS:
        return np.nan, np.nan

    if spearmanr is not None:
        corr, pvalue = spearmanr(valid.iloc[:, 0], valid.iloc[:, 1])
        return corr, pvalue

    corr = valid.iloc[:, 0].corr(valid.iloc[:, 1], method="spearman")
    return corr, np.nan


def calculate_mutual_info(x: pd.Series, y: pd.Series) -> float:
    if not RUN_MUTUAL_INFO:
        return np.nan

    valid = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()

    if len(valid) < MIN_VALID_ROWS:
        return np.nan

    sample_size = min(len(valid), 50_000)
    valid = valid.sample(sample_size, random_state=42)

    x_values = valid.iloc[:, [0]].values
    target = valid.iloc[:, 1].values

    try:
        mi = mutual_info_regression(x_values, target, random_state=42)
        return float(mi[0])
    except Exception:
        return np.nan


def score_features(df: pd.DataFrame, dataset_name: str, symbol: str) -> pd.DataFrame:
    feature_cols = get_feature_columns(df)
    target_cols = [
        f"future_return_{window}"
        for window in FORWARD_WINDOWS
        if f"future_return_{window}" in df.columns
    ]
    for target in target_cols:
        require_target(target, approved_extra_targets=target_cols)

    rows = []

    for feature in feature_cols:
        for target in target_cols:
            corr, pvalue = calculate_spearman(df[feature], df[target])
            mi = calculate_mutual_info(df[feature], df[target])

            rows.append({
                "symbol": symbol,
                "dataset": dataset_name,
                "feature": feature,
                "target": target,
                "spearman": corr,
                "abs_spearman": abs(corr) if pd.notna(corr) else np.nan,
                "pvalue": pvalue,
                "mutual_info": mi,
                "valid_rows": pd.concat([df[feature], df[target]], axis=1).dropna().shape[0],
            })

    return pd.DataFrame(rows)


def calculate_stability(
    df: pd.DataFrame,
    dataset_name: str,
    symbol: str,
    timestamp_col: str | None,
) -> pd.DataFrame:
    if timestamp_col is None:
        return pd.DataFrame()

    temp = df.copy()
    temp[timestamp_col] = pd.to_datetime(temp[timestamp_col], errors="coerce")
    temp = temp.dropna(subset=[timestamp_col])
    temp["year"] = temp[timestamp_col].dt.year

    rows = []

    for year, year_df in temp.groupby("year"):
        if len(year_df) < MIN_VALID_ROWS:
            continue

        score_df = score_features(year_df, dataset_name, symbol)

        if score_df.empty:
            continue

        score_df["year"] = year
        rows.append(score_df)

    if not rows:
        return pd.DataFrame()

    yearly = pd.concat(rows, ignore_index=True)

    stability = (
        yearly
        .groupby(["symbol", "dataset", "feature", "target"], as_index=False)
        .agg(
            yearly_mean_abs_spearman=("abs_spearman", "mean"),
            yearly_std_abs_spearman=("abs_spearman", "std"),
            years_tested=("year", "nunique"),
        )
    )

    stability["stability_score"] = (
        stability["yearly_mean_abs_spearman"]
        / stability["yearly_std_abs_spearman"].replace(0, np.nan)
    )

    return stability


def build_final_rankings(scores: pd.DataFrame, stability: pd.DataFrame) -> pd.DataFrame:
    rankings = scores.copy()

    if not stability.empty:
        rankings = rankings.merge(
            stability[
                [
                    "symbol",
                    "dataset",
                    "feature",
                    "target",
                    "stability_score",
                    "years_tested",
                ]
            ],
            on=["symbol", "dataset", "feature", "target"],
            how="left",
        )
    else:
        rankings["stability_score"] = np.nan
        rankings["years_tested"] = np.nan

    for col in ["abs_spearman", "mutual_info", "stability_score"]:
        max_val = rankings[col].replace([np.inf, -np.inf], np.nan).max()
        if pd.notna(max_val) and max_val != 0:
            rankings[f"{col}_norm"] = rankings[col] / max_val
        else:
            rankings[f"{col}_norm"] = 0

    rankings["feature_discovery_score"] = (
        rankings["abs_spearman_norm"].fillna(0) * 0.45
        + rankings["mutual_info_norm"].fillna(0) * 0.35
        + rankings["stability_score_norm"].fillna(0) * 0.20
    )

    rankings = rankings.sort_values("feature_discovery_score", ascending=False)
    rankings.insert(0, "rank", range(1, len(rankings) + 1))

    return rankings


def write_report(
    symbol: str,
    report_root: Path,
    discovered_files: list[Path],
    inventory: pd.DataFrame,
    scores: pd.DataFrame,
    rankings: pd.DataFrame,
    stability: pd.DataFrame,
) -> Path:
    report_path = report_root / "reports" / f"{symbol}_feature_discovery_report_latest.txt"

    top = rankings.head(25)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY FEATURE DISCOVERY REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input files discovered: {len(discovered_files)}\n")
        f.write(f"Inventory rows: {len(inventory)}\n")
        f.write(f"Score rows: {len(scores)}\n")
        f.write(f"Stability rows: {len(stability)}\n")
        f.write(f"Ranking rows: {len(rankings)}\n\n")

        f.write("Top 25 Feature / Target Combinations\n")
        f.write("-" * 80 + "\n")

        if top.empty:
            f.write("No ranked features produced.\n")
        else:
            f.write(
                top[
                    [
                        "rank",
                        "symbol",
                        "dataset",
                        "feature",
                        "target",
                        "spearman",
                        "mutual_info",
                        "stability_score",
                        "feature_discovery_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nReport root:\n")
        f.write(str(report_root))

    return report_path


def run_feature_discovery(symbol: str = DEFAULT_SYMBOL) -> dict:
    symbol = symbol.upper().strip()
    input_root, report_root = build_paths(symbol)

    banner("BACQE DUKASCOPY 22 - FEATURE DISCOVERY ENGINE")

    ensure_dirs(report_root)

    print(f"Symbol:      {symbol}")
    print(f"Input root:  {input_root}")
    print(f"Report root: {report_root}")
    print("-" * 90)

    discovered_files = discover_files(input_root)

    print(f"Discovered files: {len(discovered_files)}")

    if not discovered_files:
        print("[STOP] No input files discovered.")
        return {
            "status": "no_input_files",
            "symbol": symbol,
            "input_root": str(input_root),
            "report_root": str(report_root),
        }

    all_inventory = []
    all_scores = []
    all_stability = []

    for i, path in enumerate(discovered_files, start=1):
        dataset_name = path.stem

        print("-" * 90)
        print(f"[{i}/{len(discovered_files)}] {path}")

        try:
            df = load_dataset(path)

            if df.empty:
                print("[SKIP] Empty dataset.")
                continue

            timestamp_col = find_timestamp_col(df)
            price_col = find_price_col(df)

            if price_col is None:
                print("[SKIP] No usable price column found.")
                continue

            if timestamp_col:
                df = df.sort_values(timestamp_col)

            df = create_forward_returns(df, price_col)

            inventory = build_feature_inventory(df, dataset_name, symbol)
            scores = score_features(df, dataset_name, symbol)
            stability = calculate_stability(df, dataset_name, symbol, timestamp_col)

            all_inventory.append(inventory)

            if not scores.empty:
                all_scores.append(scores)

            if not stability.empty:
                all_stability.append(stability)

            print(f"Rows:        {len(df):,}")
            print(f"Price col:   {price_col}")
            print(f"Time col:    {timestamp_col}")
            print(f"Features:    {len(get_feature_columns(df))}")
            print(f"Score rows:  {len(scores)}")

        except Exception as e:
            print(f"[ERROR] {path}")
            print(f"        {e}")

    if not all_inventory:
        print("[STOP] No inventory generated.")
        return {
            "status": "no_inventory",
            "symbol": symbol,
            "input_root": str(input_root),
            "report_root": str(report_root),
        }

    inventory_df = pd.concat(all_inventory, ignore_index=True)

    if all_scores:
        scores_df = pd.concat(all_scores, ignore_index=True)
    else:
        scores_df = pd.DataFrame()

    if all_stability:
        stability_df = pd.concat(all_stability, ignore_index=True)
    else:
        stability_df = pd.DataFrame()

    inventory_path = report_root / "feature_inventory" / f"{symbol}_feature_inventory_latest.csv"
    scores_path = report_root / "feature_scores" / f"{symbol}_feature_scores_latest.csv"
    stability_path = report_root / "feature_stability" / f"{symbol}_feature_stability_latest.csv"
    rankings_path = report_root / "feature_rankings" / f"{symbol}_feature_rankings_latest.csv"
    top_path = report_root / "top_features" / f"{symbol}_top_features_latest.csv"

    if scores_df.empty:
        print("[STOP] No feature scores generated.")
        inventory_df.to_csv(inventory_path, index=False)
        return {
            "status": "no_scores",
            "symbol": symbol,
            "inventory_path": str(inventory_path),
            "input_root": str(input_root),
            "report_root": str(report_root),
        }

    rankings_df = build_final_rankings(scores_df, stability_df)
    top_features_df = rankings_df.head(100)

    inventory_df.to_csv(inventory_path, index=False)
    scores_df.to_csv(scores_path, index=False)
    stability_df.to_csv(stability_path, index=False)
    rankings_df.to_csv(rankings_path, index=False)
    top_features_df.to_csv(top_path, index=False)

    report_path = write_report(
        symbol=symbol,
        report_root=report_root,
        discovered_files=discovered_files,
        inventory=inventory_df,
        scores=scores_df,
        rankings=rankings_df,
        stability=stability_df,
    )

    print("=" * 90)
    print("[DONE] Feature discovery complete.")
    print(f"Inventory: {inventory_path}")
    print(f"Scores:    {scores_path}")
    print(f"Rankings:  {rankings_path}")
    print(f"Top:       {top_path}")
    print(f"Report:    {report_path}")
    print("=" * 90)

    return {
        "status": "ok",
        "symbol": symbol,
        "files": len(discovered_files),
        "inventory_path": str(inventory_path),
        "scores_path": str(scores_path),
        "rankings_path": str(rankings_path),
        "top_path": str(top_path),
        "report_path": str(report_path),
        "input_root": str(input_root),
        "report_root": str(report_root),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Dukascopy feature discovery.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_feature_discovery(symbol=args.symbol)


if __name__ == "__main__":
    main()
