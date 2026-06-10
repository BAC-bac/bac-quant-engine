"""
BACQE DUKASCOPY 22 - FEATURE DISCOVERY ENGINE

Purpose:
    Discover which Dukascopy-derived features have predictive relationship
    with future EURUSD returns.

Outputs:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_feature_discovery
"""

from pathlib import Path
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    from scipy.stats import spearmanr
except ImportError:
    spearmanr = None

try:
    from sklearn.feature_selection import mutual_info_regression
except ImportError:
    mutual_info_regression = None


# =============================================================================
# CONFIG
# =============================================================================

SYMBOL = "EURUSD"

QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_ROOT = QUANT_LAB / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={SYMBOL}"

REPORT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_feature_discovery"

FORWARD_WINDOWS = [1, 3, 5, 10, 20]
RUN_MUTUAL_INFO = False
MAX_ROWS_PER_DATASET = 10_000
MIN_VALID_ROWS = 1_000

EXCLUDE_COLS = {
    "timestamp",
    "datetime",
    "date",
    "time",
    "symbol",
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


# =============================================================================
# HELPERS
# =============================================================================

def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        REPORT_ROOT,
        REPORT_ROOT / "feature_inventory",
        REPORT_ROOT / "feature_scores",
        REPORT_ROOT / "feature_rankings",
        REPORT_ROOT / "feature_stability",
        REPORT_ROOT / "reports",
        REPORT_ROOT / "top_features",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_files() -> list[Path]:
    if not INPUT_ROOT.exists():
        print(f"[MISSING INPUT ROOT] {INPUT_ROOT}")
        return []

    files = sorted(INPUT_ROOT.rglob("*.parquet"))

    print(f"Input root: {INPUT_ROOT}")
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
    candidates = ["timestamp", "datetime", "date_time", "time"]

    for col in candidates:
        if col in df.columns:
            return col

    for col in df.columns:
        if "time" in col.lower() or "date" in col.lower():
            return col

    return None


def find_price_col(df: pd.DataFrame) -> str | None:
    candidates = ["close", "mid", "mid_price", "price", "bid"]

    for col in candidates:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            return col

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    return numeric_cols[0] if numeric_cols else None


def create_forward_returns(df: pd.DataFrame, price_col: str) -> pd.DataFrame:
    df = df.copy()

    for window in FORWARD_WINDOWS:
        df[f"future_return_{window}"] = (
            df[price_col].shift(-window) / df[price_col] - 1
        )

    return df


def build_feature_inventory(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    rows = []

    for col in df.columns:
        rows.append({
            "dataset": dataset_name,
            "feature": col,
            "dtype": str(df[col].dtype),
            "rows": len(df),
            "missing_pct": round(df[col].isna().mean() * 100, 4),
            "unique_values": df[col].nunique(dropna=True),
            "is_numeric": pd.api.types.is_numeric_dtype(df[col]),
        })

    return pd.DataFrame(rows)


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    future_cols = [c for c in df.columns if c.startswith("future_return_")]

    feature_cols = []

    for col in df.columns:
        if col in EXCLUDE_COLS:
            continue

        if col in future_cols:
            continue

        if not pd.api.types.is_numeric_dtype(df[col]):
            continue

        if df[col].nunique(dropna=True) < 5:
            continue

        feature_cols.append(col)

    return feature_cols


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

    X = valid.iloc[:, [0]].values
    target = valid.iloc[:, 1].values

    try:
        mi = mutual_info_regression(X, target, random_state=42)
        return float(mi[0])
    except Exception:
        return np.nan


def score_features(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    feature_cols = get_feature_columns(df)
    target_cols = [c for c in df.columns if c.startswith("future_return_")]

    rows = []

    for feature in feature_cols:
        for target in target_cols:
            corr, pvalue = calculate_spearman(df[feature], df[target])
            mi = calculate_mutual_info(df[feature], df[target])

            rows.append({
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


def calculate_stability(df: pd.DataFrame, dataset_name: str, timestamp_col: str | None) -> pd.DataFrame:
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

        score_df = score_features(year_df, dataset_name)

        if score_df.empty:
            continue

        score_df["year"] = year
        rows.append(score_df)

    if not rows:
        return pd.DataFrame()

    yearly = pd.concat(rows, ignore_index=True)

    stability = (
        yearly
        .groupby(["dataset", "feature", "target"], as_index=False)
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
                    "dataset",
                    "feature",
                    "target",
                    "stability_score",
                    "years_tested",
                ]
            ],
            on=["dataset", "feature", "target"],
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
    discovered_files: list[Path],
    inventory: pd.DataFrame,
    scores: pd.DataFrame,
    rankings: pd.DataFrame,
    stability: pd.DataFrame,
) -> None:
    report_path = REPORT_ROOT / "reports" / "feature_discovery_report_latest.txt"

    top = rankings.head(25)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY FEATURE DISCOVERY REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Symbol: {SYMBOL}\n")
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

        f.write("\n\nReport saved to:\n")
        f.write(str(REPORT_ROOT))


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    banner("BACQE DUKASCOPY 22 - FEATURE DISCOVERY ENGINE")

    ensure_dirs()

    print(f"Symbol:      {SYMBOL}")
    print(f"Report root: {REPORT_ROOT}")
    print("-" * 90)

    discovered_files = discover_files()

    print(f"Discovered files: {len(discovered_files)}")

    if not discovered_files:
        print("[STOP] No input files discovered.")
        return

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

            inventory = build_feature_inventory(df, dataset_name)
            scores = score_features(df, dataset_name)
            stability = calculate_stability(df, dataset_name, timestamp_col)

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
        return

    inventory_df = pd.concat(all_inventory, ignore_index=True)

    if all_scores:
        scores_df = pd.concat(all_scores, ignore_index=True)
    else:
        scores_df = pd.DataFrame()

    if all_stability:
        stability_df = pd.concat(all_stability, ignore_index=True)
    else:
        stability_df = pd.DataFrame()

    if scores_df.empty:
        print("[STOP] No feature scores generated.")
        inventory_df.to_csv(
            REPORT_ROOT / "feature_inventory" / "feature_inventory_latest.csv",
            index=False,
        )
        return

    rankings_df = build_final_rankings(scores_df, stability_df)
    top_features_df = rankings_df.head(100)

    inventory_df.to_csv(
        REPORT_ROOT / "feature_inventory" / "feature_inventory_latest.csv",
        index=False,
    )

    scores_df.to_csv(
        REPORT_ROOT / "feature_scores" / "feature_scores_latest.csv",
        index=False,
    )

    stability_df.to_csv(
        REPORT_ROOT / "feature_stability" / "feature_stability_latest.csv",
        index=False,
    )

    rankings_df.to_csv(
        REPORT_ROOT / "feature_rankings" / "feature_rankings_latest.csv",
        index=False,
    )

    top_features_df.to_csv(
        REPORT_ROOT / "top_features" / "top_features_latest.csv",
        index=False,
    )

    write_report(
        discovered_files=discovered_files,
        inventory=inventory_df,
        scores=scores_df,
        rankings=rankings_df,
        stability=stability_df,
    )

    print("=" * 90)
    print("[DONE] Feature discovery complete.")
    print(f"Inventory: {REPORT_ROOT / 'feature_inventory' / 'feature_inventory_latest.csv'}")
    print(f"Scores:    {REPORT_ROOT / 'feature_scores' / 'feature_scores_latest.csv'}")
    print(f"Rankings:  {REPORT_ROOT / 'feature_rankings' / 'feature_rankings_latest.csv'}")
    print(f"Top:       {REPORT_ROOT / 'top_features' / 'top_features_latest.csv'}")
    print(f"Report:    {REPORT_ROOT / 'reports' / 'feature_discovery_report_latest.txt'}")
    print("=" * 90)


if __name__ == "__main__":
    main()