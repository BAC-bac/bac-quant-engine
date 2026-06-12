"""
BACQE DUKASCOPY 25 - SIGNAL VALIDATION ENGINE
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

FEATURE_ROOT = (
    QUANT_LAB / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={SYMBOL}"
)

TOP_FEATURES_PATH = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_feature_stability"
    / "top_features" / "top_stable_features_latest.csv"
)

OUTPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_signal_validation"

TOP_N_FEATURES = 25
QUANTILE_LOW = 0.20
QUANTILE_HIGH = 0.80

MIN_SIGNALS = 100


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "signal_results",
        OUTPUT_ROOT / "top_signals",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_feature_files() -> list[Path]:
    if not FEATURE_ROOT.exists():
        print(f"[MISSING FEATURE ROOT] {FEATURE_ROOT}")
        return []
    return sorted(FEATURE_ROOT.rglob("*.parquet"))


def load_candidate_features() -> pd.DataFrame:
    if not TOP_FEATURES_PATH.exists():
        raise FileNotFoundError(f"Missing top features file: {TOP_FEATURES_PATH}")

    df = pd.read_csv(TOP_FEATURES_PATH)

    required = {"feature", "target", "dominant_direction", "rank"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns in top features file: {sorted(missing)}")

    # Remove raw mid price from signal validation because it is not a realistic standalone signal.
    df = df[df["feature"] != "mid"].copy()

    return df.head(TOP_N_FEATURES)


def evaluate_signal_returns(signal_returns: pd.Series) -> dict:
    signal_returns = signal_returns.replace([np.inf, -np.inf], np.nan).dropna()

    if len(signal_returns) < MIN_SIGNALS:
        return {
            "signal_count": len(signal_returns),
            "win_rate": np.nan,
            "mean_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "std_return": np.nan,
            "sharpe_like": np.nan,
            "profit_factor": np.nan,
        }

    wins = signal_returns[signal_returns > 0]
    losses = signal_returns[signal_returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())

    profit_factor = (
        gross_profit / gross_loss
        if gross_loss != 0
        else np.nan
    )

    std_return = signal_returns.std()

    sharpe_like = (
        signal_returns.mean() / std_return
        if std_return and std_return != 0
        else np.nan
    )

    return {
        "signal_count": len(signal_returns),
        "win_rate": (signal_returns > 0).mean(),
        "mean_return": signal_returns.mean(),
        "median_return": signal_returns.median(),
        "total_return": signal_returns.sum(),
        "std_return": std_return,
        "sharpe_like": sharpe_like,
        "profit_factor": profit_factor,
    }


def validate_feature_on_file(df: pd.DataFrame, feature: str, target: str, feature_rank: int) -> list[dict]:
    if feature not in df.columns or target not in df.columns:
        return []

    temp = df[[feature, target]].replace([np.inf, -np.inf], np.nan).dropna()

    if len(temp) < MIN_SIGNALS:
        return []

    low_threshold = temp[feature].quantile(QUANTILE_LOW)
    high_threshold = temp[feature].quantile(QUANTILE_HIGH)

    long_returns = temp.loc[temp[feature] >= high_threshold, target]
    short_returns = -temp.loc[temp[feature] <= low_threshold, target]

    long_stats = evaluate_signal_returns(long_returns)
    short_stats = evaluate_signal_returns(short_returns)

    rows = []

    for side, stats in [("long", long_stats), ("short", short_stats)]:
        rows.append({
            "feature_rank": feature_rank,
            "feature": feature,
            "target": target,
            "side": side,
            "low_threshold": low_threshold,
            "high_threshold": high_threshold,
            **stats,
        })

    return rows


def score_final_results(results: pd.DataFrame) -> pd.DataFrame:
    df = results.copy()

    df = df[df["total_signals"] >= MIN_SIGNALS].copy()

    if df.empty:
        return df

    numeric_cols = [
        "mean_return",
        "mean_win_rate",
        "mean_profit_factor",
        "mean_sharpe_like",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["mean_return_score"] = df["mean_return"].clip(lower=0)
    max_mean = df["mean_return_score"].max()

    if pd.notna(max_mean) and max_mean != 0:
        df["mean_return_score"] = df["mean_return_score"] / max_mean
    else:
        df["mean_return_score"] = 0

    df["win_rate_score"] = df["mean_win_rate"].fillna(0)
    df["profit_factor_score"] = df["mean_profit_factor"].clip(upper=5).fillna(0) / 5

    df["sharpe_score"] = df["mean_sharpe_like"].clip(lower=0).fillna(0)
    max_sharpe = df["sharpe_score"].max()

    if pd.notna(max_sharpe) and max_sharpe != 0:
        df["sharpe_score"] = df["sharpe_score"] / max_sharpe
    else:
        df["sharpe_score"] = 0

    df["final_signal_score"] = (
        df["mean_return_score"] * 0.35
        + df["win_rate_score"] * 0.25
        + df["profit_factor_score"] * 0.25
        + df["sharpe_score"] * 0.15
    )

    df = df.sort_values("final_signal_score", ascending=False)
    df.insert(0, "signal_rank", range(1, len(df) + 1))

    return df


def main() -> None:
    banner("BACQE DUKASCOPY 25 - SIGNAL VALIDATION ENGINE")

    ensure_dirs()

    print(f"Symbol:        {SYMBOL}")
    print(f"Feature root:  {FEATURE_ROOT}")
    print(f"Top features:  {TOP_FEATURES_PATH}")
    print(f"Output root:   {OUTPUT_ROOT}")
    print("-" * 90)

    candidates = load_candidate_features()
    files = discover_feature_files()

    print(f"Candidate feature-target pairs: {len(candidates)}")
    print(f"Engineered feature files:       {len(files)}")
    print("-" * 90)

    if candidates.empty or not files:
        print("[STOP] Missing candidates or feature files.")
        return

    all_results = []

    for file_idx, path in enumerate(files, start=1):
        print(f"[{file_idx}/{len(files)}] {path}")

        try:
            df = pd.read_parquet(path)

            dataset_name = path.stem

            for _, row in candidates.iterrows():
                feature = row["feature"]
                target = row["target"]
                feature_rank = int(row["rank"])

                rows = validate_feature_on_file(
                    df=df,
                    feature=feature,
                    target=target,
                    feature_rank=feature_rank,
                )

                for result in rows:
                    result["dataset"] = dataset_name
                    result["file"] = str(path)
                    all_results.append(result)

        except Exception as e:
            print(f"    [ERROR] {e}")

    if not all_results:
        print("[STOP] No signal validation results generated.")
        return

    raw_results = pd.DataFrame(all_results)

    grouped = (
        raw_results
        .groupby(["feature_rank", "feature", "target", "side"], as_index=False)
        .agg(
            days_tested=("dataset", "nunique"),
            total_signals=("signal_count", "sum"),
            avg_daily_signals=("signal_count", "mean"),
            mean_win_rate=("win_rate", "mean"),
            median_win_rate=("win_rate", "median"),
            mean_return=("mean_return", "mean"),
            median_return=("mean_return", "median"),
            total_return=("total_return", "sum"),
            mean_profit_factor=("profit_factor", "mean"),
            median_profit_factor=("profit_factor", "median"),
            mean_sharpe_like=("sharpe_like", "mean"),
            positive_day_rate=("mean_return", lambda x: (x > 0).mean()),
        )
    )

    ranked = score_final_results(grouped)

    raw_path = OUTPUT_ROOT / "signal_results" / "signal_validation_daily_latest.csv"
    ranked_path = OUTPUT_ROOT / "signal_results" / "signal_validation_ranked_latest.csv"
    top_path = OUTPUT_ROOT / "top_signals" / "top_signal_candidates_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "signal_validation_report_latest.txt"

    raw_results.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)
    ranked.head(50).to_csv(top_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY SIGNAL VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Candidate feature-target pairs: {len(candidates)}\n")
        f.write(f"Files tested: {len(files)}\n")
        f.write(f"Raw validation rows: {len(raw_results):,}\n")
        f.write(f"Ranked signal rows: {len(ranked):,}\n\n")

        f.write("Top Signal Candidates\n")
        f.write("-" * 80 + "\n")

        if ranked.empty:
            f.write("No ranked signals generated.\n")
        else:
            f.write(
                ranked.head(25)[
                    [
                        "signal_rank",
                        "feature",
                        "target",
                        "side",
                        "days_tested",
                        "total_signals",
                        "mean_win_rate",
                        "mean_return",
                        "total_return",
                        "mean_profit_factor",
                        "positive_day_rate",
                        "final_signal_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"Raw daily: {raw_path}\n")
        f.write(f"Ranked:    {ranked_path}\n")
        f.write(f"Top:       {top_path}\n")

    print("=" * 90)
    print("[DONE] Signal validation complete.")
    print(f"Raw daily: {raw_path}")
    print(f"Ranked:    {ranked_path}")
    print(f"Top:       {top_path}")
    print(f"Report:    {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()