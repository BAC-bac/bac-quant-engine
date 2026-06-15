"""
BACQE DUKASCOPY 31 - HORIZON SIGNAL VALIDATION ENGINE
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

TOP_N_FEATURES = 30
QUANTILE_LOW = 0.20
QUANTILE_HIGH = 0.80
MIN_SIGNALS = 500

TARGET_HORIZONS = [
    "future_return_25",
    "future_return_50",
    "future_return_100",
    "future_return_250",
    "future_return_500",
    "future_return_1000",
]


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def build_feature_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_horizon_features"
        / f"symbol={symbol}"
    )


def build_candidate_path(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_feature_stability"
        / f"symbol={symbol}"
        / "top_features"
        / "top_stable_features_latest.csv"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_signal_validation"
        / f"symbol={symbol}"
    )


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "signal_results",
        output_root / "top_signals",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_files(feature_root: Path) -> list[Path]:
    return sorted(feature_root.rglob("*.parquet")) if feature_root.exists() else []


def load_candidate_features(candidate_path: Path) -> list[str]:
    df = pd.read_csv(candidate_path)
    df = df[df["feature"] != "mid"].copy()

    return (
        df["feature"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .head(TOP_N_FEATURES)
        .tolist()
    )


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if len(returns) < MIN_SIGNALS:
        return {
            "signal_count": len(returns),
            "win_rate": np.nan,
            "mean_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    std = returns.std()
    sharpe_like = returns.mean() / std if pd.notna(std) and std != 0 else np.nan

    return {
        "signal_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "median_return": returns.median(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "sharpe_like": sharpe_like,
    }


def validate_feature_target(df: pd.DataFrame, feature: str, target: str) -> list[dict]:
    if feature not in df.columns or target not in df.columns:
        return []

    temp = df[[feature, target]].replace([np.inf, -np.inf], np.nan).dropna()

    if len(temp) < MIN_SIGNALS:
        return []

    low = temp[feature].quantile(QUANTILE_LOW)
    high = temp[feature].quantile(QUANTILE_HIGH)

    long_returns = temp.loc[temp[feature] >= high, target]
    short_returns = -temp.loc[temp[feature] <= low, target]

    rows = []

    for side, returns in [("long", long_returns), ("short", short_returns)]:
        stats = evaluate_returns(returns)

        rows.append({
            "feature": feature,
            "target": target,
            "side": side,
            "low_threshold": low,
            "high_threshold": high,
            **stats,
        })

    return rows


def score_results(grouped: pd.DataFrame) -> pd.DataFrame:
    df = grouped.copy()
    df = df[df["total_signals"] >= MIN_SIGNALS].copy()

    if df.empty:
        return df

    for col in [
        "mean_win_rate",
        "mean_return",
        "total_return",
        "mean_profit_factor",
        "mean_sharpe_like",
        "positive_day_rate",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["return_score"] = df["mean_return"].clip(lower=0)
    max_return = df["return_score"].max()

    if pd.notna(max_return) and max_return != 0:
        df["return_score"] = df["return_score"] / max_return
    else:
        df["return_score"] = 0

    df["profit_factor_score"] = df["mean_profit_factor"].clip(0, 3).fillna(0) / 3
    df["win_rate_score"] = df["mean_win_rate"].fillna(0)
    df["positive_day_score"] = df["positive_day_rate"].fillna(0)

    df["horizon_signal_score"] = (
        df["return_score"] * 0.30
        + df["profit_factor_score"] * 0.30
        + df["win_rate_score"] * 0.20
        + df["positive_day_score"] * 0.20
    )

    df = df.sort_values("horizon_signal_score", ascending=False)
    df.insert(0, "horizon_signal_rank", range(1, len(df) + 1))

    return df


def run_horizon_signal_validation(symbol: str = DEFAULT_SYMBOL) -> None:
    symbol = symbol.upper().strip()

    feature_root = build_feature_root(symbol)
    candidate_path = build_candidate_path(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 31 - HORIZON SIGNAL VALIDATION ENGINE")

    ensure_dirs(output_root)

    print(f"Symbol:       {symbol}")
    print(f"Feature root: {feature_root}")
    print(f"Candidates:   {candidate_path}")
    print(f"Output root:  {output_root}")
    print(f"Targets:      {TARGET_HORIZONS}")
    print("-" * 90)

    if not candidate_path.exists():
        print("[STOP] Missing candidate feature file.")
        return

    files = discover_files(feature_root)
    candidate_features = load_candidate_features(candidate_path)

    print(f"Horizon files discovered: {len(files)}")
    print(f"Candidate features:       {len(candidate_features)}")
    print("-" * 90)

    if not files or not candidate_features:
        print("[STOP] Missing files or candidate features.")
        return

    all_results = []

    for file_idx, path in enumerate(files, start=1):
        print(f"[{file_idx}/{len(files)}] {path}")

        try:
            df = pd.read_parquet(path)
            dataset_name = path.stem

            for feature in candidate_features:
                for target in TARGET_HORIZONS:
                    rows = validate_feature_target(df, feature, target)

                    for row in rows:
                        row["dataset"] = dataset_name
                        row["file"] = str(path)
                        all_results.append(row)

        except Exception as e:
            print(f"    [ERROR] {e}")

    if not all_results:
        print("[STOP] No horizon validation results generated.")
        return

    raw = pd.DataFrame(all_results)

    grouped = (
        raw.groupby(["feature", "target", "side"], as_index=False)
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

    ranked = score_results(grouped)

    raw_path = output_root / "signal_results" / "horizon_signal_validation_daily_latest.csv"
    ranked_path = output_root / "signal_results" / "horizon_signal_validation_ranked_latest.csv"
    top_path = output_root / "top_signals" / "top_horizon_signal_candidates_latest.csv"
    report_path = output_root / "reports" / "horizon_signal_validation_report_latest.txt"

    raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)
    ranked.head(100).to_csv(top_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY HORIZON SIGNAL VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Files tested: {len(files)}\n")
        f.write(f"Candidate features: {len(candidate_features)}\n")
        f.write(f"Targets tested: {TARGET_HORIZONS}\n")
        f.write(f"Raw validation rows: {len(raw):,}\n")
        f.write(f"Ranked signal rows: {len(ranked):,}\n\n")

        f.write("Top Horizon Signal Candidates\n")
        f.write("-" * 80 + "\n")

        if ranked.empty:
            f.write("No ranked candidates generated.\n")
        else:
            f.write(
                ranked.head(40)[
                    [
                        "horizon_signal_rank",
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
                        "horizon_signal_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"Raw:    {raw_path}\n")
        f.write(f"Ranked: {ranked_path}\n")
        f.write(f"Top:    {top_path}\n")

    print("=" * 90)
    print("[DONE] Horizon signal validation complete.")
    print(f"Raw:    {raw_path}")
    print(f"Ranked: {ranked_path}")
    print(f"Top:    {top_path}")
    print(f"Report: {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Dukascopy horizon signal candidates."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_horizon_signal_validation(symbol=args.symbol)


if __name__ == "__main__":
    main()