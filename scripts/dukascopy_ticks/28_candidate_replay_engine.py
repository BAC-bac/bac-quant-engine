"""
BACQE DUKASCOPY 28 - CANDIDATE REPLAY ENGINE

Purpose:
    Replay top filtered Dukascopy signal candidates into trade-level ledgers.

Input:
    - Top filtered signals from Script 27
    - Engineered feature datasets from Script 23

Output:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_candidate_replay
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

def build_feature_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_engineered_features"
        / f"symbol={symbol}"
    )


def build_candidate_path(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_signal_filter_optimizer"
        / f"symbol={symbol}"
        / "top_filtered_signals"
        / "top_filtered_signals_latest.csv"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_candidate_replay"
        / f"symbol={symbol}"
    )

TOP_N_CANDIDATES = 20
QUANTILE_LOW = 0.20
QUANTILE_HIGH = 0.80

MIN_TRADES = 500


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "trade_ledgers",
        output_root / "candidate_summaries",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_feature_files(feature_root: Path) -> list[Path]:
    return sorted(feature_root.rglob("*.parquet")) if feature_root.exists() else []


def assign_session(hour: int) -> str:
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london_morning"
    if 12 <= hour < 16:
        return "london_newyork_overlap"
    if 16 <= hour < 21:
        return "newyork_afternoon"
    return "late_us"


def add_context_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
    df = df.dropna(subset=["timestamp_utc"])

    df["year"] = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.tz_localize(None).dt.to_period("M").astype(str)
    df["day_of_week"] = df["timestamp_utc"].dt.day_name()
    df["hour"] = df["timestamp_utc"].dt.hour
    df["session"] = df["hour"].apply(assign_session)

    df["spread_regime"] = pd.qcut(
        df["spread"].rank(method="first"),
        q=3,
        labels=["low_spread", "medium_spread", "high_spread"],
    )

    if "rolling_return_std_50" in df.columns:
        df["volatility_regime"] = pd.qcut(
            df["rolling_return_std_50"].rank(method="first"),
            q=3,
            labels=["low_volatility", "medium_volatility", "high_volatility"],
        )
    else:
        df["volatility_regime"] = "unknown_volatility"

    return df


def load_candidates(candidate_path: Path) -> pd.DataFrame:
    if not candidate_path.exists():
        raise FileNotFoundError(f"Missing candidate file: {candidate_path}")

    df = pd.read_csv(candidate_path)

    required = {
        "feature",
        "target",
        "side",
        "filter_type",
        "filter_value",
        "filter_rank",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing candidate columns: {sorted(missing)}")

    df = df[df["feature"] != "mid"].copy()
    return df.head(TOP_N_CANDIDATES)


def apply_filter(df: pd.DataFrame, filter_type: str, filter_value: str) -> pd.DataFrame:
    if filter_type == "all":
        return df

    if filter_type not in df.columns:
        return pd.DataFrame()

    return df[df[filter_type].astype(str) == str(filter_value)].copy()


def create_candidate_trades(
    df: pd.DataFrame,
    candidate: pd.Series,
    dataset_name: str,
) -> pd.DataFrame:
    feature = candidate["feature"]
    target = candidate["target"]
    side = candidate["side"]
    filter_type = candidate["filter_type"]
    filter_value = candidate["filter_value"]

    required = {
        "timestamp_utc",
        "bid",
        "ask",
        "mid",
        "spread",
        feature,
        target,
        "session",
        "spread_regime",
        "volatility_regime",
        "day_of_week",
        "year",
        "month",
        "hour",
    }

    missing = required - set(df.columns)

    if missing:
        return pd.DataFrame()

    temp = df[list(required)].replace([np.inf, -np.inf], np.nan).dropna().copy()

    temp = apply_filter(temp, filter_type, filter_value)

    if len(temp) < MIN_TRADES:
        return pd.DataFrame()

    low_threshold = temp[feature].quantile(QUANTILE_LOW)
    high_threshold = temp[feature].quantile(QUANTILE_HIGH)

    if side == "long":
        trades = temp[temp[feature] >= high_threshold].copy()
        trades["signal_return"] = trades[target]
        trades["direction"] = 1
    elif side == "short":
        trades = temp[temp[feature] <= low_threshold].copy()
        trades["signal_return"] = -trades[target]
        trades["direction"] = -1
    else:
        return pd.DataFrame()

    if trades.empty:
        return pd.DataFrame()

    trades["candidate_id"] = (
        candidate["feature"]
        + "__"
        + candidate["target"]
        + "__"
        + candidate["side"]
        + "__"
        + candidate["filter_type"]
        + "="
        + candidate["filter_value"].astype(str)
        if hasattr(candidate["filter_value"], "astype")
        else f"{candidate['feature']}__{candidate['target']}__{candidate['side']}__{candidate['filter_type']}={candidate['filter_value']}"
    )

    trades["dataset"] = dataset_name
    trades["feature"] = feature
    trades["target"] = target
    trades["side"] = side
    trades["filter_type"] = filter_type
    trades["filter_value"] = filter_value
    trades["feature_value"] = trades[feature]
    trades["target_return"] = trades[target]
    trades["low_threshold"] = low_threshold
    trades["high_threshold"] = high_threshold

    keep_cols = [
        "candidate_id",
        "dataset",
        "timestamp_utc",
        "year",
        "month",
        "day_of_week",
        "hour",
        "session",
        "spread_regime",
        "volatility_regime",
        "feature",
        "target",
        "side",
        "filter_type",
        "filter_value",
        "bid",
        "ask",
        "mid",
        "spread",
        "feature_value",
        "target_return",
        "signal_return",
        "direction",
        "low_threshold",
        "high_threshold",
    ]

    return trades[keep_cols]


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if len(returns) == 0:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "mean_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "max_drawdown_return": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    std = returns.std()
    sharpe_like = returns.mean() / std if pd.notna(std) and std != 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()
    max_drawdown = drawdown.min()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "median_return": returns.median(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "sharpe_like": sharpe_like,
        "max_drawdown_return": max_drawdown,
    }


def build_candidate_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for candidate_id, group in trades.groupby("candidate_id"):
        stats = evaluate_returns(group["signal_return"])

        rows.append({
            "candidate_id": candidate_id,
            "feature": group["feature"].iloc[0],
            "target": group["target"].iloc[0],
            "side": group["side"].iloc[0],
            "filter_type": group["filter_type"].iloc[0],
            "filter_value": group["filter_value"].iloc[0],
            "days_tested": group["dataset"].nunique(),
            "months_tested": group["month"].nunique(),
            "years_tested": group["year"].nunique(),
            **stats,
            "positive_day_rate": (
                group.groupby("dataset")["signal_return"].mean() > 0
            ).mean(),
            "positive_month_rate": (
                group.groupby("month")["signal_return"].sum() > 0
            ).mean(),
            "positive_year_rate": (
                group.groupby("year")["signal_return"].sum() > 0
            ).mean(),
        })

    summary = pd.DataFrame(rows)

    if summary.empty:
        return summary

    summary["profit_factor_score"] = summary["profit_factor"].clip(0, 3).fillna(0) / 3
    summary["win_rate_score"] = summary["win_rate"].fillna(0)
    summary["day_score"] = summary["positive_day_rate"].fillna(0)
    summary["month_score"] = summary["positive_month_rate"].fillna(0)
    summary["year_score"] = summary["positive_year_rate"].fillna(0)

    summary["mean_return_score"] = summary["mean_return"].clip(lower=0)
    max_mean = summary["mean_return_score"].max()

    if pd.notna(max_mean) and max_mean != 0:
        summary["mean_return_score"] = summary["mean_return_score"] / max_mean
    else:
        summary["mean_return_score"] = 0

    summary["replay_score"] = (
        summary["profit_factor_score"] * 0.25
        + summary["win_rate_score"] * 0.20
        + summary["day_score"] * 0.20
        + summary["month_score"] * 0.15
        + summary["year_score"] * 0.10
        + summary["mean_return_score"] * 0.10
    )

    summary = summary.sort_values("replay_score", ascending=False)
    summary.insert(0, "replay_rank", range(1, len(summary) + 1))

    return summary


def run_candidate_replay(
    symbol: str = DEFAULT_SYMBOL,
) -> None:
    symbol = symbol.upper().strip()

    feature_root = build_feature_root(symbol)
    candidate_path = build_candidate_path(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 28 - CANDIDATE REPLAY ENGINE")

    ensure_dirs(output_root)

    print(f"Symbol:       {symbol}")
    print(f"Feature root: {feature_root}")
    print(f"Candidates:   {candidate_path}")
    print(f"Output root:  {output_root}")
    print("-" * 90)

    files = discover_feature_files(feature_root)
    candidates = load_candidates(candidate_path)

    print(f"Feature files discovered: {len(files)}")
    print(f"Candidates loaded:         {len(candidates)}")
    print("-" * 90)

    if not files or candidates.empty:
        print("[STOP] Missing files or candidates.")
        return

    all_trades = []

    for file_idx, path in enumerate(files, start=1):
        try:
            df = pd.read_parquet(path)
            df = add_context_columns(df)
            dataset_name = path.stem

            for _, candidate in candidates.iterrows():
                trades = create_candidate_trades(df, candidate, dataset_name)

                if not trades.empty:
                    all_trades.append(trades)

        except Exception as e:
            print(f"[ERROR] {path.name}: {e}")

        if file_idx % 50 == 0 or file_idx == len(files):
            print(f"Processed {file_idx}/{len(files)} files")

    if not all_trades:
        print("[STOP] No replay trades generated.")
        return

    ledger = pd.concat(all_trades, ignore_index=True)
    summary = build_candidate_summary(ledger)

    ledger_path = output_root / "trade_ledgers" / "candidate_replay_ledger_latest.parquet"
    ledger_csv_path = output_root / "trade_ledgers" / "candidate_replay_ledger_latest.csv"
    summary_path = output_root / "candidate_summaries" / "candidate_replay_summary_latest.csv"
    report_path = output_root / "reports" / "candidate_replay_report_latest.txt"

    ledger.to_parquet(ledger_path, index=False)
    ledger.head(250_000).to_csv(ledger_csv_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CANDIDATE REPLAY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Feature files: {len(files)}\n")
        f.write(f"Candidates replayed: {len(candidates)}\n")
        f.write(f"Replay trades generated: {len(ledger):,}\n")
        f.write(f"Candidate summaries: {len(summary):,}\n\n")

        f.write("Top Candidate Replay Summaries\n")
        f.write("-" * 80 + "\n")

        if summary.empty:
            f.write("No candidate summaries generated.\n")
        else:
            f.write(
                summary.head(30)[
                    [
                        "replay_rank",
                        "feature",
                        "target",
                        "side",
                        "filter_type",
                        "filter_value",
                        "trade_count",
                        "win_rate",
                        "mean_return",
                        "total_return",
                        "profit_factor",
                        "positive_day_rate",
                        "positive_month_rate",
                        "positive_year_rate",
                        "max_drawdown_return",
                        "replay_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"Ledger parquet: {ledger_path}\n")
        f.write(f"Ledger CSV sample: {ledger_csv_path}\n")
        f.write(f"Summary: {summary_path}\n")

    print("=" * 90)
    print("[DONE] Candidate replay complete.")
    print(f"Ledger parquet: {ledger_path}")
    print(f"Ledger CSV sample: {ledger_csv_path}")
    print(f"Summary: {summary_path}")
    print(f"Report: {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy candidate replay."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_candidate_replay(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()