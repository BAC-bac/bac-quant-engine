"""
BACQE DUKASCOPY 27 - SIGNAL FILTER OPTIMIZER
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
        / "dukascopy_signal_forensics"
        / f"symbol={symbol}"
        / "top_robust_signals"
        / "top_robust_signals_latest.csv"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_signal_filter_optimizer"
        / f"symbol={symbol}"
    )

TOP_N_SIGNALS = 15
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
        output_root / "filter_results",
        output_root / "top_filtered_signals",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_feature_files(feature_root: Path) -> list[Path]:
    return sorted(feature_root.rglob("*.parquet")) if feature_root.exists() else []


def load_candidates(candidate_path: Path) -> pd.DataFrame:
    df = pd.read_csv(candidate_path)

    df = df[df["forensic_label"].isin(["robust_candidate", "research_candidate"])].copy()

    # Remove pure raw mid price as a standalone signal candidate.
    df = df[df["feature"] != "mid"].copy()

    return df.head(TOP_N_SIGNALS)


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

    vol_col = "rolling_return_std_50"

    if vol_col in df.columns:
        df["volatility_regime"] = pd.qcut(
            df[vol_col].rank(method="first"),
            q=3,
            labels=["low_volatility", "medium_volatility", "high_volatility"],
        )
    else:
        df["volatility_regime"] = "unknown_volatility"

    return df


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if len(returns) < MIN_TRADES:
        return {
            "trade_count": len(returns),
            "win_rate": np.nan,
            "mean_return": np.nan,
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
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "sharpe_like": sharpe_like,
    }


def create_signal_returns(df: pd.DataFrame, feature: str, target: str, side: str) -> pd.DataFrame:
    if feature not in df.columns or target not in df.columns:
        return pd.DataFrame()

    temp = df[
        [
            "timestamp_utc",
            "year",
            "month",
            "day_of_week",
            "hour",
            "session",
            "spread_regime",
            "volatility_regime",
            feature,
            target,
        ]
    ].replace([np.inf, -np.inf], np.nan).dropna().copy()

    if len(temp) < MIN_TRADES:
        return pd.DataFrame()

    low = temp[feature].quantile(QUANTILE_LOW)
    high = temp[feature].quantile(QUANTILE_HIGH)

    if side == "long":
        trades = temp[temp[feature] >= high].copy()
        trades["signal_return"] = trades[target]
    elif side == "short":
        trades = temp[temp[feature] <= low].copy()
        trades["signal_return"] = -trades[target]
    else:
        return pd.DataFrame()

    trades["low_threshold"] = low
    trades["high_threshold"] = high

    return trades


def evaluate_filter_group(trades: pd.DataFrame, group_col: str) -> list[dict]:
    rows = []

    for group_value, group_df in trades.groupby(group_col):
        stats = evaluate_returns(group_df["signal_return"])
        rows.append({
            "filter_type": group_col,
            "filter_value": group_value,
            **stats,
        })

    return rows


def score_results(results: pd.DataFrame) -> pd.DataFrame:
    df = results.copy()
    df = df[df["trade_count"] >= MIN_TRADES].copy()

    if df.empty:
        return df

    for col in ["win_rate", "mean_return", "total_return", "profit_factor", "sharpe_like"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["return_score"] = df["mean_return"].clip(lower=0)
    max_return = df["return_score"].max()

    if pd.notna(max_return) and max_return != 0:
        df["return_score"] = df["return_score"] / max_return
    else:
        df["return_score"] = 0

    df["profit_factor_score"] = df["profit_factor"].clip(0, 3).fillna(0) / 3
    df["win_rate_score"] = df["win_rate"].fillna(0)

    df["sharpe_score"] = df["sharpe_like"].clip(lower=0).fillna(0)
    max_sharpe = df["sharpe_score"].max()

    if pd.notna(max_sharpe) and max_sharpe != 0:
        df["sharpe_score"] = df["sharpe_score"] / max_sharpe
    else:
        df["sharpe_score"] = 0

    df["filter_score"] = (
        df["return_score"] * 0.35
        + df["profit_factor_score"] * 0.30
        + df["win_rate_score"] * 0.20
        + df["sharpe_score"] * 0.15
    )

    df = df.sort_values("filter_score", ascending=False)
    df.insert(0, "filter_rank", range(1, len(df) + 1))

    return df


def run_signal_filter_optimizer(
    symbol: str = DEFAULT_SYMBOL
) -> None:
    symbol = symbol.upper().strip()

    feature_root = build_feature_root(symbol)
    candidate_path = build_candidate_path(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 27 - SIGNAL FILTER OPTIMIZER")

    ensure_dirs(output_root)

    print(f"Symbol:       {symbol}")
    print(f"Feature root: {feature_root}")
    print(f"Candidates:   {candidate_path}")
    print(f"Output root:  {output_root}")
    print("-" * 90)

    files = discover_feature_files(feature_root)
    candidates = load_candidates(candidate_path)

    print(f"Feature files: {len(files)}")
    print(f"Candidates:    {len(candidates)}")
    print("-" * 90)

    if not files or candidates.empty:
        print("[STOP] Missing files or candidates.")
        return

    all_rows = []

    filter_cols = [
        "session",
        "spread_regime",
        "volatility_regime",
        "day_of_week",
        "year",
    ]

    for signal_idx, signal in candidates.iterrows():
        feature = signal["feature"]
        target = signal["target"]
        side = signal["side"]

        print("-" * 90)
        print(f"[SIGNAL] {feature} | {target} | {side}")

        signal_trades = []

        for file_idx, path in enumerate(files, start=1):
            try:
                df = pd.read_parquet(path)

                required = {"timestamp_utc", "spread", feature, target}
                missing = required - set(df.columns)

                if missing:
                    continue

                df = add_context_columns(df)

                trades = create_signal_returns(df, feature, target, side)

                if not trades.empty:
                    signal_trades.append(trades)

            except Exception as e:
                print(f"    [ERROR] {path.name}: {e}")

            if file_idx % 100 == 0:
                print(f"    processed {file_idx}/{len(files)} files")

        if not signal_trades:
            continue

        trades_all = pd.concat(signal_trades, ignore_index=True)

        base_stats = evaluate_returns(trades_all["signal_return"])

        all_rows.append({
            "feature": feature,
            "target": target,
            "side": side,
            "filter_type": "all",
            "filter_value": "all",
            **base_stats,
        })

        for group_col in filter_cols:
            for row in evaluate_filter_group(trades_all, group_col):
                row.update({
                    "feature": feature,
                    "target": target,
                    "side": side,
                })
                all_rows.append(row)

    if not all_rows:
        print("[STOP] No filter results generated.")
        return

    results = pd.DataFrame(all_rows)
    ranked = score_results(results)

    output_all = output_root / "filter_results" / "signal_filter_results_latest.csv"
    output_ranked = output_root / "filter_results" / "signal_filter_ranked_latest.csv"
    output_top = output_root / "top_filtered_signals" / "top_filtered_signals_latest.csv"
    output_report = output_root / "reports" / "signal_filter_optimizer_report_latest.txt"

    results.to_csv(output_all, index=False)
    ranked.to_csv(output_ranked, index=False)
    ranked.head(50).to_csv(output_top, index=False)

    with open(output_report, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY SIGNAL FILTER OPTIMIZER REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Signals tested: {len(candidates)}\n")
        f.write(f"Feature files: {len(files)}\n")
        f.write(f"Filter result rows: {len(results):,}\n")
        f.write(f"Ranked rows: {len(ranked):,}\n\n")

        f.write("Top Filtered Signal Candidates\n")
        f.write("-" * 80 + "\n")

        if ranked.empty:
            f.write("No ranked filter candidates produced.\n")
        else:
            f.write(
                ranked.head(30)[
                    [
                        "filter_rank",
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
                        "filter_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"All:    {output_all}\n")
        f.write(f"Ranked: {output_ranked}\n")
        f.write(f"Top:    {output_top}\n")

    print("=" * 90)
    print("[DONE] Signal filter optimizer complete.")
    print(f"All:    {output_all}")
    print(f"Ranked: {output_ranked}")
    print(f"Top:    {output_top}")
    print(f"Report: {output_report}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy signal filter optimizer."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_signal_filter_optimizer(
        symbol=args.symbol
    )


if __name__ == "__main__":
    main()