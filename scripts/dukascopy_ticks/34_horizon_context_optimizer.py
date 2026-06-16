"""
BACQE DUKASCOPY 34 - HORIZON CONTEXT OPTIMIZER
"""

from pathlib import Path
import argparse
import itertools
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

def build_input_ledger(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_candidate_replay"
        / f"symbol={symbol}"
        / "trade_ledgers"
        / "candidate_replay_ledger_latest.parquet"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_context_optimizer"
        / f"symbol={symbol}"
    )

PIP_SIZE = 0.0001
MIN_TRADES = 50_000

# Use the most forgiving scenario first, because Script 33 showed the best candidate was close to breakeven here.
COST_SCENARIO_NAME = "half_spread_plus_low_commission"
SPREAD_MULTIPLIER = 0.5
COMMISSION_PIPS = 0.05

CONTEXT_COLS = [
    "session",
    "spread_regime",
    "volatility_regime",
    "day_of_week",
    "hour",
]

MAX_COMBO_SIZE = 2


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "context_results",
        output_root / "top_contexts",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if len(returns) == 0:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "mean_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown_return": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown_return": drawdown.min(),
    }


def add_costs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["spread_cost_return"] = df["spread"] * SPREAD_MULTIPLIER
    df["commission_cost_return"] = COMMISSION_PIPS * PIP_SIZE
    df["total_cost_return"] = df["spread_cost_return"] + df["commission_cost_return"]
    df["net_signal_return"] = df["signal_return"] - df["total_cost_return"]

    return df


def make_context_key(row: pd.Series, cols: tuple[str, ...]) -> str:
    return " | ".join([f"{col}={row[col]}" for col in cols])


def score_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[df["trade_count"] >= MIN_TRADES].copy()

    if df.empty:
        return df

    df["pf_score"] = df["net_profit_factor"].clip(0, 3).fillna(0) / 3
    df["win_score"] = df["net_win_rate"].fillna(0)
    df["day_score"] = df["net_positive_day_rate"].fillna(0)
    df["month_score"] = df["net_positive_month_rate"].fillna(0)
    df["year_score"] = df["net_positive_year_rate"].fillna(0)

    df["return_score"] = df["net_mean_return"].clip(lower=0)
    max_return = df["return_score"].max()

    if pd.notna(max_return) and max_return != 0:
        df["return_score"] = df["return_score"] / max_return
    else:
        df["return_score"] = 0

    df["context_score"] = (
        df["pf_score"] * 0.30
        + df["win_score"] * 0.15
        + df["day_score"] * 0.20
        + df["month_score"] * 0.20
        + df["year_score"] * 0.10
        + df["return_score"] * 0.05
    )

    df["context_label"] = np.select(
        [
            (df["net_profit_factor"] >= 1.20) & (df["net_total_return"] > 0) & (df["net_positive_month_rate"] >= 0.70),
            (df["net_profit_factor"] >= 1.05) & (df["net_total_return"] > 0),
            (df["net_profit_factor"] >= 1.00) & (df["net_total_return"] > 0),
        ],
        [
            "strong_context_survivor",
            "context_survivor",
            "fragile_context_survivor",
        ],
        default="fails_context_costs",
    )

    df = df.sort_values("context_score", ascending=False)
    df.insert(0, "context_rank", range(1, len(df) + 1))

    return df


def analyse_contexts(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for candidate_id, cand_df in ledger.groupby("candidate_id"):
        print(f"[CANDIDATE] {candidate_id} rows={len(cand_df):,}")

        available_cols = [col for col in CONTEXT_COLS if col in cand_df.columns]

        for size in range(1, MAX_COMBO_SIZE + 1):
            for cols in itertools.combinations(available_cols, size):
                temp = cand_df.dropna(subset=list(cols)).copy()

                if temp.empty:
                    continue

                temp["context_key"] = temp.apply(lambda row: make_context_key(row, cols), axis=1)

                for context_key, group in temp.groupby("context_key"):
                    if len(group) < MIN_TRADES:
                        continue

                    gross_stats = evaluate_returns(group["signal_return"])
                    net_stats = evaluate_returns(group["net_signal_return"])

                    rows.append({
                        "candidate_id": candidate_id,
                        "feature": group["feature"].iloc[0],
                        "target": group["target"].iloc[0],
                        "side": group["side"].iloc[0],
                        "context_type": "+".join(cols),
                        "context_value": context_key,
                        "cost_scenario": COST_SCENARIO_NAME,
                        "avg_spread": group["spread"].mean(),
                        "avg_total_cost_return": group["total_cost_return"].mean(),

                        "gross_trade_count": gross_stats["trade_count"],
                        "gross_win_rate": gross_stats["win_rate"],
                        "gross_mean_return": gross_stats["mean_return"],
                        "gross_total_return": gross_stats["total_return"],
                        "gross_profit_factor": gross_stats["profit_factor"],

                        "trade_count": net_stats["trade_count"],
                        "net_win_rate": net_stats["win_rate"],
                        "net_mean_return": net_stats["mean_return"],
                        "net_total_return": net_stats["total_return"],
                        "net_profit_factor": net_stats["profit_factor"],
                        "net_max_drawdown_return": net_stats["max_drawdown_return"],

                        "net_positive_day_rate": (
                            group.groupby("dataset")["net_signal_return"].mean() > 0
                        ).mean() if "dataset" in group.columns else np.nan,

                        "net_positive_month_rate": (
                            group.groupby("month")["net_signal_return"].sum() > 0
                        ).mean() if "month" in group.columns else np.nan,

                        "net_positive_year_rate": (
                            group.groupby("year")["net_signal_return"].sum() > 0
                        ).mean() if "year" in group.columns else np.nan,
                    })

    return pd.DataFrame(rows)


def run_horizon_context_optimizer(
    symbol: str = DEFAULT_SYMBOL,
) -> None:
    symbol = symbol.upper().strip()

    input_ledger = build_input_ledger(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 34 - HORIZON CONTEXT OPTIMIZER")

    ensure_dirs(output_root)

    print(f"Symbol:       {symbol}")
    print(f"Input ledger: {input_ledger}")
    print(f"Output root:  {output_root}")
    print(f"Cost model:   {COST_SCENARIO_NAME}")
    print("-" * 90)

    if not input_ledger.exists():
        print("[STOP] Missing horizon replay ledger.")
        return

    ledger = pd.read_parquet(input_ledger)
    print(f"Loaded ledger rows: {len(ledger):,}")

    required = {
        "candidate_id",
        "feature",
        "target",
        "side",
        "spread",
        "signal_return",
    }

    missing = required - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing columns: {sorted(missing)}")
        return

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["spread", "signal_return"])

    ledger = add_costs(ledger)

    results = analyse_contexts(ledger)

    if results.empty:
        print("[STOP] No context results generated.")
        return

    ranked = score_results(results)

    output_all = output_root / "context_results" / "horizon_context_results_latest.csv"
    output_ranked = output_root / "context_results" / "horizon_context_ranked_latest.csv"
    output_top = output_root / "top_contexts" / "top_horizon_contexts_latest.csv"
    output_report = output_root / "reports" / "horizon_context_optimizer_report_latest.txt"

    results.to_csv(output_all, index=False)
    ranked.to_csv(output_ranked, index=False)
    ranked.head(100).to_csv(output_top, index=False)

    with open(output_report, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY HORIZON CONTEXT OPTIMIZER REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input ledger rows: {len(ledger):,}\n")
        f.write(f"Cost scenario: {COST_SCENARIO_NAME}\n")
        f.write(f"Context result rows: {len(results):,}\n")
        f.write(f"Ranked rows: {len(ranked):,}\n\n")

        f.write("Context Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(ranked["context_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Horizon Context Candidates\n")
        f.write("-" * 80 + "\n")

        if ranked.empty:
            f.write("No ranked contexts generated.\n")
        else:
            f.write(
                ranked.head(40)[
                    [
                        "context_rank",
                        "feature",
                        "target",
                        "side",
                        "context_type",
                        "context_value",
                        "trade_count",
                        "avg_spread",
                        "net_win_rate",
                        "net_mean_return",
                        "net_total_return",
                        "net_profit_factor",
                        "net_positive_day_rate",
                        "net_positive_month_rate",
                        "net_positive_year_rate",
                        "context_label",
                        "context_score",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"All:    {output_all}\n")
        f.write(f"Ranked: {output_ranked}\n")
        f.write(f"Top:    {output_top}\n")

    print("=" * 90)
    print("[DONE] Horizon context optimizer complete.")
    print(f"All:    {output_all}")
    print(f"Ranked: {output_ranked}")
    print(f"Top:    {output_top}")
    print(f"Report: {output_report}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy horizon context optimizer."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_horizon_context_optimizer(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()