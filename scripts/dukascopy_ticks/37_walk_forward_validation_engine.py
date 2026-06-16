"""
BACQE DUKASCOPY 37 - WALK FORWARD VALIDATION ENGINE

Purpose:
    Validate Script 35 context survivors using walk-forward year splits:

    2023 -> 2024
    2024 -> 2025

Input:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_horizon_context_replay\\trade_ledgers\\horizon_context_replay_ledger_latest.parquet

Output:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_walk_forward_validation
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

def build_input_ledger(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_context_replay"
        / f"symbol={symbol}"
        / "trade_ledgers"
        / "horizon_context_replay_ledger_latest.parquet"
    )


def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_walk_forward_validation"
        / f"symbol={symbol}"
    )

MIN_TRADES_PER_SPLIT = 5_000

WALK_FORWARD_SPLITS = [
    {"train_year": 2023, "test_year": 2024, "split_name": "train_2023_test_2024"},
    {"train_year": 2024, "test_year": 2025, "split_name": "train_2024_test_2025"},
]


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "walk_forward_results",
        output_root / "rankings",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
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


def classify_walk_forward(row: pd.Series) -> str:
    if (
        row["splits_tested"] == 2
        and row["splits_passed"] == 2
        and row["min_test_profit_factor"] >= 1.05
        and row["min_test_total_return"] > 0
    ):
        return "walk_forward_pass"

    if (
        row["splits_tested"] == 2
        and row["splits_passed"] >= 1
        and row["avg_test_profit_factor"] >= 1.00
    ):
        return "walk_forward_watchlist"

    return "walk_forward_reject"


def run_walk_forward_validation(
    symbol: str = DEFAULT_SYMBOL,
) -> None:
    symbol = symbol.upper().strip()

    input_ledger = build_input_ledger(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 37 - WALK FORWARD VALIDATION ENGINE")

    ensure_dirs(output_root)

    print(f"Symbol:       {symbol}")
    print(f"Input ledger: {input_ledger}")
    print(f"Output root:  {output_root}")
    print("-" * 90)

    if not input_ledger.exists():
        print("[STOP] Missing Script 35 context replay ledger.")
        return

    ledger = pd.read_parquet(input_ledger)

    print(f"Loaded ledger rows: {len(ledger):,}")

    required = {
        "replay_id",
        "feature",
        "target",
        "side",
        "context_type",
        "context_value",
        "year",
        "net_signal_return",
    }

    missing = required - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["year", "net_signal_return"])
    ledger["year"] = ledger["year"].astype(int)

    split_rows = []

    for replay_id, group in ledger.groupby("replay_id"):
        feature = group["feature"].iloc[0]
        target = group["target"].iloc[0]
        side = group["side"].iloc[0]
        context_type = group["context_type"].iloc[0]
        context_value = group["context_value"].iloc[0]

        for split in WALK_FORWARD_SPLITS:
            train_year = split["train_year"]
            test_year = split["test_year"]
            split_name = split["split_name"]

            train_df = group[group["year"] == train_year]
            test_df = group[group["year"] == test_year]

            train_stats = evaluate_returns(train_df["net_signal_return"])
            test_stats = evaluate_returns(test_df["net_signal_return"])

            split_pass = (
                train_stats["trade_count"] >= MIN_TRADES_PER_SPLIT
                and test_stats["trade_count"] >= MIN_TRADES_PER_SPLIT
                and train_stats["profit_factor"] >= 1.00
                and test_stats["profit_factor"] >= 1.00
                and test_stats["total_return"] > 0
            )

            split_rows.append({
                "replay_id": replay_id,
                "feature": feature,
                "target": target,
                "side": side,
                "context_type": context_type,
                "context_value": context_value,
                "split_name": split_name,
                "train_year": train_year,
                "test_year": test_year,

                "train_trade_count": train_stats["trade_count"],
                "train_win_rate": train_stats["win_rate"],
                "train_mean_return": train_stats["mean_return"],
                "train_total_return": train_stats["total_return"],
                "train_profit_factor": train_stats["profit_factor"],
                "train_max_drawdown_return": train_stats["max_drawdown_return"],

                "test_trade_count": test_stats["trade_count"],
                "test_win_rate": test_stats["win_rate"],
                "test_mean_return": test_stats["mean_return"],
                "test_total_return": test_stats["total_return"],
                "test_profit_factor": test_stats["profit_factor"],
                "test_max_drawdown_return": test_stats["max_drawdown_return"],

                "split_pass": split_pass,
            })

    split_df = pd.DataFrame(split_rows)

    if split_df.empty:
        print("[STOP] No walk-forward split rows generated.")
        return

    summary_rows = []

    for replay_id, group in split_df.groupby("replay_id"):
        passed = int(group["split_pass"].sum())
        tested = int(len(group))

        summary_rows.append({
            "replay_id": replay_id,
            "feature": group["feature"].iloc[0],
            "target": group["target"].iloc[0],
            "side": group["side"].iloc[0],
            "context_type": group["context_type"].iloc[0],
            "context_value": group["context_value"].iloc[0],

            "splits_tested": tested,
            "splits_passed": passed,

            "avg_train_profit_factor": group["train_profit_factor"].mean(),
            "avg_test_profit_factor": group["test_profit_factor"].mean(),
            "min_test_profit_factor": group["test_profit_factor"].min(),
            "max_test_profit_factor": group["test_profit_factor"].max(),

            "avg_test_win_rate": group["test_win_rate"].mean(),
            "avg_test_total_return": group["test_total_return"].mean(),
            "min_test_total_return": group["test_total_return"].min(),
            "total_test_return": group["test_total_return"].sum(),

            "avg_test_drawdown": group["test_max_drawdown_return"].mean(),
            "worst_test_drawdown": group["test_max_drawdown_return"].min(),
        })

    summary = pd.DataFrame(summary_rows)

    summary["walk_forward_label"] = summary.apply(classify_walk_forward, axis=1)

    summary["wf_score"] = (
        (summary["splits_passed"] / summary["splits_tested"]).fillna(0) * 0.30
        + summary["min_test_profit_factor"].clip(0, 2).fillna(0) / 2 * 0.30
        + summary["avg_test_profit_factor"].clip(0, 2).fillna(0) / 2 * 0.20
        + summary["avg_test_win_rate"].fillna(0) * 0.10
        + (summary["total_test_return"].clip(lower=0) / summary["total_test_return"].clip(lower=0).max()).fillna(0) * 0.10
    )

    summary = summary.sort_values("wf_score", ascending=False)
    summary.insert(0, "wf_rank", range(1, len(summary) + 1))

    split_path = output_root / "walk_forward_results" / "walk_forward_split_results_latest.csv"
    summary_path = output_root / "rankings" / "walk_forward_ranked_latest.csv"
    report_path = output_root / "reports" / "walk_forward_validation_report_latest.txt"

    split_df.to_csv(split_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY WALK FORWARD VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input ledger rows: {len(ledger):,}\n")
        f.write(f"Walk-forward split rows: {len(split_df):,}\n")
        f.write(f"Candidates tested: {len(summary):,}\n\n")

        f.write("Walk Forward Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(summary["walk_forward_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Walk Forward Candidates\n")
        f.write("-" * 80 + "\n")

        f.write(
            summary.head(30)[
                [
                    "wf_rank",
                    "feature",
                    "target",
                    "side",
                    "context_type",
                    "context_value",
                    "splits_tested",
                    "splits_passed",
                    "avg_train_profit_factor",
                    "avg_test_profit_factor",
                    "min_test_profit_factor",
                    "total_test_return",
                    "walk_forward_label",
                    "wf_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nDetailed Split Results\n")
        f.write("-" * 80 + "\n")

        f.write(
            split_df.sort_values(
                ["replay_id", "split_name"]
            )[
                [
                    "feature",
                    "target",
                    "side",
                    "context_type",
                    "context_value",
                    "split_name",
                    "train_profit_factor",
                    "train_total_return",
                    "test_profit_factor",
                    "test_total_return",
                    "split_pass",
                ]
            ].head(80).to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"Split results: {split_path}\n")
        f.write(f"Ranked:        {summary_path}\n")

    print("=" * 90)
    print("[DONE] Walk-forward validation complete.")
    print(f"Split results: {split_path}")
    print(f"Ranked:        {summary_path}")
    print(f"Report:        {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy walk forward validation."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_walk_forward_validation(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()