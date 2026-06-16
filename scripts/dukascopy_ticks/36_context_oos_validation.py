"""
BACQE DUKASCOPY 36 - CONTEXT OOS VALIDATION

Purpose:
    Validate Script 35 horizon context survivors by year:
    - 2023 train
    - 2024 validation
    - 2025 out-of-sample
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
        / "dukascopy_context_oos_validation"
        / f"symbol={symbol}"
    )

MIN_TRADES_PER_YEAR = 5_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "yearly_validation",
        output_root / "oos_rankings",
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


def classify_oos(row: pd.Series) -> str:
    if (
        row["train_profit_factor"] >= 1.05
        and row["validation_profit_factor"] >= 1.05
        and row["oos_profit_factor"] >= 1.05
        and row["profitable_years"] == 3
    ):
        return "oos_pass"

    if (
        row["validation_profit_factor"] >= 1.00
        and row["oos_profit_factor"] >= 1.00
        and row["profitable_years"] >= 2
    ):
        return "oos_watchlist"

    return "oos_reject"


def run_context_oos_validation(
    symbol: str = DEFAULT_SYMBOL,
) -> None:
    symbol = symbol.upper().strip()

    input_ledger = build_input_ledger(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 36 - CONTEXT OOS VALIDATION")

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
        "month",
        "net_signal_return",
    }

    missing = required - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["net_signal_return", "year"])

    ledger["year"] = ledger["year"].astype(int)

    yearly_rows = []

    for replay_id, group in ledger.groupby("replay_id"):
        for year, year_df in group.groupby("year"):
            stats = evaluate_returns(year_df["net_signal_return"])

            yearly_rows.append({
                "replay_id": replay_id,
                "feature": group["feature"].iloc[0],
                "target": group["target"].iloc[0],
                "side": group["side"].iloc[0],
                "context_type": group["context_type"].iloc[0],
                "context_value": group["context_value"].iloc[0],
                "year": year,
                **stats,
            })

    yearly = pd.DataFrame(yearly_rows)

    if yearly.empty:
        print("[STOP] No yearly validation rows generated.")
        return

    yearly["phase"] = np.select(
        [
            yearly["year"] == 2023,
            yearly["year"] == 2024,
            yearly["year"] == 2025,
        ],
        [
            "train_2023",
            "validation_2024",
            "oos_2025",
        ],
        default="other",
    )

    pivot_rows = []

    for replay_id, group in yearly.groupby("replay_id"):
        base = {
            "replay_id": replay_id,
            "feature": group["feature"].iloc[0],
            "target": group["target"].iloc[0],
            "side": group["side"].iloc[0],
            "context_type": group["context_type"].iloc[0],
            "context_value": group["context_value"].iloc[0],
        }

        for phase_name, prefix in [
            ("train_2023", "train"),
            ("validation_2024", "validation"),
            ("oos_2025", "oos"),
        ]:
            phase_df = group[group["phase"] == phase_name]

            if phase_df.empty:
                base[f"{prefix}_trade_count"] = 0
                base[f"{prefix}_win_rate"] = np.nan
                base[f"{prefix}_mean_return"] = np.nan
                base[f"{prefix}_total_return"] = np.nan
                base[f"{prefix}_profit_factor"] = np.nan
                base[f"{prefix}_max_drawdown_return"] = np.nan
            else:
                row = phase_df.iloc[0]
                base[f"{prefix}_trade_count"] = row["trade_count"]
                base[f"{prefix}_win_rate"] = row["win_rate"]
                base[f"{prefix}_mean_return"] = row["mean_return"]
                base[f"{prefix}_total_return"] = row["total_return"]
                base[f"{prefix}_profit_factor"] = row["profit_factor"]
                base[f"{prefix}_max_drawdown_return"] = row["max_drawdown_return"]

        pivot_rows.append(base)

    oos = pd.DataFrame(pivot_rows)

    oos["profitable_years"] = (
        (oos["train_total_return"] > 0).astype(int)
        + (oos["validation_total_return"] > 0).astype(int)
        + (oos["oos_total_return"] > 0).astype(int)
    )

    oos["minimum_year_pf"] = oos[
        [
            "train_profit_factor",
            "validation_profit_factor",
            "oos_profit_factor",
        ]
    ].min(axis=1)

    oos["minimum_year_return"] = oos[
        [
            "train_total_return",
            "validation_total_return",
            "oos_total_return",
        ]
    ].min(axis=1)

    oos["oos_label"] = oos.apply(classify_oos, axis=1)

    oos["oos_score"] = (
        oos["minimum_year_pf"].clip(0, 2).fillna(0) / 2 * 0.35
        + oos["profitable_years"].fillna(0) / 3 * 0.25
        + oos["oos_profit_factor"].clip(0, 2).fillna(0) / 2 * 0.25
        + oos["oos_win_rate"].fillna(0) * 0.15
    )

    oos = oos.sort_values("oos_score", ascending=False)
    oos.insert(0, "oos_rank", range(1, len(oos) + 1))

    yearly_path = output_root / "yearly_validation" / "context_oos_yearly_latest.csv"
    oos_path = output_root / "oos_rankings" / "context_oos_ranked_latest.csv"
    report_path = output_root / "reports" / "context_oos_validation_report_latest.txt"

    yearly.to_csv(yearly_path, index=False)
    oos.to_csv(oos_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CONTEXT OOS VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input ledger rows: {len(ledger):,}\n")
        f.write(f"Yearly rows: {len(yearly):,}\n")
        f.write(f"OOS candidates: {len(oos):,}\n\n")

        f.write("OOS Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(oos["oos_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top OOS Candidates\n")
        f.write("-" * 80 + "\n")

        f.write(
            oos.head(30)[
                [
                    "oos_rank",
                    "feature",
                    "target",
                    "side",
                    "context_type",
                    "context_value",
                    "train_trade_count",
                    "train_profit_factor",
                    "train_total_return",
                    "validation_trade_count",
                    "validation_profit_factor",
                    "validation_total_return",
                    "oos_trade_count",
                    "oos_profit_factor",
                    "oos_total_return",
                    "profitable_years",
                    "minimum_year_pf",
                    "oos_label",
                    "oos_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"Yearly: {yearly_path}\n")
        f.write(f"Ranked: {oos_path}\n")

    print("=" * 90)
    print("[DONE] Context OOS validation complete.")
    print(f"Yearly: {yearly_path}")
    print(f"Ranked: {oos_path}")
    print(f"Report: {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy context OOS validation."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_context_oos_validation(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()