"""
BACQE DUKASCOPY 60 - CONTEXT REFINEMENT ENGINE

Purpose:
    Test whether removing specific sub-contexts improves
    the validated Script 59 candidate.

Initial focus:
    Hour exclusion analysis.
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"

QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_FILE = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_oos_pass_trade_profile"
    / f"symbol={SYMBOL}"
    / "tables"
    / "trade_profile_latest.csv"
)

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_context_refinement"
    / f"symbol={SYMBOL}"
)

MIN_TRADES = 50000


def ensure_dirs():
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:

    returns = (
        returns.replace([np.inf, -np.inf], np.nan)
        .dropna()
    )

    if len(returns) == 0:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())

    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else np.nan
    )

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "total_return": returns.sum(),
        "profit_factor": pf,
        "max_drawdown": drawdown.min(),
    }


def positive_month_rate(df: pd.DataFrame) -> float:

    monthly = (
        df.groupby("month")["net_signal_return"]
        .sum()
    )

    if len(monthly) == 0:
        return np.nan

    return (monthly > 0).mean()


def evaluate_scenario(
    name: str,
    scenario_df: pd.DataFrame,
    base_pf: float,
    base_return: float,
):

    stats = evaluate_returns(
        scenario_df["net_signal_return"]
    )

    if stats["trade_count"] < MIN_TRADES:
        return None

    return {
        "scenario": name,
        "trade_count": stats["trade_count"],
        "win_rate": stats["win_rate"],
        "net_return": stats["total_return"],
        "net_profit_factor": stats["profit_factor"],
        "max_drawdown": stats["max_drawdown"],
        "positive_month_rate":
            positive_month_rate(scenario_df),
        "improvement_pf":
            stats["profit_factor"] - base_pf,
        "improvement_return":
            stats["total_return"] - base_return,
    }


def main():

    print("=" * 90)
    print("BACQE DUKASCOPY 60 - CONTEXT REFINEMENT ENGINE")
    print("=" * 90)

    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs()

    if not INPUT_FILE.exists():
        print("[STOP] Missing trade profile.")
        return

    df = pd.read_csv(INPUT_FILE)

    print(f"Trades loaded: {len(df):,}")

    base_stats = evaluate_returns(
        df["net_signal_return"]
    )

    base_pf = base_stats["profit_factor"]
    base_return = base_stats["total_return"]

    print(f"Base PF:     {base_pf:.6f}")
    print(f"Base Return: {base_return:.6f}")

    results = []

    base_result = evaluate_scenario(
        "BASE",
        df,
        base_pf,
        base_return,
    )

    results.append(base_result)

    hours = sorted(df["hour"].dropna().unique())

    for hour in hours:

        scenario_df = (
            df[df["hour"] != hour]
            .copy()
        )

        result = evaluate_scenario(
            f"EXCLUDE_HOUR_{hour}",
            scenario_df,
            base_pf,
            base_return,
        )

        if result is not None:
            results.append(result)

        print(
            f"Hour {hour} tested "
            f"({len(scenario_df):,} trades)"
        )

    results_df = pd.DataFrame(results)

    results_df = results_df.sort_values(
        "improvement_pf",
        ascending=False,
    )

    results_df.insert(
        0,
        "rank",
        range(1, len(results_df) + 1)
    )

    results_path = (
        OUTPUT_ROOT
        / "tables"
        / "refinement_results_latest.csv"
    )

    ranked_path = (
        OUTPUT_ROOT
        / "tables"
        / "refinement_ranked_latest.csv"
    )

    report_path = (
        OUTPUT_ROOT
        / "reports"
        / "refinement_report_latest.txt"
    )

    results_df.to_csv(
        results_path,
        index=False,
    )

    results_df.to_csv(
        ranked_path,
        index=False,
    )

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "BACQE DUKASCOPY CONTEXT "
            "REFINEMENT REPORT\n"
        )

        f.write("=" * 80 + "\n\n")

        f.write(
            f"Trades loaded: {len(df):,}\n"
        )

        f.write(
            f"Base PF: {base_pf:.6f}\n"
        )

        f.write(
            f"Base Return: "
            f"{base_return:.6f}\n\n"
        )

        f.write(
            results_df.head(20)
            .to_string(index=False)
        )

    print("=" * 90)
    print("[DONE] Context refinement complete.")
    print(f"Results: {results_path}")
    print(f"Ranked:  {ranked_path}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()