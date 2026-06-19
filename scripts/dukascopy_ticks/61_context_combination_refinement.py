"""
BACQE DUKASCOPY 61 - CONTEXT COMBINATION REFINEMENT

Purpose:
    Extend Script 60 by testing multi-hour exclusion combinations
    for the validated EURUSD context edge.

Focus:
    Base candidate:
        EURUSD
        mid_return_1 -> future_return_1000
        long
        session=asia
        day_of_week=Monday
        spread_regime=low_spread

Question:
    Can excluding combinations of weaker hours improve PF,
    return, drawdown, and monthly consistency?
"""

from pathlib import Path
from itertools import combinations

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
    / "dukascopy_context_combination_refinement"
    / f"symbol={SYMBOL}"
)

MIN_TRADES = 50_000
MAX_COMBINATION_SIZE = 3


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
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

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown": drawdown.min(),
    }


def positive_month_rate(df: pd.DataFrame) -> float:
    monthly = df.groupby("month")["net_signal_return"].sum()

    if monthly.empty:
        return np.nan

    return (monthly > 0).mean()


def positive_year_rate(df: pd.DataFrame) -> float:
    yearly = df.groupby("year")["net_signal_return"].sum()

    if yearly.empty:
        return np.nan

    return (yearly > 0).mean()


def evaluate_scenario(
    scenario_name: str,
    scenario_df: pd.DataFrame,
    excluded_hours: tuple,
    base_pf: float,
    base_return: float,
    base_drawdown: float,
) -> dict | None:
    stats = evaluate_returns(scenario_df["net_signal_return"])

    if stats["trade_count"] < MIN_TRADES:
        return None

    return {
        "scenario": scenario_name,
        "excluded_hours": ",".join(str(h) for h in excluded_hours) if excluded_hours else "none",
        "excluded_hour_count": len(excluded_hours),
        "trade_count": stats["trade_count"],
        "win_rate": stats["win_rate"],
        "net_return": stats["total_return"],
        "net_profit_factor": stats["profit_factor"],
        "max_drawdown": stats["max_drawdown"],
        "positive_month_rate": positive_month_rate(scenario_df),
        "positive_year_rate": positive_year_rate(scenario_df),
        "improvement_pf": stats["profit_factor"] - base_pf,
        "improvement_return": stats["total_return"] - base_return,
        "drawdown_change": stats["max_drawdown"] - base_drawdown,
    }


def score_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["pf_score"] = df["net_profit_factor"].clip(0, 2).fillna(0) / 2
    df["month_score"] = df["positive_month_rate"].fillna(0)
    df["year_score"] = df["positive_year_rate"].fillna(0)
    df["return_score"] = (df["net_return"] > 0).astype(int)

    df["refinement_score"] = (
        df["pf_score"] * 0.40
        + df["month_score"] * 0.25
        + df["year_score"] * 0.20
        + df["return_score"] * 0.15
    )

    df = df.sort_values(
        ["net_profit_factor", "positive_month_rate", "net_return"],
        ascending=[False, False, False],
    )

    df.insert(0, "rank", range(1, len(df) + 1))

    return df


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 61 - CONTEXT COMBINATION REFINEMENT")
    print("=" * 90)
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs()

    if not INPUT_FILE.exists():
        print(f"[STOP] Missing input file: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)

    print(f"Trades loaded: {len(df):,}")

    required_cols = {"hour", "month", "year", "net_signal_return"}

    missing = required_cols - set(df.columns)
    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df["net_signal_return"] = pd.to_numeric(df["net_signal_return"], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["hour", "net_signal_return"])

    df["hour"] = df["hour"].astype(int)

    base_stats = evaluate_returns(df["net_signal_return"])

    base_pf = base_stats["profit_factor"]
    base_return = base_stats["total_return"]
    base_drawdown = base_stats["max_drawdown"]

    print(f"Base PF:       {base_pf:.6f}")
    print(f"Base Return:   {base_return:.6f}")
    print(f"Base Drawdown: {base_drawdown:.6f}")

    hours = sorted(df["hour"].unique())

    results = []

    base_result = evaluate_scenario(
        scenario_name="BASE",
        scenario_df=df,
        excluded_hours=tuple(),
        base_pf=base_pf,
        base_return=base_return,
        base_drawdown=base_drawdown,
    )

    if base_result is not None:
        results.append(base_result)

    for combo_size in range(1, MAX_COMBINATION_SIZE + 1):
        for excluded in combinations(hours, combo_size):
            scenario_df = df[~df["hour"].isin(excluded)].copy()

            scenario_name = "EXCLUDE_HOURS_" + "_".join(str(h) for h in excluded)

            result = evaluate_scenario(
                scenario_name=scenario_name,
                scenario_df=scenario_df,
                excluded_hours=excluded,
                base_pf=base_pf,
                base_return=base_return,
                base_drawdown=base_drawdown,
            )

            if result is not None:
                results.append(result)

        print(f"Completed combinations of size {combo_size}")

    results_df = pd.DataFrame(results)
    ranked = score_results(results_df)

    results_path = OUTPUT_ROOT / "tables" / "combination_refinement_results_latest.csv"
    ranked_path = OUTPUT_ROOT / "tables" / "combination_refinement_ranked_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "combination_refinement_report_latest.txt"

    results_df.to_csv(results_path, index=False)
    ranked.to_csv(ranked_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CONTEXT COMBINATION REFINEMENT REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Trades loaded: {len(df):,}\n")
        f.write(f"Base PF: {base_pf:.6f}\n")
        f.write(f"Base Return: {base_return:.6f}\n")
        f.write(f"Base Drawdown: {base_drawdown:.6f}\n")
        f.write(f"Scenarios tested: {len(ranked):,}\n\n")

        f.write("TOP COMBINATION REFINEMENTS\n")
        f.write("-" * 80 + "\n")
        f.write(
            ranked.head(30)[
                [
                    "rank",
                    "scenario",
                    "excluded_hours",
                    "trade_count",
                    "win_rate",
                    "net_return",
                    "net_profit_factor",
                    "max_drawdown",
                    "positive_month_rate",
                    "positive_year_rate",
                    "improvement_pf",
                    "improvement_return",
                    "drawdown_change",
                    "refinement_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Results: {results_path}\n")
        f.write(f"Ranked: {ranked_path}\n")

    print("=" * 90)
    print("[DONE] Context combination refinement complete.")
    print(f"Results: {results_path}")
    print(f"Ranked:  {ranked_path}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()