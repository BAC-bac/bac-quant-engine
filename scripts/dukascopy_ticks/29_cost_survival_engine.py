"""
BACQE DUKASCOPY 29 - COST SURVIVAL ENGINE
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_LEDGER = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_candidate_replay"
    / "trade_ledgers" / "candidate_replay_ledger_latest.parquet"
)

OUTPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_cost_survival"

# Conservative cost model for tick-level research.
# EURUSD pip = 0.0001.
PIP_SIZE = 0.0001

# Approximate round-trip commission cost in price-return terms.
# Keep simple for research; refine later with broker-specific lot/notional modelling.
COMMISSION_PIPS_ROUND_TRIP = 0.05

# Spread cost assumptions:
# half_spread = spread / 2 per side, so round-trip spread cost ~= spread.
# We test multiple harshness levels.
COST_SCENARIOS = {
    "spread_only": {
        "spread_multiplier": 1.0,
        "commission_pips": 0.0,
    },
    "spread_plus_low_commission": {
        "spread_multiplier": 1.0,
        "commission_pips": 0.05,
    },
    "spread_plus_medium_commission": {
        "spread_multiplier": 1.0,
        "commission_pips": 0.10,
    },
    "half_spread_plus_low_commission": {
        "spread_multiplier": 0.5,
        "commission_pips": 0.05,
    },
}

MIN_TRADES = 10_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "cost_results",
        OUTPUT_ROOT / "survivors",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


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


def add_cost_columns(df: pd.DataFrame, scenario_name: str, scenario: dict) -> pd.DataFrame:
    df = df.copy()

    spread_multiplier = scenario["spread_multiplier"]
    commission_pips = scenario["commission_pips"]

    df["spread_cost_return"] = df["spread"] * spread_multiplier
    df["commission_cost_return"] = commission_pips * PIP_SIZE
    df["total_cost_return"] = df["spread_cost_return"] + df["commission_cost_return"]

    df["net_signal_return"] = df["signal_return"] - df["total_cost_return"]
    df["cost_scenario"] = scenario_name

    return df


def classify_survival(row: pd.Series) -> str:
    if (
        row["net_total_return"] > 0
        and row["net_profit_factor"] >= 1.20
        and row["net_positive_month_rate"] >= 0.70
        and row["net_positive_year_rate"] >= 0.67
    ):
        return "cost_survivor"

    if (
        row["net_total_return"] > 0
        and row["net_profit_factor"] >= 1.05
        and row["net_positive_year_rate"] >= 0.67
    ):
        return "fragile_survivor"

    return "fails_costs"


def main() -> None:
    banner("BACQE DUKASCOPY 29 - COST SURVIVAL ENGINE")

    ensure_dirs()

    print(f"Symbol:       {SYMBOL}")
    print(f"Input ledger: {INPUT_LEDGER}")
    print(f"Output root:  {OUTPUT_ROOT}")
    print("-" * 90)

    if not INPUT_LEDGER.exists():
        print("[STOP] Missing candidate replay ledger.")
        return

    ledger = pd.read_parquet(INPUT_LEDGER)

    print(f"Loaded ledger rows: {len(ledger):,}")

    required_cols = {
        "candidate_id",
        "timestamp_utc",
        "year",
        "month",
        "feature",
        "target",
        "side",
        "filter_type",
        "filter_value",
        "spread",
        "signal_return",
    }

    missing = required_cols - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["spread", "signal_return"])

    all_rows = []

    for scenario_name, scenario in COST_SCENARIOS.items():
        print(f"[SCENARIO] {scenario_name}")

        cost_df = add_cost_columns(ledger, scenario_name, scenario)

        for candidate_id, group in cost_df.groupby("candidate_id"):
            if len(group) < MIN_TRADES:
                continue

            gross_stats = evaluate_returns(group["signal_return"])
            net_stats = evaluate_returns(group["net_signal_return"])

            net_positive_day_rate = (
                group.groupby("dataset")["net_signal_return"].mean() > 0
            ).mean() if "dataset" in group.columns else np.nan

            net_positive_month_rate = (
                group.groupby("month")["net_signal_return"].sum() > 0
            ).mean()

            net_positive_year_rate = (
                group.groupby("year")["net_signal_return"].sum() > 0
            ).mean()

            all_rows.append({
                "candidate_id": candidate_id,
                "cost_scenario": scenario_name,
                "feature": group["feature"].iloc[0],
                "target": group["target"].iloc[0],
                "side": group["side"].iloc[0],
                "filter_type": group["filter_type"].iloc[0],
                "filter_value": group["filter_value"].iloc[0],
                "trade_count": len(group),
                "avg_spread": group["spread"].mean(),
                "avg_total_cost_return": group["total_cost_return"].mean(),

                "gross_win_rate": gross_stats["win_rate"],
                "gross_mean_return": gross_stats["mean_return"],
                "gross_total_return": gross_stats["total_return"],
                "gross_profit_factor": gross_stats["profit_factor"],
                "gross_max_drawdown_return": gross_stats["max_drawdown_return"],

                "net_win_rate": net_stats["win_rate"],
                "net_mean_return": net_stats["mean_return"],
                "net_total_return": net_stats["total_return"],
                "net_profit_factor": net_stats["profit_factor"],
                "net_sharpe_like": net_stats["sharpe_like"],
                "net_max_drawdown_return": net_stats["max_drawdown_return"],
                "net_positive_day_rate": net_positive_day_rate,
                "net_positive_month_rate": net_positive_month_rate,
                "net_positive_year_rate": net_positive_year_rate,
            })

    if not all_rows:
        print("[STOP] No cost survival rows generated.")
        return

    results = pd.DataFrame(all_rows)

    results["survival_label"] = results.apply(classify_survival, axis=1)

    results["net_pf_score"] = results["net_profit_factor"].clip(0, 3).fillna(0) / 3
    results["net_win_score"] = results["net_win_rate"].fillna(0)
    results["month_score"] = results["net_positive_month_rate"].fillna(0)
    results["year_score"] = results["net_positive_year_rate"].fillna(0)

    results["net_return_score"] = results["net_mean_return"].clip(lower=0)
    max_net_return = results["net_return_score"].max()

    if pd.notna(max_net_return) and max_net_return != 0:
        results["net_return_score"] = results["net_return_score"] / max_net_return
    else:
        results["net_return_score"] = 0

    results["cost_survival_score"] = (
        results["net_pf_score"] * 0.30
        + results["net_win_score"] * 0.20
        + results["month_score"] * 0.20
        + results["year_score"] * 0.20
        + results["net_return_score"] * 0.10
    )

    results = results.sort_values("cost_survival_score", ascending=False)
    results.insert(0, "cost_rank", range(1, len(results) + 1))

    survivors = results[results["survival_label"] != "fails_costs"].copy()

    output_all = OUTPUT_ROOT / "cost_results" / "cost_survival_results_latest.csv"
    output_survivors = OUTPUT_ROOT / "survivors" / "cost_survivors_latest.csv"
    output_report = OUTPUT_ROOT / "reports" / "cost_survival_report_latest.txt"

    results.to_csv(output_all, index=False)
    survivors.to_csv(output_survivors, index=False)

    with open(output_report, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY COST SURVIVAL REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Input ledger rows: {len(ledger):,}\n")
        f.write(f"Cost scenarios tested: {len(COST_SCENARIOS)}\n")
        f.write(f"Result rows: {len(results):,}\n")
        f.write(f"Survivor rows: {len(survivors):,}\n\n")

        f.write("Survival Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(results["survival_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Cost Survival Candidates\n")
        f.write("-" * 80 + "\n")

        cols = [
            "cost_rank",
            "feature",
            "target",
            "side",
            "filter_type",
            "filter_value",
            "cost_scenario",
            "trade_count",
            "avg_spread",
            "avg_total_cost_return",
            "gross_profit_factor",
            "net_win_rate",
            "net_mean_return",
            "net_total_return",
            "net_profit_factor",
            "net_positive_month_rate",
            "net_positive_year_rate",
            "survival_label",
            "cost_survival_score",
        ]

        f.write(results.head(40)[cols].to_string(index=False))

        f.write("\n\nOutputs:\n")
        f.write(f"All:       {output_all}\n")
        f.write(f"Survivors: {output_survivors}\n")

    print("=" * 90)
    print("[DONE] Cost survival complete.")
    print(f"All:       {output_all}")
    print(f"Survivors: {output_survivors}")
    print(f"Report:    {output_report}")
    print("=" * 90)


if __name__ == "__main__":
    main()