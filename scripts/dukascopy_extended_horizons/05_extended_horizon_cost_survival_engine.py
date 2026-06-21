"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 05 - EXTENDED HORIZON COST SURVIVAL ENGINE

Purpose:
    Stress-test validated extended-horizon EURJPY signals after estimated
    transaction costs.

Input:
    Script 04 raw signal validation report.

Output:
    Cost-adjusted survivor reports.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"

BASE_DIR = Path("E:/Quant_Lab")

VALIDATION_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "signal_validation"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cost_survival"
)

COST_SCENARIOS = {
    "zero_cost": 0.0,
    "half_pip": 0.005,
    "one_pip": 0.010,
    "two_pips": 0.020,
    "three_pips": 0.030,
}

JPY_PIP_SIZE = 0.01


def print_header(symbol: str) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 05 - EXTENDED HORIZON COST SURVIVAL ENGINE")
    print("=" * 90)
    print(f"Symbol:          {symbol}")
    print(f"Validation root: {VALIDATION_ROOT}")
    print(f"Report root:     {REPORT_ROOT}")
    print("-" * 90)


def load_validation_raw(symbol: str) -> pd.DataFrame:
    path = VALIDATION_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_raw_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 04 raw validation file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 04 raw validation report is empty.")

    return df


def safe_profit_factor_from_totals(gross_positive: float, gross_negative: float) -> float:
    if gross_negative == 0:
        return np.inf if gross_positive > 0 else np.nan

    return float(gross_positive / abs(gross_negative))


def apply_cost_scenarios(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []

    required_cols = [
        "target",
        "feature",
        "candidate_side",
        "threshold_quantile",
        "threshold_side",
        "trades",
        "win_rate",
        "avg_return",
        "median_return",
        "total_return",
        "profit_factor",
        "files_tested",
    ]

    # files_tested is not in raw; it exists in ranked, so do not require it.
    required_cols = [col for col in required_cols if col != "files_tested"]

    missing = [col for col in required_cols if col not in raw.columns]
    if missing:
        raise ValueError(f"Missing required columns from validation raw report: {missing}")

    for scenario_name, cost_pips in COST_SCENARIOS.items():
        cost_return = cost_pips * JPY_PIP_SIZE

        temp = raw.copy()

        temp["cost_scenario"] = scenario_name
        temp["cost_pips"] = cost_pips
        temp["cost_return_per_trade"] = cost_return

        temp["net_avg_return"] = temp["avg_return"] - cost_return
        temp["net_median_return"] = temp["median_return"] - cost_return
        temp["net_total_return"] = temp["total_return"] - (temp["trades"] * cost_return)

        temp["net_positive_return_proxy"] = np.where(
            temp["net_total_return"] > 0,
            temp["net_total_return"],
            0.0,
        )

        temp["net_negative_return_proxy"] = np.where(
            temp["net_total_return"] < 0,
            temp["net_total_return"],
            0.0,
        )

        rows.append(temp)

    return pd.concat(rows, ignore_index=True)


def aggregate_cost_survival(costed: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        costed.groupby(
            [
                "cost_scenario",
                "cost_pips",
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
            ],
            dropna=False,
        )
        .agg(
            files_tested=("file", "nunique"),
            total_trades=("trades", "sum"),
            raw_mean_win_rate=("win_rate", "mean"),
            raw_median_win_rate=("win_rate", "median"),
            raw_mean_avg_return=("avg_return", "mean"),
            raw_median_avg_return=("median_return", "median"),
            raw_total_return=("total_return", "sum"),
            raw_median_profit_factor=("profit_factor", "median"),
            net_mean_avg_return=("net_avg_return", "mean"),
            net_median_avg_return=("net_avg_return", "median"),
            net_mean_median_return=("net_median_return", "mean"),
            net_median_return=("net_median_return", "median"),
            net_total_return=("net_total_return", "sum"),
            net_positive_proxy=("net_positive_return_proxy", "sum"),
            net_negative_proxy=("net_negative_return_proxy", "sum"),
            worst_file_net_return=("net_total_return", "min"),
            best_file_net_return=("net_total_return", "max"),
        )
        .reset_index()
    )

    grouped["net_profit_factor_proxy"] = grouped.apply(
        lambda row: safe_profit_factor_from_totals(
            row["net_positive_proxy"],
            row["net_negative_proxy"],
        ),
        axis=1,
    )

    grouped["cost_survival_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["net_median_avg_return"].fillna(0) * 100000
        + (grouped["raw_median_win_rate"].fillna(0.5) - 0.5) * 100
        + np.log1p(grouped["total_trades"].fillna(0))
    )

    grouped["survival_status"] = np.select(
        [
            (grouped["net_total_return"] > 0)
            & (grouped["net_median_avg_return"] > 0)
            & (grouped["raw_median_win_rate"] > 0.52)
            & (grouped["net_profit_factor_proxy"] > 1.10),

            (grouped["net_total_return"] > 0)
            & (grouped["net_median_avg_return"] > 0)
            & (grouped["raw_median_win_rate"] > 0.505),

            grouped["net_total_return"] <= 0,
        ],
        [
            "cost_survivor_primary",
            "cost_survivor_secondary",
            "cost_fail",
        ],
        default="cost_watchlist",
    )

    grouped = grouped.sort_values(
        by=[
            "cost_scenario",
            "survival_status",
            "cost_survival_score",
            "net_total_return",
            "net_median_avg_return",
        ],
        ascending=[True, True, False, False, False],
    )

    return grouped


def write_outputs(symbol: str, costed_raw: pd.DataFrame, ranked: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_cost_survival_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_cost_survival_ranked_latest.csv"
    survivors_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_cost_survivors_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_cost_survival_report_latest.txt"

    costed_raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)

    survivors = ranked[
        ranked["survival_status"].isin(
            ["cost_survivor_primary", "cost_survivor_secondary"]
        )
    ].copy()

    survivors.to_csv(survivors_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 05 - EXTENDED HORIZON COST SURVIVAL REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Raw costed rows: {len(costed_raw)}\n")
        f.write(f"Ranked cost rows: {len(ranked)}\n")
        f.write(f"Survivors: {len(survivors)}\n\n")

        f.write("STATUS COUNTS BY COST SCENARIO\n")
        f.write("-" * 90 + "\n")

        if ranked.empty:
            f.write("No ranked cost survival rows produced.\n")
        else:
            f.write(
                ranked.groupby(["cost_scenario", "survival_status"])
                .size()
                .to_string()
            )
            f.write("\n\n")

            display_cols = [
                "cost_scenario",
                "cost_pips",
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
                "survival_status",
                "cost_survival_score",
                "total_trades",
                "raw_median_win_rate",
                "net_median_avg_return",
                "net_total_return",
                "net_profit_factor_proxy",
                "files_tested",
            ]

            f.write("TOP 75 COST SURVIVAL CANDIDATES\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[display_cols].head(75).to_string(index=False))

    print(f"Raw costed file:  {raw_path}")
    print(f"Ranked file:      {ranked_path}")
    print(f"Survivors file:   {survivors_path}")
    print(f"Text report:      {txt_path}")


def main(symbol: str) -> None:
    print_header(symbol)

    raw = load_validation_raw(symbol)

    print(f"Validation raw rows loaded: {len(raw):,}")
    print("-" * 90)

    costed_raw = apply_cost_scenarios(raw)
    ranked = aggregate_cost_survival(costed_raw)

    print(f"Costed raw rows: {len(costed_raw):,}")
    print(f"Ranked rows:     {len(ranked):,}")

    if not ranked.empty:
        print("-" * 90)
        print("Survival status counts:")
        print(ranked.groupby(["cost_scenario", "survival_status"]).size())

    print("-" * 90)

    write_outputs(symbol, costed_raw, ranked)

    print("-" * 90)
    print("[DONE] Extended horizon cost survival engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol to process, e.g. EURJPY",
    )

    args = parser.parse_args()

    main(symbol=args.symbol.upper())