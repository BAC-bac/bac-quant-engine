from pathlib import Path
import numpy as np
import pandas as pd

DATA_ROOT = Path(r"E:\Quant_Lab\data")

INPUT_PATH = (
    DATA_ROOT / "analysis" / "dukascopy_ticks" / "oos_context_validation"
    / "EURUSD_2023-01-01_to_2025-12-31_oos_context_trades.csv"
)

OUTPUT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "cost_survival_refinement"

TARGET_CONTEXTS = [
    "tuesday_london_mid_morning_all",
    "friday_long",
]

COST_TESTS = [0.00000, 0.00002, 0.00005, 0.00010]

FILTERS = [
    ("none", None, None),
    ("spread_mean_lte_median", "spread_mean", "lte_median"),
    ("range_points_gte_median", "range_points", "gte_median"),
    ("tick_count_gte_median", "tick_count", "gte_median"),
    ("dominant_pressure_gte_065", "dominant_pressure", "gte_065"),
    ("dominant_pressure_gte_070", "dominant_pressure", "gte_070"),
]


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return np.nan

    equity = (1 + returns.fillna(0)).cumprod()
    running_max = equity.cummax()
    drawdown = equity / running_max - 1

    return float(drawdown.min())


def apply_filter(df: pd.DataFrame, filter_col: str | None, filter_rule: str | None) -> pd.DataFrame:
    if filter_col is None:
        return df.copy()

    if filter_col not in df.columns:
        return df.iloc[0:0].copy()

    if filter_rule == "lte_median":
        return df[df[filter_col] <= df[filter_col].median()].copy()

    if filter_rule == "gte_median":
        return df[df[filter_col] >= df[filter_col].median()].copy()

    if filter_rule == "gte_065":
        return df[df[filter_col] >= 0.65].copy()

    if filter_rule == "gte_070":
        return df[df[filter_col] >= 0.70].copy()

    return df.copy()


def summarise(df: pd.DataFrame, cost: float) -> dict:
    if df.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "net_avg_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "max_drawdown": np.nan,
            "unique_dates": 0,
            "long_trades": 0,
            "short_trades": 0,
        }

    temp = df.copy()

    temp["gross_return"] = np.where(
        temp["signal"] == "long",
        temp["next_return"],
        -temp["next_return"],
    )

    temp["net_return_refined"] = temp["gross_return"] - cost
    temp["is_win_refined"] = temp["net_return_refined"] > 0

    returns = temp["net_return_refined"]

    return {
        "trade_count": len(temp),
        "win_rate": temp["is_win_refined"].mean(),
        "net_avg_return": returns.mean(),
        "median_return": returns.median(),
        "total_return_sum": returns.sum(),
        "profit_factor": profit_factor(returns),
        "sharpe_like": returns.mean() / returns.std() if returns.std() and returns.std() > 0 else np.nan,
        "max_drawdown": max_drawdown(returns),
        "unique_dates": temp["date"].nunique(),
        "long_trades": int((temp["signal"] == "long").sum()),
        "short_trades": int((temp["signal"] == "short").sum()),
    }


def classify_result(row: dict) -> str:
    if row["trade_count"] < 20:
        return "insufficient_sample"

    if row["cost_per_trade"] > 0 and row["net_avg_return"] > 0 and row["profit_factor"] > 1:
        return "cost_survival_pass"

    if row["cost_per_trade"] == 0 and row["net_avg_return"] > 0 and row["profit_factor"] > 1:
        return "no_cost_positive"

    return "reject"


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 21 - COST SURVIVAL FILTER REFINEMENT")
    print("=" * 90)

    if not INPUT_PATH.exists():
        print(f"[ERROR] Input missing: {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded OOS trades: {len(df):,}")

    rows = []

    for context_name in TARGET_CONTEXTS:
        base = df[df["context_name"] == context_name].copy()

        print("\n" + "-" * 90)
        print(f"[CONTEXT] {context_name}")
        print(f"Base rows including cost duplicates: {len(base):,}")

        # Keep one copy of each trade before recalculating costs
        dedupe_cols = ["timestamp_start", "timestamp_end", "context_name", "signal"]
        base = base.drop_duplicates(subset=[c for c in dedupe_cols if c in base.columns]).copy()

        print(f"Unique trade rows: {len(base):,}")

        for filter_name, filter_col, filter_rule in FILTERS:
            filtered = apply_filter(base, filter_col, filter_rule)

            for cost in COST_TESTS:
                summary = summarise(filtered, cost)

                row = {
                    "context_name": context_name,
                    "filter_name": filter_name,
                    "filter_col": filter_col,
                    "filter_rule": filter_rule,
                    "cost_per_trade": cost,
                    **summary,
                }

                row["result_label"] = classify_result(row)
                rows.append(row)

                print(
                    f"{filter_name:<30} | "
                    f"cost={cost:.5f} | "
                    f"trades={row['trade_count']:>4} | "
                    f"avg={row['net_avg_return'] if pd.notna(row['net_avg_return']) else np.nan:.8f} | "
                    f"pf={row['profit_factor'] if pd.notna(row['profit_factor']) else np.nan:.3f} | "
                    f"{row['result_label']}"
                )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame(rows)

    summary_path = OUTPUT_ROOT / "EURUSD_2024-04-01_to_2024-06-30_cost_survival_filter_summary.csv"
    ranked_path = OUTPUT_ROOT / "EURUSD_2024-04-01_to_2024-06-30_cost_survival_filter_ranked.csv"

    summary_df.to_csv(summary_path, index=False)

    ranked = summary_df.sort_values(
        ["result_label", "profit_factor", "net_avg_return", "trade_count"],
        ascending=[True, False, False, False],
    )
    ranked.to_csv(ranked_path, index=False)

    print("\n" + "-" * 90)
    print("[TOP RESULTS]")
    cols = [
        "context_name",
        "filter_name",
        "cost_per_trade",
        "trade_count",
        "win_rate",
        "net_avg_return",
        "profit_factor",
        "sharpe_like",
        "max_drawdown",
        "unique_dates",
        "result_label",
    ]
    print(ranked[cols].head(20).to_string(index=False))

    print("-" * 90)
    print(f"Summary: {summary_path}")
    print(f"Ranked:  {ranked_path}")
    print("[DONE] Cost survival filter refinement complete.")


if __name__ == "__main__":
    main()