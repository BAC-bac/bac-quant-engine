"""
BACQE DUKASCOPY 19 - CONTEXT STABILITY AUDIT

Purpose:
    Test whether the strongest dominant-pressure contexts from Script 18
    remain stable across Jan, Feb, and Mar 2024 independently.

Input:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\dominant_pressure_replay\\EURUSD_2024-01-01_to_2024-03-31_dominant_pressure_trades.csv

Outputs:
    Context-level month-by-month stability reports.
"""

from pathlib import Path

import numpy as np
import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

INPUT_PATH = (
    DATA_ROOT
    / "analysis"
    / "dukascopy_ticks"
    / "dominant_pressure_replay"
    / "EURUSD_2024-01-01_to_2024-03-31_dominant_pressure_trades.csv"
)

OUTPUT_ROOT = (
    DATA_ROOT
    / "analysis"
    / "dukascopy_ticks"
    / "context_stability"
)

SYMBOL = "EURUSD"

TARGET_PRESSURE_THRESHOLD = 0.60
TARGET_COST = 0.00000

MIN_TRADES_PER_MONTH = 5
MIN_TOTAL_TRADES = 20


CONTEXTS = [
    {
        "context_name": "hour_10_all",
        "hour": 10,
        "session": None,
        "weekday": None,
        "signal": None,
    },
    {
        "context_name": "hour_11_all",
        "hour": 11,
        "session": None,
        "weekday": None,
        "signal": None,
    },
    {
        "context_name": "tuesday_london_mid_morning_all",
        "hour": None,
        "session": "london_mid_morning",
        "weekday": "Tuesday",
        "signal": None,
    },
    {
        "context_name": "pre_new_york_long",
        "hour": None,
        "session": "pre_new_york",
        "weekday": None,
        "signal": "long",
    },
    {
        "context_name": "tuesday_short",
        "hour": None,
        "session": None,
        "weekday": "Tuesday",
        "signal": "short",
    },
    {
        "context_name": "thursday_long",
        "hour": None,
        "session": None,
        "weekday": "Thursday",
        "signal": "long",
    },
    {
        "context_name": "friday_long",
        "hour": None,
        "session": None,
        "weekday": "Friday",
        "signal": "long",
    },
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


def summarise_returns(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "avg_return": np.nan,
            "median_return": np.nan,
            "total_return_sum": 0.0,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "max_drawdown": np.nan,
            "long_trades": 0,
            "short_trades": 0,
            "unique_dates": 0,
        }

    returns = df["net_return"]

    return {
        "trade_count": len(df),
        "win_rate": df["is_win"].mean(),
        "avg_return": returns.mean(),
        "median_return": returns.median(),
        "total_return_sum": returns.sum(),
        "profit_factor": profit_factor(returns),
        "sharpe_like": (
            returns.mean() / returns.std()
            if returns.std() and returns.std() > 0
            else np.nan
        ),
        "max_drawdown": max_drawdown(returns),
        "long_trades": int((df["signal"] == "long").sum()),
        "short_trades": int((df["signal"] == "short").sum()),
        "unique_dates": df["date"].nunique(),
    }


def apply_context(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    out = df.copy()

    if context.get("hour") is not None:
        out = out[out["hour"] == context["hour"]]

    if context.get("session") is not None:
        out = out[out["session"] == context["session"]]

    if context.get("weekday") is not None:
        out = out[out["weekday"] == context["weekday"]]

    if context.get("signal") is not None:
        out = out[out["signal"] == context["signal"]]

    return out


def classify_stability(month_rows: pd.DataFrame, total_summary: dict) -> str:
    valid_months = month_rows[month_rows["trade_count"] >= MIN_TRADES_PER_MONTH]

    if total_summary["trade_count"] < MIN_TOTAL_TRADES:
        return "insufficient_total_sample"

    if len(valid_months) < 3:
        return "insufficient_monthly_sample"

    positive_months = (valid_months["avg_return"] > 0).sum()
    pf_above_one_months = (valid_months["profit_factor"] > 1).sum()

    if positive_months == 3 and pf_above_one_months == 3:
        return "stable_positive"

    if positive_months >= 2 and pf_above_one_months >= 2:
        return "mixed_positive"

    return "unstable_or_negative"


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 19 - CONTEXT STABILITY AUDIT")
    print("=" * 90)

    if not INPUT_PATH.exists():
        print(f"[ERROR] Input file missing: {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded trades: {len(df):,}")
    print(f"Input: {INPUT_PATH}")

    df = df[
        (df["pressure_threshold"] == TARGET_PRESSURE_THRESHOLD)
        & (df["cost_per_trade"] == TARGET_COST)
    ].copy()

    if df.empty:
        print("[ERROR] No trades after pressure/cost filter.")
        return

    df["timestamp_start"] = pd.to_datetime(df["timestamp_start"])
    df["month"] = df["timestamp_start"].dt.to_period("M").astype(str)
    df["hour"] = df["timestamp_start"].dt.hour
    df["net_return"] = pd.to_numeric(df["net_return"], errors="coerce")
    df["is_win"] = df["is_win"].astype(bool)

    print(f"Filtered trades: {len(df):,}")
    print("-" * 90)

    monthly_rows = []
    total_rows = []

    for context in CONTEXTS:
        context_name = context["context_name"]
        context_df = apply_context(df, context)

        total_summary = summarise_returns(context_df)
        stability_month_rows = []

        print(f"\n[CONTEXT] {context_name}")
        print(f"Total trades: {total_summary['trade_count']}")

        for month, month_df in context_df.groupby("month"):
            month_summary = summarise_returns(month_df)

            row = {
                "context_name": context_name,
                "month": month,
                **month_summary,
            }

            monthly_rows.append(row)
            stability_month_rows.append(row)

            print(
                f"  {month} | "
                f"trades={month_summary['trade_count']:>3} | "
                f"win={month_summary['win_rate']:.3f} | "
                f"avg={month_summary['avg_return']:.8f} | "
                f"pf={month_summary['profit_factor']:.3f}"
            )

        month_rows_df = pd.DataFrame(stability_month_rows)

        stability_label = classify_stability(month_rows_df, total_summary)

        total_rows.append({
            "context_name": context_name,
            "pressure_threshold": TARGET_PRESSURE_THRESHOLD,
            "cost_per_trade": TARGET_COST,
            "stability_label": stability_label,
            **total_summary,
            "context_hour": context.get("hour"),
            "context_session": context.get("session"),
            "context_weekday": context.get("weekday"),
            "context_signal": context.get("signal"),
        })

        print(f"  Stability: {stability_label}")

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    monthly_df = pd.DataFrame(monthly_rows)
    total_df = pd.DataFrame(total_rows)

    monthly_path = (
        OUTPUT_ROOT
        / f"{SYMBOL}_dominant_pressure_{TARGET_PRESSURE_THRESHOLD:.2f}_monthly_context_stability.csv"
    )

    total_path = (
        OUTPUT_ROOT
        / f"{SYMBOL}_dominant_pressure_{TARGET_PRESSURE_THRESHOLD:.2f}_context_stability_summary.csv"
    )

    monthly_df.to_csv(monthly_path, index=False)
    total_df.to_csv(total_path, index=False)

    ranked = total_df.sort_values(
        ["stability_label", "profit_factor", "avg_return", "trade_count"],
        ascending=[True, False, False, False],
    )

    ranked_path = (
        OUTPUT_ROOT
        / f"{SYMBOL}_dominant_pressure_{TARGET_PRESSURE_THRESHOLD:.2f}_context_stability_ranked.csv"
    )

    ranked.to_csv(ranked_path, index=False)

    print("\n" + "-" * 90)
    print("[RANKED CONTEXTS]")
    preview_cols = [
        "context_name",
        "stability_label",
        "trade_count",
        "win_rate",
        "avg_return",
        "profit_factor",
        "sharpe_like",
        "max_drawdown",
        "unique_dates",
        "long_trades",
        "short_trades",
    ]

    print(ranked[preview_cols].to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Monthly: {monthly_path}")
    print(f"Summary: {total_path}")
    print(f"Ranked:  {ranked_path}")
    print("[DONE] Context stability audit complete.")


if __name__ == "__main__":
    main()