"""
BACQE DUKASCOPY 18 - ANALYSE DOMINANT PRESSURE BY CONTEXT

Purpose:
    Analyse the dominant-pressure replay trades by context.

Focus:
    pressure_threshold = 0.60
    cost_per_trade = 0.00000 initially

Breakdowns:
    - weekday
    - session
    - hour
    - signal direction
    - weekday + session
    - session + signal
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
    / "dominant_pressure_context"
)

SYMBOL = "EURUSD"

TARGET_PRESSURE_THRESHOLD = 0.60
TARGET_COST = 0.00000

MIN_TRADES_FOR_CONTEXT = 10


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


def summarise_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []

    for group_key, group in df.groupby(group_cols):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        returns = group["net_return"]

        row = {
            col: value for col, value in zip(group_cols, group_key)
        }

        row.update({
            "trade_count": len(group),
            "win_rate": group["is_win"].mean(),
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
            "long_trades": int((group["signal"] == "long").sum()),
            "short_trades": int((group["signal"] == "short").sum()),
            "unique_dates": group["date"].nunique(),
            "avg_dominant_pressure": group["dominant_pressure"].mean(),
            "min_timestamp": group["timestamp_start"].min(),
            "max_timestamp": group["timestamp_start"].max(),
        })

        row["context_quality"] = (
            "usable_sample"
            if row["trade_count"] >= MIN_TRADES_FOR_CONTEXT
            else "too_few_trades"
        )

        rows.append(row)

    out = pd.DataFrame(rows)

    if out.empty:
        return out

    return out.sort_values(
        ["context_quality", "profit_factor", "avg_return", "trade_count"],
        ascending=[True, False, False, False],
    )


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 18 - ANALYSE DOMINANT PRESSURE BY CONTEXT")
    print("=" * 90)

    if not INPUT_PATH.exists():
        print(f"[ERROR] Input trades file missing: {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded trades: {len(df):,}")
    print(f"Input: {INPUT_PATH}")

    df = df[
        (df["pressure_threshold"] == TARGET_PRESSURE_THRESHOLD)
        & (df["cost_per_trade"] == TARGET_COST)
    ].copy()

    if df.empty:
        print("[ERROR] No trades found for target pressure/cost.")
        return

    df["timestamp_start"] = pd.to_datetime(df["timestamp_start"])
    df["hour"] = df["timestamp_start"].dt.hour
    df["net_return"] = pd.to_numeric(df["net_return"], errors="coerce")
    df["is_win"] = df["is_win"].astype(bool)

    print("-" * 90)
    print(f"Filtered trades: {len(df):,}")
    print(f"Pressure:        {TARGET_PRESSURE_THRESHOLD}")
    print(f"Cost:            {TARGET_COST}")

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    breakdowns = {
        "by_weekday": ["weekday"],
        "by_session": ["session"],
        "by_hour": ["hour"],
        "by_signal": ["signal"],
        "by_weekday_session": ["weekday", "session"],
        "by_session_signal": ["session", "signal"],
        "by_weekday_signal": ["weekday", "signal"],
        "by_hour_signal": ["hour", "signal"],
    }

    output_paths = {}

    for name, group_cols in breakdowns.items():
        summary = summarise_group(df, group_cols)

        output_path = (
            OUTPUT_ROOT
            / f"{SYMBOL}_dominant_pressure_{TARGET_PRESSURE_THRESHOLD:.2f}_{name}.csv"
        )

        summary.to_csv(output_path, index=False)
        output_paths[name] = output_path

        print("\n" + "-" * 90)
        print(f"[{name.upper()}]")
        if summary.empty:
            print("No rows.")
        else:
            preview_cols = group_cols + [
                "trade_count",
                "win_rate",
                "avg_return",
                "profit_factor",
                "sharpe_like",
                "max_drawdown",
                "long_trades",
                "short_trades",
                "unique_dates",
                "context_quality",
            ]
            print(summary[preview_cols].head(15).to_string(index=False))

    all_contexts = []

    for name, path in output_paths.items():
        temp = pd.read_csv(path)
        temp["breakdown"] = name
        all_contexts.append(temp)

    combined = pd.concat(all_contexts, ignore_index=True)

    combined_path = (
        OUTPUT_ROOT
        / f"{SYMBOL}_dominant_pressure_{TARGET_PRESSURE_THRESHOLD:.2f}_all_contexts.csv"
    )

    combined.to_csv(combined_path, index=False)

    print("\n" + "-" * 90)
    print("[OUTPUTS]")
    for name, path in output_paths.items():
        print(f"{name}: {path}")
    print(f"combined: {combined_path}")
    print("[DONE] Dominant pressure context analysis complete.")


if __name__ == "__main__":
    main()