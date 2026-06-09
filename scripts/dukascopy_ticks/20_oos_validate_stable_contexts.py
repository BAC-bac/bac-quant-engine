"""
BACQE DUKASCOPY 20 - OOS VALIDATE STABLE CONTEXTS

Purpose:
    Validate stable contexts discovered on Jan-Mar 2024 against unseen
    Apr-Jun 2024 Dukascopy TIB data.

Discovery period:
    Jan-Mar 2024

OOS period:
    Apr-Jun 2024

Frozen contexts:
    - hour_10_all
    - hour_11_all
    - tuesday_london_mid_morning_all
    - friday_long
"""

from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
OUTPUT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "oos_context_validation"

SYMBOL = "EURUSD"

START_DATE = "2023-01-01"
END_DATE = "2025-12-31"

TIB_THRESHOLD = 25
PRESSURE_THRESHOLD = 0.60
COST_TESTS = [0.00000, 0.00002, 0.00005, 0.00010, 0.00015]


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
        "context_name": "friday_long",
        "hour": None,
        "session": None,
        "weekday": "Friday",
        "signal": "long",
    },
]


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def classify_session(timestamp) -> str:
    hour = pd.Timestamp(timestamp).hour

    if 0 <= hour <= 5:
        return "asia_late_overnight"
    if 8 <= hour <= 11:
        return "london_mid_morning"
    if 12 <= hour <= 13:
        return "pre_new_york"
    if 14 <= hour <= 16:
        return "new_york_open"
    if 17 <= hour <= 20:
        return "new_york_afternoon"
    if 21 <= hour <= 23:
        return "rollover_late"

    return "other"


def load_oos_tibs() -> pd.DataFrame:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dfs = []
    missing_files = 0

    for dt in date_range(start, end):
        path = tib_path(SYMBOL, dt, TIB_THRESHOLD)

        if not path.exists():
            missing_files += 1
            continue

        df = pd.read_parquet(path)

        if df.empty:
            continue

        df = df.copy()
        df["date"] = dt.strftime("%Y-%m-%d")
        df["weekday"] = pd.to_datetime(df["timestamp_start"]).dt.day_name()
        df["session"] = df["timestamp_start"].apply(classify_session)
        df["hour"] = pd.to_datetime(df["timestamp_start"]).dt.hour

        df["buy_pressure"] = df["buy_ticks"] / df["tick_count"]
        df["sell_pressure"] = df["sell_ticks"] / df["tick_count"]
        df["dominant_pressure"] = df[["buy_pressure", "sell_pressure"]].max(axis=1)

        df["signal"] = np.where(
            df["buy_pressure"] >= df["sell_pressure"],
            "long",
            "short",
        )

        dfs.append(df)

    if not dfs:
        print(f"[WARNING] No OOS TIB files loaded. Missing daily files: {missing_files}")
        return pd.DataFrame()

    tibs = pd.concat(dfs, ignore_index=True)
    tibs = tibs.sort_values("timestamp_start").reset_index(drop=True)

    tibs["next_close"] = tibs["close"].shift(-1)
    tibs["next_return"] = tibs["next_close"] / tibs["close"] - 1

    tibs = tibs.dropna(subset=["next_return"])

    return tibs


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


def summarise_trades(trades: pd.DataFrame, cost: float) -> dict:
    if trades.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "gross_avg_return": np.nan,
            "net_avg_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "max_drawdown": np.nan,
            "long_trades": 0,
            "short_trades": 0,
            "unique_dates": 0,
        }

    trades = trades.copy()

    trades["gross_return"] = np.where(
        trades["signal"] == "long",
        trades["next_return"],
        -trades["next_return"],
    )

    trades["net_return"] = trades["gross_return"] - cost
    trades["is_win"] = trades["net_return"] > 0

    returns = trades["net_return"]

    return {
        "trade_count": len(trades),
        "win_rate": trades["is_win"].mean(),
        "gross_avg_return": trades["gross_return"].mean(),
        "net_avg_return": returns.mean(),
        "median_return": returns.median(),
        "total_return_sum": returns.sum(),
        "profit_factor": profit_factor(returns),
        "sharpe_like": (
            returns.mean() / returns.std()
            if returns.std() and returns.std() > 0
            else np.nan
        ),
        "max_drawdown": max_drawdown(returns),
        "long_trades": int((trades["signal"] == "long").sum()),
        "short_trades": int((trades["signal"] == "short").sum()),
        "unique_dates": trades["date"].nunique(),
        "first_trade_time": trades["timestamp_start"].min(),
        "last_trade_time": trades["timestamp_start"].max(),
    }


def classify_oos_result(row: dict) -> str:
    if row["trade_count"] < 20:
        return "insufficient_oos_sample"

    if row["net_avg_return"] > 0 and row["profit_factor"] > 1:
        return "oos_pass_positive"

    return "oos_fail"


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 20 - OOS VALIDATE STABLE CONTEXTS")
    print("=" * 90)
    print(f"OOS period: {START_DATE} to {END_DATE}")
    print(f"TIB threshold: {TIB_THRESHOLD}")
    print(f"Pressure threshold: {PRESSURE_THRESHOLD}")
    print("-" * 90)

    tibs = load_oos_tibs()

    if tibs.empty:
        print("[ERROR] No OOS TIB data available. Build Apr-Jun data with Scripts 07-11 first.")
        return

    filtered = tibs[tibs["dominant_pressure"] >= PRESSURE_THRESHOLD].copy()

    print(f"Loaded OOS TIB rows:       {len(tibs):,}")
    print(f"Dominant-pressure rows:    {len(filtered):,}")
    print("-" * 90)

    summary_rows = []
    trade_rows = []

    for context in CONTEXTS:
        context_name = context["context_name"]
        context_df = apply_context(filtered, context)

        for cost in COST_TESTS:
            summary = summarise_trades(context_df, cost)

            row = {
                "context_name": context_name,
                "symbol": SYMBOL,
                "oos_start_date": START_DATE,
                "oos_end_date": END_DATE,
                "tib_threshold": TIB_THRESHOLD,
                "pressure_threshold": PRESSURE_THRESHOLD,
                "cost_per_trade": cost,
                **summary,
            }

            row["oos_result_label"] = classify_oos_result(row)
            summary_rows.append(row)

            if not context_df.empty:
                temp = context_df.copy()
                temp["context_name"] = context_name
                temp["cost_per_trade"] = cost
                temp["gross_return"] = np.where(
                    temp["signal"] == "long",
                    temp["next_return"],
                    -temp["next_return"],
                )
                temp["net_return"] = temp["gross_return"] - cost
                trade_rows.append(temp)

            print(
                f"{context_name:<35} | "
                f"cost={cost:.5f} | "
                f"trades={row['trade_count']:>4} | "
                f"win={row['win_rate']:.3f} | "
                f"avg={row['net_avg_return']:.8f} | "
                f"pf={row['profit_factor']:.3f} | "
                f"{row['oos_result_label']}"
            )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_oos_context_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    if trade_rows:
        trades_df = pd.concat(trade_rows, ignore_index=True)
    else:
        trades_df = pd.DataFrame()

    trades_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_oos_context_trades.csv"
    trades_df.to_csv(trades_path, index=False)

    ranked = summary_df.sort_values(
        ["oos_result_label", "profit_factor", "net_avg_return", "trade_count"],
        ascending=[True, False, False, False],
    )

    ranked_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_oos_context_ranked.csv"
    ranked.to_csv(ranked_path, index=False)

    print("-" * 90)
    print("[TOP OOS RESULTS]")
    preview_cols = [
        "context_name",
        "cost_per_trade",
        "trade_count",
        "win_rate",
        "net_avg_return",
        "profit_factor",
        "sharpe_like",
        "max_drawdown",
        "unique_dates",
        "oos_result_label",
    ]
    print(ranked[preview_cols].head(20).to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Summary: {summary_path}")
    print(f"Trades:  {trades_path}")
    print(f"Ranked:  {ranked_path}")
    print("[DONE] OOS context validation complete.")


if __name__ == "__main__":
    main()