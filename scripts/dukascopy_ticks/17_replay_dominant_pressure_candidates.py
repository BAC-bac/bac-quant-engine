"""
BACQE DUKASCOPY 17 - REPLAY DOMINANT PRESSURE CANDIDATES

Purpose:
    Replay stricter dominant-pressure TIB candidates on Jan-Mar 2024
    Dukascopy data after Script 16 showed the original threshold pairs
    were too loose.

Logic:
    long  if buy_pressure  >= pressure_threshold
    short if sell_pressure >= pressure_threshold

Focus:
    TIB threshold 25
    pressure thresholds: 0.60, 0.65, 0.70, 0.75, 0.80
"""

from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
OUTPUT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "dominant_pressure_replay"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

TIB_THRESHOLDS = [25]
PRESSURE_THRESHOLDS = [0.60, 0.65, 0.70, 0.75, 0.80]

REQUIRED_WEEKDAYS = ["Friday", "Thursday", "Tuesday"]
REQUIRED_SESSIONS = [
    "asia_late_overnight",
    "london_mid_morning",
    "pre_new_york",
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


def load_tibs() -> pd.DataFrame:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dfs = []

    for dt in date_range(start, end):
        for threshold in TIB_THRESHOLDS:
            path = tib_path(SYMBOL, dt, threshold)

            if not path.exists():
                continue

            df = pd.read_parquet(path)

            if df.empty:
                continue

            df = df.copy()
            df["date"] = dt.strftime("%Y-%m-%d")
            df["weekday"] = pd.to_datetime(df["timestamp_start"]).dt.day_name()
            df["session"] = df["timestamp_start"].apply(classify_session)

            df["buy_pressure"] = df["buy_ticks"] / df["tick_count"]
            df["sell_pressure"] = df["sell_ticks"] / df["tick_count"]
            df["dominant_pressure"] = df[["buy_pressure", "sell_pressure"]].max(axis=1)
            df["pressure_side"] = np.where(
                df["buy_pressure"] >= df["sell_pressure"],
                "buy",
                "sell",
            )

            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    tibs = pd.concat(dfs, ignore_index=True)
    tibs = tibs.sort_values("timestamp_start").reset_index(drop=True)

    tibs["next_close"] = tibs["close"].shift(-1)
    tibs["next_return"] = tibs["next_close"] / tibs["close"] - 1

    return tibs


def max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return np.nan

    equity = (1 + returns.fillna(0)).cumprod()
    running_max = equity.cummax()
    drawdown = equity / running_max - 1

    return float(drawdown.min())


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def replay_threshold(df: pd.DataFrame, pressure_threshold: float, cost_per_trade: float) -> tuple[pd.DataFrame, dict]:
    trades = df[df["dominant_pressure"] >= pressure_threshold].copy()

    if trades.empty:
        return trades, {
            "pressure_threshold": pressure_threshold,
            "cost_per_trade": cost_per_trade,
            "status": "no_trades",
            "trade_count": 0,
        }

    trades["signal"] = np.where(
        trades["pressure_side"] == "buy",
        "long",
        "short",
    )

    trades["gross_return"] = np.where(
        trades["signal"] == "long",
        trades["next_return"],
        -trades["next_return"],
    )

    trades["net_return"] = trades["gross_return"] - cost_per_trade
    trades["is_win"] = trades["net_return"] > 0

    returns = trades["net_return"]

    summary = {
        "pressure_threshold": pressure_threshold,
        "cost_per_trade": cost_per_trade,
        "status": "processed",
        "trade_count": len(trades),
        "net_win_rate": trades["is_win"].mean(),
        "gross_avg_return": trades["gross_return"].mean(),
        "net_avg_return": trades["net_return"].mean(),
        "net_median_return": trades["net_return"].median(),
        "net_total_return_sum": trades["net_return"].sum(),
        "net_profit_factor": profit_factor(returns),
        "net_sharpe_like": (
            returns.mean() / returns.std()
            if returns.std() and returns.std() > 0
            else np.nan
        ),
        "max_drawdown": max_drawdown(returns),
        "long_trades": int((trades["signal"] == "long").sum()),
        "short_trades": int((trades["signal"] == "short").sum()),
        "unique_dates": trades["date"].nunique(),
        "unique_sessions": trades["session"].nunique(),
        "first_trade_time": trades["timestamp_start"].min(),
        "last_trade_time": trades["timestamp_start"].max(),
        "avg_dominant_pressure": trades["dominant_pressure"].mean(),
        "min_dominant_pressure": trades["dominant_pressure"].min(),
        "max_dominant_pressure": trades["dominant_pressure"].max(),
    }

    trades["pressure_threshold"] = pressure_threshold
    trades["cost_per_trade"] = cost_per_trade

    return trades, summary


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 17 - REPLAY DOMINANT PRESSURE CANDIDATES")
    print("=" * 90)

    tibs = load_tibs()

    if tibs.empty:
        print("[ERROR] No TIB data loaded.")
        return

    filtered = tibs[
        tibs["weekday"].isin(REQUIRED_WEEKDAYS)
        & tibs["session"].isin(REQUIRED_SESSIONS)
    ].copy()

    filtered = filtered.dropna(subset=["next_return"])

    print(f"Loaded TIB rows:    {len(tibs):,}")
    print(f"Filtered TIB rows:  {len(filtered):,}")
    print(f"Pressure thresholds: {PRESSURE_THRESHOLDS}")
    print("-" * 90)

    cost_tests = [0.00000, 0.00002, 0.00005, 0.00010, 0.00015]

    all_trades = []
    summary_rows = []

    for cost in cost_tests:
        for pressure_threshold in PRESSURE_THRESHOLDS:
            trades, summary = replay_threshold(filtered, pressure_threshold, cost)
            summary_rows.append(summary)

            if not trades.empty:
                all_trades.append(trades)

            print(
                f"[{summary['status']}] "
                f"pressure={pressure_threshold:.2f} | "
                f"cost={cost:.5f} | "
                f"trades={summary.get('trade_count', 0)} | "
                f"avg={summary.get('net_avg_return', np.nan)} | "
                f"pf={summary.get('net_profit_factor', np.nan)}"
            )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_dominant_pressure_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    if all_trades:
        trades_df = pd.concat(all_trades, ignore_index=True)
    else:
        trades_df = pd.DataFrame()

    trades_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_dominant_pressure_trades.csv"
    trades_df.to_csv(trades_path, index=False)

    ranked = summary_df.sort_values(
        ["net_profit_factor", "net_avg_return", "trade_count"],
        ascending=[False, False, False],
    )

    ranked_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_dominant_pressure_ranked.csv"
    ranked.to_csv(ranked_path, index=False)

    print("-" * 90)
    print("[TOP RESULTS]")
    preview_cols = [
        "pressure_threshold",
        "cost_per_trade",
        "trade_count",
        "net_win_rate",
        "net_avg_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "long_trades",
        "short_trades",
        "unique_dates",
    ]

    print(ranked[preview_cols].head(15).to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Summary: {summary_path}")
    print(f"Trades:  {trades_path}")
    print(f"Ranked:  {ranked_path}")
    print("[DONE] Dominant pressure replay complete.")


if __name__ == "__main__":
    main()