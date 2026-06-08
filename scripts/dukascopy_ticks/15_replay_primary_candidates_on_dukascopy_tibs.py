"""
BACQE DUKASCOPY 15 - REPLAY PRIMARY CANDIDATES ON DUKASCOPY TIBS

Purpose:
    Replay primary EURUSD Script 48 candidate specs on Jan-Mar 2024
    Dukascopy Tick Imbalance Bars.

Signal logic:
    buy_pressure  = buy_ticks / tick_count
    sell_pressure = sell_ticks / tick_count

    long  if buy_pressure  >= buy_threshold
    short if sell_pressure >= sell_threshold

Target:
    next TIB close-to-close return

Returns:
    long  gross_return = next_return
    short gross_return = -next_return
    net_return = gross_return - cost_per_trade
"""

from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

SPEC_PATH = (
    DATA_ROOT
    / "analysis"
    / "dukascopy_ticks"
    / "candidate_replay_prep"
    / "eurusd_primary_replay_spec.csv"
)

TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
OUTPUT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "candidate_replay"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

IMBALANCE_THRESHOLDS = [25, 50, 100]


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


def split_csv_field(value) -> list[str]:
    if pd.isna(value):
        return []

    return [item.strip() for item in str(value).split(",") if item.strip()]


def load_tibs() -> pd.DataFrame:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dfs = []

    for dt in date_range(start, end):
        for threshold in IMBALANCE_THRESHOLDS:
            path = tib_path(SYMBOL, dt, threshold)

            if not path.exists():
                continue

            df = pd.read_parquet(path)

            if df.empty:
                continue

            df = df.copy()
            df["date"] = dt.strftime("%Y-%m-%d")
            df["source_file"] = str(path)
            df["weekday"] = pd.to_datetime(df["timestamp_start"]).dt.day_name()
            df["session"] = df["timestamp_start"].apply(classify_session)
            df["buy_pressure"] = df["buy_ticks"] / df["tick_count"]
            df["sell_pressure"] = df["sell_ticks"] / df["tick_count"]

            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    tibs = pd.concat(dfs, ignore_index=True)
    tibs = tibs.sort_values(
        ["imbalance_threshold", "timestamp_start"]
    ).reset_index(drop=True)

    tibs["next_close"] = tibs.groupby("imbalance_threshold")["close"].shift(-1)
    tibs["next_return"] = (
        tibs["next_close"] / tibs["close"]
    ) - 1

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


def replay_one_spec(spec: pd.Series, tibs: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    required_weekdays = split_csv_field(spec.get("weekdays"))
    required_sessions = split_csv_field(spec.get("sessions"))

    df = tibs.copy()

    if required_weekdays:
        df = df[df["weekday"].isin(required_weekdays)]

    if required_sessions:
        df = df[df["session"].isin(required_sessions)]

    df = df.dropna(subset=["next_return"]).copy()

    buy_threshold = float(spec["buy_threshold"])
    sell_threshold = float(spec["sell_threshold"])
    cost = float(spec["cost_per_trade"])

    long_mask = df["buy_pressure"] >= buy_threshold
    short_mask = df["sell_pressure"] >= sell_threshold

    trades = df[long_mask | short_mask].copy()

    if trades.empty:
        summary = {
            "replay_id": spec["replay_id"],
            "status": "no_trades",
            "trade_count": 0,
            "net_win_rate": np.nan,
            "gross_avg_return": np.nan,
            "net_avg_return": np.nan,
            "net_profit_factor": np.nan,
            "net_sharpe_like": np.nan,
            "max_drawdown": np.nan,
            "long_trades": 0,
            "short_trades": 0,
        }
        return trades, summary

    trades["signal"] = np.where(
        trades["buy_pressure"] >= buy_threshold,
        "long",
        "short",
    )

    trades["gross_return"] = np.where(
        trades["signal"] == "long",
        trades["next_return"],
        -trades["next_return"],
    )

    trades["net_return"] = trades["gross_return"] - cost
    trades["is_win"] = trades["net_return"] > 0

    trades["replay_id"] = spec["replay_id"]
    trades["filter_name"] = spec["filter_name"]
    trades["validation_rank"] = spec["validation_rank"]
    trades["cost_per_trade"] = cost
    trades["threshold_pair"] = spec["threshold_pair"]
    trades["buy_threshold"] = buy_threshold
    trades["sell_threshold"] = sell_threshold

    net_returns = trades["net_return"]

    summary = {
        "replay_id": spec["replay_id"],
        "status": "processed",
        "symbol": spec["symbol"],
        "filter_name": spec["filter_name"],
        "validation_rank": spec["validation_rank"],
        "cost_per_trade": cost,
        "threshold_pair": spec["threshold_pair"],
        "buy_threshold": buy_threshold,
        "sell_threshold": sell_threshold,
        "original_trade_count": spec["original_trade_count"],
        "original_net_win_rate": spec["original_net_win_rate"],
        "original_net_avg_return": spec["original_net_avg_return"],
        "original_net_profit_factor": spec["original_net_profit_factor"],
        "trade_count": len(trades),
        "net_win_rate": trades["is_win"].mean(),
        "gross_avg_return": trades["gross_return"].mean(),
        "net_avg_return": net_returns.mean(),
        "net_median_return": net_returns.median(),
        "net_total_return_sum": net_returns.sum(),
        "net_profit_factor": profit_factor(net_returns),
        "net_sharpe_like": (
            net_returns.mean() / net_returns.std()
            if net_returns.std() and net_returns.std() > 0
            else np.nan
        ),
        "max_drawdown": max_drawdown(net_returns),
        "long_trades": int((trades["signal"] == "long").sum()),
        "short_trades": int((trades["signal"] == "short").sum()),
        "unique_dates": trades["date"].nunique(),
        "unique_sessions": trades["session"].nunique(),
        "first_trade_time": trades["timestamp_start"].min(),
        "last_trade_time": trades["timestamp_start"].max(),
    }

    return trades, summary


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 15 - REPLAY PRIMARY CANDIDATES ON DUKASCOPY TIBS")
    print("=" * 90)

    if not SPEC_PATH.exists():
        print(f"[ERROR] Replay spec missing: {SPEC_PATH}")
        return

    spec_df = pd.read_csv(SPEC_PATH)
    tibs = load_tibs()

    print(f"Loaded replay specs: {len(spec_df):,}")
    print(f"Loaded TIB rows:     {len(tibs):,}")
    print("-" * 90)

    if tibs.empty:
        print("[ERROR] No TIB data loaded.")
        return

    all_trades = []
    summary_rows = []

    for _, spec in spec_df.iterrows():
        trades, summary = replay_one_spec(spec, tibs)
        summary_rows.append(summary)

        if not trades.empty:
            all_trades.append(trades)

        print(
            f"[{summary['status']}] "
            f"rank={spec['validation_rank']} | "
            f"cost={float(spec['cost_per_trade']):.5f} | "
            f"pair={spec['threshold_pair']} | "
            f"trades={summary['trade_count']}"
        )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_primary_replay_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    if all_trades:
        trades_df = pd.concat(all_trades, ignore_index=True)
    else:
        trades_df = pd.DataFrame()

    trades_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_primary_replay_trades.csv"
    trades_df.to_csv(trades_path, index=False)

    ranked = summary_df.sort_values(
        ["net_profit_factor", "net_avg_return", "trade_count"],
        ascending=[False, False, False],
    )

    ranked_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_primary_replay_ranked.csv"
    ranked.to_csv(ranked_path, index=False)

    print("-" * 90)
    print("[TOP REPLAY RESULTS]")
    preview_cols = [
        "validation_rank",
        "cost_per_trade",
        "threshold_pair",
        "trade_count",
        "net_win_rate",
        "net_avg_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
    ]
    print(ranked[preview_cols].head(10).to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Summary: {summary_path}")
    print(f"Trades:  {trades_path}")
    print(f"Ranked:  {ranked_path}")
    print("[DONE] Dukascopy candidate replay complete.")


if __name__ == "__main__":
    main()