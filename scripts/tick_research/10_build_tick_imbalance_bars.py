"""
BACQE TICK RESEARCH - 10 Build Tick Imbalance Bars

Builds fixed-threshold Tick Imbalance Bars from signed ticks.

Uses:
    signed_tick = +1 / -1 from the tick rule

A new bar closes when:
    abs(cumulative signed imbalance inside current bar) >= threshold

This is v1: fixed thresholds.
Later versions can use dynamic Lopez de Prado expected imbalance logic.
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "signed_ticks"
    / f"symbol={SYMBOL}"
    / f"{SYMBOL}_signed_ticks_latest.parquet"
)

OUTPUT_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "tick_imbalance_bars"
    / f"symbol={SYMBOL}"
)

IMBALANCE_THRESHOLDS = [25, 50, 100, 200]


def build_single_bar(rows: list, threshold: int, imbalance_sum: int) -> dict:
    mids = [float(r.mid) for r in rows]
    bids = [float(r.bid) for r in rows]
    asks = [float(r.ask) for r in rows]
    spreads = [float(r.spread) for r in rows if pd.notna(r.spread)]
    signed_ticks = [int(r.signed_tick) for r in rows]
    times = [r.time_msc_dt for r in rows]

    buy_ticks = sum(1 for x in signed_ticks if x == 1)
    sell_ticks = sum(1 for x in signed_ticks if x == -1)
    zero_ticks = sum(1 for x in signed_ticks if x == 0)

    tick_count = len(rows)

    return {
        "symbol": SYMBOL,
        "broker": BROKER,
        "bar_type": "tick_imbalance",
        "imbalance_threshold": threshold,
        "bar_start_time": times[0],
        "bar_end_time": times[-1],
        "open": mids[0],
        "high": max(mids),
        "low": min(mids),
        "close": mids[-1],
        "bid_open": bids[0],
        "bid_close": bids[-1],
        "ask_open": asks[0],
        "ask_close": asks[-1],
        "avg_spread": float(np.mean(spreads)) if spreads else np.nan,
        "max_spread": float(np.max(spreads)) if spreads else np.nan,
        "min_spread": float(np.min(spreads)) if spreads else np.nan,
        "tick_count": tick_count,
        "buy_ticks": buy_ticks,
        "sell_ticks": sell_ticks,
        "zero_ticks": zero_ticks,
        "imbalance_sum": imbalance_sum,
        "imbalance_abs": abs(imbalance_sum),
        "imbalance_ratio": imbalance_sum / tick_count if tick_count else np.nan,
        "duration_seconds": (times[-1] - times[0]).total_seconds(),
    }


def build_imbalance_bars(signed_ticks: pd.DataFrame, threshold: int) -> pd.DataFrame:
    records = []

    current_rows = []
    current_imbalance = 0

    for row in signed_ticks.itertuples(index=False):
        current_rows.append(row)
        current_imbalance += int(row.signed_tick)

        if abs(current_imbalance) >= threshold:
            records.append(
                build_single_bar(
                    rows=current_rows,
                    threshold=threshold,
                    imbalance_sum=current_imbalance,
                )
            )
            current_rows = []
            current_imbalance = 0

    if current_rows:
        records.append(
            build_single_bar(
                rows=current_rows,
                threshold=threshold,
                imbalance_sum=current_imbalance,
            )
        )

    bars = pd.DataFrame(records)

    bars["return"] = bars["close"].pct_change()
    bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))
    bars["range"] = bars["high"] - bars["low"]

    bars["direction"] = 0
    bars.loc[bars["close"] > bars["open"], "direction"] = 1
    bars.loc[bars["close"] < bars["open"], "direction"] = -1

    bars["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def save_bars(bars: pd.DataFrame, threshold: int) -> None:
    output_dir = OUTPUT_ROOT / f"imbalance_threshold={threshold}"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"{SYMBOL}_tick_imbalance_bars_{threshold}_latest.parquet"
    csv_path = output_dir / f"{SYMBOL}_tick_imbalance_bars_{threshold}_latest.csv"

    bars.to_parquet(parquet_path, index=False)
    bars.to_csv(csv_path, index=False)

    print(f"[DONE] Saved imbalance bars: threshold={threshold}")
    print(f"       Bars:    {len(bars):,}")
    print(f"       Parquet: {parquet_path}")
    print(f"       CSV:     {csv_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 10 BUILD TICK IMBALANCE BARS")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Broker: {BROKER}")
    print(f"Input:  {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Signed ticks file not found: {INPUT_PATH}")

    signed_ticks = pd.read_parquet(INPUT_PATH)

    signed_ticks["time_msc_dt"] = pd.to_datetime(
        signed_ticks["time_msc_dt"],
        errors="coerce",
        utc=True,
    )

    signed_ticks = signed_ticks.dropna(subset=["time_msc_dt", "mid", "bid", "ask", "signed_tick"])
    signed_ticks = signed_ticks.sort_values("time_msc_dt").reset_index(drop=True)

    print(f"Signed ticks loaded: {len(signed_ticks):,}")
    print(f"First tick: {signed_ticks['time_msc_dt'].min()}")
    print(f"Last tick:  {signed_ticks['time_msc_dt'].max()}")
    print("-" * 90)

    for threshold in IMBALANCE_THRESHOLDS:
        bars = build_imbalance_bars(signed_ticks, threshold)
        save_bars(bars, threshold)

        print(
            f"       Avg ticks/bar: {bars['tick_count'].mean():.2f} | "
            f"Median ticks/bar: {bars['tick_count'].median():.2f} | "
            f"Avg duration: {bars['duration_seconds'].mean():.2f}s"
        )

    print("-" * 90)
    print("[COMPLETE] Tick imbalance bar build complete.")
    print("=" * 90)


if __name__ == "__main__":
    main()