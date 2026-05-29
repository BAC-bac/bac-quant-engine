"""
BACQE TICK RESEARCH - 10 Build Tick Imbalance Bars - Multi Symbol

Builds fixed-threshold Tick Imbalance Bars from signed ticks.

Uses:
    signed_tick = +1 / -1 from the tick rule

A new bar closes when:
    abs(cumulative signed imbalance inside current bar) >= threshold

This is v1: fixed thresholds.
Later versions can use dynamic Lopez de Prado expected imbalance logic.

Multi-symbol version:
    - Reads signed tick datasets created by Script 09
    - Processes all symbols listed in SYMBOLS
    - Saves one imbalance-bar dataset per symbol and threshold
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

BROKER = "FTMO"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

SIGNED_TICK_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "signed_ticks"

OUTPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars"

SUMMARY_DIR = OUTPUT_ROOT / "_summary"

IMBALANCE_THRESHOLDS = [25, 50, 100, 200]


def load_signed_ticks(symbol: str) -> pd.DataFrame:
    input_path = (
        SIGNED_TICK_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_signed_ticks_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: signed ticks file not found: {input_path}")
        return pd.DataFrame()

    signed_ticks = pd.read_parquet(input_path)

    required = {
        "time_msc_dt",
        "mid",
        "bid",
        "ask",
        "spread",
        "signed_tick",
    }

    missing = required - set(signed_ticks.columns)

    if missing:
        print(f"[WARN] {symbol}: signed tick file missing required columns: {missing}")
        return pd.DataFrame()

    signed_ticks["time_msc_dt"] = pd.to_datetime(
        signed_ticks["time_msc_dt"],
        errors="coerce",
        utc=True,
    )

    signed_ticks = signed_ticks.dropna(
        subset=[
            "time_msc_dt",
            "mid",
            "bid",
            "ask",
            "signed_tick",
        ]
    )

    signed_ticks = signed_ticks.sort_values("time_msc_dt").reset_index(drop=True)

    return signed_ticks


def build_single_bar(
    symbol: str,
    rows: list,
    threshold: int,
    imbalance_sum: int,
) -> dict:
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
        "symbol": symbol,
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


def build_imbalance_bars(
    symbol: str,
    signed_ticks: pd.DataFrame,
    threshold: int,
) -> pd.DataFrame:
    records = []

    current_rows = []
    current_imbalance = 0

    for row in signed_ticks.itertuples(index=False):
        current_rows.append(row)
        current_imbalance += int(row.signed_tick)

        if abs(current_imbalance) >= threshold:
            records.append(
                build_single_bar(
                    symbol=symbol,
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
                symbol=symbol,
                rows=current_rows,
                threshold=threshold,
                imbalance_sum=current_imbalance,
            )
        )

    bars = pd.DataFrame(records)

    if bars.empty:
        return bars

    bars["return"] = bars["close"].pct_change()
    bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))
    bars["range"] = bars["high"] - bars["low"]

    bars["direction"] = 0
    bars.loc[bars["close"] > bars["open"], "direction"] = 1
    bars.loc[bars["close"] < bars["open"], "direction"] = -1

    bars["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def save_bars(symbol: str, bars: pd.DataFrame, threshold: int) -> tuple[Path, Path]:
    output_dir = OUTPUT_ROOT / f"symbol={symbol}" / f"imbalance_threshold={threshold}"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"{symbol}_tick_imbalance_bars_{threshold}_latest.parquet"
    csv_path = output_dir / f"{symbol}_tick_imbalance_bars_{threshold}_latest.csv"

    bars.to_parquet(parquet_path, index=False)
    bars.to_csv(csv_path, index=False)

    return parquet_path, csv_path


def process_symbol(symbol: str) -> list[dict]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    signed_ticks = load_signed_ticks(symbol)

    if signed_ticks.empty:
        print(f"[WARN] {symbol}: no signed ticks available. Skipping.")
        return [
            {
                "symbol": symbol,
                "threshold": None,
                "status": "missing_or_empty",
                "signed_tick_rows": 0,
                "bars": 0,
                "avg_ticks_per_bar": None,
                "median_ticks_per_bar": None,
                "avg_duration_seconds": None,
                "first_bar_time": None,
                "last_bar_time": None,
            }
        ]

    print(f"[INFO] {symbol}: signed ticks loaded: {len(signed_ticks):,}")
    print(f"[INFO] {symbol}: first tick: {signed_ticks['time_msc_dt'].min()}")
    print(f"[INFO] {symbol}: last tick:  {signed_ticks['time_msc_dt'].max()}")

    summary_rows = []

    for threshold in IMBALANCE_THRESHOLDS:
        bars = build_imbalance_bars(
            symbol=symbol,
            signed_ticks=signed_ticks,
            threshold=threshold,
        )

        if bars.empty:
            print(f"[WARN] {symbol}: no bars created for threshold={threshold}")

            summary_rows.append(
                {
                    "symbol": symbol,
                    "threshold": threshold,
                    "status": "empty",
                    "signed_tick_rows": len(signed_ticks),
                    "bars": 0,
                    "avg_ticks_per_bar": None,
                    "median_ticks_per_bar": None,
                    "avg_duration_seconds": None,
                    "first_bar_time": None,
                    "last_bar_time": None,
                }
            )

            continue

        parquet_path, csv_path = save_bars(symbol, bars, threshold)

        avg_ticks_per_bar = float(bars["tick_count"].mean())
        median_ticks_per_bar = float(bars["tick_count"].median())
        avg_duration_seconds = float(bars["duration_seconds"].mean())

        print(f"[DONE] {symbol}: threshold={threshold}")
        print(f"       Bars:             {len(bars):,}")
        print(f"       Avg ticks/bar:    {avg_ticks_per_bar:.2f}")
        print(f"       Median ticks/bar: {median_ticks_per_bar:.2f}")
        print(f"       Avg duration:     {avg_duration_seconds:.2f}s")
        print(f"       Parquet:          {parquet_path}")
        print(f"       CSV:              {csv_path}")

        summary_rows.append(
            {
                "symbol": symbol,
                "threshold": threshold,
                "status": "ok",
                "signed_tick_rows": len(signed_ticks),
                "bars": len(bars),
                "avg_ticks_per_bar": avg_ticks_per_bar,
                "median_ticks_per_bar": median_ticks_per_bar,
                "avg_duration_seconds": avg_duration_seconds,
                "first_bar_time": str(bars["bar_start_time"].min()),
                "last_bar_time": str(bars["bar_end_time"].max()),
            }
        )

    return summary_rows


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 10 BUILD TICK IMBALANCE BARS - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:           {BROKER}")
    print(f"Signed tick root: {SIGNED_TICK_ROOT}")
    print(f"Output root:      {OUTPUT_ROOT}")
    print(f"Symbols:          {SYMBOLS}")
    print(f"Thresholds:       {IMBALANCE_THRESHOLDS}")
    print("=" * 90)

    all_summary_rows = []

    for symbol in SYMBOLS:
        all_summary_rows.extend(process_symbol(symbol))

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

    summary = pd.DataFrame(all_summary_rows)

    summary_csv = SUMMARY_DIR / "tick_imbalance_bar_build_summary_latest.csv"
    summary_json = SUMMARY_DIR / "tick_imbalance_bar_build_summary_latest.json"

    summary.to_csv(summary_csv, index=False)
    summary.to_json(summary_json, orient="records", indent=2)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol tick imbalance bar build complete.")
    print(f"Symbols attempted: {len(SYMBOLS)}")

    if not summary.empty:
        ok_rows = summary[summary["status"] == "ok"]
        ok_symbols = ok_rows["symbol"].nunique() if not ok_rows.empty else 0
        print(f"Symbols with bars: {ok_symbols}")
        print(f"Total bar files:   {len(ok_rows)}")

    print(f"Summary CSV:       {summary_csv}")
    print(f"Summary JSON:      {summary_json}")
    print("=" * 90)


if __name__ == "__main__":
    main()