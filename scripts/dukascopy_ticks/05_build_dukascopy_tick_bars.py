"""
BACQE DUKASCOPY 05 - BUILD FIXED TICK BARS

Purpose:
    Load one processed Dukascopy daily tick Parquet file and build fixed tick-count bars.

Input:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\EURUSD_2024-01-02_ticks.parquet

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_tick_bars\\symbol=EURUSD\\tick_size=100\\year=2024\\month=01\\EURUSD_2024-01-02_tick_bars_100.parquet
    E:\\Quant_Lab\\data\\processed\\dukascopy_tick_bars\\symbol=EURUSD\\tick_size=250\\year=2024\\month=01\\EURUSD_2024-01-02_tick_bars_250.parquet
    etc.

Also saves:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\tick_bar_reports\\EURUSD_2024-01-02_tick_bar_report.csv
"""

from pathlib import Path
from datetime import datetime

import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

TICK_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
BAR_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "tick_bar_reports"

SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"

TICK_SIZES = [100, 250, 500, 1000]


# =============================================================================
# PATH HELPERS
# =============================================================================

def input_tick_path(symbol: str, dt: datetime) -> Path:
    return (
        TICK_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_ticks.parquet"
    )


def output_bar_path(symbol: str, dt: datetime, tick_size: int) -> Path:
    return (
        BAR_ROOT
        / f"symbol={symbol}"
        / f"tick_size={tick_size}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tick_bars_{tick_size}.parquet"
    )


def report_path(symbol: str, dt: datetime) -> Path:
    return REPORT_ROOT / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tick_bar_report.csv"


# =============================================================================
# BAR BUILDER
# =============================================================================

def build_tick_bars(df: pd.DataFrame, tick_size: int) -> pd.DataFrame:
    """
    Build fixed tick-count bars.

    Each bar contains tick_size rows, except possibly the final bar.
    """

    df = df.sort_values("timestamp_utc").reset_index(drop=True).copy()

    df["bar_id"] = df.index // tick_size

    grouped = df.groupby("bar_id", sort=True)

    bars = grouped.agg(
        timestamp_start=("timestamp_utc", "first"),
        timestamp_end=("timestamp_utc", "last"),
        symbol=("symbol", "first"),
        source=("source", "first"),

        open=("mid", "first"),
        high=("mid", "max"),
        low=("mid", "min"),
        close=("mid", "last"),

        bid_open=("bid", "first"),
        bid_high=("bid", "max"),
        bid_low=("bid", "min"),
        bid_close=("bid", "last"),

        ask_open=("ask", "first"),
        ask_high=("ask", "max"),
        ask_low=("ask", "min"),
        ask_close=("ask", "last"),

        spread_open=("spread_points", "first"),
        spread_high=("spread_points", "max"),
        spread_low=("spread_points", "min"),
        spread_close=("spread_points", "last"),
        spread_mean=("spread_points", "mean"),

        bid_volume_sum=("bid_volume", "sum"),
        ask_volume_sum=("ask_volume", "sum"),
        quote_volume_sum=("quote_volume", "sum"),

        tick_count=("timestamp_utc", "count"),
    ).reset_index(drop=True)

    bars["tick_size"] = tick_size
    bars["duration_seconds"] = (
        bars["timestamp_end"] - bars["timestamp_start"]
    ).dt.total_seconds()

    bars["return_close_to_close"] = bars["close"].pct_change()
    bars["range"] = bars["high"] - bars["low"]
    bars["range_points"] = bars["range"] / 0.00001

    ordered_cols = [
        "timestamp_start",
        "timestamp_end",
        "symbol",
        "source",
        "tick_size",
        "tick_count",
        "duration_seconds",

        "open",
        "high",
        "low",
        "close",
        "return_close_to_close",
        "range",
        "range_points",

        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",

        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",

        "spread_open",
        "spread_high",
        "spread_low",
        "spread_close",
        "spread_mean",

        "bid_volume_sum",
        "ask_volume_sum",
        "quote_volume_sum",
    ]

    return bars[ordered_cols]


def build_report_row(bars: pd.DataFrame, tick_size: int, output_path: Path) -> dict:
    return {
        "tick_size": tick_size,
        "bars": len(bars),
        "first_timestamp": bars["timestamp_start"].min() if not bars.empty else None,
        "last_timestamp": bars["timestamp_end"].max() if not bars.empty else None,
        "avg_tick_count": bars["tick_count"].mean() if not bars.empty else None,
        "min_tick_count": bars["tick_count"].min() if not bars.empty else None,
        "max_tick_count": bars["tick_count"].max() if not bars.empty else None,
        "avg_duration_seconds": bars["duration_seconds"].mean() if not bars.empty else None,
        "min_duration_seconds": bars["duration_seconds"].min() if not bars.empty else None,
        "max_duration_seconds": bars["duration_seconds"].max() if not bars.empty else None,
        "avg_spread_mean": bars["spread_mean"].mean() if not bars.empty else None,
        "avg_range_points": bars["range_points"].mean() if not bars.empty else None,
        "output_path": str(output_path),
    }


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    dt = datetime.strptime(DATE_STR, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 05 - BUILD FIXED TICK BARS")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Date:   {DATE_STR}")
    print("-" * 90)

    in_path = input_tick_path(SYMBOL, dt)

    if not in_path.exists():
        print(f"[ERROR] Input tick file not found: {in_path}")
        return

    df = pd.read_parquet(in_path)

    print(f"Loaded ticks: {len(df):,}")
    print(f"Input:        {in_path}")
    print("-" * 90)

    report_rows = []

    for tick_size in TICK_SIZES:
        bars = build_tick_bars(df, tick_size)

        out_path = output_bar_path(SYMBOL, dt, tick_size)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        bars.to_parquet(out_path, index=False)

        report_rows.append(build_report_row(bars, tick_size, out_path))

        print(
            f"[tick_size={tick_size:>4}] "
            f"bars={len(bars):>6,} | "
            f"avg_duration={bars['duration_seconds'].mean():>8.2f}s | "
            f"avg_range={bars['range_points'].mean():>6.2f} points"
        )

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    report_df = pd.DataFrame(report_rows)
    out_report = report_path(SYMBOL, dt)
    report_df.to_csv(out_report, index=False)

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Report: {out_report}")

    print("-" * 90)
    print("[PREVIEW REPORT]")
    print(report_df.to_string(index=False))

    print("[DONE] Dukascopy fixed tick bars built successfully.")


if __name__ == "__main__":
    main()