"""
BACQE DUKASCOPY 09 - BUILD FIXED TICK BARS FOR DATE RANGE

Purpose:
    Load processed Dukascopy daily tick Parquet files across a date range
    and build fixed tick-count bars for each valid day.

Input:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\*.parquet

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_tick_bars\\symbol=EURUSD\\tick_size=100\\year=2024\\month=01\\*.parquet

Also saves:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\tick_bar_reports\\EURUSD_2024-01-01_to_2024-01-31_tick_bar_report.csv
"""

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

TICK_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
BAR_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "tick_bar_reports"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

TICK_SIZES = [100, 250, 500, 1000]

POINT_SIZE = 0.00001


# =============================================================================
# DATE HELPERS
# =============================================================================

def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


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


def report_path(symbol: str, start: datetime, end: datetime) -> Path:
    return (
        REPORT_ROOT
        / f"{symbol}_{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_tick_bar_report.csv"
    )


# =============================================================================
# BAR BUILDER
# =============================================================================

def build_tick_bars(df: pd.DataFrame, tick_size: int) -> pd.DataFrame:
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
    bars["range_points"] = bars["range"] / POINT_SIZE

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


def build_report_row(
    dt: datetime,
    status: str,
    tick_size: int | None = None,
    bars: pd.DataFrame | None = None,
    output_path: Path | None = None,
    input_path: Path | None = None,
) -> dict:
    if bars is None or bars.empty:
        return {
            "date": dt.strftime("%Y-%m-%d"),
            "status": status,
            "tick_size": tick_size,
            "bars": 0,
            "first_timestamp": None,
            "last_timestamp": None,
            "avg_tick_count": None,
            "min_tick_count": None,
            "max_tick_count": None,
            "avg_duration_seconds": None,
            "min_duration_seconds": None,
            "max_duration_seconds": None,
            "avg_spread_mean": None,
            "avg_range_points": None,
            "input_path": str(input_path) if input_path else "",
            "output_path": str(output_path) if output_path else "",
        }

    return {
        "date": dt.strftime("%Y-%m-%d"),
        "status": status,
        "tick_size": tick_size,
        "bars": len(bars),
        "first_timestamp": bars["timestamp_start"].min(),
        "last_timestamp": bars["timestamp_end"].max(),
        "avg_tick_count": bars["tick_count"].mean(),
        "min_tick_count": bars["tick_count"].min(),
        "max_tick_count": bars["tick_count"].max(),
        "avg_duration_seconds": bars["duration_seconds"].mean(),
        "min_duration_seconds": bars["duration_seconds"].min(),
        "max_duration_seconds": bars["duration_seconds"].max(),
        "avg_spread_mean": bars["spread_mean"].mean(),
        "avg_range_points": bars["range_points"].mean(),
        "input_path": str(input_path) if input_path else "",
        "output_path": str(output_path) if output_path else "",
    }


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 09 - BUILD FIXED TICK BARS FOR DATE RANGE")
    print("=" * 90)
    print(f"Symbol:     {SYMBOL}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Tick sizes: {TICK_SIZES}")
    print("-" * 90)

    report_rows = []

    for dt in date_range(start, end):
        in_path = input_tick_path(SYMBOL, dt)

        print(f"\n[DATE] {dt.strftime('%Y-%m-%d')}")

        if not in_path.exists():
            print(f"  [SKIP] Input tick parquet missing: {in_path.name}")

            report_rows.append(
                build_report_row(
                    dt=dt,
                    status="missing_input",
                    input_path=in_path,
                )
            )
            continue

        df = pd.read_parquet(in_path)

        if df.empty:
            print("  [SKIP] Input tick parquet empty.")

            report_rows.append(
                build_report_row(
                    dt=dt,
                    status="empty_input",
                    input_path=in_path,
                )
            )
            continue

        print(f"  Loaded ticks: {len(df):,}")

        for tick_size in TICK_SIZES:
            bars = build_tick_bars(df, tick_size)

            out_path = output_bar_path(SYMBOL, dt, tick_size)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            bars.to_parquet(out_path, index=False)

            report_rows.append(
                build_report_row(
                    dt=dt,
                    status="processed",
                    tick_size=tick_size,
                    bars=bars,
                    output_path=out_path,
                    input_path=in_path,
                )
            )

            print(
                f"  [tick_size={tick_size:>4}] "
                f"bars={len(bars):>6,} | "
                f"avg_duration={bars['duration_seconds'].mean():>8.2f}s | "
                f"avg_range={bars['range_points'].mean():>6.2f} points"
            )

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    report_df = pd.DataFrame(report_rows)
    out_report = report_path(SYMBOL, start, end)
    report_df.to_csv(out_report, index=False)

    processed_rows = report_df[report_df["status"] == "processed"]
    processed_days = processed_rows["date"].nunique()
    missing_days = report_df[report_df["status"] == "missing_input"]["date"].nunique()
    total_bars = processed_rows["bars"].sum() if not processed_rows.empty else 0

    print("\n" + "-" * 90)
    print("[RANGE SUMMARY]")
    print(f"Processed days: {processed_days}")
    print(f"Missing days:   {missing_days}")
    print(f"Total bars:     {int(total_bars):,}")
    print(f"Report:         {out_report}")
    print("[DONE] Dukascopy date-range fixed tick bars built successfully.")


if __name__ == "__main__":
    main()