"""
BACQE DUKASCOPY 10 - BUILD TICK IMBALANCE BARS FOR DATE RANGE

Purpose:
    Load processed Dukascopy daily tick Parquet files across a date range
    and build threshold-based Tick Imbalance Bars for each valid day.

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_tick_imbalance_bars\\symbol=EURUSD\\threshold=25\\year=2024\\month=01\\*.parquet
"""

from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

TICK_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "tick_imbalance_bar_reports"

SYMBOL = "EURUSD"
START_DATE = "2024-04-01"
END_DATE = "2024-06-30"

IMBALANCE_THRESHOLDS = [25, 50, 100]

POINT_SIZE = 0.00001


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def input_tick_path(symbol: str, dt: datetime) -> Path:
    return (
        TICK_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_ticks.parquet"
    )


def output_tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def report_path(symbol: str, start: datetime, end: datetime) -> Path:
    return (
        REPORT_ROOT
        / f"{symbol}_{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_tib_report.csv"
    )


def add_tick_rule_signs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp_utc").reset_index(drop=True).copy()

    price_diff = df["mid"].diff()
    raw_sign = np.sign(price_diff).fillna(0).astype(int)

    signs = []
    last_sign = 1

    for sign in raw_sign:
        if sign == 0:
            signs.append(last_sign)
        else:
            signs.append(sign)
            last_sign = sign

    df["tick_sign"] = signs
    df["signed_tick"] = df["tick_sign"]

    return df


def build_tick_imbalance_bars(df: pd.DataFrame, threshold: int) -> pd.DataFrame:
    df = add_tick_rule_signs(df)

    bars = []
    current_rows = []
    cum_imbalance = 0
    bar_id = 0

    for row in df.itertuples(index=False):
        row_dict = row._asdict()

        current_rows.append(row_dict)
        cum_imbalance += int(row_dict["signed_tick"])

        if abs(cum_imbalance) >= threshold:
            bar_df = pd.DataFrame(current_rows)
            signed_tick_sum = bar_df["signed_tick"].sum()

            bars.append({
                "bar_id": bar_id,
                "timestamp_start": bar_df["timestamp_utc"].iloc[0],
                "timestamp_end": bar_df["timestamp_utc"].iloc[-1],
                "symbol": bar_df["symbol"].iloc[0],
                "source": bar_df["source"].iloc[0],
                "imbalance_threshold": threshold,
                "tick_count": len(bar_df),
                "duration_seconds": (
                    bar_df["timestamp_utc"].iloc[-1]
                    - bar_df["timestamp_utc"].iloc[0]
                ).total_seconds(),

                "open": bar_df["mid"].iloc[0],
                "high": bar_df["mid"].max(),
                "low": bar_df["mid"].min(),
                "close": bar_df["mid"].iloc[-1],

                "bid_open": bar_df["bid"].iloc[0],
                "bid_high": bar_df["bid"].max(),
                "bid_low": bar_df["bid"].min(),
                "bid_close": bar_df["bid"].iloc[-1],

                "ask_open": bar_df["ask"].iloc[0],
                "ask_high": bar_df["ask"].max(),
                "ask_low": bar_df["ask"].min(),
                "ask_close": bar_df["ask"].iloc[-1],

                "spread_open": bar_df["spread_points"].iloc[0],
                "spread_high": bar_df["spread_points"].max(),
                "spread_low": bar_df["spread_points"].min(),
                "spread_close": bar_df["spread_points"].iloc[-1],
                "spread_mean": bar_df["spread_points"].mean(),

                "bid_volume_sum": bar_df["bid_volume"].sum(),
                "ask_volume_sum": bar_df["ask_volume"].sum(),
                "quote_volume_sum": bar_df["quote_volume"].sum(),

                "signed_tick_sum": signed_tick_sum,
                "buy_ticks": int((bar_df["signed_tick"] > 0).sum()),
                "sell_ticks": int((bar_df["signed_tick"] < 0).sum()),
                "imbalance_direction": "buy" if signed_tick_sum > 0 else "sell",
            })

            current_rows = []
            cum_imbalance = 0
            bar_id += 1

    bars_df = pd.DataFrame(bars)

    if bars_df.empty:
        return bars_df

    bars_df["return_close_to_close"] = bars_df["close"].pct_change()
    bars_df["range"] = bars_df["high"] - bars_df["low"]
    bars_df["range_points"] = bars_df["range"] / POINT_SIZE

    ordered_cols = [
        "bar_id",
        "timestamp_start",
        "timestamp_end",
        "symbol",
        "source",
        "imbalance_threshold",
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

        "signed_tick_sum",
        "buy_ticks",
        "sell_ticks",
        "imbalance_direction",
    ]

    return bars_df[ordered_cols]


def build_report_row(
    dt: datetime,
    status: str,
    threshold: int | None = None,
    bars: pd.DataFrame | None = None,
    input_path: Path | None = None,
    output_path: Path | None = None,
) -> dict:
    if bars is None or bars.empty:
        return {
            "date": dt.strftime("%Y-%m-%d"),
            "status": status,
            "threshold": threshold,
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
            "buy_bars": 0,
            "sell_bars": 0,
            "input_path": str(input_path) if input_path else "",
            "output_path": str(output_path) if output_path else "",
        }

    return {
        "date": dt.strftime("%Y-%m-%d"),
        "status": status,
        "threshold": threshold,
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
        "buy_bars": int((bars["imbalance_direction"] == "buy").sum()),
        "sell_bars": int((bars["imbalance_direction"] == "sell").sum()),
        "input_path": str(input_path) if input_path else "",
        "output_path": str(output_path) if output_path else "",
    }


def main() -> None:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 10 - BUILD TICK IMBALANCE BARS FOR DATE RANGE")
    print("=" * 90)
    print(f"Symbol:     {SYMBOL}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Thresholds: {IMBALANCE_THRESHOLDS}")
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

        for threshold in IMBALANCE_THRESHOLDS:
            bars = build_tick_imbalance_bars(df, threshold)

            out_path = output_tib_path(SYMBOL, dt, threshold)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            bars.to_parquet(out_path, index=False)

            report_rows.append(
                build_report_row(
                    dt=dt,
                    status="processed",
                    threshold=threshold,
                    bars=bars,
                    input_path=in_path,
                    output_path=out_path,
                )
            )

            if bars.empty:
                print(f"  [threshold={threshold:>3}] no bars built")
            else:
                print(
                    f"  [threshold={threshold:>3}] "
                    f"bars={len(bars):>6,} | "
                    f"avg_ticks={bars['tick_count'].mean():>8.2f} | "
                    f"avg_duration={bars['duration_seconds'].mean():>8.2f}s | "
                    f"buy={int((bars['imbalance_direction'] == 'buy').sum()):>4} | "
                    f"sell={int((bars['imbalance_direction'] == 'sell').sum()):>4}"
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
    print(f"Total TIB bars: {int(total_bars):,}")
    print(f"Report:         {out_report}")
    print("[DONE] Dukascopy date-range Tick Imbalance Bars built successfully.")


if __name__ == "__main__":
    main()