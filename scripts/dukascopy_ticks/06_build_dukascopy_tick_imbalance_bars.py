"""
BACQE DUKASCOPY 06 - BUILD TICK IMBALANCE BARS

Purpose:
    Load one processed Dukascopy daily tick Parquet file and build simple
    threshold-based Tick Imbalance Bars (TIBs).

Input:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\EURUSD_2024-01-02_ticks.parquet

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_tick_imbalance_bars\\symbol=EURUSD\\threshold=25\\year=2024\\month=01\\EURUSD_2024-01-02_tib_threshold_25.parquet

Also saves:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\tick_imbalance_bar_reports\\EURUSD_2024-01-02_tib_report.csv
"""

from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

TICK_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "tick_imbalance_bar_reports"

SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"

IMBALANCE_THRESHOLDS = [25, 50, 100]


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


def output_tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def report_path(symbol: str, dt: datetime) -> Path:
    return REPORT_ROOT / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_report.csv"


# =============================================================================
# TICK RULE
# =============================================================================

def add_tick_rule_signs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add tick rule signs.

    Logic:
        price change > 0  => +1
        price change < 0  => -1
        price change == 0 => previous non-zero sign

    This follows the common tick-rule approximation where trade direction is
    inferred from price movement.
    """

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


# =============================================================================
# TIB BUILDER
# =============================================================================

def build_tick_imbalance_bars(df: pd.DataFrame, threshold: int) -> pd.DataFrame:
    """
    Build threshold-based Tick Imbalance Bars.

    A new bar closes when absolute cumulative signed tick imbalance reaches threshold.
    """

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

            bars.append(
                {
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

                    "signed_tick_sum": bar_df["signed_tick"].sum(),
                    "buy_ticks": int((bar_df["signed_tick"] > 0).sum()),
                    "sell_ticks": int((bar_df["signed_tick"] < 0).sum()),
                    "imbalance_direction": (
                        "buy" if bar_df["signed_tick"].sum() > 0 else "sell"
                    ),
                }
            )

            current_rows = []
            cum_imbalance = 0
            bar_id += 1

    bars_df = pd.DataFrame(bars)

    if bars_df.empty:
        return bars_df

    bars_df["return_close_to_close"] = bars_df["close"].pct_change()
    bars_df["range"] = bars_df["high"] - bars_df["low"]
    bars_df["range_points"] = bars_df["range"] / 0.00001

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


def build_report_row(bars: pd.DataFrame, threshold: int, output_path: Path) -> dict:
    return {
        "threshold": threshold,
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
        "buy_bars": int((bars["imbalance_direction"] == "buy").sum()) if not bars.empty else 0,
        "sell_bars": int((bars["imbalance_direction"] == "sell").sum()) if not bars.empty else 0,
        "output_path": str(output_path),
    }


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    dt = datetime.strptime(DATE_STR, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 06 - BUILD TICK IMBALANCE BARS")
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

    for threshold in IMBALANCE_THRESHOLDS:
        bars = build_tick_imbalance_bars(df, threshold)

        out_path = output_tib_path(SYMBOL, dt, threshold)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        bars.to_parquet(out_path, index=False)

        report_rows.append(build_report_row(bars, threshold, out_path))

        if bars.empty:
            print(f"[threshold={threshold:>3}] no bars built")
        else:
            print(
                f"[threshold={threshold:>3}] "
                f"bars={len(bars):>6,} | "
                f"avg_ticks={bars['tick_count'].mean():>8.2f} | "
                f"avg_duration={bars['duration_seconds'].mean():>8.2f}s | "
                f"buy={int((bars['imbalance_direction'] == 'buy').sum()):>4} | "
                f"sell={int((bars['imbalance_direction'] == 'sell').sum()):>4}"
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

    print("[DONE] Dukascopy Tick Imbalance Bars built successfully.")


if __name__ == "__main__":
    main()