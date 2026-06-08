"""
BACQE DUKASCOPY 03 - INSPECT RAW BI5 TICK FILES

Purpose:
    Decompress and inspect one day of raw Dukascopy .bi5 tick files.

This script:
    - Reads the 24 hourly .bi5 files downloaded by Script 02
    - Decompresses them using lzma
    - Decodes Dukascopy tick records
    - Builds a small inspection DataFrame
    - Saves an inspection CSV report

Important:
    Dukascopy .bi5 records are 20 bytes each:
        time_delta_ms: int32
        ask:           int32
        bid:           int32
        ask_volume:    float32
        bid_volume:    float32
"""

from pathlib import Path
from datetime import datetime, timezone, timedelta
import lzma
import struct

import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

RAW_ROOT = DATA_ROOT / "raw" / "dukascopy_ticks"
ANALYSIS_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "bi5_inspection"

SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"

PRICE_SCALE = 100000
RECORD_SIZE = 20


# =============================================================================
# HELPERS
# =============================================================================

def get_daily_raw_dir(symbol: str, dt: datetime) -> Path:
    return (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
    )


def get_bi5_file_path(symbol: str, dt: datetime, hour: int) -> Path:
    return (
        get_daily_raw_dir(symbol, dt)
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_{hour:02d}h_ticks.bi5"
    )


def decode_bi5_file(file_path: Path, dt: datetime, hour: int) -> pd.DataFrame:
    """
    Decode one Dukascopy .bi5 hourly tick file.

    Each record is:
        >iii ff

    > means big-endian.
    i = signed integer.
    f = float.
    """

    if not file_path.exists():
        return pd.DataFrame()

    compressed_bytes = file_path.read_bytes()

    if not compressed_bytes:
        return pd.DataFrame()

    try:
        raw_bytes = lzma.decompress(compressed_bytes)
    except Exception as exc:
        print(f"[ERROR] Could not decompress {file_path.name}: {exc}")
        return pd.DataFrame()

    if len(raw_bytes) % RECORD_SIZE != 0:
        print(
            f"[WARNING] {file_path.name} raw byte length "
            f"{len(raw_bytes)} is not divisible by {RECORD_SIZE}"
        )

    rows = []

    base_time = datetime(
        dt.year,
        dt.month,
        dt.day,
        hour,
        0,
        0,
        tzinfo=timezone.utc,
    )

    usable_length = len(raw_bytes) - (len(raw_bytes) % RECORD_SIZE)

    for offset in range(0, usable_length, RECORD_SIZE):
        record = raw_bytes[offset: offset + RECORD_SIZE]

        time_delta_ms, ask_raw, bid_raw, ask_volume, bid_volume = struct.unpack(
            ">iiiff",
            record,
        )

        timestamp_utc = base_time + timedelta(milliseconds=time_delta_ms)

        ask = ask_raw / PRICE_SCALE
        bid = bid_raw / PRICE_SCALE
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_points = spread / 0.00001

        rows.append(
            {
                "timestamp_utc": timestamp_utc,
                "symbol": SYMBOL,
                "source": "dukascopy",
                "hour": hour,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread": spread,
                "spread_points": spread_points,
                "bid_volume": bid_volume,
                "ask_volume": ask_volume,
                "quote_volume": bid_volume + ask_volume,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    dt = datetime.strptime(DATE_STR, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 03 - INSPECT RAW BI5 TICK FILES")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Date:   {DATE_STR}")
    print("-" * 90)

    dfs = []
    hourly_summary = []

    for hour in range(24):
        file_path = get_bi5_file_path(SYMBOL, dt, hour)

        df_hour = decode_bi5_file(file_path, dt, hour)

        if df_hour.empty:
            print(f"[{hour:02d}:00] no decoded ticks | {file_path.name}")
            hourly_summary.append(
                {
                    "hour": hour,
                    "file": file_path.name,
                    "ticks": 0,
                    "first_timestamp_utc": None,
                    "last_timestamp_utc": None,
                    "avg_spread_points": None,
                    "min_spread_points": None,
                    "max_spread_points": None,
                }
            )
            continue

        dfs.append(df_hour)

        hourly_summary.append(
            {
                "hour": hour,
                "file": file_path.name,
                "ticks": len(df_hour),
                "first_timestamp_utc": df_hour["timestamp_utc"].min(),
                "last_timestamp_utc": df_hour["timestamp_utc"].max(),
                "avg_spread_points": df_hour["spread_points"].mean(),
                "min_spread_points": df_hour["spread_points"].min(),
                "max_spread_points": df_hour["spread_points"].max(),
            }
        )

        print(
            f"[{hour:02d}:00] decoded {len(df_hour):>7,} ticks | "
            f"spread avg={df_hour['spread_points'].mean():.2f} points | "
            f"{file_path.name}"
        )

    if not dfs:
        print("[ERROR] No ticks decoded from any file.")
        return

    df = pd.concat(dfs, ignore_index=True)
    summary_df = pd.DataFrame(hourly_summary)

    ANALYSIS_ROOT.mkdir(parents=True, exist_ok=True)

    inspection_path = ANALYSIS_ROOT / f"{SYMBOL}_{DATE_STR}_tick_inspection_sample.csv"
    hourly_summary_path = ANALYSIS_ROOT / f"{SYMBOL}_{DATE_STR}_hourly_summary.csv"

    df.head(1000).to_csv(inspection_path, index=False)
    summary_df.to_csv(hourly_summary_path, index=False)

    print("-" * 90)
    print("[DAILY SUMMARY]")
    print(f"Total ticks:      {len(df):,}")
    print(f"First timestamp:  {df['timestamp_utc'].min()}")
    print(f"Last timestamp:   {df['timestamp_utc'].max()}")
    print(f"Average spread:   {df['spread_points'].mean():.2f} points")
    print(f"Min spread:       {df['spread_points'].min():.2f} points")
    print(f"Max spread:       {df['spread_points'].max():.2f} points")
    print(f"Bid min/max:      {df['bid'].min()} / {df['bid'].max()}")
    print(f"Ask min/max:      {df['ask'].min()} / {df['ask'].max()}")

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Inspection sample: {inspection_path}")
    print(f"Hourly summary:    {hourly_summary_path}")

    print("-" * 90)
    print("[PREVIEW]")
    print(df.head(10).to_string(index=False))

    print("[DONE] BI5 inspection complete.")


if __name__ == "__main__":
    main()