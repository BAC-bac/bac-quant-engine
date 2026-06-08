"""
BACQE DUKASCOPY 04 - NORMALISE ONE DAY TO PARQUET

Purpose:
    Decode one day of Dukascopy raw .bi5 tick files and save a clean
    BACQE-compatible daily Parquet file.

Inputs:
    E:\\Quant_Lab\\data\\raw\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\*.bi5

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\EURUSD_2024-01-02_ticks.parquet

Also saves:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\normalisation_reports\\EURUSD_2024-01-02_quality_report.csv
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
PROCESSED_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "normalisation_reports"

SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"

SOURCE = "dukascopy"

PRICE_SCALE = 100000
POINT_SIZE = 0.00001
RECORD_SIZE = 20


# =============================================================================
# PATH HELPERS
# =============================================================================

def raw_day_dir(symbol: str, dt: datetime) -> Path:
    return (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
    )


def raw_bi5_path(symbol: str, dt: datetime, hour: int) -> Path:
    return (
        raw_day_dir(symbol, dt)
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_{hour:02d}h_ticks.bi5"
    )


def processed_output_path(symbol: str, dt: datetime) -> Path:
    return (
        PROCESSED_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_ticks.parquet"
    )


def quality_report_path(symbol: str, dt: datetime) -> Path:
    return (
        REPORT_ROOT
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_quality_report.csv"
    )


# =============================================================================
# DECODING
# =============================================================================

def decode_hour_file(symbol: str, dt: datetime, hour: int) -> pd.DataFrame:
    file_path = raw_bi5_path(symbol, dt, hour)

    if not file_path.exists():
        print(f"[MISSING] {file_path.name}")
        return pd.DataFrame()

    compressed_bytes = file_path.read_bytes()

    if not compressed_bytes:
        print(f"[EMPTY] {file_path.name}")
        return pd.DataFrame()

    try:
        raw_bytes = lzma.decompress(compressed_bytes)
    except Exception as exc:
        print(f"[DECOMPRESS ERROR] {file_path.name}: {exc}")
        return pd.DataFrame()

    usable_length = len(raw_bytes) - (len(raw_bytes) % RECORD_SIZE)

    base_time = datetime(
        dt.year,
        dt.month,
        dt.day,
        hour,
        0,
        0,
        tzinfo=timezone.utc,
    )

    rows = []

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
        spread_points = spread / POINT_SIZE

        rows.append(
            {
                "timestamp_utc": timestamp_utc,
                "symbol": symbol,
                "source": SOURCE,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread": spread,
                "spread_points": spread_points,
                "bid_volume": float(bid_volume),
                "ask_volume": float(ask_volume),
                "quote_volume": float(bid_volume + ask_volume),
                "hour": hour,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# CLEANING / VALIDATION
# =============================================================================

def clean_ticks(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    original_rows = len(df)

    report = {
        "original_rows": original_rows,
        "removed_null_timestamps": 0,
        "removed_null_prices": 0,
        "removed_non_positive_prices": 0,
        "removed_crossed_spread": 0,
        "removed_negative_volume": 0,
        "removed_duplicates": 0,
        "final_rows": 0,
    }

    df = df.copy()

    before = len(df)
    df = df.dropna(subset=["timestamp_utc"])
    report["removed_null_timestamps"] = before - len(df)

    before = len(df)
    df = df.dropna(subset=["bid", "ask"])
    report["removed_null_prices"] = before - len(df)

    before = len(df)
    df = df[(df["bid"] > 0) & (df["ask"] > 0)]
    report["removed_non_positive_prices"] = before - len(df)

    before = len(df)
    df = df[df["ask"] >= df["bid"]]
    report["removed_crossed_spread"] = before - len(df)

    before = len(df)
    df = df[(df["bid_volume"] >= 0) & (df["ask_volume"] >= 0)]
    report["removed_negative_volume"] = before - len(df)

    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    before = len(df)
    df = df.drop_duplicates(
        subset=[
            "timestamp_utc",
            "bid",
            "ask",
            "bid_volume",
            "ask_volume",
        ]
    ).reset_index(drop=True)
    report["removed_duplicates"] = before - len(df)

    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]
    df["spread_points"] = df["spread"] / POINT_SIZE
    df["quote_volume"] = df["bid_volume"] + df["ask_volume"]

    report["final_rows"] = len(df)

    return df, report


def build_quality_report(df: pd.DataFrame, clean_report: dict) -> pd.DataFrame:
    if df.empty:
        rows = clean_report.copy()
        rows.update({
            "first_timestamp_utc": None,
            "last_timestamp_utc": None,
            "avg_spread_points": None,
            "min_spread_points": None,
            "max_spread_points": None,
            "avg_quote_volume": None,
            "total_quote_volume": None,
        })
        return pd.DataFrame([rows])

    rows = clean_report.copy()
    rows.update({
        "first_timestamp_utc": df["timestamp_utc"].min(),
        "last_timestamp_utc": df["timestamp_utc"].max(),
        "avg_spread_points": df["spread_points"].mean(),
        "min_spread_points": df["spread_points"].min(),
        "max_spread_points": df["spread_points"].max(),
        "avg_quote_volume": df["quote_volume"].mean(),
        "total_quote_volume": df["quote_volume"].sum(),
        "bid_min": df["bid"].min(),
        "bid_max": df["bid"].max(),
        "ask_min": df["ask"].min(),
        "ask_max": df["ask"].max(),
    })

    return pd.DataFrame([rows])


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    dt = datetime.strptime(DATE_STR, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 04 - NORMALISE ONE DAY TO PARQUET")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Date:   {DATE_STR}")
    print("-" * 90)

    dfs = []

    for hour in range(24):
        df_hour = decode_hour_file(SYMBOL, dt, hour)

        if df_hour.empty:
            print(f"[{hour:02d}:00] no rows decoded")
        else:
            print(f"[{hour:02d}:00] decoded {len(df_hour):>7,} rows")
            dfs.append(df_hour)

    if not dfs:
        print("[ERROR] No rows decoded. Cannot create Parquet file.")
        return

    df_raw = pd.concat(dfs, ignore_index=True)

    print("-" * 90)
    print(f"Raw decoded rows: {len(df_raw):,}")

    df_clean, clean_report = clean_ticks(df_raw)

    print(f"Clean rows:       {len(df_clean):,}")
    print(f"Removed rows:     {len(df_raw) - len(df_clean):,}")

    out_path = processed_output_path(SYMBOL, dt)
    report_path = quality_report_path(SYMBOL, dt)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    df_clean.to_parquet(out_path, index=False)

    quality_df = build_quality_report(df_clean, clean_report)
    quality_df.to_csv(report_path, index=False)

    print("-" * 90)
    print("[QUALITY SUMMARY]")
    for key, value in clean_report.items():
        print(f"{key}: {value}")

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Parquet:        {out_path}")
    print(f"Quality report: {report_path}")

    print("-" * 90)
    print("[PREVIEW]")
    print(df_clean.head(10).to_string(index=False))

    print("[DONE] Dukascopy daily normalisation complete.")


if __name__ == "__main__":
    main()