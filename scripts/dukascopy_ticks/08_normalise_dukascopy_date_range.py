"""
BACQE DUKASCOPY 08 - NORMALISE DATE RANGE TO PARQUET

Purpose:
    Decode one month of Dukascopy raw .bi5 tick files and save a clean
    BACQE-compatible daily Parquet file.

Inputs:
    E:\\Quant_Lab\\data\\raw\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\*.bi5

Outputs:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=EURUSD\\year=2024\\month=01\\*.parquet

Also saves:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\normalisation_reports\\EURUSD_2024-01-01_to_2024-01-31_normalisation_report.csv
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
START_DATE = "2023-01-01"
END_DATE = "2025-12-31"

SOURCE = "dukascopy"

PRICE_SCALE = 100000
POINT_SIZE = 0.00001
RECORD_SIZE = 20


# =============================================================================
# DATE HELPERS
# =============================================================================

def date_range(start: datetime, end: datetime):
    """
    Yield each date from start to end inclusive.
    """

    current = start

    while current <= end:
        yield current
        current += timedelta(days=1)


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
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 08 - NORMALISE DATE RANGE TO PARQUET")
    print("=" * 90)
    print(f"Symbol:     {SYMBOL}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("-" * 90)

    range_report_rows = []

    for dt in date_range(start, end):
        print(f"\n[DATE] {dt.strftime('%Y-%m-%d')}")

        dfs = []

        for hour in range(24):
            df_hour = decode_hour_file(SYMBOL, dt, hour)

            if df_hour.empty:
                print(f"  [{hour:02d}:00] no rows decoded")
            else:
                print(f"  [{hour:02d}:00] decoded {len(df_hour):>7,} rows")
                dfs.append(df_hour)

        if not dfs:
            print("  [SKIP] No rows decoded for this date.")

            range_report_rows.append({
                "date": dt.strftime("%Y-%m-%d"),
                "status": "no_rows_decoded",
                "raw_rows": 0,
                "clean_rows": 0,
                "removed_rows": 0,
                "output_path": "",
                "quality_report_path": "",
            })

            continue

        df_raw = pd.concat(dfs, ignore_index=True)
        df_clean, clean_report = clean_ticks(df_raw)

        out_path = processed_output_path(SYMBOL, dt)
        daily_report_path = quality_report_path(SYMBOL, dt)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        daily_report_path.parent.mkdir(parents=True, exist_ok=True)

        df_clean.to_parquet(out_path, index=False)

        quality_df = build_quality_report(df_clean, clean_report)
        quality_df.to_csv(daily_report_path, index=False)

        removed_rows = len(df_raw) - len(df_clean)

        range_report_rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "status": "processed",
            "raw_rows": len(df_raw),
            "clean_rows": len(df_clean),
            "removed_rows": removed_rows,
            "first_timestamp_utc": df_clean["timestamp_utc"].min() if not df_clean.empty else None,
            "last_timestamp_utc": df_clean["timestamp_utc"].max() if not df_clean.empty else None,
            "avg_spread_points": df_clean["spread_points"].mean() if not df_clean.empty else None,
            "min_spread_points": df_clean["spread_points"].min() if not df_clean.empty else None,
            "max_spread_points": df_clean["spread_points"].max() if not df_clean.empty else None,
            "output_path": str(out_path),
            "quality_report_path": str(daily_report_path),
        })

        print(
            f"  [DONE] raw={len(df_raw):,} | "
            f"clean={len(df_clean):,} | "
            f"removed={removed_rows:,}"
        )

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    range_report_path = (
        REPORT_ROOT
        / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_normalisation_report.csv"
    )

    range_report_df = pd.DataFrame(range_report_rows)
    range_report_df.to_csv(range_report_path, index=False)

    processed_days = (range_report_df["status"] == "processed").sum()
    skipped_days = (range_report_df["status"] != "processed").sum()
    total_clean_rows = range_report_df["clean_rows"].sum()

    print("\n" + "-" * 90)
    print("[RANGE SUMMARY]")
    print(f"Processed days:   {processed_days}")
    print(f"Skipped days:     {skipped_days}")
    print(f"Total clean rows: {total_clean_rows:,}")
    print(f"Range report:     {range_report_path}")
    print("[DONE] Dukascopy date-range normalisation complete.")


if __name__ == "__main__":
    main()