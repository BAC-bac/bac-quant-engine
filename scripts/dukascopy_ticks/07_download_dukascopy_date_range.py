"""
BACQE DUKASCOPY 07 - DOWNLOAD DATE RANGE OF RAW TICK FILES

Purpose:
    Download raw hourly Dukascopy .bi5 tick files across a date range.

Refactor note:
    This script can still be run standalone, but now also exposes run_download()
    so other BACQE scripts can call the proven downloader logic for any symbol/date range.
"""

from pathlib import Path
from datetime import datetime, timedelta
import argparse
import csv
import time

import requests


DATA_ROOT = Path(r"E:\Quant_Lab\data")
RAW_ROOT = DATA_ROOT / "raw" / "dukascopy_ticks"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "download_reports"

DEFAULT_SYMBOL = "EURUSD"
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_END_DATE = "2025-12-31"

BASE_URL = "https://datafeed.dukascopy.com/datafeed"

REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.20
MAX_RETRIES = 3


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_dukascopy_tick_url(symbol: str, dt: datetime, hour: int) -> str:
    zero_based_month = dt.month - 1
    return (
        f"{BASE_URL}/{symbol}/"
        f"{dt.year:04d}/{zero_based_month:02d}/{dt.day:02d}/"
        f"{hour:02d}h_ticks.bi5"
    )


def build_output_path(symbol: str, dt: datetime, hour: int) -> Path:
    return (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_{hour:02d}h_ticks.bi5"
    )


def download_file(url: str, output_path: Path) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and output_path.stat().st_size > 0:
        return {
            "status": "exists",
            "bytes": output_path.stat().st_size,
            "path": str(output_path),
            "url": url,
            "error": "",
        }

    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200 and response.content:
                output_path.write_bytes(response.content)
                return {
                    "status": "downloaded",
                    "bytes": output_path.stat().st_size,
                    "path": str(output_path),
                    "url": url,
                    "error": "",
                }

            last_error = f"HTTP {response.status_code}"

            if response.status_code == 404:
                return {
                    "status": "missing",
                    "bytes": 0,
                    "path": str(output_path),
                    "url": url,
                    "error": last_error,
                }

        except Exception as exc:
            last_error = repr(exc)

        time.sleep(SLEEP_SECONDS * attempt)

    return {
        "status": "failed",
        "bytes": 0,
        "path": str(output_path),
        "url": url,
        "error": last_error,
    }


def save_daily_manifest(rows: list[dict], symbol: str, dt: datetime) -> Path:
    manifest_dir = (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
    )
    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifest_dir / f"{symbol}_{dt.strftime('%Y-%m-%d')}_download_manifest.csv"

    fieldnames = ["date", "hour", "status", "bytes", "path", "url", "error"]

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return manifest_path


def save_range_report(rows: list[dict], symbol: str, start: datetime, end: datetime) -> Path:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    report_path = (
        REPORT_ROOT
        / f"{symbol}_{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_download_report.csv"
    )

    fieldnames = [
        "date",
        "hour",
        "status",
        "bytes",
        "path",
        "url",
        "error",
    ]

    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return report_path


def download_one_hour(
    symbol: str,
    date: str,
    hour: int,
) -> dict:
    symbol = symbol.upper().strip()
    dt = datetime.strptime(date, "%Y-%m-%d")

    url = build_dukascopy_tick_url(symbol, dt, hour)
    output_path = build_output_path(symbol, dt, hour)

    result = download_file(url, output_path)

    row = {
        "date": dt.strftime("%Y-%m-%d"),
        "hour": hour,
        "status": result["status"],
        "bytes": result["bytes"],
        "path": result["path"],
        "url": result["url"],
        "error": result["error"],
    }

    return row


def run_download(
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
) -> Path:
    symbol = symbol.upper().strip()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 07 - DOWNLOAD DATE RANGE OF RAW TICK FILES")
    print("=" * 90)
    print(f"Symbol:     {symbol}")
    print(f"Start date: {start_date}")
    print(f"End date:   {end_date}")
    print(f"Raw root:   {RAW_ROOT}")
    print("-" * 90)

    all_rows = []

    for dt in date_range(start, end):
        daily_rows = []

        print(f"\n[DATE] {dt.strftime('%Y-%m-%d')}")

        for hour in range(24):
            url = build_dukascopy_tick_url(symbol, dt, hour)
            output_path = build_output_path(symbol, dt, hour)

            result = download_file(url, output_path)

            row = {
                "date": dt.strftime("%Y-%m-%d"),
                "hour": hour,
                "status": result["status"],
                "bytes": result["bytes"],
                "path": result["path"],
                "url": result["url"],
                "error": result["error"],
            }

            daily_rows.append(row)
            all_rows.append(row)

            print(
                f"  [{hour:02d}:00] {result['status']:>10} | "
                f"{result['bytes']:>10} bytes"
            )

            time.sleep(SLEEP_SECONDS)

        save_daily_manifest(daily_rows, symbol, dt)

        daily_downloaded = sum(1 for r in daily_rows if r["status"] == "downloaded")
        daily_existing = sum(1 for r in daily_rows if r["status"] == "exists")
        daily_missing = sum(1 for r in daily_rows if r["status"] == "missing")
        daily_failed = sum(1 for r in daily_rows if r["status"] == "failed")
        daily_bytes = sum(r["bytes"] for r in daily_rows)

        print(
            f"  Daily summary | downloaded={daily_downloaded} | "
            f"existing={daily_existing} | missing={daily_missing} | "
            f"failed={daily_failed} | bytes={daily_bytes:,}"
        )

    range_report = save_range_report(all_rows, symbol, start, end)

    downloaded = sum(1 for r in all_rows if r["status"] == "downloaded")
    existing = sum(1 for r in all_rows if r["status"] == "exists")
    missing = sum(1 for r in all_rows if r["status"] == "missing")
    failed = sum(1 for r in all_rows if r["status"] == "failed")
    total_bytes = sum(r["bytes"] for r in all_rows)

    print("\n" + "-" * 90)
    print("[RANGE SUMMARY]")
    print(f"Total files attempted: {len(all_rows)}")
    print(f"Downloaded:            {downloaded}")
    print(f"Existing:              {existing}")
    print(f"Missing:               {missing}")
    print(f"Failed:                {failed}")
    print(f"Total size:            {total_bytes:,} bytes")
    print(f"Range report:          {range_report}")
    print("[DONE] Dukascopy date-range raw download complete.")

    return range_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Dukascopy raw BI5 tick files.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_download(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
