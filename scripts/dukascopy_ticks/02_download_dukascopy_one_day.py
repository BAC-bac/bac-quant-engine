"""
BACQE DUKASCOPY 02 - DOWNLOAD ONE DAY OF RAW TICK FILES

Purpose:
    Download one day of raw Dukascopy EURUSD hourly tick files.

Notes:
    Dukascopy raw tick data is stored as hourly .bi5 files.
    The month in the direct URL is zero-based:
        January = 00
        February = 01
        ...
        December = 11

Initial target:
    EURUSD
    2024-01-02
"""

from pathlib import Path
from datetime import datetime
import time
import csv
import requests


# =============================================================================
# CONFIG
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

RAW_ROOT = DATA_ROOT / "raw" / "dukascopy_ticks"

SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"

BASE_URL = "https://datafeed.dukascopy.com/datafeed"

REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.25
MAX_RETRIES = 3


# =============================================================================
# HELPERS
# =============================================================================

def build_dukascopy_tick_url(symbol: str, dt: datetime, hour: int) -> str:
    """
    Build Dukascopy hourly tick .bi5 URL.

    Dukascopy URL format:
        https://datafeed.dukascopy.com/datafeed/EURUSD/2024/00/02/00h_ticks.bi5

    Important:
        Dukascopy months are zero-indexed in the URL.
    """

    year = dt.year
    zero_based_month = dt.month - 1
    day = dt.day

    return (
        f"{BASE_URL}/{symbol}/"
        f"{year:04d}/{zero_based_month:02d}/{day:02d}/"
        f"{hour:02d}h_ticks.bi5"
    )


def build_output_path(symbol: str, dt: datetime, hour: int) -> Path:
    """Build BACQE raw storage path for one hourly .bi5 file."""

    return (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_{hour:02d}h_ticks.bi5"
    )


def download_file(url: str, output_path: Path) -> dict:
    """Download one file with basic retry handling."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and output_path.stat().st_size > 0:
        return {
            "status": "exists",
            "url": url,
            "path": str(output_path),
            "bytes": output_path.stat().st_size,
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
                    "url": url,
                    "path": str(output_path),
                    "bytes": output_path.stat().st_size,
                    "error": "",
                }

            last_error = f"HTTP {response.status_code}"

            if response.status_code == 404:
                return {
                    "status": "missing",
                    "url": url,
                    "path": str(output_path),
                    "bytes": 0,
                    "error": last_error,
                }

        except Exception as exc:
            last_error = repr(exc)

        time.sleep(SLEEP_SECONDS * attempt)

    return {
        "status": "failed",
        "url": url,
        "path": str(output_path),
        "bytes": 0,
        "error": last_error,
    }


def save_manifest(rows: list[dict], symbol: str, dt: datetime) -> Path:
    """Save a CSV manifest of all download attempts."""

    manifest_dir = (
        RAW_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
    )

    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifest_dir / f"{symbol}_{dt.strftime('%Y-%m-%d')}_download_manifest.csv"

    fieldnames = ["hour", "status", "bytes", "path", "url", "error"]

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({
                "hour": row["hour"],
                "status": row["status"],
                "bytes": row["bytes"],
                "path": row["path"],
                "url": row["url"],
                "error": row["error"],
            })

    return manifest_path


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    dt = datetime.strptime(DATE_STR, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 02 - DOWNLOAD ONE DAY OF RAW TICK FILES")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Date:   {DATE_STR}")
    print(f"Raw root: {RAW_ROOT}")
    print("-" * 90)

    results = []

    for hour in range(24):
        url = build_dukascopy_tick_url(SYMBOL, dt, hour)
        output_path = build_output_path(SYMBOL, dt, hour)

        result = download_file(url, output_path)
        result["hour"] = hour
        results.append(result)

        print(
            f"[{hour:02d}:00] {result['status']:>10} | "
            f"{result['bytes']:>10} bytes | {output_path.name}"
        )

        time.sleep(SLEEP_SECONDS)

    manifest_path = save_manifest(results, SYMBOL, dt)

    downloaded = sum(1 for r in results if r["status"] == "downloaded")
    existing = sum(1 for r in results if r["status"] == "exists")
    missing = sum(1 for r in results if r["status"] == "missing")
    failed = sum(1 for r in results if r["status"] == "failed")
    total_bytes = sum(r["bytes"] for r in results)

    print("-" * 90)
    print("[SUMMARY]")
    print(f"Downloaded: {downloaded}")
    print(f"Existing:   {existing}")
    print(f"Missing:    {missing}")
    print(f"Failed:     {failed}")
    print(f"Total size: {total_bytes:,} bytes")
    print(f"Manifest:   {manifest_path}")
    print("[DONE] Dukascopy one-day raw download attempt complete.")


if __name__ == "__main__":
    main()