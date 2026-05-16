from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests


SOURCE = "us_treasury_fiscaldata"
DATASET = "us_treasury_yield_curve"

BASE_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v2/accounting/od/avg_interest_rates"
)


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")
    if env_path:
        return Path(env_path)

    linux_path = Path("/mnt/quant_lab")
    if linux_path.exists():
        return linux_path

    raise FileNotFoundError("Could not find /mnt/quant_lab and DATA_LAKE_ROOT is not set.")


def build_output_dir(data_lake_root: Path, run_time_utc: datetime) -> Path:
    return (
        data_lake_root
        / "data"
        / "raw"
        / "information_data"
        / DATASET
        / f"source={SOURCE}"
        / f"year={run_time_utc:%Y}"
        / f"month={run_time_utc:%m}"
    )


def fetch_treasury_data() -> list[dict]:
    rows = []
    page_number = 1

    while True:
        params = {
            "page[number]": page_number,
            "page[size]": 10000,
            "sort": "record_date",
        }

        response = requests.get(BASE_URL, params=params, timeout=30)

        if response.status_code != 200:
            print(f"[WARN] HTTP {response.status_code}: {response.text[:300]}")
            break

        payload = response.json()
        data = payload.get("data", [])

        if not data:
            break

        rows.extend(data)

        meta = payload.get("meta", {})
        total_pages = int(meta.get("total-pages", page_number))

        if page_number >= total_pages:
            break

        page_number += 1

    return rows


def collect_yield_curve() -> pd.DataFrame:
    print("[FETCH] US Treasury FiscalData interest-rate data")

    run_time_utc = datetime.now(timezone.utc)
    rows = fetch_treasury_data()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["run_time_utc"] = run_time_utc.isoformat()
    df["snapshot_date"] = run_time_utc.date().isoformat()
    df["source"] = SOURCE
    df["source_url"] = BASE_URL

    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in df.columns:
        if col not in ["run_time_utc", "snapshot_date", "source", "source_url"]:
            if "date" not in col and col not in ["security_desc", "security_type_desc"]:
                converted = pd.to_numeric(df[col], errors="coerce")

                if converted.notna().sum() > 0:
                    df[col] = converted

    preferred = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "record_date",
        "security_type_desc",
        "security_desc",
        "avg_interest_rate_amt",
        "source_url",
    ]

    existing = [col for col in preferred if col in df.columns]
    remaining = [col for col in df.columns if col not in existing]

    df = df[existing + remaining]

    if "record_date" in df.columns:
        df = df.sort_values("record_date").reset_index(drop=True)

    return df


def save_outputs(df: pd.DataFrame, output_dir: Path, run_time_utc: datetime) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = run_time_utc.strftime("%Y_%m_%d_%H%M%S")

    parquet_path = output_dir / f"{DATASET}_{timestamp}.parquet"
    csv_path = output_dir / f"{DATASET}_{timestamp}.csv"
    latest_parquet = output_dir / f"{DATASET}_latest.parquet"
    latest_csv = output_dir / f"{DATASET}_latest.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    df.to_parquet(latest_parquet, index=False)
    df.to_csv(latest_csv, index=False)

    print()
    print("[DONE] US Treasury interest-rate data saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Columns:        {len(df.columns):,}")

    if "record_date" in df.columns:
        print(f"Latest date:    {df['record_date'].max()}")

    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - US TREASURY INTEREST RATE DATA")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Source URL: {BASE_URL}")
    print("-" * 90)

    df = collect_yield_curve()

    if df.empty:
        print("[WARN] No Treasury data collected.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
