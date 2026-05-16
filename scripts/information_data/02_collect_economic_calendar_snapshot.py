from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

from dotenv import load_dotenv

load_dotenv()

SOURCE = "financialmodelingprep"
DATASET = "economic_calendar_snapshots"
BASE_URL = "https://financialmodelingprep.com/stable/economic-calendar"


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


def fetch_economic_calendar(api_key: str, start_date: str, end_date: str) -> list[dict]:
    params = {
        "from": start_date,
        "to": end_date,
        "apikey": api_key,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    if response.status_code == 402:
        print(
            "[WARN] FMP returned 402 Payment Required. "
            "API key works, but this endpoint appears unavailable on the current plan."
        )
        return []

    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, dict):
        if "Error Message" in payload:
            raise RuntimeError(payload["Error Message"])
        if "error" in payload:
            raise RuntimeError(str(payload["error"]))

    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected response type: {type(payload)}")

    return payload


def normalise_calendar(payload: list[dict], run_time_utc: datetime) -> pd.DataFrame:
    df = pd.DataFrame(payload)

    if df.empty:
        return df

    df = df.copy()

    df["run_time_utc"] = run_time_utc.isoformat()
    df["snapshot_date"] = run_time_utc.date().isoformat()
    df["source"] = SOURCE

    preferred_first = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "date",
        "country",
        "event",
        "currency",
        "impact",
        "actual",
        "previous",
        "estimate",
        "change",
        "changePercentage",
        "unit",
    ]

    existing_first = [col for col in preferred_first if col in df.columns]
    remaining = [col for col in df.columns if col not in existing_first]

    df = df[existing_first + remaining]

    for col in ["actual", "previous", "estimate", "change", "changePercentage"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if {"actual", "estimate"}.issubset(df.columns):
        df["surprise_vs_estimate"] = df["actual"] - df["estimate"]

    if {"actual", "previous"}.issubset(df.columns):
        df["change_vs_previous"] = df["actual"] - df["previous"]

    return df


def save_outputs(df: pd.DataFrame, output_dir: Path, run_time_utc: datetime) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = run_time_utc.strftime("%Y_%m_%d")

    parquet_path = output_dir / f"{DATASET}_{date_str}.parquet"
    csv_path = output_dir / f"{DATASET}_{date_str}.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    print()
    print("[DONE] Economic calendar snapshot saved.")
    print(f"Rows:    {len(df):,}")
    print(f"Parquet: {parquet_path}")
    print(f"CSV:     {csv_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - ECONOMIC CALENDAR SNAPSHOT")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)

    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        print("[WARN] FMP_API_KEY is not set.")
        print()
        print("To use this collector, add your API key temporarily with:")
        print("export FMP_API_KEY='your_api_key_here'")
        print()
        print("Or permanently add it to ~/.bashrc later.")
        return

    start_date = (run_time_utc.date() - timedelta(days=2)).isoformat()
    end_date = (run_time_utc.date() + timedelta(days=14)).isoformat()

    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Window:     {start_date} to {end_date}")
    print("-" * 90)

    payload = fetch_economic_calendar(api_key, start_date, end_date)
    df = normalise_calendar(payload, run_time_utc)

    if df.empty:
        print("[WARN] No economic calendar rows returned.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
