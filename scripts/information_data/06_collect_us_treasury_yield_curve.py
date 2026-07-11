from __future__ import annotations

import os
import platform
from pathlib import Path

import yaml
from datetime import datetime, timezone

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

SOURCE = "us_treasury_fiscaldata"
DATASET = "us_treasury_average_interest_rates"

BASE_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v2/accounting/od/avg_interest_rates"
)


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Could not find configuration file: {CONFIG_FILE}")

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML configuration: {CONFIG_FILE}")

    return config


def select_existing_path(candidates: list[str | None]) -> Path:
    valid_candidates = [candidate for candidate in candidates if candidate]

    if not valid_candidates:
        raise ValueError("No Data Lake path candidates were configured.")

    for candidate in valid_candidates:
        path = Path(candidate)

        if path.exists():
            return path

    raise FileNotFoundError(
        "None of the configured Data Lake paths exists: "
        + ", ".join(valid_candidates)
    )


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")

    if env_path:
        environment_root = Path(env_path)

        if environment_root.exists():
            return environment_root

        print(
            f"[WARN] DATA_LAKE_ROOT is set but does not exist: "
            f"{environment_root}"
        )

    config = load_config()
    paths = config["data_lake_root"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    linux_path = paths.get("linux")

    if not linux_path:
        raise KeyError("'data_lake_root.linux' is missing from config/paths.yaml")

    resolved_path = Path(linux_path)

    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Configured Linux Data Lake does not exist: {resolved_path}"
        )

    return resolved_path


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


def validate_treasury_data(df: pd.DataFrame) -> None:
    required_columns = {
        "record_date",
        "security_type_desc",
        "security_desc",
        "avg_interest_rate_amt",
    }

    missing_columns = required_columns.difference(df.columns)

    if missing_columns:
        raise ValueError(
            "Treasury output is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("Treasury output contains no rows.")

    if df["record_date"].isna().all():
        raise ValueError("Treasury output contains no valid record dates.")

    current_date = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    future_rows = df[df["record_date"] > current_date]

    if not future_rows.empty:
        raise ValueError(
            "Treasury output contains future record dates: "
            f"{future_rows['record_date'].dt.strftime('%Y-%m-%d').tolist()[:10]}"
        )

    invalid_rates = df[
        (df["avg_interest_rate_amt"] < 0)
        | (df["avg_interest_rate_amt"] > 30)
    ]

    if not invalid_rates.empty:
        raise ValueError(
            "Treasury output contains implausible average interest rates."
        )


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
    print("BACQE INFORMATION DATA - US TREASURY AVERAGE INTEREST RATES")
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

    validate_treasury_data(df)

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
