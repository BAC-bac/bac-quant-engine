from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone
import platform
import yaml
import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

SOURCE = "fred"
DATASET = "fred_macro_series"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


FRED_SERIES = {
    # Policy / rates
    "FEDFUNDS": "effective_federal_funds_rate",
    "DFF": "daily_federal_funds_rate",

    # Treasury yields
    "DGS2": "us_2y_treasury_yield",
    "DGS10": "us_10y_treasury_yield",
    "DGS30": "us_30y_treasury_yield",
    "T10Y2Y": "us_10y_2y_yield_spread",
    "T10Y3M": "us_10y_3m_yield_spread",

    # Inflation
    "CPIAUCSL": "us_cpi_all_items",
    "CPILFESL": "us_core_cpi",
    "PCEPI": "us_pce_price_index",
    "PCEPILFE": "us_core_pce_price_index",

    # Labour market
    "UNRATE": "us_unemployment_rate",
    "PAYEMS": "us_nonfarm_payrolls",
    "ICSA": "us_initial_jobless_claims",

    # Growth / activity
    "GDP": "us_gross_domestic_product",
    "GDPC1": "us_real_gdp",
    "INDPRO": "us_industrial_production",
    "RSAFS": "us_retail_sales",

    # Money / credit / stress
    "M2SL": "us_m2_money_stock",
    "BAMLH0A0HYM2": "us_high_yield_option_adjusted_spread",

    # Recession indicator
    "USREC": "us_recession_indicator",
}


def load_config() -> dict:
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def select_existing_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        if candidate is None:
            continue

        path = Path(candidate)
        if path.exists():
            return path

    return Path(next(candidate for candidate in candidates if candidate is not None))


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")
    if env_path and Path(env_path).exists():
        return Path(env_path)

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

    return Path(paths["linux"])

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


def fetch_series(series_id: str, api_key: str) -> list[dict]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
    }

    response = requests.get(BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        print(f"[WARN] {series_id} returned HTTP {response.status_code}: {response.text[:200]}")
        return []

    payload = response.json()

    observations = payload.get("observations", [])

    if not observations:
        print(f"[WARN] No observations returned for {series_id}")

    return observations


def collect_fred_series() -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY")

    if not api_key:
        print("[WARN] FRED_API_KEY is not set in .env")
        return pd.DataFrame()

    run_time_utc = datetime.now(timezone.utc)
    frames = []

    for series_id, series_name in FRED_SERIES.items():
        print(f"[FETCH] {series_id:<12} -> {series_name}")

        observations = fetch_series(series_id, api_key)

        if not observations:
            continue

        df = pd.DataFrame(observations)

        if df.empty:
            continue

        df["run_time_utc"] = run_time_utc.isoformat()
        df["snapshot_date"] = run_time_utc.date().isoformat()
        df["source"] = SOURCE
        df["series_id"] = series_id
        df["series_name"] = series_name

        df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    preferred = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "series_id",
        "series_name",
        "date",
        "value",
        "realtime_start",
        "realtime_end",
    ]

    out = out[[col for col in preferred if col in out.columns]]
    out = out.sort_values(["series_id", "date"]).reset_index(drop=True)

    return out


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
    print("[DONE] FRED macro series snapshot saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Series:         {df['series_id'].nunique():,}")
    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - FRED MACRO SERIES")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print("-" * 90)

    df = collect_fred_series()

    if df.empty:
        print("[WARN] No FRED data collected.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
