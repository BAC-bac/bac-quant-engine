from __future__ import annotations

import os
import zipfile
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests


SOURCE = "cftc"
DATASET = "cftc_cot_tff"

# Traders in Financial Futures - Futures Only
# Annual historical compressed files.
URL_TEMPLATE = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"


YEARS_TO_COLLECT = [2024, 2025, 2026]


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


def download_zip(year: int) -> bytes | None:
    url = URL_TEMPLATE.format(year=year)

    print(f"[FETCH] {year} -> {url}")

    headers = {
        "User-Agent": "Mozilla/5.0 BACQE CFTC COT collector"
    }

    response = requests.get(url, headers=headers, timeout=60)

    if response.status_code != 200:
        print(f"[WARN] {year} returned HTTP {response.status_code}")
        return None

    return response.content


def read_cftc_zip(content: bytes, year: int) -> pd.DataFrame:
    with zipfile.ZipFile(BytesIO(content)) as zf:
        members = zf.namelist()

        if not members:
            print(f"[WARN] Empty zip for {year}")
            return pd.DataFrame()

        # Usually one text/CSV-like file per annual archive.
        member = members[0]
        print(f"[INFO] Reading {member}")

        with zf.open(member) as f:
            df = pd.read_csv(f)

    df["archive_year"] = year
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out.columns = [
        str(col)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "pct")
        .replace(".", "")
        for col in out.columns
    ]

    return out


def collect_cftc_tff() -> pd.DataFrame:
    run_time_utc = datetime.now(timezone.utc)
    frames = []

    for year in YEARS_TO_COLLECT:
        content = download_zip(year)

        if content is None:
            continue

        try:
            df = read_cftc_zip(content, year)

            if df.empty:
                continue

            df = normalise_columns(df)

            df["run_time_utc"] = run_time_utc.isoformat()
            df["snapshot_date"] = run_time_utc.date().isoformat()
            df["source"] = SOURCE
            df["report_type"] = "traders_in_financial_futures_futures_only"
            df["source_url"] = URL_TEMPLATE.format(year=year)

            frames.append(df)

        except Exception as exc:
            print(f"[ERROR] Failed processing {year}: {exc}")

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    # Try to parse report date columns if present.
    for col in out.columns:
        if "report_date" in col or col in ["date"]:
            out[col] = pd.to_datetime(out[col], errors="coerce")

    preferred = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "report_type",
        "archive_year",
        "market_and_exchange_names",
        "cftc_contract_market_code",
        "cftc_market_code",
        "cftc_region_code",
        "cftc_commodity_code",
        "report_date_as_yyyy_mm_dd",
        "open_interest_all",
        "dealer_positions_long_all",
        "dealer_positions_short_all",
        "asset_mgr_positions_long_all",
        "asset_mgr_positions_short_all",
        "lev_money_positions_long_all",
        "lev_money_positions_short_all",
        "other_rept_positions_long_all",
        "other_rept_positions_short_all",
        "nonrept_positions_long_all",
        "nonrept_positions_short_all",
        "source_url",
    ]

    existing = [col for col in preferred if col in out.columns]
    remaining = [col for col in out.columns if col not in existing]

    out = out[existing + remaining]

    sort_cols = [
        col for col in [
            "archive_year",
            "market_and_exchange_names",
            "report_date_as_yyyy_mm_dd",
        ]
        if col in out.columns
    ]

    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)

    return out


def add_research_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    # Leveraged money net positioning is one of the most useful macro/speculative signals.
    long_col = "lev_money_positions_long_all"
    short_col = "lev_money_positions_short_all"

    if long_col in out.columns and short_col in out.columns:
        out[long_col] = pd.to_numeric(out[long_col], errors="coerce")
        out[short_col] = pd.to_numeric(out[short_col], errors="coerce")
        out["lev_money_net_all"] = out[long_col] - out[short_col]

    # Asset manager net positioning can proxy institutional directional exposure.
    long_col = "asset_mgr_positions_long_all"
    short_col = "asset_mgr_positions_short_all"

    if long_col in out.columns and short_col in out.columns:
        out[long_col] = pd.to_numeric(out[long_col], errors="coerce")
        out[short_col] = pd.to_numeric(out[short_col], errors="coerce")
        out["asset_mgr_net_all"] = out[long_col] - out[short_col]

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
    print("[DONE] CFTC COT TFF data saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Columns:        {len(df.columns):,}")

    if "market_and_exchange_names" in df.columns:
        print(f"Markets:        {df['market_and_exchange_names'].nunique():,}")

    if "report_date_as_yyyy_mm_dd" in df.columns:
        print(f"Latest report:  {df['report_date_as_yyyy_mm_dd'].max()}")

    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - CFTC COT TRADERS IN FINANCIAL FUTURES")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Years:      {YEARS_TO_COLLECT}")
    print("-" * 90)

    df = collect_cftc_tff()
    df = add_research_features(df)

    if df.empty:
        print("[WARN] No CFTC COT data collected.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
