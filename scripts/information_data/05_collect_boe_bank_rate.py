from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests


SOURCE = "bank_of_england"
DATASET = "boe_bank_rate"
URL = "https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp"


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


def fetch_bank_rate_tables() -> list[pd.DataFrame]:
    headers = {
        "User-Agent": "Mozilla/5.0 BACQE research data collector"
    }

    response = requests.get(URL, headers=headers, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    return tables


def find_bank_rate_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for table in tables:
        cols = [str(c).lower() for c in table.columns]
        joined_cols = " ".join(cols)

        if "date" in joined_cols and ("rate" in joined_cols or "bank" in joined_cols):
            return table

    if tables:
        return tables[0]

    return pd.DataFrame()


def normalise_bank_rate(df: pd.DataFrame, run_time_utc: datetime) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    out.columns = [
        str(col)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("%", "percent")
        .replace("(", "")
        .replace(")", "")
        for col in out.columns
    ]

    date_col = None
    rate_col = None

    for col in out.columns:
        if "date" in col:
            date_col = col
        if "rate" in col or "bank_rate" in col:
            rate_col = col

    if date_col is None:
        date_col = out.columns[0]

    if rate_col is None:
        rate_col = out.columns[-1]

    out = out.rename(
        columns={
            date_col: "date",
            rate_col: "bank_rate_percent",
        }
    )

    out["date"] = pd.to_datetime(
        out["date"],
        format="%d %b %y",
        errors="coerce"
    )
    out["bank_rate_percent"] = (
        out["bank_rate_percent"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    out["bank_rate_percent"] = pd.to_numeric(out["bank_rate_percent"], errors="coerce")

    out = out.dropna(subset=["date", "bank_rate_percent"])

    out["run_time_utc"] = run_time_utc.isoformat()
    out["snapshot_date"] = run_time_utc.date().isoformat()
    out["source"] = SOURCE
    out["source_url"] = URL
    out["series_name"] = "official_bank_rate"

    preferred_cols = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "series_name",
        "date",
        "bank_rate_percent",
        "source_url",
    ]

    out = out[[col for col in preferred_cols if col in out.columns]]
    out = out.sort_values("date").reset_index(drop=True)

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

    latest_row = df.tail(1).iloc[0]

    print()
    print("[DONE] BoE Bank Rate history saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Latest date:    {latest_row['date']}")
    print(f"Latest rate:    {latest_row['bank_rate_percent']}%")
    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - BANK OF ENGLAND BANK RATE")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Source URL: {URL}")
    print("-" * 90)

    tables = fetch_bank_rate_tables()
    raw_table = find_bank_rate_table(tables)
    df = normalise_bank_rate(raw_table, run_time_utc)

    if df.empty:
        print("[WARN] No BoE Bank Rate data collected.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
