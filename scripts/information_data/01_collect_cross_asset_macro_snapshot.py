from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf


SOURCE = "yfinance"
DATASET = "cross_asset_macro_snapshots"


TICKERS = {
    "^GSPC": "sp500",
    "^IXIC": "nasdaq",
    "^DJI": "dow_jones",
    "^FTSE": "ftse_100",
    "^GDAXI": "dax",
    "^N225": "nikkei_225",
    "^VIX": "vix",

    "GBPUSD=X": "gbpusd",
    "EURUSD=X": "eurusd",
    "USDJPY=X": "usdjpy",
    "GBPJPY=X": "gbpjpy",
    "EURGBP=X": "eurgbp",

    "GC=F": "gold_futures",
    "SI=F": "silver_futures",
    "CL=F": "wti_crude",
    "BZ=F": "brent_crude",

    "^TNX": "us_10y_yield",
    "^IRX": "us_13w_yield",
    "^TYX": "us_30y_yield",

    "BTC-USD": "bitcoin",
    "ETH-USD": "ethereum",
}


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


def fetch_latest_daily_row(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period="7d",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    # yfinance can sometimes return MultiIndex columns even for one ticker.
    # This flattens the DataFrame back to simple Open/High/Low/Close columns.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def collect_snapshot() -> pd.DataFrame:
    run_time_utc = datetime.now(timezone.utc)
    rows = []

    for ticker, asset_name in TICKERS.items():
        print(f"[FETCH] {ticker:<12} -> {asset_name}")

        try:
            df = fetch_latest_daily_row(ticker)

            if df.empty:
                print(f"[WARN] No data returned for {ticker}")
                continue

            latest = df.dropna(how="all").tail(1)

            if latest.empty:
                print(f"[WARN] Latest usable row empty for {ticker}")
                continue

            row = latest.iloc[0]
            price_date = latest.index[-1].date().isoformat()

            rows.append(
                {
                    "run_time_utc": run_time_utc.isoformat(),
                    "snapshot_date": run_time_utc.date().isoformat(),
                    "source": SOURCE,
                    "ticker": ticker,
                    "asset_name": asset_name,
                    "price_date": price_date,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "adj_close": float(row["Adj Close"]) if "Adj Close" in row.index else float(row["Close"]),
                    "volume": float(row["Volume"]) if "Volume" in row.index else 0.0,
                }
            )

        except Exception as exc:
            print(f"[ERROR] {ticker} failed: {exc}")

    out = pd.DataFrame(rows)

    if out.empty:
        return out

    out["daily_range"] = out["high"] - out["low"]
    out["close_minus_open"] = out["close"] - out["open"]
    out["is_fx"] = out["ticker"].str.endswith("=X")
    out["is_crypto"] = out["ticker"].str.endswith("-USD")
    out["is_yield"] = out["asset_name"].str.contains("yield", case=False, na=False)
    out["is_volatility"] = out["asset_name"].str.contains("vix", case=False, na=False)

    return out


def save_snapshot(df: pd.DataFrame, output_dir: Path, run_time_utc: datetime) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = run_time_utc.strftime("%Y_%m_%d")

    parquet_path = output_dir / f"{DATASET}_{date_str}.parquet"
    csv_path = output_dir / f"{DATASET}_{date_str}.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    print()
    print("[DONE] Cross-asset macro snapshot saved.")
    print(f"Rows:    {len(df):,}")
    print(f"Parquet: {parquet_path}")
    print(f"CSV:     {csv_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - CROSS-ASSET MACRO SNAPSHOT")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print("-" * 90)

    snapshot = collect_snapshot()

    if snapshot.empty:
        print("[WARN] No data collected.")
        return

    save_snapshot(snapshot, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
