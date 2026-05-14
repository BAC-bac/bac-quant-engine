from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "tick_data.yaml"


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def health_check_symbol(tick_root: Path, broker: str, symbol: str):
    symbol_dir = tick_root / f"broker={broker}" / f"symbol={symbol}"
    files = sorted(symbol_dir.rglob("*.parquet"))

    print("=" * 80)
    print("BACQE TICK DATA HEALTH CHECK")
    print("=" * 80)
    print(f"Broker:      {broker}")
    print(f"Symbol:      {symbol}")
    print(f"Searched:    {symbol_dir}")
    print(f"Files found: {len(files):,}")

    if not files:
        print("[WARN] No parquet tick files found.")
        print("=" * 80)
        return

    latest_file = files[-1]

    print(f"Latest file: {latest_file}")
    print("-" * 80)

    df = pd.read_parquet(latest_file)

    print(f"Rows:        {len(df):,}")
    print(f"Columns:     {list(df.columns)}")

    required_cols = [
        "time",
        "time_msc",
        "time_msc_dt",
        "bid",
        "ask",
        "spread",
        "mid",
        "symbol",
        "broker",
        "capture_time_utc",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        print(f"[FAIL] Missing columns: {missing_cols}")
    else:
        print("[PASS] Required columns present.")

    print("-" * 80)

    if "time_msc_dt" in df.columns:
        df["time_msc_dt"] = pd.to_datetime(df["time_msc_dt"], utc=True)
        print(f"From:        {df['time_msc_dt'].min()}")
        print(f"To:          {df['time_msc_dt'].max()}")
        print(f"Duration:    {df['time_msc_dt'].max() - df['time_msc_dt'].min()}")

    print("-" * 80)

    null_counts = df.isna().sum()
    null_counts = null_counts[null_counts > 0]

    if null_counts.empty:
        print("[PASS] No null values found.")
    else:
        print("[WARN] Null values found:")
        print(null_counts)

    print("-" * 80)

    duplicate_subset = ["time_msc", "bid", "ask"]

    if all(col in df.columns for col in duplicate_subset):
        duplicate_count = df.duplicated(subset=duplicate_subset).sum()
        print(f"Duplicate ticks by {duplicate_subset}: {duplicate_count:,}")
    else:
        print("[WARN] Cannot check duplicates; required columns missing.")

    print("-" * 80)

    if "spread" in df.columns:
        print("Spread summary:")
        print(df["spread"].describe())

        negative_spreads = (df["spread"] < 0).sum()
        zero_spreads = (df["spread"] == 0).sum()

        print(f"Negative spreads: {negative_spreads:,}")
        print(f"Zero spreads:     {zero_spreads:,}")

        if negative_spreads > 0:
            print("[FAIL] Negative spreads detected.")
        else:
            print("[PASS] No negative spreads.")

    print("-" * 80)

    if "mid" in df.columns:
        print("Mid price summary:")
        print(df["mid"].describe())

    print("=" * 80)
    print("[DONE] Tick data health check complete.")
    print("=" * 80)


def main():
    config = load_config(CONFIG_PATH)

    broker = config["broker"]
    symbols = config["symbols"]
    tick_root = Path(config["paths"]["output_root"])

    for symbol in symbols:
        health_check_symbol(
            tick_root=tick_root,
            broker=broker,
            symbol=symbol,
        )


if __name__ == "__main__":
    main()