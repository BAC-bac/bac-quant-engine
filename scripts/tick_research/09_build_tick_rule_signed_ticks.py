"""
BACQE TICK RESEARCH - 09 Build Tick-Rule Signed Ticks

Uses the tick rule to assign direction to each tick:

    mid price rises   -> +1
    mid price falls   -> -1
    mid unchanged     -> previous non-zero direction

This creates the signed tick flow needed for Tick Imbalance Bars.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5" / f"broker={BROKER}" / f"symbol={SYMBOL}"

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "signed_ticks" / f"symbol={SYMBOL}"

MAX_FILES = None
# For testing:
# MAX_FILES = 100


def load_raw_ticks() -> pd.DataFrame:
    files = sorted(TICK_ROOT.rglob("*.parquet"))

    if MAX_FILES is not None:
        files = files[:MAX_FILES]

    if not files:
        raise FileNotFoundError(f"No raw tick files found: {TICK_ROOT}")

    print(f"Raw tick files selected: {len(files):,}")

    frames = []

    for i, file_path in enumerate(files, start=1):
        df = pd.read_parquet(file_path)

        required = {"time_msc_dt", "bid", "ask", "mid", "spread"}
        missing = required - set(df.columns)

        if missing:
            print(f"[WARN] Skipping {file_path.name}, missing: {missing}")
            continue

        frames.append(
            df[
                [
                    "time_msc_dt",
                    "bid",
                    "ask",
                    "mid",
                    "spread",
                ]
            ].copy()
        )

        if i % 500 == 0:
            print(f"[INFO] Loaded {i:,}/{len(files):,} files")

    ticks = pd.concat(frames, ignore_index=True)

    ticks["time_msc_dt"] = pd.to_datetime(ticks["time_msc_dt"], errors="coerce", utc=True)

    ticks = ticks.dropna(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.sort_values("time_msc_dt")
    ticks = ticks.drop_duplicates(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.reset_index(drop=True)

    ticks["symbol"] = SYMBOL
    ticks["broker"] = BROKER

    return ticks


def apply_tick_rule(ticks: pd.DataFrame) -> pd.DataFrame:
    signed = ticks.copy()

    signed["price_change"] = signed["mid"].diff()

    signed["raw_tick_direction"] = 0
    signed.loc[signed["price_change"] > 0, "raw_tick_direction"] = 1
    signed.loc[signed["price_change"] < 0, "raw_tick_direction"] = -1

    signed["tick_direction"] = signed["raw_tick_direction"].replace(0, pd.NA)
    signed["tick_direction"] = signed["tick_direction"].ffill().fillna(0).astype(int)

    signed["signed_tick"] = signed["tick_direction"]

    signed["cumulative_signed_ticks"] = signed["signed_tick"].cumsum()

    signed["is_price_change_tick"] = signed["raw_tick_direction"] != 0

    signed["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return signed


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 09 BUILD TICK-RULE SIGNED TICKS")
    print("=" * 90)
    print(f"Symbol:    {SYMBOL}")
    print(f"Broker:    {BROKER}")
    print(f"Tick root: {TICK_ROOT}")
    print("-" * 90)

    ticks = load_raw_ticks()

    print(f"Ticks loaded after cleaning: {len(ticks):,}")
    print(f"First tick: {ticks['time_msc_dt'].min()}")
    print(f"Last tick:  {ticks['time_msc_dt'].max()}")
    print("-" * 90)

    signed = apply_tick_rule(ticks)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    parquet_path = OUTPUT_DIR / f"{SYMBOL}_signed_ticks_latest.parquet"
    csv_path = OUTPUT_DIR / f"{SYMBOL}_signed_ticks_latest.csv"

    signed.to_parquet(parquet_path, index=False)
    signed.to_csv(csv_path, index=False)

    direction_counts = signed["tick_direction"].value_counts(dropna=False).to_dict()
    raw_direction_counts = signed["raw_tick_direction"].value_counts(dropna=False).to_dict()

    print("[DONE] Signed tick dataset created.")
    print(f"Rows:      {len(signed):,}")
    print(f"Parquet:   {parquet_path}")
    print(f"CSV:       {csv_path}")
    print("-" * 90)

    print("Tick direction counts after tick rule:")
    print(direction_counts)

    print("\nRaw tick direction counts before forward fill:")
    print(raw_direction_counts)

    print("\nPrice-change tick percentage:")
    print(round((signed["is_price_change_tick"].mean()) * 100, 4))

    print("\nFinal cumulative signed ticks:")
    print(int(signed["cumulative_signed_ticks"].iloc[-1]))

    print("=" * 90)


if __name__ == "__main__":
    main()