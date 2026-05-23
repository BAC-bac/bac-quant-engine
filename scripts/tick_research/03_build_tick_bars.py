"""
BACQE TICK RESEARCH - 03 Build Tick Bars

Builds fixed-size tick bars from raw MT5 tick parquet files.

Default development symbol:
    GBPUSD

Outputs:
    E:/Quant_Lab/data/processed/tick_research/tick_bars/symbol=GBPUSD/tick_size=100/GBPUSD_tick_bars_100_latest.parquet
    E:/Quant_Lab/data/processed/tick_research/tick_bars/symbol=GBPUSD/tick_size=250/GBPUSD_tick_bars_250_latest.parquet
    E:/Quant_Lab/data/processed/tick_research/tick_bars/symbol=GBPUSD/tick_size=500/GBPUSD_tick_bars_500_latest.parquet
    E:/Quant_Lab/data/processed/tick_research/tick_bars/symbol=GBPUSD/tick_size=1000/GBPUSD_tick_bars_1000_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5"

OUTPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars"

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_BAR_SIZES = [100, 250, 500, 1000]

MAX_FILES = None
# For testing, you can temporarily use:
# MAX_FILES = 100


# =============================================================================
# LOAD TICKS
# =============================================================================

def find_symbol_tick_files(symbol: str, broker: str) -> list[Path]:
    symbol_dir = TICK_ROOT / f"broker={broker}" / f"symbol={symbol}"

    if not symbol_dir.exists():
        raise FileNotFoundError(f"Symbol tick directory not found: {symbol_dir}")

    return sorted(symbol_dir.rglob("*.parquet"))


def load_ticks(files: list[Path]) -> pd.DataFrame:
    frames = []

    selected_files = files if MAX_FILES is None else files[:MAX_FILES]

    print(f"Files selected: {len(selected_files):,}")

    for i, file_path in enumerate(selected_files, start=1):
        df = pd.read_parquet(file_path)

        required = {"bid", "ask", "mid", "spread", "time_msc_dt"}

        missing = required - set(df.columns)
        if missing:
            print(f"[WARN] Skipping {file_path.name}, missing columns: {missing}")
            continue

        use_cols = [
            "time_msc_dt",
            "bid",
            "ask",
            "mid",
            "spread",
            "symbol",
            "broker",
            "capture_time_utc",
        ]

        available_cols = [col for col in use_cols if col in df.columns]

        frames.append(df[available_cols].copy())

        if i % 500 == 0:
            print(f"[INFO] Loaded {i:,}/{len(selected_files):,} files")

    if not frames:
        raise ValueError("No valid tick files loaded.")

    ticks = pd.concat(frames, ignore_index=True)

    ticks["time_msc_dt"] = pd.to_datetime(ticks["time_msc_dt"], errors="coerce", utc=True)

    ticks = ticks.dropna(subset=["time_msc_dt", "bid", "ask", "mid"])

    ticks = ticks.sort_values("time_msc_dt").reset_index(drop=True)

    ticks = ticks.drop_duplicates(subset=["time_msc_dt", "bid", "ask", "mid"]).reset_index(drop=True)

    return ticks


# =============================================================================
# BUILD TICK BARS
# =============================================================================

def build_tick_bars(ticks: pd.DataFrame, tick_size: int) -> pd.DataFrame:
    data = ticks.copy()

    data["bar_id"] = data.index // tick_size

    grouped = data.groupby("bar_id", sort=True)

    bars = grouped.agg(
        bar_start_time=("time_msc_dt", "first"),
        bar_end_time=("time_msc_dt", "last"),
        open=("mid", "first"),
        high=("mid", "max"),
        low=("mid", "min"),
        close=("mid", "last"),
        bid_open=("bid", "first"),
        bid_close=("bid", "last"),
        ask_open=("ask", "first"),
        ask_close=("ask", "last"),
        avg_spread=("spread", "mean"),
        max_spread=("spread", "max"),
        min_spread=("spread", "min"),
        tick_count=("mid", "count"),
    ).reset_index(drop=True)

    bars["symbol"] = SYMBOL
    bars["broker"] = BROKER
    bars["tick_size"] = tick_size

    bars["duration_seconds"] = (
        bars["bar_end_time"] - bars["bar_start_time"]
    ).dt.total_seconds()

    bars["return"] = bars["close"].pct_change()

    bars["range"] = bars["high"] - bars["low"]
    bars["direction"] = 0
    bars.loc[bars["close"] > bars["open"], "direction"] = 1
    bars.loc[bars["close"] < bars["open"], "direction"] = -1

    bars["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def add_log_returns(bars: pd.DataFrame) -> pd.DataFrame:
    import numpy as np

    bars = bars.copy()
    bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))
    return bars


def save_bars(bars: pd.DataFrame, symbol: str, tick_size: int) -> None:
    output_dir = OUTPUT_ROOT / f"symbol={symbol}" / f"tick_size={tick_size}"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"{symbol}_tick_bars_{tick_size}_latest.parquet"
    csv_path = output_dir / f"{symbol}_tick_bars_{tick_size}_latest.csv"

    bars.to_parquet(parquet_path, index=False)
    bars.to_csv(csv_path, index=False)

    print(f"[DONE] Saved tick bars: tick_size={tick_size}")
    print(f"       Bars:    {len(bars):,}")
    print(f"       Parquet: {parquet_path}")
    print(f"       CSV:     {csv_path}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 03 BUILD TICK BARS")
    print("=" * 90)
    print(f"Symbol:      {SYMBOL}")
    print(f"Broker:      {BROKER}")
    print(f"Tick root:   {TICK_ROOT}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    files = find_symbol_tick_files(SYMBOL, BROKER)

    print(f"Tick files found for {SYMBOL}: {len(files):,}")
    print("-" * 90)

    ticks = load_ticks(files)

    print(f"Ticks loaded after cleaning: {len(ticks):,}")
    print(f"First tick: {ticks['time_msc_dt'].min()}")
    print(f"Last tick:  {ticks['time_msc_dt'].max()}")
    print("-" * 90)

    for tick_size in TICK_BAR_SIZES:
        bars = build_tick_bars(ticks, tick_size)
        bars = add_log_returns(bars)
        save_bars(bars, SYMBOL, tick_size)

    print("-" * 90)
    print("[COMPLETE] Tick bar build complete.")
    print("=" * 90)


if __name__ == "__main__":
    main()