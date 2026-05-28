from pathlib import Path
import numpy as np
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "tick_data.yaml"

RAW_ROOT = Path("E:/Quant_Lab/data/raw/ticks/mt5")
OUTPUT_ROOT = Path("E:/Quant_Lab/data/processed/tick_research/tick_bars")

TICK_BAR_SIZES = [100, 250, 500, 1000]


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_symbol_ticks(broker: str, symbol: str) -> pd.DataFrame:
    symbol_dir = RAW_ROOT / f"broker={broker}" / f"symbol={symbol}"
    files = sorted(symbol_dir.rglob("*.parquet"))

    if not files:
        print(f"[WARN] No raw tick files found for {symbol}: {symbol_dir}")
        return pd.DataFrame()

    frames = []

    for file in files:
        try:
            df = pd.read_parquet(file)
            frames.append(df)
        except Exception as exc:
            print(f"[WARN] Failed reading {file}: {exc}")

    if not frames:
        return pd.DataFrame()

    ticks = pd.concat(frames, ignore_index=True)

    if "time_msc_dt" not in ticks.columns:
        raise ValueError(f"{symbol}: missing required column time_msc_dt")

    ticks["time_msc_dt"] = pd.to_datetime(ticks["time_msc_dt"], utc=True)
    ticks = ticks.sort_values("time_msc_dt").drop_duplicates(
        subset=["time_msc", "bid", "ask"],
        keep="last",
    )

    return ticks.reset_index(drop=True)


def build_tick_bars(ticks: pd.DataFrame, tick_size: int) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame()

    df = ticks.copy()
    df["bar_id"] = df.index // tick_size

    bars = (
        df.groupby("bar_id")
        .agg(
            start_time=("time_msc_dt", "first"),
            end_time=("time_msc_dt", "last"),
            open_mid=("mid", "first"),
            high_mid=("mid", "max"),
            low_mid=("mid", "min"),
            close_mid=("mid", "last"),
            mean_spread=("spread", "mean"),
            max_spread=("spread", "max"),
            tick_count=("mid", "size"),
        )
        .reset_index(drop=True)
    )

    bars["return"] = bars["close_mid"].pct_change()
    price_ratio = bars["close_mid"] / bars["close_mid"].shift(1)
    bars["log_return"] = np.where(price_ratio > 0, np.log(price_ratio), pd.NA)

    return bars


def save_tick_bars(symbol: str, tick_size: int, bars: pd.DataFrame) -> None:
    output_dir = OUTPUT_ROOT / f"symbol={symbol}" / f"tick_size={tick_size}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{symbol}_tick_bars_{tick_size}_latest.parquet"
    bars.to_parquet(output_file, index=False)

    print(f"[DONE] {symbol} tick_size={tick_size} rows={len(bars):,}")
    print(f"       {output_file}")


def process_symbol(broker: str, symbol: str) -> None:
    print("-" * 80)
    print(f"[SYMBOL] {symbol}")

    ticks = load_symbol_ticks(broker=broker, symbol=symbol)

    if ticks.empty:
        print(f"[SKIP] {symbol}: no ticks available")
        return

    print(f"[INFO] Loaded {len(ticks):,} clean ticks for {symbol}")
    print(f"[INFO] From {ticks['time_msc_dt'].min()} to {ticks['time_msc_dt'].max()}")

    for tick_size in TICK_BAR_SIZES:
        bars = build_tick_bars(ticks=ticks, tick_size=tick_size)

        if bars.empty:
            print(f"[SKIP] {symbol} tick_size={tick_size}: no bars created")
            continue

        save_tick_bars(symbol=symbol, tick_size=tick_size, bars=bars)


def main():
    config = load_config()
    broker = config["broker"]
    symbols = config["symbols"]

    print("=" * 80)
    print("BACQE MULTI-SYMBOL TICK BAR BUILDER")
    print("=" * 80)
    print(f"Broker:  {broker}")
    print(f"Symbols: {symbols}")
    print(f"Sizes:   {TICK_BAR_SIZES}")
    print("=" * 80)

    for symbol in symbols:
        process_symbol(broker=broker, symbol=symbol)

    print("=" * 80)
    print("[DONE] Multi-symbol tick bar build complete.")
    print("=" * 80)


if __name__ == "__main__":
    main()