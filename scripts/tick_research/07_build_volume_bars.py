"""
BACQE TICK RESEARCH - 07 Build Volume Bars

Builds volume bars from raw MT5 tick parquet files.

Important:
    FX broker tick data may not always contain true traded volume.
    This script checks volume_real and volume. If both are unusable,
    it falls back to 1 unit per tick, which makes the output behave
    like tick bars. The volume_source column tells us which was used.
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5" / f"broker={BROKER}" / f"symbol={SYMBOL}"

OUTPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "volume_bars" / f"symbol={SYMBOL}"

TICK_EQUIVALENT_SIZES = [100, 250, 500, 1000]

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

        wanted = [
            "time_msc_dt",
            "bid",
            "ask",
            "mid",
            "spread",
            "volume",
            "volume_real",
            "symbol",
            "broker",
        ]

        cols = [col for col in wanted if col in df.columns]
        frames.append(df[cols].copy())

        if i % 500 == 0:
            print(f"[INFO] Loaded {i:,}/{len(files):,} files")

    ticks = pd.concat(frames, ignore_index=True)

    ticks["time_msc_dt"] = pd.to_datetime(ticks["time_msc_dt"], errors="coerce", utc=True)

    ticks = ticks.dropna(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.sort_values("time_msc_dt")
    ticks = ticks.drop_duplicates(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.reset_index(drop=True)

    return ticks


def choose_volume_source(ticks: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Prefer volume_real if positive.
    Else use volume if positive.
    Else use 1 per tick as a fallback.
    """

    if "volume_real" in ticks.columns:
        volume_real = pd.to_numeric(ticks["volume_real"], errors="coerce").fillna(0)
        if volume_real.sum() > 0:
            return volume_real.clip(lower=0), "volume_real"

    if "volume" in ticks.columns:
        volume = pd.to_numeric(ticks["volume"], errors="coerce").fillna(0)
        if volume.sum() > 0:
            return volume.clip(lower=0), "volume"

    return pd.Series(1.0, index=ticks.index), "tick_proxy"


def build_volume_bars(ticks: pd.DataFrame, volume_threshold: float, volume_source: str) -> pd.DataFrame:
    records = []

    current_rows = []
    current_volume = 0.0

    for row in ticks.itertuples(index=False):
        vol = float(getattr(row, "bar_volume_unit"))
        current_volume += vol
        current_rows.append(row)

        if current_volume >= volume_threshold:
            records.append(build_single_bar(current_rows, current_volume, volume_threshold, volume_source))
            current_rows = []
            current_volume = 0.0

    if current_rows:
        records.append(build_single_bar(current_rows, current_volume, volume_threshold, volume_source))

    bars = pd.DataFrame(records)

    bars["return"] = bars["close"].pct_change()
    bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))
    bars["range"] = bars["high"] - bars["low"]

    bars["direction"] = 0
    bars.loc[bars["close"] > bars["open"], "direction"] = 1
    bars.loc[bars["close"] < bars["open"], "direction"] = -1

    bars["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def build_single_bar(rows: list, volume_sum: float, threshold: float, volume_source: str) -> dict:
    mids = [float(r.mid) for r in rows]
    bids = [float(r.bid) for r in rows]
    asks = [float(r.ask) for r in rows]
    spreads = [float(r.spread) for r in rows if pd.notna(r.spread)]
    times = [r.time_msc_dt for r in rows]

    return {
        "symbol": SYMBOL,
        "broker": BROKER,
        "bar_type": "volume",
        "volume_threshold": threshold,
        "volume_source": volume_source,
        "bar_start_time": times[0],
        "bar_end_time": times[-1],
        "open": mids[0],
        "high": max(mids),
        "low": min(mids),
        "close": mids[-1],
        "bid_open": bids[0],
        "bid_close": bids[-1],
        "ask_open": asks[0],
        "ask_close": asks[-1],
        "avg_spread": float(np.mean(spreads)) if spreads else np.nan,
        "max_spread": float(np.max(spreads)) if spreads else np.nan,
        "min_spread": float(np.min(spreads)) if spreads else np.nan,
        "tick_count": len(rows),
        "volume_sum": volume_sum,
        "duration_seconds": (times[-1] - times[0]).total_seconds(),
    }


def save_bars(bars: pd.DataFrame, threshold: float) -> None:
    threshold_label = str(int(threshold)) if float(threshold).is_integer() else str(threshold).replace(".", "_")

    output_dir = OUTPUT_ROOT / f"volume_threshold={threshold_label}"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"{SYMBOL}_volume_bars_{threshold_label}_latest.parquet"
    csv_path = output_dir / f"{SYMBOL}_volume_bars_{threshold_label}_latest.csv"

    bars.to_parquet(parquet_path, index=False)
    bars.to_csv(csv_path, index=False)

    print(f"[DONE] Saved volume bars: threshold={threshold_label}")
    print(f"       Bars:    {len(bars):,}")
    print(f"       Parquet: {parquet_path}")
    print(f"       CSV:     {csv_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 07 BUILD VOLUME BARS")
    print("=" * 90)
    print(f"Symbol:    {SYMBOL}")
    print(f"Broker:    {BROKER}")
    print(f"Tick root: {TICK_ROOT}")
    print("-" * 90)

    ticks = load_raw_ticks()

    volume_units, volume_source = choose_volume_source(ticks)
    ticks["bar_volume_unit"] = volume_units

    avg_volume_per_tick = ticks["bar_volume_unit"].mean()

    thresholds = [
        max(1, round(avg_volume_per_tick * tick_equiv, 6))
        for tick_equiv in TICK_EQUIVALENT_SIZES
    ]

    print(f"Ticks loaded:             {len(ticks):,}")
    print(f"First tick:               {ticks['time_msc_dt'].min()}")
    print(f"Last tick:                {ticks['time_msc_dt'].max()}")
    print(f"Volume source selected:   {volume_source}")
    print(f"Avg volume per tick:      {avg_volume_per_tick}")
    print(f"Volume thresholds:        {thresholds}")
    print("-" * 90)

    if volume_source == "tick_proxy":
        print("[WARN] No usable volume_real or volume found.")
        print("[WARN] Volume bars will behave like tick-equivalent bars for now.")
        print("[WARN] This is still useful structurally, but not true volume-bar research yet.")
        print("-" * 90)

    for threshold in thresholds:
        bars = build_volume_bars(ticks, threshold, volume_source)
        save_bars(bars, threshold)

    print("-" * 90)
    print("[COMPLETE] Volume bar build complete.")
    print("=" * 90)


if __name__ == "__main__":
    main()