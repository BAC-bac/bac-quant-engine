"""
BACQE TICK RESEARCH - 05 Compare Tick Bars vs Time Bars

Compares GBPUSD tick bars against synthetic time bars built from the same raw tick stream.

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_time_bar_comparison_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_time_bar_comparison_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5" / f"broker={BROKER}" / f"symbol={SYMBOL}"
TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars" / f"symbol={SYMBOL}"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

TIME_FREQS = ["1min", "5min", "15min"]
TICK_SIZES = [100, 250, 500, 1000]


def load_raw_ticks() -> pd.DataFrame:
    files = sorted(TICK_ROOT.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No raw tick files found: {TICK_ROOT}")

    frames = []

    print(f"Raw tick files found: {len(files):,}")

    for i, file_path in enumerate(files, start=1):
        df = pd.read_parquet(file_path)

        required = {"time_msc_dt", "mid", "bid", "ask", "spread"}
        missing = required - set(df.columns)

        if missing:
            print(f"[WARN] Skipping {file_path.name}, missing: {missing}")
            continue

        frames.append(df[["time_msc_dt", "mid", "bid", "ask", "spread"]].copy())

        if i % 500 == 0:
            print(f"[INFO] Loaded {i:,}/{len(files):,} raw tick files")

    ticks = pd.concat(frames, ignore_index=True)

    ticks["time_msc_dt"] = pd.to_datetime(ticks["time_msc_dt"], errors="coerce", utc=True)

    ticks = ticks.dropna(subset=["time_msc_dt", "mid", "bid", "ask"])
    ticks = ticks.sort_values("time_msc_dt")
    ticks = ticks.drop_duplicates(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.set_index("time_msc_dt")

    return ticks


def build_time_bars(ticks: pd.DataFrame, freq: str) -> pd.DataFrame:
    bars = ticks.resample(freq).agg(
        open=("mid", "first"),
        high=("mid", "max"),
        low=("mid", "min"),
        close=("mid", "last"),
        avg_spread=("spread", "mean"),
        max_spread=("spread", "max"),
        tick_count=("mid", "count"),
    )

    bars = bars.dropna(subset=["open", "high", "low", "close"]).reset_index()

    bars = bars.rename(columns={"time_msc_dt": "bar_start_time"})

    bars["bar_end_time"] = bars["bar_start_time"] + pd.to_timedelta(freq)
    bars["bar_type"] = f"time_{freq}"
    bars["bar_parameter"] = freq
    bars["symbol"] = SYMBOL
    bars["broker"] = BROKER
    bars["duration_seconds"] = pd.to_timedelta(freq).total_seconds()
    bars["range"] = bars["high"] - bars["low"]
    bars["return"] = bars["close"].pct_change()
    bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))

    bars["direction"] = 0
    bars.loc[bars["close"] > bars["open"], "direction"] = 1
    bars.loc[bars["close"] < bars["open"], "direction"] = -1

    return bars


def load_tick_bars(tick_size: int) -> pd.DataFrame:
    file_path = (
        TICK_BAR_ROOT
        / f"tick_size={tick_size}"
        / f"{SYMBOL}_tick_bars_{tick_size}_latest.parquet"
    )

    if not file_path.exists():
        raise FileNotFoundError(f"Tick bar file not found: {file_path}")

    bars = pd.read_parquet(file_path)

    bars = bars.copy()
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = tick_size

    return bars


def lag1_autocorr(series: pd.Series) -> float | None:
    clean = series.dropna()

    if len(clean) < 3:
        return None

    return clean.autocorr(lag=1)


def summarise_bars(bars: pd.DataFrame, bar_type: str, bar_parameter) -> dict:
    returns = bars["return"].replace([np.inf, -np.inf], np.nan).dropna()
    log_returns = bars["log_return"].replace([np.inf, -np.inf], np.nan).dropna()

    direction_counts = bars["direction"].value_counts(dropna=False).to_dict()

    up_bars = direction_counts.get(1, 0)
    down_bars = direction_counts.get(-1, 0)
    flat_bars = direction_counts.get(0, 0)

    bar_count = len(bars)

    return {
        "symbol": SYMBOL,
        "broker": BROKER,
        "bar_type": bar_type,
        "bar_parameter": bar_parameter,
        "bar_count": bar_count,
        "first_bar_time": bars["bar_start_time"].min(),
        "last_bar_time": bars["bar_end_time"].max(),
        "avg_duration_seconds": bars["duration_seconds"].mean(),
        "median_duration_seconds": bars["duration_seconds"].median(),
        "min_duration_seconds": bars["duration_seconds"].min(),
        "max_duration_seconds": bars["duration_seconds"].max(),
        "avg_tick_count": bars["tick_count"].mean() if "tick_count" in bars.columns else None,
        "median_tick_count": bars["tick_count"].median() if "tick_count" in bars.columns else None,
        "min_tick_count": bars["tick_count"].min() if "tick_count" in bars.columns else None,
        "max_tick_count": bars["tick_count"].max() if "tick_count" in bars.columns else None,
        "avg_range": bars["range"].mean(),
        "median_range": bars["range"].median(),
        "max_range": bars["range"].max(),
        "avg_spread": bars["avg_spread"].mean(),
        "max_spread": bars["max_spread"].max(),
        "return_mean": returns.mean(),
        "return_std": returns.std(),
        "return_skew": returns.skew(),
        "return_kurtosis": returns.kurtosis(),
        "abs_return_mean": returns.abs().mean(),
        "log_return_mean": log_returns.mean(),
        "log_return_std": log_returns.std(),
        "log_return_skew": log_returns.skew(),
        "log_return_kurtosis": log_returns.kurtosis(),
        "lag1_return_autocorr": lag1_autocorr(returns),
        "up_bars": up_bars,
        "down_bars": down_bars,
        "flat_bars": flat_bars,
        "up_pct": round((up_bars / bar_count) * 100, 2) if bar_count else None,
        "down_pct": round((down_bars / bar_count) * 100, 2) if bar_count else None,
        "flat_pct": round((flat_bars / bar_count) * 100, 2) if bar_count else None,
        "comparison_time_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 05 COMPARE TICK VS TIME BARS")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Broker: {BROKER}")
    print("-" * 90)

    ticks = load_raw_ticks()

    print(f"Clean raw ticks loaded: {len(ticks):,}")
    print(f"First tick: {ticks.index.min()}")
    print(f"Last tick:  {ticks.index.max()}")
    print("-" * 90)

    records = []

    for freq in TIME_FREQS:
        bars = build_time_bars(ticks, freq)
        records.append(summarise_bars(bars, f"time_{freq}", freq))
        print(f"[DONE] Built time bars: {freq} | bars={len(bars):,}")

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(tick_size)
        records.append(summarise_bars(bars, f"tick_{tick_size}", tick_size))
        print(f"[DONE] Loaded tick bars: {tick_size} | bars={len(bars):,}")

    comparison = pd.DataFrame(records)

    numeric_cols = comparison.select_dtypes(include=["float", "int"]).columns
    comparison[numeric_cols] = comparison[numeric_cols].round(8)

    comparison = comparison.sort_values(
        by=["bar_type"],
        ascending=True,
    ).reset_index(drop=True)

    comparison["bar_parameter"] = comparison["bar_parameter"].astype(str)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "tick_vs_time_bar_comparison_latest.csv"
    parquet_path = OUTPUT_DIR / "tick_vs_time_bar_comparison_latest.parquet"

    comparison.to_csv(csv_path, index=False)
    comparison.to_parquet(parquet_path, index=False)

    print("-" * 90)
    print("[DONE] Tick vs time bar comparison created.")
    print(f"Rows:      {len(comparison):,}")
    print(f"CSV:       {csv_path}")
    print(f"Parquet:   {parquet_path}")
    print("-" * 90)

    display_cols = [
        "bar_type",
        "bar_count",
        "avg_duration_seconds",
        "median_duration_seconds",
        "avg_tick_count",
        "avg_range",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "up_pct",
        "down_pct",
        "flat_pct",
    ]

    print(comparison[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()