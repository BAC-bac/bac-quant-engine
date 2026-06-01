"""
BACQE MICROSTRUCTURE 03 - BUILD CORE VOLUME BARS

Purpose:
    Convert raw MT5 tick parquet files into BACQE core volume bars.

Important:
    MT5 FX/CFD volume and volume_real are often zero.
    This script supports proxy volume where each tick = 1 volume unit.

Outputs:
    E:/Quant_Lab/data/processed/microstructure/volume_bars/
        symbol=GBPUSD/
            volume_threshold=100/
            volume_bars.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_tick_files(tick_data_dir: Path, symbol: str) -> list[Path]:
    patterns = [
        f"**/symbol={symbol}/**/*.parquet",
        f"**/{symbol}/**/*.parquet",
        f"**/*{symbol}*.parquet",
    ]

    files = []
    for pattern in patterns:
        files.extend(tick_data_dir.glob(pattern))

    return sorted(set(files))


def load_ticks(files: list[Path], symbol: str) -> pd.DataFrame:
    frames = []

    for file in files:
        try:
            df = pd.read_parquet(file)
            if not df.empty:
                df["source_file"] = str(file)
                frames.append(df)
        except Exception as exc:
            print(f"[WARN] Could not read {file}: {exc}")

    if not frames:
        return pd.DataFrame()

    ticks = pd.concat(frames, ignore_index=True)
    ticks["symbol"] = symbol
    return ticks


def normalise_tick_columns(df: pd.DataFrame, use_proxy_volume: bool) -> pd.DataFrame:
    df = df.copy()

    if "time_msc" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True, errors="coerce")
    elif "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    else:
        raise ValueError("No recognised timestamp column found. Expected time_msc, time, or timestamp.")

    for col in ["bid", "ask", "last", "volume", "volume_real"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
    df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
    df["last"] = pd.to_numeric(df["last"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    df["volume_real"] = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)

    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]

    if use_proxy_volume:
        df["bar_volume_input"] = 1.0
        df["volume_mode"] = "proxy_tick_volume"
    else:
        df["bar_volume_input"] = df["volume_real"]

        if df["bar_volume_input"].sum() <= 0:
            df["bar_volume_input"] = df["volume"]
            df["volume_mode"] = "mt5_tick_volume"
        else:
            df["volume_mode"] = "mt5_real_volume"

    df = df.dropna(subset=["timestamp", "bid", "ask", "mid"])
    df = df[df["ask"] >= df["bid"]]
    df = df[df["mid"] > 0]
    df = df[df["bar_volume_input"] >= 0]

    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp", "bid", "ask", "last"], keep="last")
    df = df.reset_index(drop=True)

    return df


def build_volume_bars(ticks: pd.DataFrame, volume_threshold: int) -> pd.DataFrame:
    df = ticks.copy()

    df["cum_volume_input"] = df["bar_volume_input"].cumsum()
    df["bar_id"] = ((df["cum_volume_input"] - 1) // volume_threshold).astype(int)

    grouped = df.groupby("bar_id", observed=True)

    bars = grouped.agg(
        symbol=("symbol", "first"),
        start_time=("timestamp", "first"),
        end_time=("timestamp", "last"),
        open_mid=("mid", "first"),
        high_mid=("mid", "max"),
        low_mid=("mid", "min"),
        close_mid=("mid", "last"),
        open_bid=("bid", "first"),
        high_bid=("bid", "max"),
        low_bid=("bid", "min"),
        close_bid=("bid", "last"),
        open_ask=("ask", "first"),
        high_ask=("ask", "max"),
        low_ask=("ask", "min"),
        close_ask=("ask", "last"),
        avg_spread=("spread", "mean"),
        max_spread=("spread", "max"),
        tick_count=("timestamp", "count"),
        source_volume=("volume", "sum"),
        source_volume_real=("volume_real", "sum"),
        bar_volume=("bar_volume_input", "sum"),
        volume_mode=("volume_mode", "first"),
    ).reset_index(drop=True)

    bars["volume_threshold"] = volume_threshold
    bars["duration_seconds"] = (
        bars["end_time"] - bars["start_time"]
    ).dt.total_seconds()

    bars["return_mid"] = bars["close_mid"].pct_change()
    bars["range_mid"] = bars["high_mid"] - bars["low_mid"]
    bars["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def save_volume_bars(
    bars: pd.DataFrame,
    output_dir: Path,
    symbol: str,
    volume_threshold: int,
) -> Path:
    save_dir = (
        output_dir
        / "volume_bars"
        / f"symbol={symbol}"
        / f"volume_threshold={volume_threshold}"
    )
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / "volume_bars.parquet"
    bars.to_parquet(save_path, index=False)

    return save_path


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 03 - BUILD CORE VOLUME BARS")

    config = load_config()
    micro_cfg = config["microstructure"]

    tick_data_dir = Path(micro_cfg["input"]["tick_data_dir"])
    output_dir = Path(micro_cfg["output"]["microstructure_dir"])
    symbols = micro_cfg["symbols"]

    volume_cfg = micro_cfg["volume_bars"]
    thresholds = volume_cfg["thresholds"]
    use_proxy_volume = volume_cfg.get("use_proxy_volume", True)

    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    print(f"Config:             {CONFIG_PATH}")
    print(f"Input dir:          {tick_data_dir}")
    print(f"Output dir:         {output_dir}")
    print(f"Symbols:            {symbols}")
    print(f"Volume thresholds:  {thresholds}")
    print(f"Use proxy volume:   {use_proxy_volume}")
    print("-" * 90)

    if not tick_data_dir.exists():
        raise FileNotFoundError(f"Tick data directory does not exist: {tick_data_dir}")

    total_outputs = 0

    for symbol in symbols:
        print(f"\n[SYMBOL] {symbol}")

        files = find_tick_files(tick_data_dir, symbol)
        print(f"[INFO] Files found: {len(files)}")

        if not files:
            print(f"[WARN] No tick files found for {symbol}")
            continue

        raw_ticks = load_ticks(files, symbol)

        if raw_ticks.empty:
            print(f"[WARN] No readable tick data for {symbol}")
            continue

        try:
            ticks = normalise_tick_columns(
                raw_ticks,
                use_proxy_volume=use_proxy_volume,
            )
        except Exception as exc:
            print(f"[ERROR] Failed to clean ticks for {symbol}: {exc}")
            continue

        print(f"[INFO] Clean ticks: {len(ticks):,}")
        print(f"[INFO] Volume mode: {ticks['volume_mode'].iloc[0]}")
        print(f"[INFO] Total bar volume input: {ticks['bar_volume_input'].sum():,.0f}")

        if len(ticks) < min_rows:
            print(f"[WARN] Skipping {symbol}; below minimum rows: {len(ticks)} < {min_rows}")
            continue

        print(f"[INFO] Time range: {ticks['timestamp'].min()} -> {ticks['timestamp'].max()}")

        for threshold in thresholds:
            if ticks["bar_volume_input"].sum() < threshold:
                print(f"[WARN] Skipping threshold={threshold}; not enough volume input")
                continue

            bars = build_volume_bars(ticks, threshold)
            save_path = save_volume_bars(bars, output_dir, symbol, threshold)

            print(
                f"[DONE] threshold={threshold:<5} "
                f"bars={len(bars):,} -> {save_path}"
            )

            total_outputs += 1

    print("-" * 90)
    print("[DONE] Core volume bar build complete.")
    print(f"Outputs created: {total_outputs}")
    print("=" * 90)


if __name__ == "__main__":
    main()