"""
BACQE MICROSTRUCTURE 05 - BUILD CORE TICK IMBALANCE BARS

Purpose:
    Build fixed-threshold tick imbalance bars from raw MT5 tick parquet files.

Method:
    1. Load raw ticks.
    2. Clean and normalise bid/ask/mid prices.
    3. Apply tick rule:
        +1 = uptick
        -1 = downtick
         0 = unchanged, forward-filled from previous non-zero direction.
    4. Accumulate signed tick imbalance.
    5. Close a bar when abs(cumulative imbalance) >= threshold.

Outputs:
    E:/Quant_Lab/data/processed/microstructure/tick_imbalance_bars/
        symbol=GBPUSD/
            imbalance_threshold=25/
            tick_imbalance_bars.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import yaml
import pandas as pd
import numpy as np


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


def normalise_tick_columns(df: pd.DataFrame) -> pd.DataFrame:
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

    df = df.dropna(subset=["timestamp", "bid", "ask", "mid"])
    df = df[df["ask"] >= df["bid"]]
    df = df[df["mid"] > 0]

    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp", "bid", "ask", "last"], keep="last")
    df = df.reset_index(drop=True)

    return df


def apply_tick_rule(ticks: pd.DataFrame) -> pd.DataFrame:
    df = ticks.copy()

    df["mid_change"] = df["mid"].diff()

    df["tick_direction_raw"] = np.where(
        df["mid_change"] > 0,
        1,
        np.where(df["mid_change"] < 0, -1, np.nan),
    )

    df["tick_direction"] = (
        pd.Series(df["tick_direction_raw"])
        .ffill()
        .fillna(1)
        .astype(int)
    )

    df["signed_tick"] = df["tick_direction"]

    return df


def build_tick_imbalance_bars(
    ticks: pd.DataFrame,
    imbalance_threshold: int,
) -> pd.DataFrame:
    records = []

    cumulative_imbalance = 0
    bar_ticks = []
    bar_id = 0

    for row in ticks.itertuples(index=False):
        signed_tick = int(row.signed_tick)
        cumulative_imbalance += signed_tick
        bar_ticks.append(row)

        if abs(cumulative_imbalance) >= imbalance_threshold:
            bar_df = pd.DataFrame(bar_ticks)

            record = {
                "symbol": bar_df["symbol"].iloc[0],
                "start_time": bar_df["timestamp"].iloc[0],
                "end_time": bar_df["timestamp"].iloc[-1],
                "open_mid": bar_df["mid"].iloc[0],
                "high_mid": bar_df["mid"].max(),
                "low_mid": bar_df["mid"].min(),
                "close_mid": bar_df["mid"].iloc[-1],
                "open_bid": bar_df["bid"].iloc[0],
                "high_bid": bar_df["bid"].max(),
                "low_bid": bar_df["bid"].min(),
                "close_bid": bar_df["bid"].iloc[-1],
                "open_ask": bar_df["ask"].iloc[0],
                "high_ask": bar_df["ask"].max(),
                "low_ask": bar_df["ask"].min(),
                "close_ask": bar_df["ask"].iloc[-1],
                "avg_spread": bar_df["spread"].mean(),
                "max_spread": bar_df["spread"].max(),
                "tick_count": len(bar_df),
                "uptick_count": int((bar_df["signed_tick"] > 0).sum()),
                "downtick_count": int((bar_df["signed_tick"] < 0).sum()),
                "signed_tick_sum": int(bar_df["signed_tick"].sum()),
                "abs_signed_tick_sum": int(abs(bar_df["signed_tick"].sum())),
                "imbalance_threshold": imbalance_threshold,
                "volume": bar_df["volume"].sum(),
                "volume_real": bar_df["volume_real"].sum(),
                "bar_id": bar_id,
            }

            records.append(record)

            bar_ticks = []
            cumulative_imbalance = 0
            bar_id += 1

    # Keep a final partial bar if it contains useful information.
    if bar_ticks:
        bar_df = pd.DataFrame(bar_ticks)

        record = {
            "symbol": bar_df["symbol"].iloc[0],
            "start_time": bar_df["timestamp"].iloc[0],
            "end_time": bar_df["timestamp"].iloc[-1],
            "open_mid": bar_df["mid"].iloc[0],
            "high_mid": bar_df["mid"].max(),
            "low_mid": bar_df["mid"].min(),
            "close_mid": bar_df["mid"].iloc[-1],
            "open_bid": bar_df["bid"].iloc[0],
            "high_bid": bar_df["bid"].max(),
            "low_bid": bar_df["bid"].min(),
            "close_bid": bar_df["bid"].iloc[-1],
            "open_ask": bar_df["ask"].iloc[0],
            "high_ask": bar_df["ask"].max(),
            "low_ask": bar_df["ask"].min(),
            "close_ask": bar_df["ask"].iloc[-1],
            "avg_spread": bar_df["spread"].mean(),
            "max_spread": bar_df["spread"].max(),
            "tick_count": len(bar_df),
            "uptick_count": int((bar_df["signed_tick"] > 0).sum()),
            "downtick_count": int((bar_df["signed_tick"] < 0).sum()),
            "signed_tick_sum": int(bar_df["signed_tick"].sum()),
            "abs_signed_tick_sum": int(abs(bar_df["signed_tick"].sum())),
            "imbalance_threshold": imbalance_threshold,
            "volume": bar_df["volume"].sum(),
            "volume_real": bar_df["volume_real"].sum(),
            "bar_id": bar_id,
            "is_partial_bar": True,
        }

        records.append(record)

    bars = pd.DataFrame(records)

    if bars.empty:
        return bars

    if "is_partial_bar" not in bars.columns:
        bars["is_partial_bar"] = False
    else:
        bars["is_partial_bar"] = (bars["is_partial_bar"].fillna(False).astype(bool))

    bars["duration_seconds"] = (
        pd.to_datetime(bars["end_time"], utc=True)
        - pd.to_datetime(bars["start_time"], utc=True)
    ).dt.total_seconds()

    bars["return_mid"] = bars["close_mid"].pct_change()
    bars["range_mid"] = bars["high_mid"] - bars["low_mid"]
    bars["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    return bars


def save_tick_imbalance_bars(
    bars: pd.DataFrame,
    output_dir: Path,
    symbol: str,
    imbalance_threshold: int,
) -> Path:
    save_dir = (
        output_dir
        / "tick_imbalance_bars"
        / f"symbol={symbol}"
        / f"imbalance_threshold={imbalance_threshold}"
    )
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / "tick_imbalance_bars.parquet"
    bars.to_parquet(save_path, index=False)

    return save_path


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 05 - BUILD CORE TICK IMBALANCE BARS")

    config = load_config()
    micro_cfg = config["microstructure"]

    tick_data_dir = Path(micro_cfg["input"]["tick_data_dir"])
    output_dir = Path(micro_cfg["output"]["microstructure_dir"])
    symbols = micro_cfg["symbols"]

    imbalance_cfg = micro_cfg["imbalance_bars"]
    thresholds = imbalance_cfg["tick_imbalance_thresholds"]

    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    print(f"Config:                 {CONFIG_PATH}")
    print(f"Input dir:              {tick_data_dir}")
    print(f"Output dir:             {output_dir}")
    print(f"Symbols:                {symbols}")
    print(f"Imbalance thresholds:   {thresholds}")
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
            ticks = normalise_tick_columns(raw_ticks)
            ticks = apply_tick_rule(ticks)
        except Exception as exc:
            print(f"[ERROR] Failed to prepare ticks for {symbol}: {exc}")
            continue

        print(f"[INFO] Clean ticks: {len(ticks):,}")
        print(f"[INFO] Time range: {ticks['timestamp'].min()} -> {ticks['timestamp'].max()}")
        print(f"[INFO] Signed tick balance: {ticks['signed_tick'].sum():,}")

        if len(ticks) < min_rows:
            print(f"[WARN] Skipping {symbol}; below minimum rows: {len(ticks)} < {min_rows}")
            continue

        for threshold in thresholds:
            bars = build_tick_imbalance_bars(
                ticks=ticks,
                imbalance_threshold=threshold,
            )

            if bars.empty:
                print(f"[WARN] No bars created for threshold={threshold}")
                continue

            save_path = save_tick_imbalance_bars(
                bars=bars,
                output_dir=output_dir,
                symbol=symbol,
                imbalance_threshold=threshold,
            )

            avg_ticks = bars["tick_count"].mean()
            avg_duration = bars["duration_seconds"].mean()
            partial_count = int(bars["is_partial_bar"].sum())

            print(
                f"[DONE] threshold={threshold:<5} "
                f"bars={len(bars):,} "
                f"avg_ticks={avg_ticks:,.2f} "
                f"avg_duration={avg_duration:,.2f}s "
                f"partial={partial_count} -> {save_path}"
            )

            total_outputs += 1

    print("-" * 90)
    print("[DONE] Core tick imbalance bar build complete.")
    print(f"Outputs created: {total_outputs}")
    print("=" * 90)


if __name__ == "__main__":
    main()