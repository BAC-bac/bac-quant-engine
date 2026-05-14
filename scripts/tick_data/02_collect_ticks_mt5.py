from datetime import datetime, timedelta, timezone
from pathlib import Path

import MetaTrader5 as mt5
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = PROJECT_ROOT / "config" / "tick_data.yaml"


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_tick_type(tick_type_name: str):
    tick_type_name = tick_type_name.lower()

    if tick_type_name == "all":
        return mt5.COPY_TICKS_ALL
    if tick_type_name == "info":
        return mt5.COPY_TICKS_INFO
    if tick_type_name == "trade":
        return mt5.COPY_TICKS_TRADE

    raise ValueError(f"Unsupported tick_type: {tick_type_name}")


def collect_symbol_ticks(symbol: str, broker: str, output_root: Path, lookback_seconds: int, tick_type):
    utc_to = datetime.now(timezone.utc)
    utc_from = utc_to - timedelta(seconds=lookback_seconds)

    ticks = mt5.copy_ticks_range(
        symbol,
        utc_from,
        utc_to,
        tick_type,
    )

    if ticks is None or len(ticks) == 0:
        print(f"[WARN] No ticks received for {symbol}")
        return

    df = pd.DataFrame(ticks)

    df["symbol"] = symbol
    df["broker"] = broker
    df["capture_time_utc"] = pd.Timestamp.utcnow()

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df["time_msc_dt"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)

    df["spread"] = df["ask"] - df["bid"]
    df["mid"] = (df["bid"] + df["ask"]) / 2

    trade_date = df["time_msc_dt"].dt.date.max()

    output_dir = (
        output_root
        / f"broker={broker}"
        / f"symbol={symbol}"
        / f"year={trade_date.year}"
        / f"month={trade_date.month:02d}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{symbol}_ticks_{timestamp}.parquet"

    df.to_parquet(output_file, index=False, compression="snappy")

    print("[DONE] Tick data saved")
    print(f"Symbol:      {symbol}")
    print(f"Rows:        {len(df):,}")
    print(f"From:        {df['time_msc_dt'].min()}")
    print(f"To:          {df['time_msc_dt'].max()}")
    print(f"Output file: {output_file}")
    print("-" * 80)


def main():
    config = load_config(CONFIG_PATH)

    broker = config["broker"]
    symbols = config["symbols"]
    output_root = Path(config["paths"]["output_root"])
    lookback_seconds = int(config["capture"]["lookback_seconds"])
    tick_type = get_tick_type(config["capture"]["tick_type"])

    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize() failed: {mt5.last_error()}")

    try:
        print("=" * 80)
        print("BACQE CONFIG-DRIVEN MT5 TICK COLLECTOR")
        print("=" * 80)
        print(f"Broker:           {broker}")
        print(f"Symbols:          {symbols}")
        print(f"Lookback seconds: {lookback_seconds}")
        print(f"Output root:      {output_root}")
        print("-" * 80)

        for symbol in symbols:
            collect_symbol_ticks(
                symbol=symbol,
                broker=broker,
                output_root=output_root,
                lookback_seconds=lookback_seconds,
                tick_type=tick_type,
            )

    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()