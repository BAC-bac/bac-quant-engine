"""
01_download_market_data.py
==========================

BAC Quant Engine - Regime Engine
Stage 01: Download OHLCV market data from MetaTrader 5.

Purpose:
- Connect to MetaTrader 5
- Download OHLCV bars for selected symbols and timeframes
- Save files as Parquet into the Quant Lab data lake
- Support full first download and incremental updates
"""

from pathlib import Path
from datetime import datetime, timezone
import logging
import sys
import time

import MetaTrader5 as mt5
import pandas as pd
import yaml


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "market_universe.yaml"

DATA_ROOT = Path("E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

DATA_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"regime_download_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ============================================================
# MT5 TIMEFRAME MAP
# ============================================================

MT5_TF_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M2": mt5.TIMEFRAME_M2,
    "M3": mt5.TIMEFRAME_M3,
    "M4": mt5.TIMEFRAME_M4,
    "M5": mt5.TIMEFRAME_M5,
    "M6": mt5.TIMEFRAME_M6,
    "M10": mt5.TIMEFRAME_M10,
    "M12": mt5.TIMEFRAME_M12,
    "M15": mt5.TIMEFRAME_M15,
    "M20": mt5.TIMEFRAME_M20,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H2": mt5.TIMEFRAME_H2,
    "H3": mt5.TIMEFRAME_H3,
    "H4": mt5.TIMEFRAME_H4,
    "H6": mt5.TIMEFRAME_H6,
    "H8": mt5.TIMEFRAME_H8,
    "H12": mt5.TIMEFRAME_H12,
    "D1": mt5.TIMEFRAME_D1,
    "W1": mt5.TIMEFRAME_W1,
    "MN1": mt5.TIMEFRAME_MN1,
}


# ============================================================
# CONFIG LOADING
# ============================================================

def load_market_universe(config_path: Path) -> tuple[list[str], list[str]]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    symbols = []

    for key, value in config.items():
        if key == "timeframes":
            continue

        if isinstance(value, list):
            symbols.extend(value)

    timeframes = config.get("timeframes", [])

    symbols = sorted(set(symbols))
    timeframes = sorted(set(timeframes))

    return symbols, timeframes


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_full_history(symbol: str, timeframe: int) -> pd.DataFrame:
    frames = []
    chunk_size = 5000
    position = 0

    while True:
        rates = mt5.copy_rates_from_pos(symbol, timeframe, position, chunk_size)

        if rates is None or len(rates) == 0:
            break

        frames.append(pd.DataFrame(rates))
        position += chunk_size

        if len(rates) < chunk_size:
            break

        time.sleep(0.05)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def fetch_incremental(
    symbol: str,
    timeframe: int,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    rates = mt5.copy_rates_range(symbol, timeframe, from_date, to_date)

    if rates is None or len(rates) == 0:
        return pd.DataFrame()

    return pd.DataFrame(rates)


def normalise_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "time" not in df.columns:
        raise ValueError(f"Missing 'time' column. Columns returned: {df.columns.tolist()}")

    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    else:
        df["time"] = pd.to_datetime(df["time"], utc=True)

    keep_cols = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "tick_volume",
        "spread",
        "real_volume",
    ]

    existing_cols = [col for col in keep_cols if col in df.columns]

    return (
        df[existing_cols]
        .sort_values("time")
        .drop_duplicates(subset=["time"])
        .reset_index(drop=True)
    )


# ============================================================
# MAIN DOWNLOAD LOOP
# ============================================================

def main() -> None:
    logger.info("Starting BACQE Regime Engine market data download")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Config path: {CONFIG_PATH}")
    logger.info(f"Output root: {DATA_ROOT}")
    logger.info(f"Log folder: {LOG_DIR}")

    symbols, timeframes = load_market_universe(CONFIG_PATH)

    logger.info(f"Loaded {len(symbols)} symbols")
    logger.info(f"Loaded {len(timeframes)} timeframes")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Timeframes: {timeframes}")

    if not mt5.initialize():
        logger.error(f"MT5 initialization failed: {mt5.last_error()}")
        sys.exit(1)

    logger.info("MT5 initialized successfully")

    utc_now = datetime.now(timezone.utc)
    total_saved = 0

    try:
        for symbol in symbols:
            for tf_name in timeframes:
                mt5_tf = MT5_TF_MAP.get(tf_name)

                if mt5_tf is None:
                    logger.warning(f"Skipping unsupported timeframe: {tf_name}")
                    continue

                output_dir = DATA_ROOT / tf_name
                output_dir.mkdir(parents=True, exist_ok=True)

                output_path = output_dir / f"{symbol}_{tf_name}.parquet"

                try:
                    full_download = not output_path.exists()

                    if full_download:
                        logger.info(f"{symbol} {tf_name}: first run, downloading full history")
                        df_new = fetch_full_history(symbol, mt5_tf)
                    else:
                        df_existing = pd.read_parquet(output_path)

                        if df_existing.empty:
                            logger.info(f"{symbol} {tf_name}: existing file empty, downloading full history")
                            df_new = fetch_full_history(symbol, mt5_tf)
                            full_download = True
                        else:
                            df_existing["time"] = pd.to_datetime(df_existing["time"], utc=True)
                            last_time = df_existing["time"].max()
                            from_date = last_time.to_pydatetime()

                            logger.info(f"{symbol} {tf_name}: updating from {from_date}")
                            df_new = fetch_incremental(symbol, mt5_tf, from_date, utc_now)

                    if df_new.empty:
                        logger.warning(f"{symbol} {tf_name}: no new data returned")
                        continue

                    df_new = normalise_ohlcv(df_new)

                    if output_path.exists() and not full_download:
                        df_existing = pd.read_parquet(output_path)
                        df_existing = normalise_ohlcv(df_existing)

                        df_final = (
                            pd.concat([df_existing, df_new], ignore_index=True)
                            .drop_duplicates(subset=["time"])
                            .sort_values("time")
                            .reset_index(drop=True)
                        )
                    else:
                        df_final = df_new

                    df_final.to_parquet(output_path, index=False)

                    logger.info(
                        f"{symbol} {tf_name}: saved {len(df_final)} bars -> {output_path}"
                    )

                    total_saved += 1

                except Exception as exc:
                    logger.error(f"{symbol} {tf_name}: failed with error: {exc}")
                    continue

    finally:
        mt5.shutdown()
        logger.info("MT5 connection closed")

    logger.info(f"Completed market data download. Files updated: {total_saved}")


if __name__ == "__main__":
    main()