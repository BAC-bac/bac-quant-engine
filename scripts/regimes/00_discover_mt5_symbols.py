"""
00_discover_mt5_symbols.py
==========================

BAC Quant Engine - Regime Engine
Stage 00: Discover available MetaTrader 5 symbols.

Purpose:
- Connect to MetaTrader 5
- Pull all symbols available from the broker terminal
- Save symbol metadata to the Quant Lab data lake
- Create a readable CSV we can use to build the full FTMO universe
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import MetaTrader5 as mt5
import pandas as pd


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = Path("E:/Quant_Lab/data/raw/fx/metadata/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

DATA_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"discover_mt5_symbols_{datetime.now():%Y%m%d_%H%M%S}.log"

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
# SYMBOL DISCOVERY
# ============================================================

def discover_symbols() -> pd.DataFrame:
    symbols = mt5.symbols_get()

    if symbols is None:
        raise RuntimeError(f"MT5 symbols_get failed: {mt5.last_error()}")

    rows = []

    for symbol in symbols:
        info = mt5.symbol_info(symbol.name)

        if info is None:
            continue

        rows.append(
            {
                "name": symbol.name,
                "path": getattr(info, "path", None),
                "description": getattr(info, "description", None),
                "currency_base": getattr(info, "currency_base", None),
                "currency_profit": getattr(info, "currency_profit", None),
                "currency_margin": getattr(info, "currency_margin", None),
                "digits": getattr(info, "digits", None),
                "point": getattr(info, "point", None),
                "spread": getattr(info, "spread", None),
                "trade_contract_size": getattr(info, "trade_contract_size", None),
                "trade_tick_size": getattr(info, "trade_tick_size", None),
                "trade_tick_value": getattr(info, "trade_tick_value", None),
                "volume_min": getattr(info, "volume_min", None),
                "volume_max": getattr(info, "volume_max", None),
                "volume_step": getattr(info, "volume_step", None),
                "visible": getattr(info, "visible", None),
                "select": getattr(info, "select", None),
                "trade_mode": getattr(info, "trade_mode", None),
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("No MT5 symbols were discovered.")

    return df.sort_values("name").reset_index(drop=True)


def classify_asset_class(row: pd.Series) -> str:
    name = str(row.get("name", "")).upper()
    path = str(row.get("path", "")).upper()
    description = str(row.get("description", "")).upper()

    combined = f"{name} {path} {description}"

    if "CRYPTO" in combined or any(x in name for x in ["BTC", "ETH", "LTC", "XRP", "DOGE"]):
        return "crypto"

    if any(x in combined for x in ["FOREX", "FX", "MAJOR", "MINOR", "EXOTIC"]):
        return "forex"

    if any(x in name for x in ["XAU", "XAG", "XPT", "XPD"]) or "METAL" in combined:
        return "metals"

    if any(x in combined for x in ["INDEX", "INDICES", "CASH CFD"]):
        return "indices"

    if any(x in combined for x in ["OIL", "BRENT", "WTI", "NATGAS", "GAS", "COMMOD"]):
        return "commodities"

    if len(name) == 6 and name.isalpha():
        return "forex"

    return "other"


def main() -> None:
    logger.info("Starting MT5 symbol discovery")
    logger.info(f"Output folder: {DATA_ROOT}")

    if not mt5.initialize():
        logger.error(f"MT5 initialization failed: {mt5.last_error()}")
        sys.exit(1)

    logger.info("MT5 initialized successfully")

    try:
        df = discover_symbols()
        df["asset_class_guess"] = df.apply(classify_asset_class, axis=1)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = DATA_ROOT / f"ftmo_symbols_discovered_{timestamp}.csv"
        latest_csv_path = DATA_ROOT / "ftmo_symbols_discovered_latest.csv"

        parquet_path = DATA_ROOT / f"ftmo_symbols_discovered_{timestamp}.parquet"
        latest_parquet_path = DATA_ROOT / "ftmo_symbols_discovered_latest.parquet"

        df.to_csv(csv_path, index=False)
        df.to_csv(latest_csv_path, index=False)

        df.to_parquet(parquet_path, index=False)
        df.to_parquet(latest_parquet_path, index=False)

        logger.info(f"Discovered {len(df)} symbols")
        logger.info("Symbol count by guessed asset class:")
        logger.info(df["asset_class_guess"].value_counts().to_string())

        logger.info(f"Saved CSV: {csv_path}")
        logger.info(f"Saved latest CSV: {latest_csv_path}")
        logger.info(f"Saved Parquet: {parquet_path}")
        logger.info(f"Saved latest Parquet: {latest_parquet_path}")

        preview_cols = ["name", "path", "description", "asset_class_guess"]
        logger.info("Preview:")
        logger.info(df[preview_cols].head(30).to_string(index=False))

    finally:
        mt5.shutdown()
        logger.info("MT5 connection closed")


if __name__ == "__main__":
    main()