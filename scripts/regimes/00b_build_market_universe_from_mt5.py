"""
00b_build_market_universe_from_mt5.py
=====================================

BAC Quant Engine - Regime Engine
Stage 00b: Build market_universe.yaml from discovered MT5 symbols.

Purpose:
- Read FTMO symbol metadata discovered from MT5
- Group symbols using the broker path field
- Write config/market_universe.yaml
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_CSV = Path(
    "E:/Quant_Lab/data/raw/fx/metadata/FTMO/ftmo_symbols_discovered_latest.csv"
)

OUTPUT_YAML = PROJECT_ROOT / "config" / "market_universe.yaml"
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

LOG_DIR.mkdir(parents=True, exist_ok=True)

log_path = LOG_DIR / f"build_market_universe_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def normalise_group_name(path: str, symbol_name: str) -> str:
    path_upper = str(path).upper()
    name_upper = str(symbol_name).upper()

    if "FOREX" in path_upper:
        return "forex"

    if "EXOTIC" in path_upper:
        return "exotics"

    if "METAL" in path_upper:
        return "metals"

    if "AGRICULTURE" in path_upper or "AGRICULT" in path_upper:
        return "agriculture"

    if "COMMOD" in path_upper or "ENERGY" in path_upper:
        return "commodities"

    if "CASH I CFD" in path_upper:
        return "indices_cash_1"

    if "CASH II CFD" in path_upper:
        return "indices_cash_2"

    if "CASH III CFD" in path_upper:
        return "indices_cash_3"

    if "CRYPTO I CFD" in path_upper:
        return "crypto_1"

    if "CRYPTO II CFD" in path_upper:
        return "crypto_2"

    if "EQUITIES I CFD" in path_upper:
        return "equities_1"

    if "EQUITIES II CFD" in path_upper:
        return "equities_2"

    if name_upper.startswith(("XAU", "XAG", "XPT", "XPD")):
        return "metals"

    return "other"


def build_universe(df: pd.DataFrame) -> dict:
    universe = {
        "broker": "FTMO",
        "data_source": "MT5",
        "storage_format": "parquet",
        "data_root": "E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO",
        "forex": [],
        "exotics": [],
        "metals": [],
        "commodities": [],
        "agriculture": [],
        "indices_cash_1": [],
        "indices_cash_2": [],
        "indices_cash_3": [],
        "crypto_1": [],
        "crypto_2": [],
        "equities_1": [],
        "equities_2": [],
        "other": [],
        "timeframes": [
            "M1",
            "M2",
            "M3",
            "M5",
            "M10",
            "M15",
            "M30",
            "H1",
            "H2",
            "H3",
            "H4",
            "H8",
            "H12",
            "D1",
            "W1",
            "MN1",
        ],
        "download": {
            "chunk_size": 5000,
            "sleep_seconds": 0.05,
            "skip_existing": False,
            "max_symbols": None,
            "max_timeframes": None,
        },
    }

    for _, row in df.iterrows():
        symbol = str(row["name"]).strip()
        path = str(row.get("path", "")).strip()

        if not symbol:
            continue

        group = normalise_group_name(path, symbol)

        if group not in universe:
            universe["other"].append(symbol)
        else:
            universe[group].append(symbol)

    ordered_groups = [
        "forex",
        "exotics",
        "metals",
        "commodities",
        "agriculture",
        "indices_cash_1",
        "indices_cash_2",
        "indices_cash_3",
        "crypto_1",
        "crypto_2",
        "equities_1",
        "equities_2",
        "other",
    ]

    for group in ordered_groups:
        universe[group] = sorted(set(universe[group]))

    return universe


def main(mode: str = "full"):
    logger.info("Starting market universe build from MT5 discovery file")
    logger.info(f"Source CSV: {SOURCE_CSV}")
    logger.info(f"Output YAML: {OUTPUT_YAML}")

    if not SOURCE_CSV.exists():
        raise FileNotFoundError(f"Missing source file: {SOURCE_CSV}")

    df = pd.read_csv(SOURCE_CSV)

    required_cols = {"name", "path"}

    if not required_cols.issubset(df.columns):
        raise ValueError(f"Source file missing required columns: {required_cols}")

    universe = build_universe(df)

    with open(OUTPUT_YAML, "w", encoding="utf-8") as f:
        yaml.safe_dump(
            universe,
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )

    logger.info("Market universe YAML created successfully")

    for key, value in universe.items():
        if isinstance(value, list):
            logger.info(f"{key}: {len(value)}")


if __name__ == "__main__":
    main()