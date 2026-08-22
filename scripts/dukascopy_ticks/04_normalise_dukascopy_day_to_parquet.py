"""Deprecated one-day entry point for the certified Script 08 normaliser.

This file intentionally contains no decoding or point-size logic. It remains as a
backward-compatible convenience entry point, but all writes are delegated to the
authoritative D1 implementation in Script 08.
"""

from __future__ import annotations

from datetime import datetime
import importlib.util
from pathlib import Path
import sys

from dukascopy_contract import get_symbol_metadata


SYMBOL = "EURUSD"
DATE_STR = "2024-01-02"


def load_run_normalisation():
    script_path = Path(__file__).with_name("08_normalise_dukascopy_date_range.py")
    spec = importlib.util.spec_from_file_location("bacqe_dukascopy_normaliser_08", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load certified normaliser: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_normalisation


def main() -> None:
    metadata = get_symbol_metadata(SYMBOL)
    datetime.strptime(DATE_STR, "%Y-%m-%d")
    print(
        "[DEPRECATED] Script 04 delegates to the certified Script 08 contract: "
        f"symbol={metadata.symbol}, date={DATE_STR}."
    )
    load_run_normalisation()(
        symbol=metadata.symbol,
        start_date=DATE_STR,
        end_date=DATE_STR,
    )


if __name__ == "__main__":
    main()
