#!/usr/bin/env python3
from __future__ import annotations

"""
BACQE EH00 - Validate Extended Horizons Foundation.

This executable performs deterministic tests against the shared institutional
foundation. It does not read or write the Quant Lab data lake.
"""

import argparse

from extended_horizons_foundation import (
    EVIDENCE_SCHEMA_VERSION,
    FOUNDATION_ID,
    FOUNDATION_VERSION,
    EngineMetadata,
    print_engine_header,
    run_foundation_self_tests,
)


ENGINE_METADATA = EngineMetadata(
    engine_id=FOUNDATION_ID,
    engine_name="BACQE EH00 - EXTENDED HORIZONS FOUNDATION VALIDATION",
    engine_version=FOUNDATION_VERSION,
    methodology_version="EH00_FOUNDATION_V1.0",
    evidence_schema_version=EVIDENCE_SCHEMA_VERSION,
)


def main() -> int:
    print_engine_header(
        ENGINE_METADATA,
        fields={
            "Purpose": "Validate shared institutional infrastructure",
            "Data-lake access": "None",
        },
    )
    run_foundation_self_tests()
    print("Overall status:             PASS")
    print("=" * 110)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate BACQE Extended Horizons shared foundation."
    )
    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
    raise SystemExit(main())
