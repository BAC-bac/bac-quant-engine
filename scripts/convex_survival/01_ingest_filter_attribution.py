"""BACQE Convex Survival Engine 01: ingest MQL5 filter attribution evidence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from bacqe.convex_survival.ingestion import (
    AttributionIngestionError,
    ingest_attribution_csv,
)

DEFAULT_STAGING_ROOT = Path(
    r"E:\Quant_Lab\data\staging\convex_survival\filter_attribution"
)
DEFAULT_REPORT_ROOT = Path(
    r"E:\Quant_Lab\reports\convex_survival\ingestion"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate an immutable MQL5 Convex Survival attribution CSV and "
            "write a lineage-stamped staged Parquet dataset plus ingestion audits."
        )
    )
    parser.add_argument("source_csv", type=Path, help="Path to the source MQL5 CSV.")
    parser.add_argument(
        "--staging-root",
        type=Path,
        default=DEFAULT_STAGING_ROOT,
        help=f"Staged Parquet directory (default: {DEFAULT_STAGING_ROOT}).",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
        help=f"Audit output directory (default: {DEFAULT_REPORT_ROOT}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = ingest_attribution_csv(
            source_csv=args.source_csv,
            staging_root=args.staging_root,
            report_root=args.report_root,
        )
    except AttributionIngestionError as exc:
        print("=" * 88)
        print("BACQE CONVEX SURVIVAL - ENGINE 01")
        print("VALIDATION RESULT: FAIL")
        print(f"Reason: {exc}")
        print("=" * 88)
        return 1

    print("=" * 88)
    print("BACQE CONVEX SURVIVAL - ENGINE 01 FILTER ATTRIBUTION INGESTION")
    print("=" * 88)
    print(f"Rows:             {result.row_count:,}")
    print(f"Columns:          {result.column_count}")
    print(f"Run IDs:          {', '.join(result.run_ids)}")
    print(f"Symbols:          {', '.join(result.symbols)}")
    print(f"Timeframes:       {', '.join(result.timeframes)}")
    print(f"Date range:       {result.min_bar_time} -> {result.max_bar_time}")
    print(f"All-pass rows:    {result.all_pass_rows:,}")
    print(f"Sole-veto rows:   {result.sole_veto_rows:,}")
    print(f"Duplicate rows:   {result.duplicate_rows:,}")
    print(f"Source SHA-256:   {result.source_sha256}")
    print(f"Staged Parquet:   {result.staged_parquet}")
    print(f"Audit JSON:       {result.audit_json}")
    print(f"Audit report:     {result.audit_report}")
    print("VALIDATION RESULT: PASS")
    print("=" * 88)
    return 0


if __name__ == "__main__":
    sys.exit(main())
