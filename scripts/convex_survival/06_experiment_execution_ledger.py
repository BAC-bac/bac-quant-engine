from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from bacqe.convex_survival.execution_ledger import (
    ENGINE_VERSION,
    ExecutionLedgerError,
    merge_ledger,
    run_execution_ledger,
)

DEFAULT_INPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\experiment_registry")
DEFAULT_OUTPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\experiment_execution")


def deterministic_self_tests() -> None:
    row = {
        "registry_rank": 1, "registry_id": "CS05-REG-ABC", "experiment_id": "CS04-ABC",
        "candidate_id": "CS03-ABC", "experiment_title": "Replication Expansion: ADX",
        "experiment_family": "REPLICATION_EXPANSION", "primary_filter": "ADX",
        "registry_status": "DRAFT", "review_status": "PENDING",
        "execution_authorised": False, "protocol_revision": 1,
        "protocol_fingerprint": "a" * 64, "minimum_observations": 1000,
        "minimum_sole_veto_observations": 30, "maximum_calendar_days": 60,
        "priority_score": 0.1, "priority_band": "REPLICATION_REQUIRED",
        "principal_risk": "Sparse evidence", "stopping_rule": "Stop at target",
        "success_criteria": "Stable", "failure_criteria": "Unstable",
        "inconclusive_criteria": "Insufficient", "risk_guardrails": "No live changes",
        "source_designs_sha256": "b" * 64, "source_audit_sha256": "c" * 64,
        "generated_utc": "2026-01-01T00:00:00+00:00", "engine_version": "1.0.0",
    }
    first, history = merge_ledger(pd.DataFrame([row]), None, "d" * 64, "e" * 64, "2026-01-02T00:00:00+00:00")
    second, rerun = merge_ledger(pd.DataFrame([row]), first, "d" * 64, "e" * 64, "2026-01-03T00:00:00+00:00")
    assert first.loc[0, "ledger_id"].startswith("CS06-LED-")
    assert first.loc[0, "execution_status"] == "AWAITING_APPROVAL"
    assert len(history) == 1 and len(rerun) == 0
    assert second.loc[0, "ledger_id"] == first.loc[0, "ledger_id"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BACQE Convex Survival CS06 Experiment Execution Ledger")
    parser.add_argument("--registry", type=Path, default=DEFAULT_INPUT_DIR / "experiment_registry_latest.csv")
    parser.add_argument("--audit", type=Path, default=DEFAULT_INPUT_DIR / "experiment_registry_audit_latest.json")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--existing-ledger", type=Path, default=None)
    parser.add_argument("--existing-history", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("=" * 100)
    print("BACQE CONVEX SURVIVAL CS06 - EXPERIMENT EXECUTION LEDGER")
    print("=" * 100)
    print(f"Engine version:             {ENGINE_VERSION}")
    print("Running deterministic self-tests.")
    try:
        deterministic_self_tests()
        print("Self-tests passed.")
        print(f"Registry input:             {args.registry}")
        print(f"Source audit:               {args.audit}")
        print(f"Output directory:           {args.output_dir}")
        print("-" * 100)
        outputs = run_execution_ledger(args.registry, args.audit, args.output_dir, args.existing_ledger, args.existing_history)
    except (ExecutionLedgerError, FileNotFoundError, ValueError) as exc:
        print("ENGINE RESULT:              FAIL")
        print(f"Reason:                     {exc}")
        print("=" * 100)
        return 1
    print("ENGINE RESULT:              PASS")
    print(f"Ledger experiments:         {len(outputs.ledger)}")
    print(f"Execution history rows:     {len(outputs.history)}")
    print(f"Readiness queue rows:       {len(outputs.readiness)}")
    print(f"Execution ledger CSV:       {outputs.paths.ledger_csv}")
    print(f"Execution history CSV:      {outputs.paths.history_csv}")
    print(f"Readiness queue CSV:        {outputs.paths.readiness_csv}")
    print(f"Ledger report:              {outputs.paths.report_txt}")
    print(f"Audit JSON:                 {outputs.paths.audit_json}")
    if outputs.validation.warnings:
        print("Warnings:")
        for warning in outputs.validation.warnings:
            print(f"  - {warning}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
