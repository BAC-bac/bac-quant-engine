from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bacqe.convex_survival.experiment_registry import (
    ENGINE_VERSION,
    ExperimentRegistryError,
    merge_registry,
    run_experiment_registry,
)

DEFAULT_INPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\experiment_design")
DEFAULT_OUTPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\experiment_registry")


def deterministic_self_tests() -> None:
    import pandas as pd

    design = {
        "design_rank": 1, "experiment_id": "CS04-TEST-ABC", "candidate_id": "CS03-TEST-ABC",
        "experiment_title": "Replication Expansion: ADX", "experiment_family": "REPLICATION_EXPANSION",
        "primary_filter": "ADX", "research_question": "Does ADX matter?", "null_hypothesis": "No",
        "alternative_hypothesis": "Yes", "objective": "Replicate evidence", "design_status": "DRAFT_PROTOCOL",
        "minimum_observations": 1000, "minimum_sole_veto_observations": 30,
        "maximum_calendar_days": 60, "stopping_rule": "Stop at evidence target",
        "success_criteria": "Stable effect", "failure_criteria": "No effect",
        "inconclusive_criteria": "Insufficient evidence", "risk_guardrails": "No production changes",
        "priority_score": 0.04, "priority_band": "REPLICATION_REQUIRED",
        "sample_adequacy_score": 0.25, "confounding_risk_score": 0.60,
        "principal_risk": "Sparse evidence", "source_candidates_sha256": "a" * 64,
        "source_audit_sha256": "b" * 64, "generated_utc": "2026-01-01T00:00:00+00:00",
        "engine_version": "1.0.0",
    }
    a, history_a = merge_registry(pd.DataFrame([design]), None, "c" * 64, "d" * 64, "2026-01-02T00:00:00+00:00")
    b, history_b = merge_registry(pd.DataFrame([design]), a, "c" * 64, "d" * 64, "2026-01-03T00:00:00+00:00")
    assert a.loc[0, "registry_id"].startswith("CS05-REG-")
    assert a.loc[0, "registry_status"] == "DRAFT"
    assert bool(a.loc[0, "execution_authorised"]) is False
    assert len(history_a) == 1
    assert len(history_b) == 0
    assert b.loc[0, "registry_id"] == a.loc[0, "registry_id"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BACQE Convex Survival CS05 Experiment Registry Engine")
    parser.add_argument("--designs", type=Path, default=DEFAULT_INPUT_DIR / "experiment_designs_latest.csv")
    parser.add_argument("--audit", type=Path, default=DEFAULT_INPUT_DIR / "experiment_design_audit_latest.json")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--existing-registry", type=Path, default=None)
    parser.add_argument("--existing-history", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("=" * 100)
    print("BACQE CONVEX SURVIVAL CS05 - EXPERIMENT REGISTRY ENGINE")
    print("=" * 100)
    print(f"Engine version:             {ENGINE_VERSION}")
    print("Running deterministic self-tests.")
    try:
        deterministic_self_tests()
        print("Self-tests passed.")
        print(f"Design input:               {args.designs}")
        print(f"Source audit:               {args.audit}")
        print(f"Output directory:           {args.output_dir}")
        print("-" * 100)
        outputs = run_experiment_registry(
            args.designs, args.audit, args.output_dir,
            args.existing_registry, args.existing_history,
        )
    except (ExperimentRegistryError, FileNotFoundError, ValueError) as exc:
        print("ENGINE RESULT:              FAIL")
        print(f"Reason:                     {exc}")
        print("=" * 100)
        return 1
    print("ENGINE RESULT:              PASS")
    print(f"Registered experiments:     {len(outputs.registry)}")
    print(f"Registry history rows:      {len(outputs.history)}")
    print(f"Experiment registry CSV:    {outputs.paths.registry_csv}")
    print(f"Registry history CSV:       {outputs.paths.history_csv}")
    print(f"Registry report:            {outputs.paths.report_txt}")
    print(f"Audit JSON:                 {outputs.paths.audit_json}")
    if outputs.validation.warnings:
        print("Warnings:")
        for warning in outputs.validation.warnings:
            print(f"  - {warning}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
