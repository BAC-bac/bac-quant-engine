from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bacqe.convex_survival.experiment_design import ENGINE_VERSION, ExperimentDesignError, build_experiment_designs, run_experiment_design

DEFAULT_INPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\candidate_opportunity_analysis")
DEFAULT_OUTPUT_DIR = Path(r"E:\Quant_Lab\data\analysis\convex_survival\experiment_design")


def deterministic_self_tests() -> None:
    import pandas as pd
    row = {
        "priority_rank": 1, "candidate_id": "CS03-TEST-ABC", "candidate_type": "REPLICATION_EXPANSION",
        "primary_filter": "ADX", "secondary_filter": "", "candidate_title": "Test", "research_question": "Q?",
        "null_hypothesis": "Null", "alternative_hypothesis": "Alt", "control_definition": "Frozen policy",
        "treatment_definition": "Collect more", "evidence_basis": "Evidence", "sample_adequacy_score": 0.25,
        "confounding_risk_score": 0.6, "information_gain_proxy": 0.05, "expected_research_value": 0.4,
        "priority_score": 0.04, "priority_band": "REPLICATION_REQUIRED", "principal_risk": "Small sample",
        "recommended_next_action": "Replicate", "source_engine_id": "CS02", "source_engine_version": "1.0.0",
        "generated_utc": "2026-01-01T00:00:00+00:00", "engine_version": "1.0.0",
    }
    a = build_experiment_designs(pd.DataFrame([row]), "a" * 64, "b" * 64, "2026-01-02T00:00:00+00:00")
    b = build_experiment_designs(pd.DataFrame([row]), "a" * 64, "b" * 64, "2026-01-02T00:00:00+00:00")
    assert a.equals(b)
    assert a.loc[0, "experiment_id"].startswith("CS04-")
    assert a.loc[0, "design_status"] == "DRAFT_PROTOCOL"
    assert int(a.loc[0, "minimum_sole_veto_observations"]) == 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BACQE Convex Survival CS04 Experiment Design Engine")
    parser.add_argument("--candidates", type=Path, default=DEFAULT_INPUT_DIR / "candidate_opportunity_analysis_latest.csv")
    parser.add_argument("--audit", type=Path, default=DEFAULT_INPUT_DIR / "candidate_opportunity_analysis_audit_latest.json")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("=" * 100)
    print("BACQE CONVEX SURVIVAL CS04 - EXPERIMENT DESIGN ENGINE")
    print("=" * 100)
    print(f"Engine version:             {ENGINE_VERSION}")
    print("Running deterministic self-tests.")
    try:
        deterministic_self_tests()
        print("Self-tests passed.")
        print(f"Candidate input:            {args.candidates}")
        print(f"Source audit:               {args.audit}")
        print(f"Output directory:           {args.output_dir}")
        print("-" * 100)
        outputs = run_experiment_design(args.candidates, args.audit, args.output_dir)
    except (ExperimentDesignError, FileNotFoundError, ValueError) as exc:
        print("ENGINE RESULT:              FAIL")
        print(f"Reason:                     {exc}")
        print("=" * 100)
        return 1
    print("ENGINE RESULT:              PASS")
    print(f"Protocols generated:        {len(outputs.designs)}")
    print(f"Execution manifest rows:    {len(outputs.execution_manifest)}")
    print(f"Experiment designs CSV:     {outputs.paths.designs_csv}")
    print(f"Execution manifest CSV:     {outputs.paths.execution_manifest_csv}")
    print(f"Design report:              {outputs.paths.report_txt}")
    print(f"Audit JSON:                 {outputs.paths.audit_json}")
    if outputs.validation.warnings:
        print("Warnings:")
        for warning in outputs.validation.warnings:
            print(f"  - {warning}")
    print("=" * 100)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
