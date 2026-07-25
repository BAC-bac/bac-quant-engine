from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from bacqe.convex_survival.institutional_governance import (
    ENGINE_VERSION,
    InstitutionalGovernanceError,
    build_health,
    build_readiness,
    run_institutional_governance,
)

BASE = Path(r"E:\Quant_Lab\data\analysis\convex_survival")
OUTPUT = BASE / "institutional_governance"


def self_tests() -> None:
    dashboard = pd.DataFrame([
        {
            "production_ready": False,
            "registry_id": "CS05-X",
            "execution_authorised": False,
            "execution_status": "AWAITING_APPROVAL",
            "data_quality_passed": True,
            "evidence_target_reached": False,
            "replication_required": True,
            "combined_progress": 0.0,
            "evidence_state": "NO_EVIDENCE",
            "scientific_recommendation": "AWAIT_APPROVAL",
            "review_priority": "LOW",
        }
    ])
    health = build_health(dashboard, {}, "2026-01-01T00:00:00+00:00")
    readiness = build_readiness(dashboard, health, "2026-01-01T00:00:00+00:00")
    assert health.score.between(0, 100).all()
    assert readiness.loc[0, "overall_readiness_status"] == "FOUNDATIONAL"
    assert readiness.loc[0, "production_changes_authorised"] is False or not readiness.loc[0, "production_changes_authorised"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BACQE Convex Survival CS08 governance dashboard")
    parser.add_argument("--cs03-dir", type=Path, default=BASE / "candidate_opportunity_analysis")
    parser.add_argument("--cs04-dir", type=Path, default=BASE / "experiment_design")
    parser.add_argument("--cs05-dir", type=Path, default=BASE / "experiment_registry")
    parser.add_argument("--cs06-dir", type=Path, default=BASE / "experiment_execution")
    parser.add_argument("--cs07-dir", type=Path, default=BASE / "experiment_evidence")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_paths = {
        "CS03": args.cs03_dir / "candidate_opportunity_analysis_latest.csv",
        "CS04": args.cs04_dir / "experiment_designs_latest.csv",
        "CS05": args.cs05_dir / "experiment_registry_latest.csv",
        "CS06": args.cs06_dir / "experiment_execution_ledger_latest.csv",
        "CS07": args.cs07_dir / "experiment_evidence_latest.csv",
    }
    audit_paths = {
        "CS03": args.cs03_dir / "candidate_opportunity_analysis_audit_latest.json",
        "CS04": args.cs04_dir / "experiment_design_audit_latest.json",
        "CS05": args.cs05_dir / "experiment_registry_audit_latest.json",
        "CS06": args.cs06_dir / "experiment_execution_ledger_audit_latest.json",
        "CS07": args.cs07_dir / "experiment_evidence_audit_latest.json",
    }
    print("=" * 108)
    print("BACQE CONVEX SURVIVAL CS08 - INSTITUTIONAL RESEARCH GOVERNANCE DASHBOARD")
    print("=" * 108)
    print(f"Engine version:             {ENGINE_VERSION}")
    print("Running deterministic self-tests.")
    try:
        self_tests()
        print("Self-tests passed.")
        print(f"Output directory:           {args.output_dir}")
        print("-" * 108)
        outputs = run_institutional_governance(source_paths, audit_paths, args.output_dir)
    except (InstitutionalGovernanceError, FileNotFoundError, ValueError) as exc:
        print("ENGINE RESULT:              FAIL")
        print(f"Reason:                     {exc}")
        print("=" * 108)
        return 1

    r = outputs.readiness.iloc[0]
    print("ENGINE RESULT:              PASS")
    print(f"Dashboard experiments:      {len(outputs.dashboard)}")
    print(f"Health dimensions:          {len(outputs.health)}")
    print(f"Human review actions:       {len(outputs.review_queue)}")
    print(f"Institutional health:       {r.institutional_health_score:.2f}/100")
    print(f"Research readiness:         {r.overall_readiness_score:.2f}/100 ({r.overall_readiness_status})")
    print(f"Dashboard CSV:              {outputs.paths.dashboard_csv}")
    print(f"Health CSV:                 {outputs.paths.health_csv}")
    print(f"Review queue CSV:           {outputs.paths.review_queue_csv}")
    print(f"Readiness CSV:              {outputs.paths.readiness_csv}")
    print(f"Governance report:          {outputs.paths.report_txt}")
    print(f"Audit JSON:                 {outputs.paths.audit_json}")
    if outputs.validation.warnings:
        print("Warnings:")
        for warning in outputs.validation.warnings:
            print(f"  - {warning}")
    print("=" * 108)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
