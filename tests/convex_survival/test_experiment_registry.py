from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bacqe.convex_survival.experiment_registry import (
    ExperimentRegistryError,
    merge_registry,
    protocol_fingerprint,
    run_experiment_registry,
    validate_existing_registry,
    validate_inputs,
    validate_registry,
)


def design(experiment_id="CS04-ABC", rank=1, objective="Replicate evidence"):
    return {
        "design_rank": rank, "experiment_id": experiment_id, "candidate_id": f"CS03-{experiment_id}",
        "experiment_title": "Replication Expansion: ADX", "experiment_family": "REPLICATION_EXPANSION",
        "primary_filter": "ADX", "research_question": "Does it matter?", "null_hypothesis": "No effect",
        "alternative_hypothesis": "Stable effect", "objective": objective, "design_status": "DRAFT_PROTOCOL",
        "minimum_observations": 1000, "minimum_sole_veto_observations": 30,
        "maximum_calendar_days": 60, "stopping_rule": "Stop at target", "success_criteria": "Stable",
        "failure_criteria": "Unstable", "inconclusive_criteria": "Insufficient", "risk_guardrails": "No live changes",
        "priority_score": 0.04, "priority_band": "REPLICATION_REQUIRED", "sample_adequacy_score": 0.25,
        "confounding_risk_score": 0.60, "principal_risk": "Sparse evidence",
        "source_candidates_sha256": "a" * 64, "source_audit_sha256": "b" * 64,
        "generated_utc": "2026-07-24T00:00:00+00:00", "engine_version": "1.0.0",
    }


def audit():
    return {
        "engine_id": "CS04", "engine_name": "Experiment Design Engine", "engine_version": "1.0.0",
        "generated_utc": "2026-07-24T00:00:00+00:00", "validation": {"passed": True},
        "policy": {"production_changes_authorised": False},
    }


def test_input_validation_accepts_consistent_inputs():
    assert validate_inputs(pd.DataFrame([design()]), audit()).passed


def test_input_validation_rejects_wrong_audit():
    bad = audit(); bad["engine_id"] = "CS03"
    assert not validate_inputs(pd.DataFrame([design()]), bad).passed


def test_input_validation_requires_no_production_authority():
    bad = audit(); bad["policy"]["production_changes_authorised"] = True
    assert not validate_inputs(pd.DataFrame([design()]), bad).passed


def test_new_protocol_is_registered_as_unapproved_draft():
    registry, history = merge_registry(pd.DataFrame([design()]), None, "c" * 64, "d" * 64, "2026-01-01T00:00:00+00:00")
    assert registry.loc[0, "registry_status"] == "DRAFT"
    assert registry.loc[0, "review_status"] == "PENDING"
    assert not bool(registry.loc[0, "execution_authorised"])
    assert history.loc[0, "event_type"] == "REGISTERED"


def test_registry_ids_are_stable():
    frame = pd.DataFrame([design()])
    a, _ = merge_registry(frame, None, "c", "d", "x")
    b, _ = merge_registry(frame, None, "c", "d", "y")
    assert a.loc[0, "registry_id"] == b.loc[0, "registry_id"]


def test_registry_id_does_not_depend_on_rank():
    a, _ = merge_registry(pd.DataFrame([design(rank=1)]), None, "c", "d", "x")
    b, _ = merge_registry(pd.DataFrame([design(rank=99)]), None, "c", "d", "x")
    assert a.loc[0, "registry_id"] == b.loc[0, "registry_id"]


def test_idempotent_rerun_creates_no_history_event():
    first, history = merge_registry(pd.DataFrame([design()]), None, "c", "d", "x")
    second, rerun_history = merge_registry(pd.DataFrame([design()]), first, "c", "d", "y")
    assert len(history) == 1
    assert len(rerun_history) == 0
    assert second.loc[0, "protocol_revision"] == 1


def test_human_governance_fields_are_preserved():
    first, _ = merge_registry(pd.DataFrame([design()]), None, "c", "d", "x")
    first.loc[0, "owner"] = "Ben"
    first.loc[0, "review_status"] = "IN_REVIEW"
    second, _ = merge_registry(pd.DataFrame([design()]), first, "c", "d", "y")
    assert second.loc[0, "owner"] == "Ben"
    assert second.loc[0, "review_status"] == "IN_REVIEW"


def test_protocol_change_increments_revision_and_history():
    first, _ = merge_registry(pd.DataFrame([design()]), None, "c", "d", "x")
    revised, history = merge_registry(pd.DataFrame([design(objective="Revised objective")]), first, "e", "f", "y")
    assert revised.loc[0, "protocol_revision"] == 2
    assert history.loc[0, "event_type"] == "PROTOCOL_REVISED"


def test_protocol_fingerprint_ignores_rank():
    a = protocol_fingerprint(pd.Series(design(rank=1)))
    b = protocol_fingerprint(pd.Series(design(rank=9)))
    assert a == b


def test_existing_registry_rejects_unauthorised_running_status():
    registry, _ = merge_registry(pd.DataFrame([design()]), None, "c", "d", "x")
    registry.loc[0, "registry_status"] = "RUNNING"
    assert not validate_existing_registry(registry).passed


def test_registry_validation_preserves_identity():
    designs = pd.DataFrame([design()])
    registry, _ = merge_registry(designs, None, "c", "d", "x")
    assert validate_registry(registry, designs).passed


def test_run_writes_outputs_and_is_idempotent(tmp_path: Path):
    designs_path = tmp_path / "designs.csv"
    audit_path = tmp_path / "audit.json"
    output = tmp_path / "out"
    pd.DataFrame([design()]).to_csv(designs_path, index=False)
    audit_path.write_text(json.dumps(audit()), encoding="utf-8")
    first = run_experiment_registry(designs_path, audit_path, output)
    second = run_experiment_registry(designs_path, audit_path, output)
    assert first.paths.registry_csv.exists()
    assert first.paths.history_csv.exists()
    assert first.paths.report_txt.exists()
    assert first.paths.audit_json.exists()
    assert len(second.history) == 1
    payload = json.loads(second.paths.audit_json.read_text(encoding="utf-8"))
    assert payload["new_history_event_count"] == 0
    assert payload["policy"]["registration_implies_approval"] is False


def test_run_rejects_invalid_source(tmp_path: Path):
    designs_path = tmp_path / "designs.csv"
    audit_path = tmp_path / "audit.json"
    pd.DataFrame([design()]).drop(columns=["experiment_id"]).to_csv(designs_path, index=False)
    audit_path.write_text(json.dumps(audit()), encoding="utf-8")
    with pytest.raises(ExperimentRegistryError):
        run_experiment_registry(designs_path, audit_path, tmp_path / "out")
