from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bacqe.convex_survival.execution_ledger import (
    ExecutionLedgerError,
    build_readiness,
    merge_ledger,
    run_execution_ledger,
    validate_existing_ledger,
    validate_inputs,
    validate_ledger,
)


def registry(status="DRAFT", authorised=False, revision=1):
    return {
        "registry_rank": 1, "registry_id": "CS05-REG-ABC", "experiment_id": "CS04-ABC",
        "candidate_id": "CS03-ABC", "experiment_title": "Replication Expansion: ADX",
        "experiment_family": "REPLICATION_EXPANSION", "primary_filter": "ADX",
        "registry_status": status, "review_status": "APPROVED" if authorised else "PENDING",
        "execution_authorised": authorised, "protocol_revision": revision,
        "protocol_fingerprint": "a" * 64, "minimum_observations": 1000,
        "minimum_sole_veto_observations": 30, "maximum_calendar_days": 60,
        "priority_score": 0.1, "priority_band": "REPLICATION_REQUIRED",
        "principal_risk": "Sparse", "stopping_rule": "Stop at target",
        "success_criteria": "Stable", "failure_criteria": "Unstable",
        "inconclusive_criteria": "Insufficient", "risk_guardrails": "No live changes",
        "source_designs_sha256": "b" * 64, "source_audit_sha256": "c" * 64,
        "generated_utc": "2026-01-01T00:00:00+00:00", "engine_version": "1.0.0",
    }


def audit():
    return {
        "engine_id": "CS05", "engine_name": "Experiment Registry Engine", "engine_version": "1.0.0",
        "generated_utc": "2026-01-01T00:00:00+00:00", "validation": {"passed": True},
        "policy": {"execution_requires_explicit_authorisation": True, "registration_implies_approval": False},
    }


def test_input_validation_accepts_registry():
    assert validate_inputs(pd.DataFrame([registry()]), audit()).passed


def test_input_validation_rejects_wrong_audit():
    bad = audit(); bad["engine_id"] = "CS04"
    assert not validate_inputs(pd.DataFrame([registry()]), bad).passed


def test_input_validation_rejects_inconsistent_authority():
    assert not validate_inputs(pd.DataFrame([registry(status="DRAFT", authorised=True)]), audit()).passed


def test_new_draft_waits_for_approval():
    ledger, history = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    assert ledger.loc[0, "execution_status"] == "AWAITING_APPROVAL"
    assert history.loc[0, "event_type"] == "LEDGER_REGISTERED"


def test_approved_authorised_protocol_is_ready():
    ledger, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), None, "d", "e", "x")
    assert ledger.loc[0, "execution_status"] == "READY"


def test_ledger_id_is_stable():
    a, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    b, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "y")
    assert a.loc[0, "ledger_id"] == b.loc[0, "ledger_id"]


def test_idempotent_rerun_adds_no_event():
    first, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    _, events = merge_ledger(pd.DataFrame([registry()]), first, "d", "e", "y")
    assert events.empty


def test_authority_change_updates_status_and_history():
    first, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    second, events = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), first, "f", "g", "y")
    assert second.loc[0, "execution_status"] == "READY"
    assert events.loc[0, "event_type"] == "AUTHORITY_STATUS_SYNCED"


def test_protocol_revision_is_synchronised():
    first, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    second, events = merge_ledger(pd.DataFrame([registry(revision=2)]), first, "f", "g", "y")
    assert second.loc[0, "protocol_revision"] == 2
    assert events.loc[0, "event_type"] == "PROTOCOL_REVISION_SYNCED"


def test_evidence_progress_and_target_trigger():
    first, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), None, "d", "e", "x")
    first.loc[0, "observations_collected"] = 1000
    first.loc[0, "sole_veto_observations_collected"] = 30
    second, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), first, "d", "e", "y")
    assert bool(second.loc[0, "evidence_target_reached"])
    assert second.loc[0, "stop_reason"] == "TARGET_REACHED"


def test_guardrail_breach_triggers_stop():
    first, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), None, "d", "e", "x")
    first.loc[0, "guardrail_breach"] = True
    second, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), first, "d", "e", "y")
    assert second.loc[0, "stop_reason"] == "GUARDRAIL_BREACH"


def test_readiness_requires_execution_metadata():
    ledger, _ = merge_ledger(pd.DataFrame([registry(status="APPROVED", authorised=True)]), None, "d", "e", "x")
    readiness = build_readiness(ledger)
    assert readiness.loc[0, "readiness_class"] == "CONFIGURATION_REQUIRED"


def test_existing_ledger_rejects_unauthorised_running():
    ledger, _ = merge_ledger(pd.DataFrame([registry()]), None, "d", "e", "x")
    ledger.loc[0, "execution_status"] = "RUNNING"
    assert not validate_existing_ledger(ledger).passed


def test_ledger_validation_preserves_registry_identity():
    source = pd.DataFrame([registry()])
    ledger, _ = merge_ledger(source, None, "d", "e", "x")
    assert validate_ledger(ledger, source).passed


def test_run_writes_outputs_and_is_idempotent(tmp_path: Path):
    registry_path, audit_path, out = tmp_path / "registry.csv", tmp_path / "audit.json", tmp_path / "out"
    pd.DataFrame([registry()]).to_csv(registry_path, index=False)
    audit_path.write_text(json.dumps(audit()), encoding="utf-8")
    first = run_execution_ledger(registry_path, audit_path, out)
    second = run_execution_ledger(registry_path, audit_path, out)
    assert first.paths.ledger_csv.exists() and first.paths.readiness_csv.exists()
    assert len(second.history) == 1
    payload = json.loads(second.paths.audit_json.read_text())
    assert payload["new_history_event_count"] == 0
    assert payload["policy"]["stopping_trigger_is_not_scientific_conclusion"] is True


def test_run_rejects_invalid_source(tmp_path: Path):
    rp, ap = tmp_path / "registry.csv", tmp_path / "audit.json"
    pd.DataFrame([registry()]).drop(columns=["registry_id"]).to_csv(rp, index=False)
    ap.write_text(json.dumps(audit()), encoding="utf-8")
    with pytest.raises(ExecutionLedgerError):
        run_execution_ledger(rp, ap, tmp_path / "out")
