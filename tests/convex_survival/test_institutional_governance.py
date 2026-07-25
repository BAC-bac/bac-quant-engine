from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bacqe.convex_survival.institutional_governance import (
    DASHBOARD_COLUMNS,
    HEALTH_COLUMNS,
    READINESS_COLUMNS,
    InstitutionalGovernanceError,
    _governance_id,
    build_dashboard,
    build_health,
    build_readiness,
    build_review_queue,
    run_institutional_governance,
    validate_outputs,
    validate_sources,
)


def frames() -> dict[str, pd.DataFrame]:
    return {
        "CS03": pd.DataFrame([{"candidate_id":"C1","priority_rank":1,"priority_score":0.5,"priority_band":"HIGH","candidate_type":"REPLICATION_EXPANSION","primary_filter":"ADX"}]),
        "CS04": pd.DataFrame([{"experiment_id":"E1","candidate_id":"C1","experiment_title":"Replication Expansion: ADX","design_status":"DRAFT_PROTOCOL","experiment_family":"REPLICATION_EXPANSION","primary_filter":"ADX","minimum_observations":100,"maximum_calendar_days":30}]),
        "CS05": pd.DataFrame([{"registry_id":"R1","experiment_id":"E1","registry_status":"DRAFT","review_status":"PENDING","execution_authorised":False,"protocol_revision":1}]),
        "CS06": pd.DataFrame([{"ledger_id":"L1","registry_id":"R1","experiment_id":"E1","execution_status":"AWAITING_APPROVAL","execution_authorised":False,"observations_collected":0,"evidence_target_reached":False,"guardrail_breach":False,"data_quality_passed":True}]),
        "CS07": pd.DataFrame([{"evidence_id":"V1","ledger_id":"L1","registry_id":"R1","experiment_id":"E1","evidence_state":"NO_EVIDENCE","scientific_recommendation":"AWAIT_APPROVAL","review_priority":"LOW","review_reason":"No governed authority.","combined_progress":0.0,"replication_required":True,"final_conclusion_authorised":False,"production_changes_authorised":False,"execution_status":"AWAITING_APPROVAL","execution_authorised":False,"observations_collected":0,"evidence_target_reached":False,"guardrail_breach":False,"data_quality_passed":True,"protocol_revision":1}]),
    }


def audits() -> dict[str, dict]:
    return {e:{"engine_id":e,"engine_version":"1.0.0","generated_utc":"2026-01-01T00:00:00+00:00","validation":{"passed":True},"policy":{"production_changes_authorised":False}} for e in ("CS03","CS04","CS05","CS06","CS07")}


def test_validate_sources_accepts_consistent_inputs():
    assert validate_sources(frames(), audits()).passed


def test_validate_sources_rejects_wrong_audit():
    a=audits(); a["CS07"]["engine_id"]="CS06"
    assert not validate_sources(frames(), a).passed


def test_validate_sources_rejects_failed_source():
    a=audits(); a["CS04"]["validation"]["passed"]=False
    assert not validate_sources(frames(), a).passed


def test_validate_sources_rejects_identity_break():
    f=frames(); f["CS06"].loc[0,"experiment_id"]="OTHER"
    assert not validate_sources(f, audits()).passed


def test_validate_sources_rejects_final_conclusion_authority():
    f=frames(); f["CS07"].loc[0,"final_conclusion_authorised"]=True
    assert not validate_sources(f, audits()).passed


def test_dashboard_preserves_full_lineage():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    assert d.loc[0,"candidate_id"]=="C1" and d.loc[0,"evidence_id"]=="V1"


def test_dashboard_governance_id_is_stable():
    assert _governance_id("E1")==_governance_id("E1")


def test_dashboard_waiting_approval_requires_attention():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    assert d.loc[0,"attention_required"] and d.loc[0,"pipeline_stage"]=="AWAITING_GOVERNANCE"


def test_dashboard_never_declares_production_ready():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    assert not d.production_ready.any()


def test_guardrail_breach_is_invalidated_review():
    f=frames(); f["CS06"].loc[0,"guardrail_breach"]=True; f["CS07"].loc[0,"guardrail_breach"]=True; f["CS07"].loc[0,"review_priority"]="CRITICAL"
    d=build_dashboard(f, "2026-01-01T00:00:00+00:00")
    assert d.loc[0,"pipeline_stage"]=="INVALIDATED_REVIEW"


def test_health_has_six_dimensions():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    h=build_health(d, frames(), "2026-01-01T00:00:00+00:00")
    assert len(h)==6 and tuple(h.columns)==HEALTH_COLUMNS


def test_health_scores_are_bounded():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    h=build_health(d, frames(), "2026-01-01T00:00:00+00:00")
    assert h.score.between(0,100).all()


def test_readiness_is_foundational_without_authority():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00"); h=build_health(d,frames(),"x")
    r=build_readiness(d,h,"x")
    assert r.loc[0,"overall_readiness_status"]=="FOUNDATIONAL"


def test_readiness_recommends_human_governance_first():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00"); h=build_health(d,frames(),"x")
    r=build_readiness(d,h,"x")
    assert "human governance" in r.loc[0,"next_institutional_action"].lower()


def test_review_queue_contains_attention_items():
    d=build_dashboard(frames(), "2026-01-01T00:00:00+00:00")
    q=build_review_queue(d)
    assert len(q)==1 and q.loc[0,"review_rank"]==1


def test_validate_outputs_rejects_production_ready():
    d=build_dashboard(frames(), "x"); h=build_health(d,frames(),"x"); r=build_readiness(d,h,"x")
    d.loc[0,"production_ready"]=True
    assert not validate_outputs(d,h,r).passed


def _write_sources(tmp_path: Path, f: dict[str,pd.DataFrame], a: dict[str,dict]):
    sp={}; ap={}
    for e in f:
        sp[e]=tmp_path/f"{e}.csv"; ap[e]=tmp_path/f"{e}.json"
        f[e].to_csv(sp[e],index=False); ap[e].write_text(json.dumps(a[e]),encoding="utf-8")
    return sp,ap


def test_run_writes_all_outputs(tmp_path):
    sp,ap=_write_sources(tmp_path,frames(),audits())
    o=run_institutional_governance(sp,ap,tmp_path/"out")
    assert all(p.exists() for p in [o.paths.dashboard_csv,o.paths.health_csv,o.paths.review_queue_csv,o.paths.readiness_csv,o.paths.report_txt,o.paths.audit_json])


def test_run_rejects_invalid_source(tmp_path):
    f=frames(); f["CS07"].loc[0,"production_changes_authorised"]=True
    sp,ap=_write_sources(tmp_path,f,audits())
    with pytest.raises(InstitutionalGovernanceError): run_institutional_governance(sp,ap,tmp_path/"out")
