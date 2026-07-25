from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import pytest
from bacqe.convex_survival.experiment_design import (
    ExperimentDesignError, build_experiment_designs, build_manifest,
    run_experiment_design, validate_designs, validate_inputs,
)


def candidate(candidate_type="REPLICATION_EXPANSION", candidate_id="CS03-ABC", rank=1, adequacy=0.25):
    return {
        "priority_rank": rank, "candidate_id": candidate_id, "candidate_type": candidate_type,
        "primary_filter": "ADX", "secondary_filter": "", "candidate_title": "Candidate",
        "research_question": "Does it matter?", "null_hypothesis": "No effect",
        "alternative_hypothesis": "Stable effect", "control_definition": "Frozen production policy",
        "treatment_definition": "Specified treatment", "evidence_basis": "Evidence",
        "sample_adequacy_score": adequacy, "confounding_risk_score": 0.6,
        "information_gain_proxy": 0.05, "expected_research_value": 0.4,
        "priority_score": 0.04, "priority_band": "REPLICATION_REQUIRED",
        "principal_risk": "Sparse evidence", "recommended_next_action": "Replicate",
        "source_engine_id": "CS02", "source_engine_version": "1.0.0",
        "generated_utc": "2026-07-24T00:00:00+00:00", "engine_version": "1.0.0",
    }


def audit():
    return {"engine_id":"CS03","engine_name":"Candidate Opportunity Analysis","engine_version":"1.0.0","generated_utc":"2026-07-24T00:00:00+00:00","validation":{"passed":True}}


def test_input_validation_accepts_consistent_frame():
    assert validate_inputs(pd.DataFrame([candidate()]), audit()).passed


def test_input_validation_rejects_wrong_audit():
    bad=audit(); bad["engine_id"]="CS02"
    assert not validate_inputs(pd.DataFrame([candidate()]), bad).passed


def test_input_validation_rejects_unknown_family():
    result=validate_inputs(pd.DataFrame([candidate("UNKNOWN")]), audit())
    assert not result.passed


def test_all_candidate_families_generate_protocols():
    families=["THRESHOLD_SENSITIVITY","REPLICATION_EXPANSION","INTERACTION_ISOLATION","DATA_QUALITY_INVESTIGATION"]
    frame=pd.DataFrame([candidate(f, f"CS03-{i}", i) for i,f in enumerate(families,1)])
    out=build_experiment_designs(frame,"a"*64,"b"*64,"2026-07-24T00:00:00+00:00")
    assert set(out.experiment_family)==set(families)
    assert len(out)==4


def test_ids_are_stable():
    frame=pd.DataFrame([candidate()])
    a=build_experiment_designs(frame,"a"*64,"b"*64,"x")
    b=build_experiment_designs(frame,"a"*64,"b"*64,"y")
    assert a.loc[0,"experiment_id"]==b.loc[0,"experiment_id"]


def test_id_does_not_depend_on_rank():
    a=pd.DataFrame([candidate(rank=1)])
    b=pd.DataFrame([candidate(rank=99)])
    assert build_experiment_designs(a,"a","b","x").loc[0,"experiment_id"]==build_experiment_designs(b,"a","b","x").loc[0,"experiment_id"]


def test_low_adequacy_increases_minimum_sample():
    low=build_experiment_designs(pd.DataFrame([candidate(adequacy=0.1)]),"a","b","x")
    high=build_experiment_designs(pd.DataFrame([candidate(adequacy=0.9)]),"a","b","x")
    assert low.loc[0,"minimum_observations"] > high.loc[0,"minimum_observations"]


def test_replication_requires_sole_veto_target():
    out=build_experiment_designs(pd.DataFrame([candidate()]),"a","b","x")
    assert out.loc[0,"minimum_sole_veto_observations"]==30
    assert "do not stop" in out.loc[0,"stopping_rule"].lower()


def test_threshold_protocol_has_bounded_arms():
    out=build_experiment_designs(pd.DataFrame([candidate("THRESHOLD_SENSITIVITY")]),"a","b","x")
    assert "bounded tightening" in out.loc[0,"treatment_arms"]
    assert "production optimisation" in out.loc[0,"objective"]


def test_design_validation_preserves_identity():
    frame=pd.DataFrame([candidate()])
    out=build_experiment_designs(frame,"a","b","x")
    assert validate_designs(out,{"CS03-ABC"}).passed


def test_manifest_preserves_experiment_identity():
    out=build_experiment_designs(pd.DataFrame([candidate()]),"a","b","x")
    manifest=build_manifest(out)
    assert manifest.loc[0,"experiment_id"]==out.loc[0,"experiment_id"]


def test_run_writes_outputs(tmp_path: Path):
    c=tmp_path/"c.csv"; a=tmp_path/"a.json"; out=tmp_path/"out"
    pd.DataFrame([candidate()]).to_csv(c,index=False)
    a.write_text(json.dumps(audit()),encoding="utf-8")
    result=run_experiment_design(c,a,out)
    assert result.paths.designs_csv.exists()
    assert result.paths.execution_manifest_csv.exists()
    assert result.paths.report_txt.exists()
    assert result.paths.audit_json.exists()
    payload=json.loads(result.paths.audit_json.read_text())
    assert payload["policy"]["production_changes_authorised"] is False


def test_run_rejects_invalid_source(tmp_path: Path):
    c=tmp_path/"c.csv"; a=tmp_path/"a.json"
    pd.DataFrame([candidate()]).drop(columns=["candidate_id"]).to_csv(c,index=False)
    a.write_text(json.dumps(audit()),encoding="utf-8")
    with pytest.raises(ExperimentDesignError):
        run_experiment_design(c,a,tmp_path/"out")
