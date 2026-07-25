from __future__ import annotations
import json
from pathlib import Path
import pandas as pd, pytest
from bacqe.convex_survival.experiment_evidence import *
def row(auth=False,status='AWAITING_APPROVAL',obs=0,sole=0,target=False,guard=False,dq=True,stop=False,family='REPLICATION_EXPANSION'):
 return {'ledger_rank':1,'ledger_id':'CS06-LED-X','registry_id':'CS05-X','experiment_id':'CS04-X','candidate_id':'CS03-X','experiment_title':'Test','experiment_family':family,'primary_filter':'ADX','execution_status':status,'execution_authorised':auth,'protocol_revision':1,'protocol_fingerprint':'a'*64,'observations_collected':obs,'sole_veto_observations_collected':sole,'minimum_observations':1000,'minimum_sole_veto_observations':30,'maximum_calendar_days':60,'elapsed_calendar_days':1,'observation_progress':min(obs/1000,1),'sole_veto_progress':min(sole/30,1),'evidence_target_reached':target,'calendar_limit_reached':False,'guardrail_breach':guard,'data_quality_passed':dq,'interim_review_due':False,'stopping_rule_triggered':stop,'stop_reason':'GUARDRAIL_BREACH' if stop else '','current_evidence_state':'NO_EVIDENCE','priority_score':.1,'priority_band':'REPLICATION_REQUIRED' if family=='REPLICATION_EXPANSION' else 'HIGH','principal_risk':'risk','success_criteria':'success','failure_criteria':'failure','inconclusive_criteria':'inc','risk_guardrails':'guard','source_registry_sha256':'b'*64,'source_audit_sha256':'c'*64,'generated_utc':'2026-01-01T00:00:00+00:00','engine_version':'1.0.0'}
def audit(): return {'engine_id':'CS06','engine_name':'Experiment Execution Ledger','engine_version':'1.0.0','generated_utc':'x','validation':{'passed':True},'policy':{'ledger_is_observational_not_approving':True,'stopping_trigger_is_not_scientific_conclusion':True}}
def assess(r): return merge_evidence(pd.DataFrame([r]),None,'d','e','x')[0].iloc[0]
def test_validate_ok(): assert validate_inputs(pd.DataFrame([row()]),audit()).passed
def test_wrong_audit():
 a=audit();a['engine_id']='CS05';assert not validate_inputs(pd.DataFrame([row()]),a).passed
def test_no_authority_no_evidence(): assert assess(row()).evidence_state=='NO_EVIDENCE'
def test_authorised_zero_requires_execution(): assert assess(row(True,'READY')).scientific_recommendation=='REQUIRES_EXECUTION'
def test_low_progress_insufficient(): assert assess(row(True,'RUNNING',200,10)).evidence_state=='INSUFFICIENT'
def test_emerging_progress(): assert assess(row(True,'RUNNING',500,15)).evidence_state=='EMERGING'
def test_moderate_progress(): assert assess(row(True,'RUNNING',800,25)).evidence_state=='MODERATE'
def test_target_replication_review(): assert assess(row(True,'COMPLETED',1000,30,True)).scientific_recommendation=='READY_FOR_SCIENTIFIC_REVIEW'
def test_target_nonreplication_requests_replication(): assert assess(row(True,'COMPLETED',1000,30,True,family='THRESHOLD_SENSITIVITY')).scientific_recommendation=='READY_FOR_REPLICATION'
def test_guardrail_invalidates(): assert assess(row(True,'RUNNING',200,5,guard=True)).evidence_state=='INVALIDATED'
def test_stop_before_target_redesign(): assert assess(row(True,'STOPPED',200,5,stop=True)).scientific_recommendation=='STOP_AND_REDESIGN'
def test_id_stable(): assert assess(row()).evidence_id==assess(row()).evidence_id
def test_idempotent_no_event():
 f,_=merge_evidence(pd.DataFrame([row()]),None,'d','e','x');_,h=merge_evidence(pd.DataFrame([row()]),f,'d','e','y');assert h.empty
def test_progress_change_revision():
 f,_=merge_evidence(pd.DataFrame([row(True,'RUNNING',100,3)]),None,'d','e','x');s,h=merge_evidence(pd.DataFrame([row(True,'RUNNING',500,15)]),f,'d','e','y');assert s.iloc[0].assessment_revision==2 and len(h)==1
def test_validation_forbids_conclusion():
 e,_=merge_evidence(pd.DataFrame([row()]),None,'d','e','x');e.loc[0,'final_conclusion_authorised']=True;assert not validate_evidence(e,pd.DataFrame([row()])).passed
def test_run_idempotent(tmp_path:Path):
 lp=tmp_path/'l.csv';ap=tmp_path/'a.json';pd.DataFrame([row()]).to_csv(lp,index=False);ap.write_text(json.dumps(audit()));o1=run_experiment_evidence(lp,ap,tmp_path/'o');o2=run_experiment_evidence(lp,ap,tmp_path/'o');assert len(o2.history)==1 and json.loads(o2.paths.audit_json.read_text())['new_history_event_count']==0
def test_run_invalid(tmp_path:Path):
 lp=tmp_path/'l.csv';ap=tmp_path/'a.json';pd.DataFrame([row()]).drop(columns=['ledger_id']).to_csv(lp,index=False);ap.write_text(json.dumps(audit()));
 with pytest.raises(ExperimentEvidenceError): run_experiment_evidence(lp,ap,tmp_path/'o')
