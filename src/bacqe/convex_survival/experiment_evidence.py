from __future__ import annotations
import hashlib, json, os, tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import pandas as pd

ENGINE_ID='CS07'; ENGINE_NAME='Experiment Evidence Engine'; ENGINE_VERSION='1.0.0'; SCHEMA_VERSION='1.0.0'; SOURCE_ENGINE_ID='CS06'
REQUIRED_LEDGER_COLUMNS=(
'ledger_rank','ledger_id','registry_id','experiment_id','candidate_id','experiment_title','experiment_family','primary_filter',
'execution_status','execution_authorised','protocol_revision','protocol_fingerprint','observations_collected','sole_veto_observations_collected',
'minimum_observations','minimum_sole_veto_observations','maximum_calendar_days','elapsed_calendar_days','observation_progress','sole_veto_progress',
'evidence_target_reached','calendar_limit_reached','guardrail_breach','data_quality_passed','interim_review_due','stopping_rule_triggered','stop_reason',
'current_evidence_state','priority_score','priority_band','principal_risk','success_criteria','failure_criteria','inconclusive_criteria','risk_guardrails',
'source_registry_sha256','source_audit_sha256','generated_utc','engine_version')
VALID_STATES={'NO_EVIDENCE','INSUFFICIENT','EMERGING','MODERATE','STRONG','REPLICATION_REQUIRED','CONFLICTING','INVALIDATED'}
VALID_TRENDS={'UNKNOWN','IMPROVING','STABLE','WEAKENING','OSCILLATING'}
VALID_RECOMMENDATIONS={'AWAIT_APPROVAL','REQUIRES_EXECUTION','REQUIRES_MORE_DATA','CONTINUE_AND_MONITOR','READY_FOR_INTERIM_REVIEW','READY_FOR_SCIENTIFIC_REVIEW','READY_FOR_REPLICATION','INVALIDATE_AND_REVIEW','STOP_AND_REDESIGN'}
EVIDENCE_COLUMNS=(
'evidence_rank','evidence_id','ledger_id','registry_id','experiment_id','candidate_id','experiment_title','experiment_family','primary_filter',
'execution_status','execution_authorised','protocol_revision','assessment_revision','evidence_state','evidence_trend','scientific_recommendation',
'review_priority','review_reason','observations_collected','minimum_observations','observation_progress','sole_veto_observations_collected',
'minimum_sole_veto_observations','sole_veto_progress','combined_progress','evidence_target_reached','elapsed_calendar_days','maximum_calendar_days',
'calendar_progress','data_quality_passed','guardrail_breach','interim_review_due','stopping_rule_triggered','stop_reason','replication_required',
'final_conclusion_authorised','production_changes_authorised','principal_risk','success_criteria','failure_criteria','inconclusive_criteria','risk_guardrails',
'first_assessed_utc','last_assessed_utc','source_engine_id','source_engine_version','source_ledger_sha256','source_audit_sha256','source_generated_utc','generated_utc','engine_version')
HISTORY_COLUMNS=('history_id','evidence_id','ledger_id','experiment_id','event_utc','event_type','from_evidence_state','to_evidence_state','from_recommendation','to_recommendation','observations_collected','sole_veto_observations_collected','assessment_revision','event_reason','source_ledger_sha256','engine_version')
REVIEW_COLUMNS=('review_rank','evidence_id','ledger_id','experiment_id','experiment_title','primary_filter','evidence_state','evidence_trend','scientific_recommendation','review_priority','review_reason','combined_progress','observations_collected','minimum_observations','sole_veto_observations_collected','minimum_sole_veto_observations','execution_status','stopping_rule_triggered','guardrail_breach','data_quality_passed')

class ExperimentEvidenceError(ValueError): pass
@dataclass(frozen=True)
class ValidationResult: passed: bool; errors: tuple[str,...]; warnings: tuple[str,...]
@dataclass(frozen=True)
class SourceLineage: engine_id:str; engine_name:str; engine_version:str; generated_utc:str; ledger_sha256:str; audit_sha256:str
@dataclass(frozen=True)
class OutputPaths: output_directory:Path; evidence_csv:Path; history_csv:Path; review_queue_csv:Path; report_txt:Path; audit_json:Path
@dataclass(frozen=True)
class EvidenceOutputs: evidence:pd.DataFrame; history:pd.DataFrame; review_queue:pd.DataFrame; validation:ValidationResult; source_lineage:SourceLineage; paths:OutputPaths

def _req(f:pd.DataFrame, cols:Iterable[str], label:str): return [f'{label} missing required column: {c}' for c in cols if c not in f.columns]
def _bool(v:Any)->bool:
    if isinstance(v,bool): return v
    return str(v).strip().lower() in {'true','1','yes','y'}
def _sha(path:Path)->str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for c in iter(lambda:f.read(1048576),b''): h.update(c)
    return h.hexdigest()
def _eid(ledger_id:str)->str: return 'CS07-EVD-'+hashlib.sha256(f'{ledger_id}|CS07'.encode()).hexdigest()[:12].upper()
def _hid(eid:str,event:str,utc:str,rev:int)->str: return 'CS07-HIST-'+hashlib.sha256(f'{eid}|{event}|{utc}|{rev}'.encode()).hexdigest()[:16].upper()
def _num(v,default=0.0):
    try:
        x=float(v); return default if pd.isna(x) else x
    except: return default

def validate_inputs(ledger:pd.DataFrame,audit:dict[str,Any])->ValidationResult:
    errors=_req(ledger,REQUIRED_LEDGER_COLUMNS,'execution ledger'); warnings=[]
    if ledger.empty: errors.append('execution ledger contains no rows')
    if not errors:
        if ledger.ledger_id.duplicated().any(): errors.append('ledger_id values must be unique')
        if ledger.experiment_id.duplicated().any(): errors.append('experiment_id values must be unique')
        if (pd.to_numeric(ledger.observations_collected,errors='coerce')<0).any(): errors.append('observations must be non-negative')
        if (~ledger.execution_authorised.map(_bool)).any(): warnings.append('One or more experiments have no execution authority and therefore contain no assessable experimental evidence.')
    if audit.get('engine_id')!='CS06': errors.append('source audit must identify engine_id CS06')
    if not audit.get('validation',{}).get('passed',False): errors.append('source CS06 audit validation did not pass')
    pol=audit.get('policy',{})
    if pol.get('ledger_is_observational_not_approving') is not True: errors.append('CS06 audit must identify ledger as observational')
    if pol.get('stopping_trigger_is_not_scientific_conclusion') is not True: errors.append('CS06 audit must separate stopping triggers from scientific conclusions')
    return ValidationResult(not errors,tuple(errors),tuple(warnings))

def _assessment(row:pd.Series)->tuple[str,str,str,str,str,bool]:
    auth=_bool(row.execution_authorised); status=str(row.execution_status); obs=int(_num(row.observations_collected)); minobs=max(1,int(_num(row.minimum_observations,1)))
    sole=int(_num(row.sole_veto_observations_collected)); minsole=max(0,int(_num(row.minimum_sole_veto_observations)))
    op=min(obs/minobs,1.0); sp=1.0 if minsole==0 else min(sole/minsole,1.0); combined=min(op,sp)
    dq=_bool(row.data_quality_passed); guard=_bool(row.guardrail_breach); stop=_bool(row.stopping_rule_triggered); target=_bool(row.evidence_target_reached)
    replication=str(row.experiment_family)=='REPLICATION_EXPANSION' or str(row.priority_band)=='REPLICATION_REQUIRED'
    if not dq or guard:
        return 'INVALIDATED','WEAKENING','INVALIDATE_AND_REVIEW','CRITICAL','Guardrail or data-quality failure invalidates current evidence.',replication
    if not auth or status=='AWAITING_APPROVAL':
        return 'NO_EVIDENCE','UNKNOWN','AWAIT_APPROVAL','LOW','No governed execution authority has been granted.',replication
    if obs==0:
        return 'NO_EVIDENCE','UNKNOWN','REQUIRES_EXECUTION','MEDIUM','Authorised experiment has not yet accumulated observations.',replication
    if target:
        if replication:
            return 'STRONG','STABLE','READY_FOR_SCIENTIFIC_REVIEW','HIGH','Protocol evidence targets have been met; replication evidence is ready for governed review.',replication
        return 'STRONG','STABLE','READY_FOR_REPLICATION','HIGH','Primary evidence targets have been met; independent replication should precede production consideration.',replication
    if stop:
        return 'CONFLICTING','OSCILLATING','STOP_AND_REDESIGN','CRITICAL',f'Stopping rule triggered before complete evidence: {row.stop_reason}.',replication
    if combined>=0.75:
        return 'MODERATE','IMPROVING','READY_FOR_INTERIM_REVIEW','HIGH','At least 75% of the binding evidence target has accumulated.',replication
    if combined>=0.40:
        return 'EMERGING','IMPROVING','CONTINUE_AND_MONITOR','MEDIUM','Evidence is emerging but remains below protocol targets.',replication
    return 'INSUFFICIENT','UNKNOWN','REQUIRES_MORE_DATA','LOW','Evidence remains below 40% of the binding protocol target.',replication

def merge_evidence(ledger:pd.DataFrame,existing:pd.DataFrame|None,ledger_hash:str,audit_hash:str,utc:str)->tuple[pd.DataFrame,pd.DataFrame]:
    old={} if existing is None or existing.empty else {str(r.evidence_id):r.to_dict() for _,r in existing.iterrows()}; rows=[]; events=[]
    for _,r in ledger.iterrows():
        eid=_eid(str(r.ledger_id)); prior=old.get(eid); state,trend,rec,prio,reason,repl=_assessment(r)
        op=round(min(_num(r.observations_collected)/max(1,_num(r.minimum_observations,1)),1.0),6)
        sp=round(1.0 if int(_num(r.minimum_sole_veto_observations))==0 else min(_num(r.sole_veto_observations_collected)/max(1,_num(r.minimum_sole_veto_observations)),1.0),6)
        combined=round(min(op,sp),6); cal=round(min(_num(r.elapsed_calendar_days)/max(1,_num(r.maximum_calendar_days,1)),1.0),6)
        rev=1 if prior is None else int(_num(prior.get('assessment_revision'),1))
        changed=prior is None or state!=prior.get('evidence_state') or rec!=prior.get('scientific_recommendation') or int(_num(r.observations_collected))!=int(_num(prior.get('observations_collected')))
        if prior is not None and changed: rev+=1
        first=utc if prior is None else prior.get('first_assessed_utc',utc)
        row={
        'evidence_rank':int(r.ledger_rank),'evidence_id':eid,'ledger_id':r.ledger_id,'registry_id':r.registry_id,'experiment_id':r.experiment_id,'candidate_id':r.candidate_id,
        'experiment_title':r.experiment_title,'experiment_family':r.experiment_family,'primary_filter':r.primary_filter,'execution_status':r.execution_status,
        'execution_authorised':_bool(r.execution_authorised),'protocol_revision':int(r.protocol_revision),'assessment_revision':rev,'evidence_state':state,'evidence_trend':trend,
        'scientific_recommendation':rec,'review_priority':prio,'review_reason':reason,'observations_collected':int(_num(r.observations_collected)),'minimum_observations':int(_num(r.minimum_observations)),
        'observation_progress':op,'sole_veto_observations_collected':int(_num(r.sole_veto_observations_collected)),'minimum_sole_veto_observations':int(_num(r.minimum_sole_veto_observations)),
        'sole_veto_progress':sp,'combined_progress':combined,'evidence_target_reached':_bool(r.evidence_target_reached),'elapsed_calendar_days':int(_num(r.elapsed_calendar_days)),
        'maximum_calendar_days':int(_num(r.maximum_calendar_days)),'calendar_progress':cal,'data_quality_passed':_bool(r.data_quality_passed),'guardrail_breach':_bool(r.guardrail_breach),
        'interim_review_due':_bool(r.interim_review_due),'stopping_rule_triggered':_bool(r.stopping_rule_triggered),'stop_reason':'' if pd.isna(r.stop_reason) else str(r.stop_reason),
        'replication_required':repl,'final_conclusion_authorised':False,'production_changes_authorised':False,'principal_risk':r.principal_risk,'success_criteria':r.success_criteria,
        'failure_criteria':r.failure_criteria,'inconclusive_criteria':r.inconclusive_criteria,'risk_guardrails':r.risk_guardrails,'first_assessed_utc':first,
        'last_assessed_utc':utc if changed else (prior.get('last_assessed_utc',utc) if prior else utc),'source_engine_id':'CS06','source_engine_version':r.engine_version,
        'source_ledger_sha256':ledger_hash,'source_audit_sha256':audit_hash,'source_generated_utc':r.generated_utc,'generated_utc':utc,'engine_version':ENGINE_VERSION}
        rows.append(row)
        if changed:
            event='EVIDENCE_REGISTERED' if prior is None else 'EVIDENCE_ASSESSMENT_UPDATED'
            events.append({'history_id':_hid(eid,event,utc,rev),'evidence_id':eid,'ledger_id':r.ledger_id,'experiment_id':r.experiment_id,'event_utc':utc,'event_type':event,
            'from_evidence_state':'' if prior is None else prior.get('evidence_state',''),'to_evidence_state':state,'from_recommendation':'' if prior is None else prior.get('scientific_recommendation',''),
            'to_recommendation':rec,'observations_collected':int(_num(r.observations_collected)),'sole_veto_observations_collected':int(_num(r.sole_veto_observations_collected)),
            'assessment_revision':rev,'event_reason':reason,'source_ledger_sha256':ledger_hash,'engine_version':ENGINE_VERSION})
    out=pd.DataFrame(rows).sort_values(['evidence_rank','experiment_id']).reset_index(drop=True).loc[:,EVIDENCE_COLUMNS]
    hist=pd.DataFrame(events,columns=HISTORY_COLUMNS)
    return out,hist

def validate_evidence(e:pd.DataFrame,ledger:pd.DataFrame)->ValidationResult:
    errors=_req(e,EVIDENCE_COLUMNS,'experiment evidence')
    if not errors:
        if e.evidence_id.duplicated().any(): errors.append('evidence_id values must be unique')
        if set(e.ledger_id)!=set(ledger.ledger_id): errors.append('evidence ledger identity must match CS06')
        unknown=set(e.evidence_state)-VALID_STATES
        if unknown: errors.append('invalid evidence states: '+', '.join(sorted(unknown)))
        if e.final_conclusion_authorised.map(_bool).any(): errors.append('CS07 cannot authorise final scientific conclusions')
        if e.production_changes_authorised.map(_bool).any(): errors.append('CS07 cannot authorise production changes')
    return ValidationResult(not errors,tuple(errors),())

def build_review_queue(e:pd.DataFrame)->pd.DataFrame:
    q=e.copy(); order={'CRITICAL':0,'HIGH':1,'MEDIUM':2,'LOW':3}; q['_o']=q.review_priority.map(order).fillna(9)
    q=q.sort_values(['_o','combined_progress','evidence_rank'],ascending=[True,False,True]).drop(columns='_o').reset_index(drop=True)
    q.insert(0,'review_rank',range(1,len(q)+1)); return q.loc[:,REVIEW_COLUMNS]

def _write(path:Path,text:str):
    path.parent.mkdir(parents=True,exist_ok=True); fd,tmp=tempfile.mkstemp(dir=path.parent,prefix='.'+path.name+'.',text=True)
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='') as f:f.write(text)
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
def _report(e,q,v,utc):
    L=['='*100,'BACQE CONVEX SURVIVAL CS07 - EXPERIMENT EVIDENCE ENGINE','='*100,f'Generated UTC:              {utc}',f'Engine version:             {ENGINE_VERSION}','Source engine:              CS06',f'Evidence assessments:       {len(e)}',f'Validation:                 {"PASS" if v.passed else "FAIL"}','','INSTITUTIONAL PURPOSE','-'*100,'CS07 assesses the maturity and review readiness of evidence accumulated under governed experiments.','It does not determine final scientific conclusions, approve experiments or authorise production changes.','','EVIDENCE STATE COUNTS','-'*100]
    for k,n in e.evidence_state.value_counts().sort_index().items():L.append(f'{k:<40} {n}')
    L+=['','RECOMMENDATION COUNTS','-'*100]
    for k,n in e.scientific_recommendation.value_counts().sort_index().items():L.append(f'{k:<40} {n}')
    L+=['','EVIDENCE ASSESSMENTS','-'*100]
    for _,r in e.head(10).iterrows():L += [f"{int(r.evidence_rank):2d}. {r.experiment_title}",f"    Evidence ID:            {r.evidence_id}",f"    Evidence state:         {r.evidence_state}",f"    Recommendation:         {r.scientific_recommendation}",f"    Progress:               {r.combined_progress:.1%}",f"    Review priority:        {r.review_priority}",'']
    if v.warnings:L+=['VALIDATION WARNINGS','-'*100]+[f'- {x}' for x in v.warnings]+['']
    L+=['GOVERNANCE INTERPRETATION','-'*100,'NO_EVIDENCE means no governed observations are available for scientific assessment.','Evidence strength reflects protocol progress and integrity, not proof of profitability or permission to trade live.','READY_FOR_SCIENTIFIC_REVIEW is a recommendation for human review, not a final conclusion.','='*100]
    return '\n'.join(L)+'\n'

def run_experiment_evidence(ledger_path:Path,audit_path:Path,output_dir:Path,existing_evidence_path:Path|None=None,existing_history_path:Path|None=None)->EvidenceOutputs:
    ledger_path,audit_path,output_dir=Path(ledger_path),Path(audit_path),Path(output_dir)
    if not ledger_path.exists() or not audit_path.exists(): raise FileNotFoundError('CS07 source ledger or audit does not exist')
    ledger=pd.read_csv(ledger_path); audit=json.loads(audit_path.read_text(encoding='utf-8')); val=validate_inputs(ledger,audit)
    if not val.passed: raise ExperimentEvidenceError('; '.join(val.errors))
    output_dir.mkdir(parents=True,exist_ok=True); ep=output_dir/'experiment_evidence_latest.csv'; hp=output_dir/'experiment_evidence_history.csv'; qp=output_dir/'experiment_evidence_review_queue.csv'; rp=output_dir/'experiment_evidence_report_latest.txt'; ap=output_dir/'experiment_evidence_audit_latest.json'
    existing_evidence_path=existing_evidence_path or (ep if ep.exists() else None); existing_history_path=existing_history_path or (hp if hp.exists() else None)
    existing=pd.read_csv(existing_evidence_path) if existing_evidence_path and Path(existing_evidence_path).exists() else None
    oldhist=pd.read_csv(existing_history_path) if existing_history_path and Path(existing_history_path).exists() else pd.DataFrame(columns=HISTORY_COLUMNS)
    utc=datetime.now(timezone.utc).isoformat(); lh,ah=_sha(ledger_path),_sha(audit_path); evidence,new=merge_evidence(ledger,existing,lh,ah,utc); ev=validate_evidence(evidence,ledger)
    if not ev.passed: raise ExperimentEvidenceError('; '.join(ev.errors))
    hist=pd.concat([oldhist,new],ignore_index=True).drop_duplicates('history_id',keep='first') if not new.empty else oldhist.copy(); hist=hist.loc[:,HISTORY_COLUMNS]
    queue=build_review_queue(evidence); combined=ValidationResult(True,(),val.warnings); paths=OutputPaths(output_dir,ep,hp,qp,rp,ap)
    _write(ep,evidence.to_csv(index=False)); _write(hp,hist.to_csv(index=False)); _write(qp,queue.to_csv(index=False)); _write(rp,_report(evidence,queue,combined,utc))
    lineage=SourceLineage('CS06',audit.get('engine_name','Experiment Execution Ledger'),audit.get('engine_version',''),audit.get('generated_utc',''),lh,ah)
    payload={'engine_id':ENGINE_ID,'engine_name':ENGINE_NAME,'engine_version':ENGINE_VERSION,'schema_version':SCHEMA_VERSION,'generated_utc':utc,'evidence_count':len(evidence),'history_count':len(hist),'new_history_event_count':len(new),'review_queue_count':len(queue),'evidence_state_counts':evidence.evidence_state.value_counts().sort_index().to_dict(),'recommendation_counts':evidence.scientific_recommendation.value_counts().sort_index().to_dict(),'review_priority_counts':evidence.review_priority.value_counts().sort_index().to_dict(),'policy':{'evidence_assessment_is_recommendatory':True,'final_scientific_conclusion_requires_human_review':True,'history_is_append_only':True,'cross_symbol_replication_required_before_production':True,'production_changes_authorised':False},'source_lineage':asdict(lineage),'validation':asdict(combined),'output_paths':{k:str(v) for k,v in asdict(paths).items()}}
    _write(ap,json.dumps(payload,indent=2,sort_keys=True)+'\n'); return EvidenceOutputs(evidence,hist,queue,combined,lineage,paths)
