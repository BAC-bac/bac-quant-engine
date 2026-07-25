from __future__ import annotations
import argparse,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
import pandas as pd
from bacqe.convex_survival.experiment_evidence import ENGINE_VERSION,ExperimentEvidenceError,merge_evidence,run_experiment_evidence
IN=Path(r'E:\Quant_Lab\data\analysis\convex_survival\experiment_execution'); OUT=Path(r'E:\Quant_Lab\data\analysis\convex_survival\experiment_evidence')
def self_tests():
 r={'ledger_rank':1,'ledger_id':'CS06-LED-X','registry_id':'CS05-X','experiment_id':'CS04-X','candidate_id':'CS03-X','experiment_title':'Replication Expansion: ADX','experiment_family':'REPLICATION_EXPANSION','primary_filter':'ADX','execution_status':'AWAITING_APPROVAL','execution_authorised':False,'protocol_revision':1,'protocol_fingerprint':'a'*64,'observations_collected':0,'sole_veto_observations_collected':0,'minimum_observations':1000,'minimum_sole_veto_observations':30,'maximum_calendar_days':60,'elapsed_calendar_days':0,'observation_progress':0.0,'sole_veto_progress':0.0,'evidence_target_reached':False,'calendar_limit_reached':False,'guardrail_breach':False,'data_quality_passed':True,'interim_review_due':False,'stopping_rule_triggered':False,'stop_reason':'','current_evidence_state':'NO_EVIDENCE','priority_score':.1,'priority_band':'REPLICATION_REQUIRED','principal_risk':'Sparse','success_criteria':'Stable','failure_criteria':'Unstable','inconclusive_criteria':'Insufficient','risk_guardrails':'No live','source_registry_sha256':'b'*64,'source_audit_sha256':'c'*64,'generated_utc':'2026-01-01T00:00:00+00:00','engine_version':'1.0.0'}
 a,h=merge_evidence(pd.DataFrame([r]),None,'d'*64,'e'*64,'2026-01-02T00:00:00+00:00'); b,h2=merge_evidence(pd.DataFrame([r]),a,'d'*64,'e'*64,'2026-01-03T00:00:00+00:00'); assert a.loc[0,'evidence_state']=='NO_EVIDENCE' and len(h)==1 and h2.empty and a.loc[0,'evidence_id']==b.loc[0,'evidence_id']
def args():
 p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,default=IN/'experiment_execution_ledger_latest.csv');p.add_argument('--audit',type=Path,default=IN/'experiment_execution_ledger_audit_latest.json');p.add_argument('--output-dir',type=Path,default=OUT);p.add_argument('--existing-evidence',type=Path,default=None);p.add_argument('--existing-history',type=Path,default=None);return p.parse_args()
def main():
 a=args();print('='*100);print('BACQE CONVEX SURVIVAL CS07 - EXPERIMENT EVIDENCE ENGINE');print('='*100);print(f'Engine version:             {ENGINE_VERSION}');print('Running deterministic self-tests.')
 try:self_tests();print('Self-tests passed.');print(f'Ledger input:               {a.ledger}');print(f'Source audit:               {a.audit}');print(f'Output directory:           {a.output_dir}');print('-'*100);o=run_experiment_evidence(a.ledger,a.audit,a.output_dir,a.existing_evidence,a.existing_history)
 except (ExperimentEvidenceError,FileNotFoundError,ValueError) as e:print('ENGINE RESULT:              FAIL');print(f'Reason:                     {e}');print('='*100);return 1
 print('ENGINE RESULT:              PASS');print(f'Evidence assessments:       {len(o.evidence)}');print(f'Evidence history rows:      {len(o.history)}');print(f'Review queue rows:          {len(o.review_queue)}');print(f'Evidence CSV:               {o.paths.evidence_csv}');print(f'Evidence history CSV:       {o.paths.history_csv}');print(f'Review queue CSV:           {o.paths.review_queue_csv}');print(f'Evidence report:            {o.paths.report_txt}');print(f'Audit JSON:                 {o.paths.audit_json}')
 if o.validation.warnings:
  print('Warnings:');[print(f'  - {w}') for w in o.validation.warnings]
 print('='*100);return 0
if __name__=='__main__':raise SystemExit(main())
