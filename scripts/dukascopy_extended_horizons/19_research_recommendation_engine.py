from __future__ import annotations

import argparse
import hashlib
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

VERSION = "1.0.0"
SCHEMA_VERSION = "1.0"
WIDTH = 116
ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
CENSUS_REL = Path("candidate_census/candidate_family_registry_latest.csv")
EVOLUTION_REL = Path("evolution_analytics/edge_family_evolution_analytics_latest.csv")
ATTRIBUTION_REL = Path("family_attribution/family_attribution_latest.csv")
OUTPUT_REL = Path("research_recommendations")
IDENTITY = ["target", "feature_family", "threshold_side", "context_type", "parent_context"]

OUTPUT_COLUMNS = [
    "recommendation_schema_version", "engine_version", "generated_utc",
    "recommendation_id", "analysis_mode", "research_rank", "edge_family_id",
    *IDENTITY, "recommended_action", "priority_band", "research_priority_score",
    "evidence_strength_score", "opportunity_score", "validation_need_score",
    "diversification_score", "longitudinal_score", "risk_penalty",
    "family_member_count", "priority_member_count", "tier_1_count", "tier_2_count",
    "tier_3_count", "reject_count", "symbol_count", "context_count",
    "candidate_layer_count", "family_independence_score", "transfer_success_rate",
    "year_stable_rate", "median_candidate_score", "median_confidence_score",
    "family_concentration_risk", "family_population_class", "is_orphan_family",
    "evolution_class", "attribution_confidence", "primary_driver",
    "net_attribution_score", "recommended_research_status", "recommended_next_step",
    "recommendation_reason", "supporting_evidence", "evidence_limit",
]
THEME_COLUMNS = [
    "theme_rank", "research_theme", "family_count", "mean_priority_score",
    "max_priority_score", "top_family_id", "dominant_action",
    "dominant_priority_band", "theme_reason",
]
SUMMARY_COLUMNS = ["metric", "value", "interpretation"]

ALIASES = {
    "member_count": ["member_count", "family_member_count"],
    "priority_member_count": ["priority_member_count"],
    "tier_1_count": ["tier_1_count"], "tier_2_count": ["tier_2_count"],
    "tier_3_count": ["tier_3_count"], "reject_count": ["reject_count"],
    "symbol_count": ["symbol_count"], "context_count": ["context_count"],
    "candidate_layer_count": ["candidate_layer_count"],
    "family_independence_score": ["family_independence_score", "independence_score"],
    "transfer_success_rate": ["transfer_success_rate"],
    "year_stable_rate": ["year_stable_rate"],
    "median_candidate_score": ["median_candidate_score", "candidate_score_median"],
    "median_confidence_score": ["median_confidence_score", "confidence_score_median"],
    "family_concentration_risk": ["family_concentration_risk", "concentration_risk"],
    "family_population_class": ["family_population_class", "population_class"],
    "is_orphan_family": ["is_orphan_family", "orphan_family"],
    "recommended_research_status": ["recommended_research_status", "research_status"],
    "recommended_next_step": ["recommended_next_step", "next_step"],
}
NUMERIC = {
    "member_count": 0.0, "priority_member_count": 0.0, "tier_1_count": 0.0,
    "tier_2_count": 0.0, "tier_3_count": 0.0, "reject_count": 0.0,
    "symbol_count": 0.0, "context_count": 0.0, "candidate_layer_count": 0.0,
    "family_independence_score": 50.0, "transfer_success_rate": 0.0,
    "year_stable_rate": 0.0, "median_candidate_score": 0.0,
    "median_confidence_score": 0.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BACQE EH19 research recommendation engine")
    parser.add_argument("--analysis-root", type=Path, default=ROOT)
    parser.add_argument("--census", type=Path)
    parser.add_argument("--evolution", type=Path)
    parser.add_argument("--attribution", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_id(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    return "rec_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def safe_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", suffix=".tmp") as handle:
        temp = Path(handle.name)
        handle.write(text)
    temp.replace(path)


def atomic_csv(path: Path, frame: pd.DataFrame) -> None:
    atomic_text(path, frame.to_csv(index=False))


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def first_present(frame: pd.DataFrame, names: list[str]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def canonicalise_census(frame: pd.DataFrame) -> pd.DataFrame:
    if "edge_family_id" not in frame.columns:
        raise ValueError("EH14 census is missing edge_family_id.")
    result = frame.copy()
    for canonical, aliases in ALIASES.items():
        source = first_present(result, aliases)
        if source is None:
            result[canonical] = NUMERIC.get(canonical, False if canonical == "is_orphan_family" else "")
        elif source != canonical:
            result[canonical] = result[source]
    for column, default in NUMERIC.items():
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(default)
    result["is_orphan_family"] = result["is_orphan_family"].map(safe_bool)
    for column in IDENTITY:
        if column not in result.columns:
            result[column] = ""
    if result.duplicated("edge_family_id").any():
        raise ValueError("EH14 census contains duplicate edge_family_id rows.")
    return result


def read_optional(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        return pd.DataFrame()
    if "edge_family_id" not in frame.columns:
        raise ValueError(f"{label} is missing edge_family_id.")
    if frame.duplicated("edge_family_id").any():
        raise ValueError(f"{label} contains duplicate edge_family_id rows.")
    return frame


def read_inputs(census_path: Path, evolution_path: Path, attribution_path: Path):
    if not census_path.exists():
        raise FileNotFoundError(f"EH14 census not found: {census_path}")
    census = pd.read_csv(census_path, low_memory=False)
    if census.empty:
        raise ValueError(f"EH14 census is empty: {census_path}")
    return canonicalise_census(census), read_optional(evolution_path, "EH16 analytics"), read_optional(attribution_path, "EH18 attribution")


def minmax(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    valid = values.dropna()
    if valid.empty or abs(float(valid.max()) - float(valid.min())) < 1e-12:
        return pd.Series(0.5, index=series.index, dtype=float)
    return ((values - valid.min()) / (valid.max() - valid.min())).fillna(0.5).clip(0, 1)


def risk_value(value: object) -> float:
    return {"low": 0.15, "medium": 0.45, "high": 0.75, "very_high": 1.0, "very high": 1.0}.get(str(value).strip().lower(), 0.25)


def evolution_value(value: object) -> float:
    return {"EXPANDING": 1.0, "GROWING": 0.8, "NEW": 0.65, "STABLE": 0.45, "BASELINE": 0.35, "DECLINING": -0.45, "RETIRED": -1.0}.get(str(value).strip().upper(), 0.0)


def confidence_value(value: object) -> float:
    return {"high": 1.0, "medium": 0.65, "low": 0.35, "not_available": 0.0}.get(str(value).strip().lower(), 0.2)


def analysis_mode(evolution: pd.DataFrame, attribution: pd.DataFrame) -> str:
    statuses: list[str] = []
    for frame in (evolution, attribution):
        if not frame.empty and "analysis_status" in frame.columns:
            statuses.extend(str(value).lower() for value in frame["analysis_status"].dropna().unique())
    return "longitudinal" if "comparison_available" in statuses else "baseline"


def merge_evidence(census: pd.DataFrame, evolution: pd.DataFrame, attribution: pd.DataFrame) -> pd.DataFrame:
    result = census.copy()
    if not evolution.empty:
        columns = [column for column in ["edge_family_id", "evolution_class", "analysis_status"] if column in evolution.columns]
        result = result.merge(evolution[columns].rename(columns={"analysis_status": "evolution_analysis_status"}), on="edge_family_id", how="left")
    if not attribution.empty:
        columns = [column for column in ["edge_family_id", "attribution_confidence", "primary_driver", "net_attribution_score", "analysis_status"] if column in attribution.columns]
        result = result.merge(attribution[columns].rename(columns={"analysis_status": "attribution_analysis_status"}), on="edge_family_id", how="left")
    for column, default in {"evolution_class": "BASELINE", "attribution_confidence": "not_available", "primary_driver": "", "net_attribution_score": 0.0}.items():
        if column not in result.columns:
            result[column] = default
        result[column] = result[column].fillna(default)
    return result


def score_families(frame: pd.DataFrame, mode: str) -> pd.DataFrame:
    result = frame.copy()
    tier_quality = result.tier_1_count + 0.55 * result.tier_2_count + 0.15 * result.tier_3_count - 0.35 * result.reject_count
    result["_quality"] = minmax(tier_quality)
    result["_priority_density"] = (result.priority_member_count / result.member_count.replace(0, np.nan)).fillna(0).clip(0, 1)
    result["_candidate_quality"] = minmax(result.median_candidate_score)
    result["_confidence"] = minmax(result.median_confidence_score)
    result["_transfer"] = result.transfer_success_rate.clip(0, 1)
    result["_stability"] = result.year_stable_rate.clip(0, 1)
    result["_independence"] = (result.family_independence_score / 100).clip(0, 1)
    result["_symbol"] = minmax(result.symbol_count)
    result["_context"] = minmax(result.context_count)
    result["_layer"] = minmax(result.candidate_layer_count)
    result["_population"] = minmax(result.member_count)
    result["opportunity_score"] = 100 * (0.24*result._quality + 0.18*result._priority_density + 0.16*result._candidate_quality + 0.10*result._confidence + 0.12*result._transfer + 0.10*result._stability + 0.10*result._population)
    result["diversification_score"] = 100 * (0.40*result._independence + 0.25*result._symbol + 0.20*result._context + 0.15*result._layer)
    result["validation_need_score"] = 100 * (0.24*(1-result._population) + 0.22*(1-result._transfer) + 0.22*(1-result._stability) + 0.18*(1-result._confidence) + 0.14*result.is_orphan_family.astype(float))
    result["risk_penalty"] = 100 * (0.45*result.family_concentration_risk.map(risk_value) + 0.30*(result.reject_count/result.member_count.replace(0,np.nan)).fillna(0).clip(0,1) + 0.25*result.is_orphan_family.astype(float))
    if mode == "longitudinal":
        evolution = result.evolution_class.map(evolution_value)
        attribution = minmax(pd.to_numeric(result.net_attribution_score, errors="coerce").fillna(0))*2-1
        confidence = result.attribution_confidence.map(confidence_value)
        result["longitudinal_score"] = 100*(0.55*evolution + 0.30*attribution*confidence + 0.15*confidence)
        result["evidence_strength_score"] = 100*(0.55*(0.35*result._confidence + 0.35*result._transfer + 0.30*result._stability) + 0.45*confidence)
        result["research_priority_score"] = 0.36*result.opportunity_score + 0.20*result.diversification_score + 0.18*result.validation_need_score + 0.16*result.longitudinal_score + 0.10*result.evidence_strength_score - 0.24*result.risk_penalty
    else:
        result["longitudinal_score"] = 0.0
        result["evidence_strength_score"] = 100*(0.40*result._confidence + 0.30*result._transfer + 0.30*result._stability)
        result["research_priority_score"] = 0.42*result.opportunity_score + 0.22*result.diversification_score + 0.22*result.validation_need_score + 0.14*result.evidence_strength_score - 0.25*result.risk_penalty
    result["research_priority_score"] = result.research_priority_score.clip(0,100)
    return result


def action_for(row: pd.Series, mode: str) -> str:
    evolution = str(row.evolution_class).upper()
    if evolution == "RETIRED": return "ARCHIVE_OR_REVIEW"
    if evolution == "DECLINING" and row.risk_penalty >= 45: return "DIAGNOSE_DECLINE"
    if row.is_orphan_family: return "REPLICATE_OR_DISPROVE"
    if row.validation_need_score >= 62: return "VALIDATE"
    if row.opportunity_score >= 68 and row.diversification_score >= 58: return "EXPAND"
    if row.opportunity_score >= 60: return "DEEPEN"
    if row.risk_penalty >= 62: return "RISK_REVIEW"
    return "MONITOR_AND_VALIDATE" if mode == "baseline" else "MONITOR"


def priority_band(score: float) -> str:
    return "CRITICAL" if score >= 72 else "HIGH" if score >= 58 else "MEDIUM" if score >= 42 else "LOW"


def reason_for(row: pd.Series, action: str, mode: str) -> str:
    parts: list[str] = []
    if row.opportunity_score >= 65: parts.append("strong present-day opportunity evidence")
    if row.validation_need_score >= 60: parts.append("material validation gap")
    if row.diversification_score >= 60: parts.append("useful cross-symbol or contextual breadth")
    if row.risk_penalty >= 55: parts.append("elevated concentration or rejection risk")
    if row.is_orphan_family: parts.append("orphan-family status requires replication")
    if mode == "longitudinal":
        parts.append(f"{str(row.evolution_class).lower()} longitudinal state")
        if str(row.primary_driver).strip(): parts.append(f"leading observed driver: {row.primary_driver}")
    else:
        parts.append("baseline-only evidence; longitudinal confirmation pending")
    return action.replace("_", " ").title() + ": " + "; ".join(parts or ["continued observation warranted"]) + "."


def build_recommendations(census: pd.DataFrame, evolution: pd.DataFrame, attribution: pd.DataFrame):
    mode = analysis_mode(evolution, attribution)
    scored = score_families(merge_evidence(census, evolution, attribution), mode)
    generated = utc_iso()
    rows: list[dict[str, Any]] = []
    for _, row in scored.iterrows():
        action = action_for(row, mode)
        score = safe_float(row.research_priority_score)
        evidence = f"tier1={int(row.tier_1_count)}; priority_members={int(row.priority_member_count)}/{int(row.member_count)}; symbols={int(row.symbol_count)}; contexts={int(row.context_count)}; independence={row.family_independence_score:.2f}; transfer={row.transfer_success_rate:.3f}; year_stable={row.year_stable_rate:.3f}"
        if mode == "longitudinal":
            evidence += f"; evolution={row.evolution_class}"
            if str(row.primary_driver).strip(): evidence += f"; primary_driver={row.primary_driver}"
        rows.append({
            "recommendation_schema_version": SCHEMA_VERSION, "engine_version": VERSION,
            "generated_utc": generated, "recommendation_id": stable_id(mode,row.edge_family_id,action),
            "analysis_mode": mode, "research_rank": 0, "edge_family_id": row.edge_family_id,
            **{column: row.get(column, "") for column in IDENTITY},
            "recommended_action": action, "priority_band": priority_band(score),
            "research_priority_score": score, "evidence_strength_score": safe_float(row.evidence_strength_score),
            "opportunity_score": safe_float(row.opportunity_score), "validation_need_score": safe_float(row.validation_need_score),
            "diversification_score": safe_float(row.diversification_score), "longitudinal_score": safe_float(row.longitudinal_score),
            "risk_penalty": safe_float(row.risk_penalty), "family_member_count": safe_float(row.member_count),
            "priority_member_count": safe_float(row.priority_member_count), "tier_1_count": safe_float(row.tier_1_count),
            "tier_2_count": safe_float(row.tier_2_count), "tier_3_count": safe_float(row.tier_3_count),
            "reject_count": safe_float(row.reject_count), "symbol_count": safe_float(row.symbol_count),
            "context_count": safe_float(row.context_count), "candidate_layer_count": safe_float(row.candidate_layer_count),
            "family_independence_score": safe_float(row.family_independence_score),
            "transfer_success_rate": safe_float(row.transfer_success_rate), "year_stable_rate": safe_float(row.year_stable_rate),
            "median_candidate_score": safe_float(row.median_candidate_score), "median_confidence_score": safe_float(row.median_confidence_score),
            "family_concentration_risk": row.family_concentration_risk, "family_population_class": row.family_population_class,
            "is_orphan_family": safe_bool(row.is_orphan_family), "evolution_class": row.evolution_class,
            "attribution_confidence": row.attribution_confidence, "primary_driver": row.primary_driver,
            "net_attribution_score": safe_float(row.net_attribution_score), "recommended_research_status": row.recommended_research_status,
            "recommended_next_step": row.recommended_next_step, "recommendation_reason": reason_for(row,action,mode),
            "supporting_evidence": evidence,
            "evidence_limit": "Priority ranking is decision support, not proof of tradable edge. " + ("Longitudinal evidence included." if mode=="longitudinal" else "Only baseline census evidence is currently available."),
        })
    result = pd.DataFrame(rows, columns=OUTPUT_COLUMNS).sort_values(["research_priority_score","evidence_strength_score","edge_family_id"], ascending=[False,False,True], kind="mergesort").reset_index(drop=True)
    result["research_rank"] = np.arange(1,len(result)+1)
    return result, mode


def build_themes(queue: pd.DataFrame) -> pd.DataFrame:
    if queue.empty: return pd.DataFrame(columns=THEME_COLUMNS)
    work = queue.copy()
    work["research_theme"] = work.feature_family.fillna("").replace("","unknown") + " | " + work.context_type.fillna("").replace("","unknown")
    rows=[]
    for theme, group in work.groupby("research_theme", sort=False):
        ordered=group.sort_values(["research_priority_score","edge_family_id"],ascending=[False,True])
        top=ordered.iloc[0]
        rows.append({"theme_rank":0,"research_theme":theme,"family_count":len(group),"mean_priority_score":float(group.research_priority_score.mean()),"max_priority_score":float(group.research_priority_score.max()),"top_family_id":top.edge_family_id,"dominant_action":group.recommended_action.mode().iloc[0],"dominant_priority_band":group.priority_band.mode().iloc[0],"theme_reason":f"{len(group)} families; top score {top.research_priority_score:.2f}."})
    result=pd.DataFrame(rows,columns=THEME_COLUMNS).sort_values(["max_priority_score","mean_priority_score","research_theme"],ascending=[False,False,True]).reset_index(drop=True)
    result["theme_rank"]=np.arange(1,len(result)+1)
    return result


def build_summary(queue: pd.DataFrame, themes: pd.DataFrame, mode: str) -> pd.DataFrame:
    top=queue.iloc[0] if len(queue) else None
    rows=[("analysis_mode",mode,"Baseline or longitudinal recommendation mode."),("families_ranked",len(queue),"Families included in the queue."),("critical_priority_families",int((queue.priority_band=="CRITICAL").sum()),"Strongest near-term attention."),("high_priority_families",int((queue.priority_band=="HIGH").sum()),"High research priority."),("research_themes",len(themes),"Distinct themes."),("top_family_id","" if top is None else top.edge_family_id,"Highest-ranked family."),("top_recommended_action","" if top is None else top.recommended_action,"Action for top family."),("top_priority_score",0 if top is None else round(float(top.research_priority_score),6),"Top score.")]
    return pd.DataFrame(rows,columns=SUMMARY_COLUMNS)


def render_report(queue: pd.DataFrame, themes: pd.DataFrame, summary: pd.DataFrame, top_n: int, census: Path, evolution: Path, attribution: Path) -> str:
    sm=dict(zip(summary.metric,summary.value))
    lines=["="*WIDTH,"BACQE EH19 - RESEARCH RECOMMENDATION REPORT","="*WIDTH,f"Generated UTC:               {utc_iso()}",f"Engine version:              {VERSION}",f"EH14 census:                 {census}",f"EH16 evolution:              {evolution}",f"EH18 attribution:            {attribution}","-"*WIDTH,f"Analysis mode:               {sm.get('analysis_mode','')}",f"Families ranked:             {sm.get('families_ranked',0)}",f"Critical-priority families:  {sm.get('critical_priority_families',0)}",f"High-priority families:      {sm.get('high_priority_families',0)}","-"*WIDTH,"TOP RESEARCH QUEUE"]
    for _,row in queue.head(max(top_n,1)).iterrows():
        lines += [f"#{int(row.research_rank):02d} {row.edge_family_id} | {row.priority_band} | {row.recommended_action} | score={row.research_priority_score:.2f}",f"     {row.recommendation_reason}",f"     Evidence: {row.supporting_evidence}"]
    lines += ["-"*WIDTH,"SCIENTIFIC LIMIT",("The queue uses current census evidence only; longitudinal confirmation will activate after a second EH15 snapshot." if sm.get("analysis_mode")=="baseline" else "The queue includes longitudinal evidence but remains research prioritisation, not proof of tradable edge."),"="*WIDTH]
    return "\n".join(lines)+"\n"


def run_engine(census_path: Path, evolution_path: Path, attribution_path: Path, output_dir: Path, top_n: int=20):
    census,evolution,attribution=read_inputs(census_path,evolution_path,attribution_path)
    queue,mode=build_recommendations(census,evolution,attribution)
    themes=build_themes(queue); summary=build_summary(queue,themes,mode)
    output_dir.mkdir(parents=True,exist_ok=True)
    outputs={"queue":output_dir/"research_recommendation_queue_latest.csv","themes":output_dir/"research_theme_rankings_latest.csv","summary":output_dir/"research_recommendation_summary_latest.csv","report":output_dir/"research_recommendation_report_latest.txt","state":output_dir/"research_recommendation_state_latest.json"}
    atomic_csv(outputs["queue"],queue); atomic_csv(outputs["themes"],themes); atomic_csv(outputs["summary"],summary)
    report=render_report(queue,themes,summary,top_n,census_path,evolution_path,attribution_path); atomic_text(outputs["report"],report)
    top=queue.iloc[0] if len(queue) else None
    state={"engine_version":VERSION,"recommendation_schema_version":SCHEMA_VERSION,"generated_utc":utc_iso(),"analysis_mode":mode,"families_ranked":len(queue),"research_themes":len(themes),"top_family_id":"" if top is None else str(top.edge_family_id),"top_recommended_action":"" if top is None else str(top.recommended_action),"top_priority_score":0 if top is None else float(top.research_priority_score),"outputs":{k:str(v) for k,v in outputs.items()},"evidence_limit":"Research prioritisation only; not proof of live-trading suitability."}
    atomic_json(outputs["state"],state); print(report,end="")
    return {"recommendations":queue,"themes":themes,"summary":summary,"state":state,"outputs":outputs}


def synthetic_inputs():
    census=pd.DataFrame([
        dict(edge_family_id="STRONG",target="r20",feature_family="hour",threshold_side="upper",context_type="hour",parent_context="13",member_count=12,priority_member_count=8,tier_1_count=5,tier_2_count=3,tier_3_count=2,reject_count=1,symbol_count=5,context_count=3,candidate_layer_count=3,family_independence_score=82,transfer_success_rate=.82,year_stable_rate=.78,median_candidate_score=88,median_confidence_score=75,family_concentration_risk="low",family_population_class="established",is_orphan_family=False,recommended_research_status="priority",recommended_next_step="expand"),
        dict(edge_family_id="ORPHAN",target="r10",feature_family="spread",threshold_side="lower",context_type="session",parent_context="asia",member_count=1,priority_member_count=1,tier_1_count=1,tier_2_count=0,tier_3_count=0,reject_count=0,symbol_count=1,context_count=1,candidate_layer_count=1,family_independence_score=90,transfer_success_rate=0,year_stable_rate=0,median_candidate_score=95,median_confidence_score=40,family_concentration_risk="high",family_population_class="orphan",is_orphan_family=True,recommended_research_status="investigate",recommended_next_step="replicate"),
        dict(edge_family_id="WEAK",target="r5",feature_family="volume",threshold_side="upper",context_type="day",parent_context="mon",member_count=5,priority_member_count=0,tier_1_count=0,tier_2_count=0,tier_3_count=1,reject_count=4,symbol_count=1,context_count=1,candidate_layer_count=1,family_independence_score=20,transfer_success_rate=.1,year_stable_rate=.1,median_candidate_score=30,median_confidence_score=20,family_concentration_risk="high",family_population_class="small",is_orphan_family=False,recommended_research_status="low",recommended_next_step="monitor")])
    evolution=pd.DataFrame([dict(edge_family_id="STRONG",analysis_status="comparison_available",evolution_class="EXPANDING"),dict(edge_family_id="ORPHAN",analysis_status="comparison_available",evolution_class="NEW"),dict(edge_family_id="WEAK",analysis_status="comparison_available",evolution_class="DECLINING")])
    attribution=pd.DataFrame([dict(edge_family_id="STRONG",analysis_status="comparison_available",attribution_confidence="high",primary_driver="priority_quality",net_attribution_score=2.5),dict(edge_family_id="ORPHAN",analysis_status="comparison_available",attribution_confidence="medium",primary_driver="population",net_attribution_score=.8),dict(edge_family_id="WEAK",analysis_status="comparison_available",attribution_confidence="high",primary_driver="rejection_pressure",net_attribution_score=-2)])
    return census,evolution,attribution


def self_test() -> int:
    checks=[]
    def check(name: str, condition: bool): checks.append((name,bool(condition)))
    with tempfile.TemporaryDirectory(prefix="bacqe_eh19_") as name:
        root=Path(name); census,evolution,attribution=synthetic_inputs(); cp=root/"census.csv"; ep=root/"evolution.csv"; ap=root/"attribution.csv"
        census.to_csv(cp,index=False); evolution.to_csv(ep,index=False); attribution.to_csv(ap,index=False)
        result=run_engine(cp,ep,ap,root/"out",10); queue=result["recommendations"].set_index("edge_family_id")
        check("longitudinal mode",result["state"]["analysis_mode"]=="longitudinal")
        check("three families ranked",len(queue)==3)
        check("strong family first",result["state"]["top_family_id"]=="STRONG")
        check("orphan replication action",queue.loc["ORPHAN","recommended_action"]=="REPLICATE_OR_DISPROVE")
        check("weak below strong",queue.loc["WEAK","research_priority_score"]<queue.loc["STRONG","research_priority_score"])
        check("longitudinal score active",queue.loc["STRONG","longitudinal_score"]>0)
        check("primary driver retained",queue.loc["STRONG","primary_driver"]=="priority_quality")
        check("output columns stable",list(result["recommendations"].columns)==OUTPUT_COLUMNS)
        check("themes produced",len(result["themes"])>0)
        check("report exists",result["outputs"]["report"].exists())
        check("state exists",result["outputs"]["state"].exists())
        base_e=evolution.copy(); base_e["analysis_status"]="insufficient_history"; base_e["evolution_class"]="BASELINE"
        base_a=attribution.copy(); base_a["analysis_status"]="insufficient_history"; base_a["attribution_confidence"]="not_available"; base_a["primary_driver"]=""; base_a["net_attribution_score"]=0
        bep=root/"base_e.csv"; bap=root/"base_a.csv"; base_e.to_csv(bep,index=False); base_a.to_csv(bap,index=False)
        baseline=run_engine(cp,bep,bap,root/"baseline",10)
        check("baseline mode",baseline["state"]["analysis_mode"]=="baseline")
        check("baseline ranks all",len(baseline["recommendations"])==3)
        check("baseline longitudinal zero",baseline["recommendations"].longitudinal_score.eq(0).all())
        absent=run_engine(cp,root/"missing_e.csv",root/"missing_a.csv",root/"absent",10)
        check("optional evidence may be absent",absent["state"]["analysis_mode"]=="baseline")
        duplicate=pd.concat([census,census.iloc[[0]]],ignore_index=True); dp=root/"duplicate.csv"; duplicate.to_csv(dp,index=False)
        try: run_engine(dp,ep,ap,root/"dup"); rejected=False
        except ValueError: rejected=True
        check("duplicate census rejected",rejected)
    passed=sum(ok for _,ok in checks)
    print("="*WIDTH); print("BACQE EH19 - SELF TEST"); print("="*WIDTH)
    for name,ok in checks: print(f"{'PASS' if ok else 'FAIL':<6} {name}")
    print("-"*WIDTH); print(f"Passed: {passed}/{len(checks)}"); print("="*WIDTH)
    return 0 if passed==len(checks) else 1


def main() -> int:
    arguments=parse_args()
    if arguments.self_test: return self_test()
    root=arguments.analysis_root.resolve()
    census=arguments.census.resolve() if arguments.census else root/CENSUS_REL
    evolution=arguments.evolution.resolve() if arguments.evolution else root/EVOLUTION_REL
    attribution=arguments.attribution.resolve() if arguments.attribution else root/ATTRIBUTION_REL
    output=arguments.output_dir.resolve() if arguments.output_dir else root/OUTPUT_REL
    try:
        run_engine(census,evolution,attribution,output,arguments.top_n)
        return 0
    except (FileNotFoundError,ValueError,OSError) as exc:
        print(f"EH19 ERROR: {exc}")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())