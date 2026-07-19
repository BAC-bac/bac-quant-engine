from __future__ import annotations

"""BACQE EH18 - Edge Family Attribution Engine.

Explains observed edge-family changes using EH15 history and EH16 evolution
analytics. The engine reports contribution and association, not unsupported
causality. With fewer than two distinct snapshots it emits a valid baseline
schema and an explicit insufficient-history status.
"""

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

ENGINE_VERSION = "1.0.0"
ATTRIBUTION_SCHEMA_VERSION = "1.0"
WIDTH = 112

ANALYSIS_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
INPUT_HISTORY = Path("evolution_memory") / "edge_family_history.csv"
INPUT_ANALYTICS = Path("evolution_analytics") / "edge_family_evolution_analytics_latest.csv"
OUTPUT_NAME = "family_attribution"

IDENTITY_COLUMNS = [
    "target", "feature_family", "threshold_side", "context_type", "parent_context"
]

CORE_DRIVER_SPECS = [
    ("member_count", "population", 1.00),
    ("priority_member_count", "priority_quality", 1.20),
    ("symbol_count", "symbol_breadth", 1.10),
    ("context_count", "context_breadth", 0.90),
    ("tier_1_count", "tier_1_quality", 1.30),
    ("tier_2_count", "tier_2_quality", 0.70),
    ("tier_3_count", "tier_3_population", 0.35),
    ("reject_count", "rejection_pressure", -0.55),
    ("distinct_candidate_count", "candidate_diversity", 0.80),
    ("candidate_layer_count", "layer_breadth", 0.65),
    ("total_trades", "sample_depth", 0.25),
    ("total_net_return", "aggregate_return", 0.35),
    ("median_candidate_score", "candidate_score", 0.85),
    ("max_candidate_score", "peak_candidate_score", 0.30),
    ("median_confidence_score", "confidence", 0.80),
    ("median_win_rate", "win_rate", 0.45),
    ("median_profit_factor", "profit_factor", 0.65),
    ("median_positive_year_rate", "temporal_positivity", 0.70),
    ("transfer_success_rate", "transferability", 1.00),
    ("year_stable_rate", "year_stability", 1.00),
    ("family_independence_score", "independence", 0.75),
]

SET_COLUMNS = [
    ("symbols_present", "symbol"),
    ("contexts_present", "context"),
    ("candidate_layers_present", "candidate_layer"),
]

REQUIRED_HISTORY_COLUMNS = {
    "snapshot_id", "observation_utc", "edge_family_id", "member_count",
    "priority_member_count", "symbol_count", "context_count",
}

REQUIRED_ANALYTICS_COLUMNS = {
    "analysis_status", "edge_family_id", "evolution_class",
    "previous_snapshot_id", "current_snapshot_id",
    "previous_member_count", "current_member_count", "member_count_delta",
    "previous_priority_member_count", "current_priority_member_count",
    "priority_member_count_delta",
}

ATTRIBUTION_COLUMNS = [
    "attribution_schema_version", "engine_version", "analysis_status",
    "comparison_id", "previous_snapshot_id", "current_snapshot_id",
    "previous_observation_utc", "current_observation_utc",
    "edge_family_id", *IDENTITY_COLUMNS, "evolution_class",
    "primary_driver", "primary_driver_direction", "primary_driver_delta",
    "primary_driver_contribution", "secondary_driver",
    "secondary_driver_direction", "secondary_driver_delta",
    "secondary_driver_contribution", "positive_driver_count",
    "negative_driver_count", "neutral_driver_count",
    "positive_contribution_total", "negative_contribution_total",
    "net_attribution_score", "attribution_confidence",
    "symbols_added", "symbols_removed", "contexts_added", "contexts_removed",
    "candidate_layers_added", "candidate_layers_removed",
    "attribution_explanation", "evidence_limit",
]

DRIVER_COLUMNS = [
    "attribution_schema_version", "engine_version", "analysis_status",
    "comparison_id", "edge_family_id", "evolution_class", "driver_rank",
    "metric_name", "driver_name", "driver_direction",
    "previous_value", "current_value", "delta", "relative_change",
    "normalised_magnitude", "driver_weight", "signed_contribution",
    "absolute_contribution", "evidence_type",
]

SUMMARY_COLUMNS = ["metric", "value", "interpretation"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Explain EH16 edge-family changes using EH15 longitudinal evidence."
    )
    parser.add_argument("--analysis-root", type=Path, default=ANALYSIS_ROOT)
    parser.add_argument("--history", type=Path)
    parser.add_argument("--analytics", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_id(prefix: str, *parts: object, length: int = 20) -> str:
    payload = "|".join("" if value is None else str(value) for value in parts)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:length]}"


def atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, dir=path.parent,
        newline="", encoding="utf-8"
    ) as handle:
        temp = Path(handle.name)
        frame.to_csv(handle, index=False)
    temp.replace(path)


def atomic_write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", suffix=".txt", delete=False, dir=path.parent, encoding="utf-8"
    ) as handle:
        temp = Path(handle.name)
        handle.write(text)
    temp.replace(path)


def atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    atomic_write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", path)


def safe_float(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if math.isfinite(result) else math.nan


def split_set(value: object) -> set[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    return {item.strip() for item in text.split(",") if item.strip()}


def read_inputs(history_path: Path, analytics_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not history_path.exists():
        raise FileNotFoundError(f"EH15 history not found: {history_path}")
    if not analytics_path.exists():
        raise FileNotFoundError(f"EH16 analytics not found: {analytics_path}")

    history = pd.read_csv(history_path, low_memory=False)
    analytics = pd.read_csv(analytics_path, low_memory=False)

    if history.empty:
        raise ValueError(f"EH15 history is empty: {history_path}")
    if analytics.empty:
        raise ValueError(f"EH16 analytics is empty: {analytics_path}")

    missing_history = sorted(REQUIRED_HISTORY_COLUMNS - set(history.columns))
    missing_analytics = sorted(REQUIRED_ANALYTICS_COLUMNS - set(analytics.columns))
    if missing_history:
        raise ValueError("EH15 history missing columns: " + ", ".join(missing_history))
    if missing_analytics:
        raise ValueError("EH16 analytics missing columns: " + ", ".join(missing_analytics))

    history["observation_utc"] = pd.to_datetime(
        history["observation_utc"], utc=True, errors="coerce"
    )
    if history["observation_utc"].isna().any():
        raise ValueError("EH15 history contains invalid observation_utc values.")

    if history.duplicated(["snapshot_id", "edge_family_id"]).any():
        raise ValueError("EH15 history contains duplicate snapshot/family observations.")

    return history, analytics


def latest_snapshot_rows(history: pd.DataFrame, snapshot_id: str) -> pd.DataFrame:
    return history.loc[history["snapshot_id"].astype(str).eq(str(snapshot_id))].copy()


def observation_lookup(history: pd.DataFrame, snapshot_id: str) -> dict[str, dict[str, Any]]:
    frame = latest_snapshot_rows(history, snapshot_id)
    return {
        str(row["edge_family_id"]): row.to_dict()
        for _, row in frame.iterrows()
    }


def robust_scale(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return 1.0
    median_abs = float(values.abs().median())
    q75 = float(values.abs().quantile(0.75))
    maximum = float(values.abs().max())
    scale = max(median_abs, q75, maximum * 0.10, 1e-12)
    return scale


def metric_scales(analytics: pd.DataFrame) -> dict[str, float]:
    scales: dict[str, float] = {}
    for metric, _, _ in CORE_DRIVER_SPECS:
        column = f"{metric}_delta"
        scales[metric] = robust_scale(analytics[column]) if column in analytics.columns else 1.0
    return scales


def signed_direction(value: float) -> str:
    if math.isnan(value) or abs(value) < 1e-15:
        return "neutral"
    return "positive" if value > 0 else "negative"


def contribution_for(
    metric: str,
    weight: float,
    delta: float,
    scale: float,
) -> tuple[float, float]:
    if math.isnan(delta):
        return 0.0, 0.0
    magnitude = min(abs(delta) / max(scale, 1e-12), 3.0)
    signed = math.copysign(magnitude * abs(weight), delta)
    if weight < 0:
        signed *= -1.0
    return magnitude, signed


def build_driver_rows(
    analytics: pd.DataFrame,
    scales: dict[str, float],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for _, family in analytics.iterrows():
        analysis_status = str(family.get("analysis_status", ""))
        comparison_id = family.get("comparison_id", "")
        family_id = str(family["edge_family_id"])
        evolution_class = str(family.get("evolution_class", ""))

        family_rows: list[dict[str, Any]] = []
        if analysis_status == "comparison_available":
            for metric, driver_name, weight in CORE_DRIVER_SPECS:
                previous = safe_float(family.get(f"previous_{metric}"))
                current = safe_float(family.get(f"current_{metric}"))
                delta = safe_float(family.get(f"{metric}_delta"))
                if math.isnan(delta):
                    continue

                relative_change = math.nan
                if not math.isnan(previous) and abs(previous) > 1e-12:
                    relative_change = delta / abs(previous)

                magnitude, contribution = contribution_for(
                    metric, weight, delta, scales.get(metric, 1.0)
                )
                family_rows.append({
                    "attribution_schema_version": ATTRIBUTION_SCHEMA_VERSION,
                    "engine_version": ENGINE_VERSION,
                    "analysis_status": analysis_status,
                    "comparison_id": comparison_id,
                    "edge_family_id": family_id,
                    "evolution_class": evolution_class,
                    "metric_name": metric,
                    "driver_name": driver_name,
                    "driver_direction": signed_direction(contribution),
                    "previous_value": previous,
                    "current_value": current,
                    "delta": delta,
                    "relative_change": relative_change,
                    "normalised_magnitude": magnitude,
                    "driver_weight": weight,
                    "signed_contribution": contribution,
                    "absolute_contribution": abs(contribution),
                    "evidence_type": "aggregate_metric_change",
                })

        family_rows.sort(
            key=lambda row: (-row["absolute_contribution"], row["driver_name"])
        )
        for rank, row in enumerate(family_rows, start=1):
            row["driver_rank"] = rank
            rows.append(row)

    return pd.DataFrame(rows, columns=DRIVER_COLUMNS)


def set_changes(
    previous_row: dict[str, Any] | None,
    current_row: dict[str, Any] | None,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for column, label in SET_COLUMNS:
        previous = split_set(previous_row.get(column)) if previous_row else set()
        current = split_set(current_row.get(column)) if current_row else set()
        result[f"{label}s_added"] = ", ".join(sorted(current - previous))
        result[f"{label}s_removed"] = ", ".join(sorted(previous - current))
    return result


def confidence_label(
    analysis_status: str,
    driver_rows: pd.DataFrame,
    evolution_class: str,
) -> str:
    if analysis_status != "comparison_available":
        return "not_available"
    if driver_rows.empty:
        return "low"
    active = driver_rows.loc[driver_rows["absolute_contribution"] > 1e-12]
    if active.empty:
        return "low"
    top = float(active.iloc[0]["absolute_contribution"])
    total = float(active["absolute_contribution"].sum())
    dominance = top / total if total > 0 else 0.0
    if evolution_class in {"NEW", "RETIRED"}:
        return "medium"
    if len(active) >= 3 and dominance >= 0.40:
        return "high"
    if len(active) >= 2:
        return "medium"
    return "low"


def driver_phrase(name: str, direction: str, delta: object) -> str:
    if not name:
        return "no measurable driver"
    value = safe_float(delta)
    delta_text = "n/a" if math.isnan(value) else f"{value:+.6g}"
    return f"{name} ({direction}, delta {delta_text})"


def explanation_for(
    evolution_class: str,
    primary: dict[str, Any] | None,
    secondary: dict[str, Any] | None,
    changes: dict[str, str],
    status: str,
) -> str:
    if status != "comparison_available":
        return (
            "Attribution is unavailable because at least two distinct EH15 snapshots "
            "are required."
        )

    primary_text = driver_phrase(
        str(primary.get("driver_name", "")) if primary else "",
        str(primary.get("driver_direction", "")) if primary else "",
        primary.get("delta") if primary else math.nan,
    )
    secondary_text = driver_phrase(
        str(secondary.get("driver_name", "")) if secondary else "",
        str(secondary.get("driver_direction", "")) if secondary else "",
        secondary.get("delta") if secondary else math.nan,
    )

    coverage_bits: list[str] = []
    for key, label in [
        ("symbols_added", "symbols added"),
        ("symbols_removed", "symbols removed"),
        ("contexts_added", "contexts added"),
        ("contexts_removed", "contexts removed"),
        ("candidate_layers_added", "candidate layers added"),
        ("candidate_layers_removed", "candidate layers removed"),
    ]:
        if changes.get(key):
            coverage_bits.append(f"{label}: {changes[key]}")

    sentence = (
        f"{evolution_class} is principally associated with {primary_text}"
    )
    if secondary:
        sentence += f", followed by {secondary_text}"
    sentence += "."
    if coverage_bits:
        sentence += " Coverage evidence — " + "; ".join(coverage_bits) + "."
    return sentence


def build_attribution(
    history: pd.DataFrame,
    analytics: pd.DataFrame,
    drivers: pd.DataFrame,
) -> pd.DataFrame:
    previous_ids = [
        str(value) for value in analytics["previous_snapshot_id"].dropna().unique()
        if str(value).strip()
    ]
    current_ids = [
        str(value) for value in analytics["current_snapshot_id"].dropna().unique()
        if str(value).strip()
    ]
    previous_id = previous_ids[0] if previous_ids else ""
    current_id = current_ids[0] if current_ids else ""

    previous_lookup = observation_lookup(history, previous_id) if previous_id else {}
    current_lookup = observation_lookup(history, current_id) if current_id else {}

    rows: list[dict[str, Any]] = []
    for _, family in analytics.iterrows():
        family_id = str(family["edge_family_id"])
        status = str(family.get("analysis_status", ""))
        evolution_class = str(family.get("evolution_class", ""))

        family_drivers = drivers.loc[
            drivers["edge_family_id"].astype(str).eq(family_id)
        ].sort_values(["driver_rank"])
        active = family_drivers.loc[family_drivers["absolute_contribution"] > 1e-12]
        primary = active.iloc[0].to_dict() if len(active) >= 1 else None
        secondary = active.iloc[1].to_dict() if len(active) >= 2 else None

        changes = set_changes(
            previous_lookup.get(family_id),
            current_lookup.get(family_id),
        )

        positive = family_drivers.loc[family_drivers["signed_contribution"] > 1e-12]
        negative = family_drivers.loc[family_drivers["signed_contribution"] < -1e-12]
        neutral = family_drivers.loc[
            family_drivers["signed_contribution"].abs() <= 1e-12
        ]
        positive_total = float(positive["signed_contribution"].sum()) if not positive.empty else 0.0
        negative_total = float(negative["signed_contribution"].sum()) if not negative.empty else 0.0
        net_score = positive_total + negative_total

        row: dict[str, Any] = {
            "attribution_schema_version": ATTRIBUTION_SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "analysis_status": status,
            "comparison_id": family.get("comparison_id", ""),
            "previous_snapshot_id": family.get("previous_snapshot_id", ""),
            "current_snapshot_id": family.get("current_snapshot_id", ""),
            "previous_observation_utc": family.get("previous_observation_utc", ""),
            "current_observation_utc": family.get("current_observation_utc", ""),
            "edge_family_id": family_id,
            "evolution_class": evolution_class,
            "primary_driver": primary.get("driver_name", "") if primary else "",
            "primary_driver_direction": primary.get("driver_direction", "") if primary else "",
            "primary_driver_delta": primary.get("delta", math.nan) if primary else math.nan,
            "primary_driver_contribution": primary.get("signed_contribution", 0.0) if primary else 0.0,
            "secondary_driver": secondary.get("driver_name", "") if secondary else "",
            "secondary_driver_direction": secondary.get("driver_direction", "") if secondary else "",
            "secondary_driver_delta": secondary.get("delta", math.nan) if secondary else math.nan,
            "secondary_driver_contribution": secondary.get("signed_contribution", 0.0) if secondary else 0.0,
            "positive_driver_count": int(len(positive)),
            "negative_driver_count": int(len(negative)),
            "neutral_driver_count": int(len(neutral)),
            "positive_contribution_total": positive_total,
            "negative_contribution_total": negative_total,
            "net_attribution_score": net_score,
            "attribution_confidence": confidence_label(status, family_drivers, evolution_class),
            **changes,
            "attribution_explanation": explanation_for(
                evolution_class, primary, secondary, changes, status
            ),
            "evidence_limit": (
                "Contribution-level attribution from aggregate snapshots; "
                "not proof of causal mechanism or exact member-level source counts."
            ),
        }
        for column in IDENTITY_COLUMNS:
            row[column] = family.get(column, "")
        rows.append(row)

    return pd.DataFrame(rows, columns=ATTRIBUTION_COLUMNS)


def build_summary(
    attribution: pd.DataFrame,
    history: pd.DataFrame,
    drivers: pd.DataFrame,
) -> pd.DataFrame:
    status = (
        str(attribution["analysis_status"].iloc[0])
        if not attribution.empty else "unknown"
    )
    snapshots = int(history["snapshot_id"].nunique())
    available = status == "comparison_available"
    active_drivers = int((drivers["absolute_contribution"] > 1e-12).sum()) if not drivers.empty else 0
    high_conf = int((attribution["attribution_confidence"] == "high").sum()) if not attribution.empty else 0

    rows = [
        ("analysis_status", status, "Whether change attribution can be calculated."),
        ("historical_snapshots", snapshots, "Distinct EH15 snapshots available."),
        ("families_attributed", len(attribution), "Families represented in the latest attribution output."),
        ("active_driver_records", active_drivers, "Non-zero metric driver contributions."),
        ("high_confidence_attributions", high_conf, "Families with concentrated multi-driver evidence."),
        ("attribution_available", str(available), "Whether at least two comparable snapshots exist."),
    ]
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def build_report(
    attribution: pd.DataFrame,
    summary: pd.DataFrame,
    top_n: int,
    history_path: Path,
    analytics_path: Path,
) -> str:
    summary_map = dict(zip(summary["metric"], summary["value"]))
    lines = [
        "=" * WIDTH,
        "BACQE EH18 - EDGE FAMILY ATTRIBUTION REPORT",
        "=" * WIDTH,
        f"Generated UTC:              {utc_now_iso()}",
        f"Engine version:             {ENGINE_VERSION}",
        f"EH15 history:               {history_path}",
        f"EH16 analytics:             {analytics_path}",
        "-" * WIDTH,
        f"Analysis status:            {summary_map.get('analysis_status', '')}",
        f"Historical snapshots:       {summary_map.get('historical_snapshots', 0)}",
        f"Families attributed:        {summary_map.get('families_attributed', 0)}",
        f"Active driver records:      {summary_map.get('active_driver_records', 0)}",
        f"High-confidence findings:   {summary_map.get('high_confidence_attributions', 0)}",
        "-" * WIDTH,
    ]

    if summary_map.get("analysis_status") != "comparison_available":
        lines.extend([
            "ATTRIBUTION STATUS",
            "At least two distinct EH15 snapshots are required before family change",
            "drivers can be measured. EH18 has created the baseline output schema but",
            "has not invented explanations from a single observation.",
            "=" * WIDTH,
        ])
        return "\n".join(lines) + "\n"

    ranked = attribution.copy()
    ranked["abs_net"] = pd.to_numeric(
        ranked["net_attribution_score"], errors="coerce"
    ).abs()
    ranked = ranked.sort_values(
        ["abs_net", "edge_family_id"], ascending=[False, True]
    ).head(max(top_n, 1))

    lines.append("LEADING FAMILY ATTRIBUTIONS")
    for _, row in ranked.iterrows():
        lines.extend([
            f"{row['edge_family_id']} | {row['evolution_class']} | "
            f"confidence={row['attribution_confidence']}",
            f"  {row['attribution_explanation']}",
            f"  Net attribution score: {row['net_attribution_score']:.6f}",
            "",
        ])
    lines.extend([
        "SCIENTIFIC LIMIT",
        "These findings describe contribution and association across aggregate snapshots.",
        "They do not establish causal market mechanisms or exact member-level provenance.",
        "=" * WIDTH,
    ])
    return "\n".join(lines) + "\n"


def run_engine(
    history_path: Path,
    analytics_path: Path,
    output_dir: Path,
    top_n: int = 20,
) -> dict[str, Any]:
    history, analytics = read_inputs(history_path, analytics_path)
    scales = metric_scales(analytics)
    drivers = build_driver_rows(analytics, scales)
    attribution = build_attribution(history, analytics, drivers)
    summary = build_summary(attribution, history, drivers)

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "attribution": output_dir / "family_attribution_latest.csv",
        "drivers": output_dir / "family_driver_rankings_latest.csv",
        "summary": output_dir / "family_attribution_summary_latest.csv",
        "report": output_dir / "family_attribution_report_latest.txt",
        "state": output_dir / "family_attribution_state_latest.json",
    }

    atomic_write_csv(attribution, outputs["attribution"])
    atomic_write_csv(drivers, outputs["drivers"])
    atomic_write_csv(summary, outputs["summary"])
    report = build_report(
        attribution, summary, top_n, history_path, analytics_path
    )
    atomic_write_text(report, outputs["report"])

    analysis_status = (
        str(attribution["analysis_status"].iloc[0])
        if not attribution.empty else "unknown"
    )
    comparison_ids = [
        str(value) for value in attribution["comparison_id"].dropna().unique()
        if str(value).strip()
    ]
    state = {
        "engine_version": ENGINE_VERSION,
        "attribution_schema_version": ATTRIBUTION_SCHEMA_VERSION,
        "generated_utc": utc_now_iso(),
        "analysis_status": analysis_status,
        "comparison_id": comparison_ids[0] if comparison_ids else "",
        "historical_snapshots": int(history["snapshot_id"].nunique()),
        "families_attributed": int(len(attribution)),
        "driver_records": int(len(drivers)),
        "active_driver_records": int(
            (drivers["absolute_contribution"] > 1e-12).sum()
        ) if not drivers.empty else 0,
        "outputs": {key: str(value) for key, value in outputs.items()},
        "evidence_limit": (
            "Aggregate longitudinal contribution analysis; causal interpretation "
            "requires additional experimental or member-level evidence."
        ),
    }
    atomic_write_json(state, outputs["state"])

    print(report, end="")
    return {
        "history": history,
        "analytics": analytics,
        "drivers": drivers,
        "attribution": attribution,
        "summary": summary,
        "outputs": outputs,
        "state": state,
    }


def synthetic_history() -> pd.DataFrame:
    common = {
        "history_schema_version": "1.0",
        "engine_version": "1.0.0",
        "target": "future_return_20000",
        "feature_family": "hour",
        "threshold_side": "upper",
        "context_type": "context_hour",
        "parent_context": "hour_13",
        "tier_2_count": 1,
        "tier_3_count": 1,
        "reject_count": 1,
        "distinct_candidate_count": 2,
        "candidate_layer_count": 1,
        "total_trades": 1000,
        "total_net_return": 10,
        "median_candidate_score": 80,
        "max_candidate_score": 100,
        "median_confidence_score": 55,
        "median_win_rate": 0.53,
        "median_profit_factor": 1.2,
        "median_positive_year_rate": 0.6,
        "transfer_success_rate": 0.5,
        "year_stable_rate": 0.5,
        "family_independence_score": 55,
        "recommended_research_status": "research",
        "recommended_next_step": "retain",
        "family_concentration_risk": "medium",
        "family_population_class": "established_family",
        "is_orphan_family": False,
    }
    rows = [
        {
            **common, "snapshot_id": "S1",
            "observation_utc": "2026-07-01T00:00:00+00:00",
            "edge_family_id": "A", "member_count": 5,
            "priority_member_count": 2, "tier_1_count": 1,
            "symbol_count": 2, "symbols_present": "EURJPY, USDJPY",
            "context_count": 1, "contexts_present": "context_hour::13",
            "candidate_layers_present": "base_symbol_mc",
        },
        {
            **common, "snapshot_id": "S2",
            "observation_utc": "2026-07-15T00:00:00+00:00",
            "edge_family_id": "A", "member_count": 9,
            "priority_member_count": 5, "tier_1_count": 3,
            "symbol_count": 3, "symbols_present": "EURJPY, GBPJPY, USDJPY",
            "context_count": 2,
            "contexts_present": "context_hour::13, context_hour::14",
            "candidate_layer_count": 2,
            "candidate_layers_present": "base_symbol_mc, cross_symbol_transfer",
            "transfer_success_rate": 0.8,
        },
        {
            **common, "snapshot_id": "S1",
            "observation_utc": "2026-07-01T00:00:00+00:00",
            "edge_family_id": "B", "member_count": 8,
            "priority_member_count": 4, "tier_1_count": 2,
            "symbol_count": 4, "symbols_present": "EURJPY, GBPJPY, GBPUSD, USDJPY",
            "context_count": 2,
            "contexts_present": "context_hour::13, context_hour::14",
            "candidate_layers_present": "base_symbol_mc, cross_symbol_transfer",
            "candidate_layer_count": 2,
        },
        {
            **common, "snapshot_id": "S2",
            "observation_utc": "2026-07-15T00:00:00+00:00",
            "edge_family_id": "B", "member_count": 4,
            "priority_member_count": 1, "tier_1_count": 0,
            "symbol_count": 2, "symbols_present": "EURJPY, USDJPY",
            "context_count": 1, "contexts_present": "context_hour::13",
            "candidate_layers_present": "base_symbol_mc",
        },
    ]
    return pd.DataFrame(rows)


def synthetic_analytics(status: str = "comparison_available") -> pd.DataFrame:
    rows = []
    specs = {
        "A": {
            "evolution_class": "EXPANDING",
            "previous_member_count": 5, "current_member_count": 9,
            "previous_priority_member_count": 2, "current_priority_member_count": 5,
            "previous_symbol_count": 2, "current_symbol_count": 3,
            "previous_context_count": 1, "current_context_count": 2,
            "previous_tier_1_count": 1, "current_tier_1_count": 3,
            "previous_tier_2_count": 1, "current_tier_2_count": 1,
            "previous_tier_3_count": 1, "current_tier_3_count": 1,
            "previous_reject_count": 1, "current_reject_count": 1,
            "previous_distinct_candidate_count": 2, "current_distinct_candidate_count": 2,
            "previous_candidate_layer_count": 1, "current_candidate_layer_count": 2,
            "previous_total_trades": 1000, "current_total_trades": 1500,
            "previous_total_net_return": 10, "current_total_net_return": 20,
            "previous_median_candidate_score": 80, "current_median_candidate_score": 90,
            "previous_max_candidate_score": 100, "current_max_candidate_score": 120,
            "previous_median_confidence_score": 55, "current_median_confidence_score": 65,
            "previous_median_win_rate": 0.53, "current_median_win_rate": 0.55,
            "previous_median_profit_factor": 1.2, "current_median_profit_factor": 1.4,
            "previous_median_positive_year_rate": 0.6, "current_median_positive_year_rate": 0.7,
            "previous_transfer_success_rate": 0.5, "current_transfer_success_rate": 0.8,
            "previous_year_stable_rate": 0.5, "current_year_stable_rate": 0.7,
            "previous_family_independence_score": 55, "current_family_independence_score": 60,
        },
        "B": {
            "evolution_class": "DECLINING",
            "previous_member_count": 8, "current_member_count": 4,
            "previous_priority_member_count": 4, "current_priority_member_count": 1,
            "previous_symbol_count": 4, "current_symbol_count": 2,
            "previous_context_count": 2, "current_context_count": 1,
            "previous_tier_1_count": 2, "current_tier_1_count": 0,
            "previous_tier_2_count": 1, "current_tier_2_count": 1,
            "previous_tier_3_count": 1, "current_tier_3_count": 1,
            "previous_reject_count": 1, "current_reject_count": 2,
            "previous_distinct_candidate_count": 2, "current_distinct_candidate_count": 1,
            "previous_candidate_layer_count": 2, "current_candidate_layer_count": 1,
            "previous_total_trades": 1000, "current_total_trades": 700,
            "previous_total_net_return": 10, "current_total_net_return": 4,
            "previous_median_candidate_score": 80, "current_median_candidate_score": 60,
            "previous_max_candidate_score": 100, "current_max_candidate_score": 80,
            "previous_median_confidence_score": 55, "current_median_confidence_score": 45,
            "previous_median_win_rate": 0.53, "current_median_win_rate": 0.50,
            "previous_median_profit_factor": 1.2, "current_median_profit_factor": 1.0,
            "previous_median_positive_year_rate": 0.6, "current_median_positive_year_rate": 0.4,
            "previous_transfer_success_rate": 0.5, "current_transfer_success_rate": 0.2,
            "previous_year_stable_rate": 0.5, "current_year_stable_rate": 0.3,
            "previous_family_independence_score": 55, "current_family_independence_score": 45,
        },
    }
    for family_id, values in specs.items():
        row = {
            "engine_version": "1.0.0",
            "analysis_status": status,
            "comparison_id": "cmp_S1_S2" if status == "comparison_available" else "",
            "previous_snapshot_id": "S1" if status == "comparison_available" else "",
            "current_snapshot_id": "S2",
            "previous_observation_utc": "2026-07-01T00:00:00+00:00" if status == "comparison_available" else "",
            "current_observation_utc": "2026-07-15T00:00:00+00:00",
            "edge_family_id": family_id,
            "target": "future_return_20000",
            "feature_family": "hour",
            "threshold_side": "upper",
            "context_type": "context_hour",
            "parent_context": "hour_13",
            **values,
        }
        for metric, _, _ in CORE_DRIVER_SPECS:
            previous = safe_float(row.get(f"previous_{metric}"))
            current = safe_float(row.get(f"current_{metric}"))
            row[f"{metric}_delta"] = (
                current - previous
                if not math.isnan(previous) and not math.isnan(current)
                else math.nan
            )
        if status != "comparison_available":
            row["evolution_class"] = "BASELINE"
            for metric, _, _ in CORE_DRIVER_SPECS:
                row[f"previous_{metric}"] = math.nan
                row[f"{metric}_delta"] = math.nan
        rows.append(row)
    return pd.DataFrame(rows)


def self_test() -> int:
    tests: list[tuple[str, bool]] = []

    def check(name: str, condition: bool) -> None:
        tests.append((name, bool(condition)))

    with tempfile.TemporaryDirectory(prefix="bacqe_eh18_") as temp_name:
        root = Path(temp_name)
        history_path = root / "history.csv"
        analytics_path = root / "analytics.csv"
        output = root / "output"

        history = synthetic_history()
        analytics = synthetic_analytics()
        history.to_csv(history_path, index=False)
        analytics.to_csv(analytics_path, index=False)

        result = run_engine(history_path, analytics_path, output, top_n=10)
        attribution = result["attribution"].set_index("edge_family_id")
        drivers = result["drivers"]

        check("comparison status", result["state"]["analysis_status"] == "comparison_available")
        check("two families attributed", len(attribution) == 2)
        check("driver records created", len(drivers) > 0)
        check("driver ranks unique within family",
              not drivers.duplicated(["edge_family_id", "driver_rank"]).any())
        check("growing family positive score", attribution.loc["A", "net_attribution_score"] > 0)
        check("declining family negative score", attribution.loc["B", "net_attribution_score"] < 0)
        check("symbol addition detected", attribution.loc["A", "symbols_added"] == "GBPJPY")
        check("symbol removals detected",
              attribution.loc["B", "symbols_removed"] == "GBPJPY, GBPUSD")
        check("context addition detected",
              attribution.loc["A", "contexts_added"] == "context_hour::14")
        check("layer addition detected",
              attribution.loc["A", "candidate_layers_added"] == "cross_symbol_transfer")
        check("explanation produced",
              "principally associated" in attribution.loc["A", "attribution_explanation"])
        check("evidence limit present",
              attribution["evidence_limit"].str.contains("not proof", case=False).all())
        check("summary produced", not result["summary"].empty)
        check("report produced", result["outputs"]["report"].exists())
        check("state produced", result["outputs"]["state"].exists())
        check("output columns stable",
              list(result["attribution"].columns) == ATTRIBUTION_COLUMNS)
        check("driver columns stable", list(drivers.columns) == DRIVER_COLUMNS)

        baseline_analytics_path = root / "baseline_analytics.csv"
        baseline_output = root / "baseline_output"
        synthetic_analytics("insufficient_history").to_csv(
            baseline_analytics_path, index=False
        )
        baseline = run_engine(
            history_path, baseline_analytics_path, baseline_output, top_n=10
        )
        baseline_attr = baseline["attribution"]
        check("baseline status retained",
              baseline["state"]["analysis_status"] == "insufficient_history")
        check("baseline driver table empty", baseline["drivers"].empty)
        check("baseline explanations honest",
              baseline_attr["attribution_explanation"].str.contains(
                  "at least two", case=False
              ).all())
        check("baseline confidence unavailable",
              baseline_attr["attribution_confidence"].eq("not_available").all())
        check("baseline report valid", baseline["outputs"]["report"].exists())

        # Determinism.
        second = run_engine(history_path, analytics_path, root / "second", top_n=10)
        check("attribution deterministic",
              result["attribution"].equals(second["attribution"]))
        check("drivers deterministic",
              result["drivers"].equals(second["drivers"]))

        # Input validation.
        broken_history = history.drop(columns=["member_count"])
        broken_path = root / "broken.csv"
        broken_history.to_csv(broken_path, index=False)
        try:
            run_engine(broken_path, analytics_path, root / "broken_out")
            broken_rejected = False
        except ValueError:
            broken_rejected = True
        check("missing history field rejected", broken_rejected)

        duplicate_history = pd.concat([history, history.iloc[[0]]], ignore_index=True)
        duplicate_path = root / "duplicate.csv"
        duplicate_history.to_csv(duplicate_path, index=False)
        try:
            run_engine(duplicate_path, analytics_path, root / "duplicate_out")
            duplicate_rejected = False
        except ValueError:
            duplicate_rejected = True
        check("duplicate history rejected", duplicate_rejected)

    passed = sum(ok for _, ok in tests)
    print("=" * WIDTH)
    print("BACQE EH18 - SELF TEST")
    print("=" * WIDTH)
    for name, ok in tests:
        print(f"{'PASS' if ok else 'FAIL':<6} {name}")
    print("-" * WIDTH)
    print(f"Passed: {passed}/{len(tests)}")
    print("=" * WIDTH)
    return 0 if passed == len(tests) else 1


def main() -> int:
    args = parse_args()
    if args.self_test:
        return self_test()

    analysis_root = args.analysis_root.resolve()
    history_path = (
        args.history.resolve()
        if args.history
        else analysis_root / INPUT_HISTORY
    )
    analytics_path = (
        args.analytics.resolve()
        if args.analytics
        else analysis_root / INPUT_ANALYTICS
    )
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else analysis_root / OUTPUT_NAME
    )

    try:
        run_engine(
            history_path=history_path,
            analytics_path=analytics_path,
            output_dir=output_dir,
            top_n=args.top_n,
        )
        return 0
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"EH18 ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())