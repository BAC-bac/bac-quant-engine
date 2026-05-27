"""
BACQE REGIME ENGINE - 64 Build BACQE State Registry

Creates a canonical latest-state registry for BACQE.

This registry acts as the current "what do we know?" state object for:
    - adaptive strategy selection
    - operational health
    - market snapshot
    - GBPUSD microstructure state

Outputs:
    E:/Quant_Lab/data/state/bacqe_state_registry_latest.csv
    E:/Quant_Lab/data/state/bacqe_state_registry_latest.parquet
    E:/Quant_Lab/data/state/bacqe_state_registry_latest.json
    E:/Quant_Lab/reports/bacqe_state_registry/bacqe_state_registry_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SELECTION_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "adaptive_strategy_selection_dashboard_latest.csv"
)

HEALTH_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "bacqe_live_status_health_latest.csv"
)

SNAPSHOT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "bacqe_live_status_snapshot_latest.json"
)

MICRO_PATH = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "live_state_dashboard"
    / "live_state_dashboard_latest.json"
)

OUTPUT_STATE_DIR = DATA_LAKE_ROOT / "data" / "state"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_state_registry"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_csv_required(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{name} not found: {path}")

    df = pd.read_csv(path, low_memory=False)

    if df.empty:
        raise ValueError(f"{name} is empty: {path}")

    return df


def read_json_optional(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def classify_overall_health(health: pd.DataFrame) -> str:
    if health.empty:
        return "unknown"

    if "freshness_status" not in health.columns:
        return "unknown"

    statuses = set(health["freshness_status"].dropna().astype(str))

    if "missing" in statuses:
        return "missing_outputs"

    if "stale" in statuses:
        return "stale_outputs"

    if "warning" in statuses:
        return "freshness_warning"

    return "healthy"


def extract_cycle_status(health: pd.DataFrame) -> dict:
    if health.empty or "check_name" not in health.columns:
        return {}

    cycle = health[health["check_name"] == "adaptive_operator_cycle"].copy()

    if cycle.empty:
        return {}

    row = cycle.iloc[0]

    return {
        "adaptive_cycle_status": row.get("json_cycle_status"),
        "adaptive_cycle_scripts_successful": row.get("json_scripts_successful"),
        "adaptive_cycle_scripts_failed": row.get("json_scripts_failed"),
        "adaptive_cycle_elapsed_seconds": row.get("json_total_elapsed_seconds"),
        "adaptive_cycle_age_hours": row.get("age_hours"),
    }


def build_registry(selection: pd.DataFrame, health: pd.DataFrame, snapshot: dict, micro: dict) -> pd.DataFrame:
    rows = []

    overall_health = classify_overall_health(health)
    cycle_status = extract_cycle_status(health)

    adaptive_market_mode = snapshot.get("adaptive_market_mode")

    micro_fields = {
        "micro_symbol": micro.get("symbol"),
        "micro_bar_type": micro.get("bar_type"),
        "micro_regime": micro.get("microstructure_regime"),
        "micro_m15_composite_regime": micro.get("m15_composite_regime"),
        "micro_primary_state": micro.get("primary_current_state"),
        "micro_expected_next_state": micro.get("primary_expected_next_state"),
        "micro_transition_probability": micro.get("primary_transition_probability"),
        "micro_live_bias": micro.get("primary_live_bias"),
        "micro_actionability": micro.get("primary_actionability"),
        "micro_latest_bar_start": micro.get("latest_bar_start_time"),
        "micro_latest_bar_end": micro.get("latest_bar_end_time"),
    }

    for _, row in selection.iterrows():
        record = {
            "registry_time_utc": utc_now(),
            "symbol": row.get("symbol"),
            "adaptive_market_mode": adaptive_market_mode,
            "overall_health": overall_health,
            "selection_bucket": row.get("selection_bucket"),
            "selection_confidence": row.get("selection_confidence"),
            "operator_instruction": row.get("operator_instruction"),
            "execution_posture": row.get("execution_posture"),
            "research_priority": row.get("research_priority"),
            "primary_strategy_environment": row.get("primary_strategy_environment"),
            "primary_strategy": row.get("primary_strategy"),
            "candidate_strategy_name": row.get("candidate_strategy_name"),
            "candidate_timeframe": row.get("candidate_timeframe"),
            "candidate_composite_regime": row.get("candidate_composite_regime"),
            "opportunity_score": row.get("opportunity_score"),
            "opportunity_quality": row.get("opportunity_quality"),
            "recommended_action": row.get("recommended_action"),
            "directional_bias": row.get("directional_bias"),
            "risk_mode": row.get("risk_mode"),
            "elite_count": row.get("elite_count"),
            "strong_count": row.get("strong_count"),
            "watch_count": row.get("watch_count"),
            "defensive_count": row.get("defensive_count"),
        }

        record.update(cycle_status)

        if str(row.get("symbol")) == "GBPUSD":
            record.update(micro_fields)
        else:
            for key in micro_fields:
                record[key] = None

        rows.append(record)

    registry = pd.DataFrame(rows)

    numeric_cols = [
        "opportunity_score",
        "elite_count",
        "strong_count",
        "watch_count",
        "defensive_count",
        "adaptive_cycle_scripts_successful",
        "adaptive_cycle_scripts_failed",
        "adaptive_cycle_elapsed_seconds",
        "adaptive_cycle_age_hours",
        "micro_transition_probability",
    ]

    for col in numeric_cols:
        if col in registry.columns:
            registry[col] = pd.to_numeric(registry[col], errors="coerce")

    bucket_rank = {
        "PRIORITY_RESEARCH": 1,
        "PRIMARY_WATCHLIST": 2,
        "EXPANSION_CONFIRMATION": 3,
        "DEFENSIVE_FILTER": 4,
        "BACKGROUND_MONITORING": 5,
    }

    registry["selection_rank"] = registry["selection_bucket"].map(bucket_rank).fillna(99).astype(int)

    registry = registry.sort_values(
        ["selection_rank", "opportunity_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    return registry


def build_payload(registry: pd.DataFrame, snapshot: dict, micro: dict) -> dict:
    return {
        "registry_time_utc": utc_now(),
        "rows": len(registry),
        "adaptive_market_mode": snapshot.get("adaptive_market_mode"),
        "priority_research": snapshot.get("priority_research", []),
        "primary_watchlist": snapshot.get("primary_watchlist", []),
        "expansion_confirmation": snapshot.get("expansion_confirmation", []),
        "defensive_filter": snapshot.get("defensive_filter", []),
        "microstructure": {
            "symbol": micro.get("symbol"),
            "microstructure_regime": micro.get("microstructure_regime"),
            "m15_composite_regime": micro.get("m15_composite_regime"),
            "primary_current_state": micro.get("primary_current_state"),
            "primary_expected_next_state": micro.get("primary_expected_next_state"),
            "primary_transition_probability": micro.get("primary_transition_probability"),
            "primary_live_bias": micro.get("primary_live_bias"),
            "primary_actionability": micro.get("primary_actionability"),
        },
        "registry": registry.to_dict(orient="records"),
    }


def build_report(registry: pd.DataFrame, snapshot: dict, micro: dict) -> str:
    display_cols = [
        "symbol",
        "selection_bucket",
        "selection_confidence",
        "operator_instruction",
        "overall_health",
        "adaptive_market_mode",
        "primary_strategy_environment",
        "primary_strategy",
        "candidate_strategy_name",
        "candidate_timeframe",
        "candidate_composite_regime",
        "opportunity_score",
        "directional_bias",
        "risk_mode",
        "recommended_action",
    ]

    available_cols = [col for col in display_cols if col in registry.columns]

    lines = []

    lines.append("=" * 150)
    lines.append("BACQE STATE REGISTRY")
    lines.append("=" * 150)
    lines.append(f"Registry time UTC:       {utc_now()}")
    lines.append(f"Adaptive market mode:    {snapshot.get('adaptive_market_mode')}")
    lines.append(f"Priority research:       {snapshot.get('priority_research')}")
    lines.append(f"Primary watchlist:       {snapshot.get('primary_watchlist')}")
    lines.append(f"Expansion confirmation:  {snapshot.get('expansion_confirmation')}")
    lines.append(f"Defensive filter:        {snapshot.get('defensive_filter')}")
    lines.append("-" * 150)

    lines.append("")
    lines.append("CURRENT STATE REGISTRY")
    lines.append("-" * 150)
    lines.append(registry[available_cols].to_string(index=False))

    lines.append("")
    lines.append("GBPUSD MICROSTRUCTURE STATE")
    lines.append("-" * 150)
    lines.append(f"Microstructure regime:   {micro.get('microstructure_regime')}")
    lines.append(f"M15 composite regime:    {micro.get('m15_composite_regime')}")
    lines.append(f"Primary state:           {micro.get('primary_current_state')}")
    lines.append(f"Expected next state:     {micro.get('primary_expected_next_state')}")
    lines.append(f"Transition probability:  {micro.get('primary_transition_probability')}")
    lines.append(f"Live bias:               {micro.get('primary_live_bias')}")
    lines.append(f"Actionability:           {micro.get('primary_actionability')}")

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 150)
    lines.append("The state registry is BACQE's canonical latest-state object.")
    lines.append("It is designed for future dashboards, APIs, alert systems, schedulers, and execution research.")
    lines.append("This registry is a state snapshot, not a trading signal.")
    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    print("=" * 150)
    print("BACQE REGIME ENGINE - 64 BUILD BACQE STATE REGISTRY")
    print("=" * 150)

    selection = read_csv_required(SELECTION_PATH, "Adaptive strategy selection dashboard")
    health = read_csv_required(HEALTH_PATH, "BACQE live status health")
    snapshot = read_json_optional(SNAPSHOT_PATH)
    micro = read_json_optional(MICRO_PATH)

    registry = build_registry(selection, health, snapshot, micro)
    payload = build_payload(registry, snapshot, micro)
    report = build_report(registry, snapshot, micro)

    OUTPUT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_STATE_DIR / "bacqe_state_registry_latest.csv"
    parquet_path = OUTPUT_STATE_DIR / "bacqe_state_registry_latest.parquet"
    json_path = OUTPUT_STATE_DIR / "bacqe_state_registry_latest.json"
    report_path = OUTPUT_REPORT_DIR / "bacqe_state_registry_latest.txt"

    registry.to_csv(csv_path, index=False)
    registry.to_parquet(parquet_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    report_path.write_text(report, encoding="utf-8")

    print("[DONE] BACQE state registry created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"JSON:    {json_path}")
    print(f"Report:  {report_path}")
    print("-" * 150)
    print(report)
    print("=" * 150)


if __name__ == "__main__":
    main()