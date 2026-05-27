"""
BACQE REGIME ENGINE - 62 Build Live BACQE Status Monitor

Checks operational freshness and health of key BACQE outputs.

This creates BACQE operational awareness:
    - file existence
    - file freshness
    - adaptive cycle status
    - latest dashboard availability
    - stale-data warnings
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

CHECKS = {
    "adaptive_operator_cycle": DATA_LAKE_ROOT / "reports" / "bacqe_adaptive_operator_cycle" / "bacqe_adaptive_operator_cycle_latest.json",
    "adaptive_operator_console": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_adaptive_operator_console_latest.csv",
    "adaptive_strategy_selection": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "adaptive_strategy_selection_dashboard_latest.csv",
    "adaptive_opportunities": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "adaptive_strategy_opportunities_latest.csv",
    "strategy_performance_registry": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "strategy_performance_registry_latest.csv",
    "master_operator_dashboard": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_master_operator_dashboard_latest.csv",
    "market_regime_alignment": DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "market_regime_alignment_latest.csv",
    "microstructure_dashboard": DATA_LAKE_ROOT / "reports" / "tick_research" / "live_state_dashboard" / "live_state_dashboard_latest.json",
}

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_status_monitor"

FRESHNESS_WARNING_HOURS = 6
FRESHNESS_STALE_HOURS = 24


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def file_age_hours(path: Path) -> float | None:
    if not path.exists():
        return None

    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return (utc_now() - modified).total_seconds() / 3600


def classify_freshness(age_hours: float | None) -> str:
    if age_hours is None:
        return "missing"

    if age_hours <= FRESHNESS_WARNING_HOURS:
        return "fresh"

    if age_hours <= FRESHNESS_STALE_HOURS:
        return "warning"

    return "stale"


def read_json_if_possible(path: Path) -> dict:
    if not path.exists() or path.suffix.lower() != ".json":
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def read_csv_shape_if_possible(path: Path) -> tuple[int | None, int | None]:
    if not path.exists() or path.suffix.lower() != ".csv":
        return None, None

    try:
        df = pd.read_csv(path, low_memory=False)
        return len(df), len(df.columns)
    except Exception:
        return None, None


def build_file_health() -> pd.DataFrame:
    records = []

    for name, path in CHECKS.items():
        exists = path.exists()
        age = file_age_hours(path)
        freshness = classify_freshness(age)
        rows, cols = read_csv_shape_if_possible(path)
        json_payload = read_json_if_possible(path)

        record = {
            "check_name": name,
            "file_path": str(path),
            "file_exists": exists,
            "file_size_mb": round(path.stat().st_size / (1024 * 1024), 6) if exists else None,
            "modified_time_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() if exists else None,
            "age_hours": round(age, 4) if age is not None else None,
            "freshness_status": freshness,
            "csv_rows": rows,
            "csv_columns": cols,
            "json_cycle_status": json_payload.get("cycle_status"),
            "json_scripts_successful": json_payload.get("scripts_successful"),
            "json_scripts_failed": json_payload.get("scripts_failed"),
            "json_total_elapsed_seconds": json_payload.get("total_elapsed_seconds"),
            "status_time_utc": utc_now().isoformat(),
        }

        records.append(record)

    return pd.DataFrame(records)


def classify_overall_status(health: pd.DataFrame) -> str:
    if health["freshness_status"].eq("missing").any():
        return "missing_outputs"

    if health["json_cycle_status"].dropna().eq("failed").any():
        return "cycle_failed"

    if health["freshness_status"].eq("stale").any():
        return "stale_outputs"

    if health["freshness_status"].eq("warning").any():
        return "freshness_warning"

    return "healthy"


def build_latest_market_snapshot() -> dict:
    snapshot = {
        "adaptive_market_mode": None,
        "priority_research": [],
        "primary_watchlist": [],
        "expansion_confirmation": [],
        "defensive_filter": [],
    }

    selection_path = CHECKS["adaptive_strategy_selection"]
    console_path = CHECKS["adaptive_operator_console"]

    if selection_path.exists():
        try:
            selection = pd.read_csv(selection_path, low_memory=False)

            if "selection_bucket" in selection.columns:
                snapshot["priority_research"] = selection.loc[
                    selection["selection_bucket"] == "PRIORITY_RESEARCH", "symbol"
                ].dropna().astype(str).tolist()

                snapshot["primary_watchlist"] = selection.loc[
                    selection["selection_bucket"] == "PRIMARY_WATCHLIST", "symbol"
                ].dropna().astype(str).tolist()

                snapshot["expansion_confirmation"] = selection.loc[
                    selection["selection_bucket"] == "EXPANSION_CONFIRMATION", "symbol"
                ].dropna().astype(str).tolist()

                snapshot["defensive_filter"] = selection.loc[
                    selection["selection_bucket"] == "DEFENSIVE_FILTER", "symbol"
                ].dropna().astype(str).tolist()

        except Exception:
            pass

    if console_path.exists():
        try:
            console = pd.read_csv(console_path, low_memory=False)
            # Approximate from existing console state.
            ready = (console["execution_posture"] == "research_ready_environment").sum() if "execution_posture" in console.columns else 0
            defensive = (console["execution_posture"] == "observation_or_defensive_only").sum() if "execution_posture" in console.columns else 0

            if ready >= 2:
                snapshot["adaptive_market_mode"] = "active_adaptive_research_environment"
            elif defensive >= len(console) / 2:
                snapshot["adaptive_market_mode"] = "defensive_observation_environment"
            else:
                snapshot["adaptive_market_mode"] = "selective_or_mixed_environment"

        except Exception:
            pass

    return snapshot


def build_report(health: pd.DataFrame, snapshot: dict) -> str:
    now = utc_now().isoformat()
    overall = classify_overall_status(health)

    display_cols = [
        "check_name",
        "file_exists",
        "freshness_status",
        "age_hours",
        "csv_rows",
        "csv_columns",
        "json_cycle_status",
        "json_scripts_successful",
        "json_scripts_failed",
        "json_total_elapsed_seconds",
        "file_path",
    ]

    lines = []

    lines.append("=" * 150)
    lines.append("BACQE LIVE STATUS MONITOR")
    lines.append("=" * 150)
    lines.append(f"Status time UTC: {now}")
    lines.append(f"Overall status:  {overall}")
    lines.append("-" * 150)

    lines.append("")
    lines.append("LATEST MARKET SNAPSHOT")
    lines.append("-" * 150)
    lines.append(f"Adaptive market mode:       {snapshot.get('adaptive_market_mode')}")
    lines.append(f"Priority research:          {snapshot.get('priority_research')}")
    lines.append(f"Primary watchlist:          {snapshot.get('primary_watchlist')}")
    lines.append(f"Expansion confirmation:     {snapshot.get('expansion_confirmation')}")
    lines.append(f"Defensive filter:           {snapshot.get('defensive_filter')}")

    lines.append("")
    lines.append("OUTPUT HEALTH CHECKS")
    lines.append("-" * 150)
    lines.append(health[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 150)
    lines.append("healthy means all monitored outputs exist, are fresh, and the adaptive cycle has not reported failure.")
    lines.append("warning means at least one output is older than the warning threshold.")
    lines.append("stale means at least one output is older than the stale threshold.")
    lines.append("missing_outputs means at least one required output was not found.")
    lines.append("This monitor is operational awareness, not market analysis.")
    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    print("=" * 150)
    print("BACQE REGIME ENGINE - 62 BUILD LIVE BACQE STATUS MONITOR")
    print("=" * 150)

    health = build_file_health()
    snapshot = build_latest_market_snapshot()
    overall_status = classify_overall_status(health)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    health_csv = OUTPUT_ANALYSIS_DIR / "bacqe_live_status_health_latest.csv"
    health_parquet = OUTPUT_ANALYSIS_DIR / "bacqe_live_status_health_latest.parquet"

    snapshot_json = OUTPUT_ANALYSIS_DIR / "bacqe_live_status_snapshot_latest.json"

    report_txt = OUTPUT_REPORT_DIR / "bacqe_live_status_monitor_latest.txt"
    report_json = OUTPUT_REPORT_DIR / "bacqe_live_status_monitor_latest.json"

    health.to_csv(health_csv, index=False)
    health.to_parquet(health_parquet, index=False)

    payload = {
        "status_time_utc": utc_now().isoformat(),
        "overall_status": overall_status,
        "freshness_warning_hours": FRESHNESS_WARNING_HOURS,
        "freshness_stale_hours": FRESHNESS_STALE_HOURS,
        "market_snapshot": snapshot,
        "health": health.to_dict(orient="records"),
    }

    with open(snapshot_json, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=4, default=str)

    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    report = build_report(health, snapshot)
    report_txt.write_text(report, encoding="utf-8")

    print("[DONE] BACQE live status monitor created.")
    print(f"Health CSV:      {health_csv}")
    print(f"Health Parquet:  {health_parquet}")
    print(f"Snapshot JSON:   {snapshot_json}")
    print(f"Report TXT:      {report_txt}")
    print(f"Report JSON:     {report_json}")
    print("-" * 150)
    print(report)
    print("=" * 150)


if __name__ == "__main__":
    main()