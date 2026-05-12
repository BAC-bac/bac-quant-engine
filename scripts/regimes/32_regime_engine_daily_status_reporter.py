"""
BACQE Script 32
Regime Engine Daily Status Reporter

Purpose:
- Create a readable daily operational status report for the BACQE Regime Engine
- Combine operational health, sync verification, append readiness, and latest ledger state
- Produce TXT, CSV, and JSON outputs

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

HEALTH_JSON = LEDGER_DIR / "operational_health" / "regime_engine_operational_health_latest.json"
HEALTH_CSV = LEDGER_DIR / "operational_health" / "regime_engine_operational_health_latest.csv"

SYNC_JSON = LEDGER_DIR / "sync_verification" / "incremental_sync_verification_overall_latest.json"
APPEND_READINESS_JSON = LEDGER_DIR / "incremental_append_readiness" / "incremental_append_readiness_latest.json"
APPEND_DRY_RUN_JSON = LEDGER_DIR / "incremental_append_dry_run_plans" / "incremental_append_dry_run_status_latest.json"

LEDGER_SUMMARY = LEDGER_DIR / "regime_incremental_summary_latest.csv"
CHANGE_SUMMARY = LEDGER_DIR / "regime_incremental_change_summary_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "daily_status_reports"


def read_json_safe(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def status_icon(status: str) -> str:
    value = str(status).upper()
    if value == "GREEN":
        return "[GREEN]"
    if value == "AMBER":
        return "[AMBER]"
    if value == "RED":
        return "[RED]"
    return "[UNKNOWN]"


def summarise_ledger(ledger_summary: pd.DataFrame) -> dict:
    if ledger_summary.empty:
        return {
            "stage_count": 0,
            "total_files": 0,
            "total_rows": 0,
            "total_size_mb": 0,
            "latest_modified": None,
        }

    return {
        "stage_count": int(ledger_summary["stage"].nunique()) if "stage" in ledger_summary.columns else 0,
        "total_files": int(pd.to_numeric(ledger_summary.get("file_count", 0), errors="coerce").fillna(0).sum()),
        "total_rows": int(pd.to_numeric(ledger_summary.get("total_rows", 0), errors="coerce").fillna(0).sum()),
        "total_size_mb": round(
            float(pd.to_numeric(ledger_summary.get("total_size_mb", 0), errors="coerce").fillna(0).sum()),
            3,
        ),
        "latest_modified": (
            ledger_summary["latest_modified"].max()
            if "latest_modified" in ledger_summary.columns
            else None
        ),
    }


def summarise_change_summary(change_summary: pd.DataFrame) -> dict:
    if change_summary.empty:
        return {
            "updates_needed": None,
            "symbols_checked": None,
        }

    return {
        "updates_needed": int(pd.to_numeric(change_summary.get("updates_needed", 0), errors="coerce").fillna(0).sum()),
        "symbols_checked": int(pd.to_numeric(change_summary.get("symbols_checked", 0), errors="coerce").fillna(0).sum()),
    }


def build_text_report(payload: dict, health_checks: pd.DataFrame, ledger_summary: pd.DataFrame) -> str:
    lines = []

    lines.append("=" * 90)
    lines.append("BACQE REGIME ENGINE DAILY STATUS REPORT")
    lines.append("=" * 90)
    lines.append(f"Generated at: {payload['generated_at']}")
    lines.append(f"Overall status: {status_icon(payload['overall_status'])} {payload['overall_status']}")
    lines.append("-" * 90)

    lines.append("CORE STATUS")
    lines.append(f"Operational health: {status_icon(payload['operational_health_status'])} {payload['operational_health_status']}")
    lines.append(f"Sync verification:  {status_icon(payload['sync_status'])} {payload['sync_status']}")
    lines.append(f"Append readiness:   {status_icon(payload['append_readiness_status'])} {payload['append_readiness_status']}")
    lines.append(f"Append dry-run:     {status_icon(payload['append_dry_run_status'])} {payload['append_dry_run_status']}")
    lines.append("")

    lines.append("INCREMENTAL STATE")
    lines.append(f"Missing-base updates needed: {payload['updates_needed']}")
    lines.append(f"Append candidates:            {payload['append_candidates']}")
    lines.append(f"Append blocked:               {payload['append_blocked']}")
    lines.append(f"Append dry-run ready:          {payload['append_dry_run_ready']}")
    lines.append(f"Append dry-run blocked:        {payload['append_dry_run_blocked']}")
    lines.append("")

    lines.append("LEDGER SCALE")
    lines.append(f"Stages tracked:       {payload['ledger_stage_count']}")
    lines.append(f"Files tracked:        {payload['ledger_total_files']}")
    lines.append(f"Rows tracked:         {payload['ledger_total_rows']:,}")
    lines.append(f"Total size MB:        {payload['ledger_total_size_mb']:,}")
    lines.append(f"Latest file modified: {payload['ledger_latest_modified']}")
    lines.append("")

    if not health_checks.empty:
        lines.append("HEALTH CHECKS")
        for _, row in health_checks.iterrows():
            lines.append(
                f"- {status_icon(row.get('status'))} "
                f"{row.get('check_name')}: observed={row.get('observed')} expected={row.get('expected')}"
            )
        lines.append("")

    if not ledger_summary.empty:
        lines.append("TOP LEDGER SUMMARY")
        cols = ["stage", "broker", "timeframe", "file_count", "total_size_mb", "total_rows", "latest_data_timestamp"]
        available_cols = [c for c in cols if c in ledger_summary.columns]
        preview = ledger_summary[available_cols].head(40).to_string(index=False)
        lines.append(preview)
        lines.append("")

    lines.append("RECOMMENDED NEXT STEP")
    lines.append(payload["next_recommended_step"])
    lines.append("=" * 90)

    return "\n".join(lines)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    health = read_json_safe(HEALTH_JSON)
    sync = read_json_safe(SYNC_JSON)
    append_readiness = read_json_safe(APPEND_READINESS_JSON)
    append_dry_run = read_json_safe(APPEND_DRY_RUN_JSON)

    health_checks = read_csv_safe(HEALTH_CSV)
    ledger_summary = read_csv_safe(LEDGER_SUMMARY)
    change_summary = read_csv_safe(CHANGE_SUMMARY)

    ledger_stats = summarise_ledger(ledger_summary)
    change_stats = summarise_change_summary(change_summary)

    operational_status = str(health.get("overall_status", "UNKNOWN")).upper()
    sync_status = str(sync.get("overall_status", "UNKNOWN")).upper()
    readiness_status = str(append_readiness.get("overall_status", "UNKNOWN")).upper()
    dry_run_status = str(append_dry_run.get("overall_status", "UNKNOWN")).upper()

    status_values = [operational_status, sync_status, readiness_status, dry_run_status]

    if "RED" in status_values:
        overall_status = "RED"
    elif "AMBER" in status_values:
        overall_status = "AMBER"
    elif all(s == "GREEN" for s in status_values):
        overall_status = "GREEN"
    else:
        overall_status = "AMBER"

    if overall_status == "GREEN":
        next_step = "No immediate repair action required. Continue scheduled refreshes and monitoring."
    elif overall_status == "AMBER":
        next_step = "Review append candidates or unknown status files before execution."
    else:
        next_step = "Investigate failed health/sync/readiness checks before continuing."

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "operational_health_status": operational_status,
        "sync_status": sync_status,
        "append_readiness_status": readiness_status,
        "append_dry_run_status": dry_run_status,
        "updates_needed": change_stats["updates_needed"],
        "symbols_checked": change_stats["symbols_checked"],
        "append_candidates": append_readiness.get("append_candidates"),
        "append_blocked": append_readiness.get("blocked"),
        "append_dry_run_ready": append_dry_run.get("append_ready"),
        "append_dry_run_blocked": append_dry_run.get("blocked"),
        "ledger_stage_count": ledger_stats["stage_count"],
        "ledger_total_files": ledger_stats["total_files"],
        "ledger_total_rows": ledger_stats["total_rows"],
        "ledger_total_size_mb": ledger_stats["total_size_mb"],
        "ledger_latest_modified": ledger_stats["latest_modified"],
        "health_json": str(HEALTH_JSON),
        "sync_json": str(SYNC_JSON),
        "append_readiness_json": str(APPEND_READINESS_JSON),
        "append_dry_run_json": str(APPEND_DRY_RUN_JSON),
        "next_recommended_step": next_step,
    }

    report_text = build_text_report(payload, health_checks, ledger_summary)

    text_latest = OUTPUT_DIR / "regime_engine_daily_status_latest.txt"
    text_ts = OUTPUT_DIR / f"regime_engine_daily_status_{run_ts}.txt"

    json_latest = OUTPUT_DIR / "regime_engine_daily_status_latest.json"
    json_ts = OUTPUT_DIR / f"regime_engine_daily_status_{run_ts}.json"

    csv_latest = OUTPUT_DIR / "regime_engine_daily_status_latest.csv"
    csv_ts = OUTPUT_DIR / f"regime_engine_daily_status_{run_ts}.csv"

    text_latest.write_text(report_text, encoding="utf-8")
    text_ts.write_text(report_text, encoding="utf-8")

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    pd.DataFrame([payload]).to_csv(csv_latest, index=False)
    pd.DataFrame([payload]).to_csv(csv_ts, index=False)

    print(report_text)
    print("")
    print(f"Saved TXT:  {text_latest}")
    print(f"Saved JSON: {json_latest}")
    print(f"Saved CSV:  {csv_latest}")


if __name__ == "__main__":
    main()