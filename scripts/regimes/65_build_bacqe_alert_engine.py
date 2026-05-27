"""
BACQE REGIME ENGINE - 65 Build BACQE Alert Engine
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

STATE_PATH = DATA_LAKE_ROOT / "data" / "state" / "bacqe_state_registry_latest.csv"
PREVIOUS_STATE_PATH = DATA_LAKE_ROOT / "data" / "state" / "bacqe_state_registry_previous.csv"

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "alerts"
REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_alerts"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def alert_record(symbol, severity, alert_type, message, current_value=None, previous_value=None):
    return {
        "alert_time_utc": utc_now(),
        "symbol": symbol,
        "severity": severity,
        "alert_type": alert_type,
        "message": message,
        "current_value": current_value,
        "previous_value": previous_value,
    }


def build_alerts(current: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    alerts = []

    if current.empty:
        alerts.append(alert_record(
            "SYSTEM",
            "critical",
            "missing_state",
            "BACQE state registry is missing or empty.",
        ))
        return pd.DataFrame(alerts)

    if "overall_health" in current.columns:
        health_values = set(current["overall_health"].dropna().astype(str))
        if "healthy" not in health_values:
            alerts.append(alert_record(
                "SYSTEM",
                "warning",
                "system_health",
                f"BACQE health is not fully healthy: {health_values}",
                current_value=str(health_values),
            ))

    for _, row in current.iterrows():
        symbol = row.get("symbol")
        bucket = row.get("selection_bucket")
        confidence = row.get("selection_confidence")
        instruction = row.get("operator_instruction")
        score = row.get("opportunity_score")

        if bucket == "PRIORITY_RESEARCH":
            alerts.append(alert_record(
                symbol,
                "high",
                "priority_research",
                f"{symbol} is in PRIORITY_RESEARCH with instruction: {instruction}",
                current_value=f"{bucket} | {confidence} | score={score}",
            ))

        elif bucket == "EXPANSION_CONFIRMATION":
            alerts.append(alert_record(
                symbol,
                "medium",
                "expansion_confirmation",
                f"{symbol} requires volatility/participation expansion confirmation.",
                current_value=f"{bucket} | {confidence} | score={score}",
            ))

        elif bucket == "DEFENSIVE_FILTER":
            alerts.append(alert_record(
                symbol,
                "info",
                "defensive_filter",
                f"{symbol} is blocked by current environment.",
                current_value=f"{bucket} | {confidence}",
            ))

    if previous.empty:
        alerts.append(alert_record(
            "SYSTEM",
            "info",
            "first_alert_run",
            "No previous state registry found. Change detection will begin from next run.",
        ))
        return pd.DataFrame(alerts)

    prev_by_symbol = previous.set_index("symbol").to_dict(orient="index")

    for _, row in current.iterrows():
        symbol = row.get("symbol")

        if symbol not in prev_by_symbol:
            alerts.append(alert_record(
                symbol,
                "info",
                "new_symbol_state",
                f"{symbol} appeared in the BACQE state registry.",
            ))
            continue

        prev = prev_by_symbol[symbol]

        for col in ["selection_bucket", "operator_instruction", "primary_strategy_environment", "risk_mode"]:
            current_value = row.get(col)
            previous_value = prev.get(col)

            if str(current_value) != str(previous_value):
                severity = "medium" if col in ["selection_bucket", "operator_instruction"] else "info"

                alerts.append(alert_record(
                    symbol,
                    severity,
                    f"{col}_changed",
                    f"{symbol} changed {col}: {previous_value} -> {current_value}",
                    current_value=current_value,
                    previous_value=previous_value,
                ))

    return pd.DataFrame(alerts)


def build_report(alerts: pd.DataFrame) -> str:
    lines = []
    lines.append("=" * 140)
    lines.append("BACQE ALERT ENGINE")
    lines.append("=" * 140)
    lines.append(f"Alert time UTC: {utc_now()}")
    lines.append("-" * 140)

    if alerts.empty:
        lines.append("No alerts generated.")
    else:
        lines.append(alerts.to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 140)
    lines.append("Alerts are research/operator events, not trading signals.")
    lines.append("Priority research alerts identify symbols requiring deeper analysis.")
    lines.append("Expansion confirmation alerts identify symbols where structure exists but confirmation is still required.")
    lines.append("Defensive filter alerts identify symbols blocked by current market structure.")
    lines.append("=" * 140)

    return "\n".join(lines)


def main() -> None:
    print("=" * 140)
    print("BACQE REGIME ENGINE - 65 BUILD BACQE ALERT ENGINE")
    print("=" * 140)

    current = load_state(STATE_PATH)
    previous = load_state(PREVIOUS_STATE_PATH)

    alerts = build_alerts(current, previous)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    alerts_csv = OUTPUT_DIR / "bacqe_alerts_latest.csv"
    alerts_json = OUTPUT_DIR / "bacqe_alerts_latest.json"
    report_path = REPORT_DIR / "bacqe_alerts_latest.txt"

    alerts.to_csv(alerts_csv, index=False)

    with open(alerts_json, "w", encoding="utf-8") as f:
        json.dump(alerts.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(alerts)
    report_path.write_text(report, encoding="utf-8")

    if not current.empty:
        current.to_csv(PREVIOUS_STATE_PATH, index=False)

    print("[DONE] BACQE alerts created.")
    print(f"Alerts CSV:  {alerts_csv}")
    print(f"Alerts JSON: {alerts_json}")
    print(f"Report:      {report_path}")
    print("-" * 140)
    print(report)
    print("=" * 140)


if __name__ == "__main__":
    main()