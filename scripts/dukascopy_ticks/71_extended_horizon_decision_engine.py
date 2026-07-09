"""
BACQE DUKASCOPY EXTENDED HORIZONS 71
DECISION ENGINE

Purpose:
    Read Script 70 extended-horizon state registry and decide the next
    extended-horizon research action for each symbol.

Reads:
    E:/Quant_Lab/data/analysis/dukascopy_extended_horizons/research_state_registry/
        extended_horizon_research_state_registry_latest.csv
        extended_horizon_research_symbol_summary_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/dukascopy_extended_horizons/decision_engine/
        extended_horizon_execution_plan_latest.csv
        extended_horizon_actions_latest.csv
        extended_horizon_decision_report_latest.txt

This script does not execute research.
It decides what should run next.
"""

from pathlib import Path
import pandas as pd
import numpy as np


BASE_DIR = Path("E:/Quant_Lab")

STATE_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "research_state_registry"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "decision_engine"
)


EH_STAGE_COMMANDS = {
    "EH01": {
        "script": "scripts/dukascopy_extended_horizons/01_build_extended_horizon_targets.py",
        "priority": "high",
    },
    "EH02": {
        "script": "scripts/dukascopy_extended_horizons/02_extended_horizon_feature_discovery.py",
        "priority": "high",
    },
    "EH03": {
        "script": "scripts/dukascopy_extended_horizons/03_extended_horizon_stability_engine.py",
        "priority": "high",
    },
    "EH04": {
        "script": "scripts/dukascopy_extended_horizons/04_extended_horizon_signal_validation.py",
        "priority": "high",
    },
    "EH05": {
        "script": "scripts/dukascopy_extended_horizons/05_extended_horizon_cost_survival_engine.py",
        "priority": "medium",
    },
    "EH06": {
        "script": "scripts/dukascopy_extended_horizons/06_extended_horizon_dynamic_cost_engine.py",
        "priority": "medium",
    },
    "EH07": {
        "script": "scripts/dukascopy_extended_horizons/07_extended_horizon_context_conditioning.py",
        "priority": "medium",
    },
    "EH08": {
        "script": "scripts/dukascopy_extended_horizons/08_extended_horizon_regime_edge_engine.py",
        "priority": "medium",
    },
    "EH09": {
        "script": "scripts/dukascopy_extended_horizons/09_extended_horizon_regime_replay_engine.py",
        "priority": "medium",
    },
    "EH10": {
        "script": "scripts/dukascopy_extended_horizons/10_extended_horizon_monte_carlo_robustness.py",
        "priority": "medium",
    },
}


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"{label} is empty: {path}")

    return df


def priority_rank(priority: str) -> int:
    mapping = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
        "complete": 99,
    }
    return mapping.get(str(priority).lower(), 50)


def build_command(symbol: str, next_stage: str) -> str:
    stage_info = EH_STAGE_COMMANDS.get(next_stage)

    if not stage_info:
        return ""

    return f"python {stage_info['script']} --symbol {symbol}"


def decide_next_action(symbol_summary: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in symbol_summary.iterrows():
        symbol = row["symbol"]
        next_stage = row["next_missing_stage"]
        research_status = row["research_status"]

        if next_stage == "none" or research_status == "complete":
            decision_status = "complete"
            priority = "complete"
            command = ""
            reason = "All tracked extended-horizon symbol stages complete."
        else:
            stage_info = EH_STAGE_COMMANDS.get(next_stage)

            if stage_info is None:
                decision_status = "definition_error"
                priority = "critical"
                command = ""
                reason = f"No command mapping defined for next stage: {next_stage}"
            else:
                decision_status = "action_required"
                priority = stage_info["priority"]
                command = build_command(symbol, next_stage)
                reason = f"Next missing extended-horizon stage is {next_stage}."

        rows.append(
            {
                "symbol": symbol,
                "decision_status": decision_status,
                "next_stage": next_stage,
                "priority": priority,
                "research_status": research_status,
                "stages_tracked": row.get("stages_tracked"),
                "stages_complete": row.get("stages_complete"),
                "stages_missing": row.get("stages_missing"),
                "completion_pct": row.get("completion_pct"),
                "reason": reason,
                "command": command,
            }
        )

    plan = pd.DataFrame(rows)

    plan["priority_rank"] = plan["priority"].map(priority_rank).fillna(50)

    plan = plan.sort_values(
        by=["priority_rank", "next_stage", "symbol"],
        ascending=[True, True, True],
    )

    return plan


def write_outputs(plan: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    plan_path = REPORT_ROOT / "extended_horizon_execution_plan_latest.csv"
    actions_path = REPORT_ROOT / "extended_horizon_actions_latest.csv"
    commands_path = REPORT_ROOT / "extended_horizon_commands_latest.txt"
    report_path = REPORT_ROOT / "extended_horizon_decision_report_latest.txt"

    plan.to_csv(plan_path, index=False)

    actions = plan[plan["decision_status"] == "action_required"].copy()
    actions.to_csv(actions_path, index=False)

    with open(commands_path, "w", encoding="utf-8") as f:
        if actions.empty:
            f.write("# No extended-horizon actions required.\n")
        else:
            for command in actions["command"].tolist():
                f.write(command + "\n")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS 71 - DECISION ENGINE\n")
        f.write("=" * 90 + "\n\n")

        f.write("DECISION STATUS COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(plan["decision_status"].value_counts().to_string())
        f.write("\n\n")

        f.write("NEXT STAGE COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(plan["next_stage"].value_counts().to_string())
        f.write("\n\n")

        f.write("EXECUTION PLAN\n")
        f.write("-" * 90 + "\n")
        f.write(plan.to_string(index=False))
        f.write("\n\n")

        f.write("COMMANDS\n")
        f.write("-" * 90 + "\n")
        if actions.empty:
            f.write("No actions required.\n")
        else:
            for command in actions["command"].tolist():
                f.write(command + "\n")

    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS 71 - DECISION ENGINE")
    print("=" * 90)
    print(plan.to_string(index=False))
    print("-" * 90)
    print(f"Execution plan: {plan_path}")
    print(f"Actions:        {actions_path}")
    print(f"Commands:       {commands_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


def main() -> None:
    symbol_summary_path = STATE_ROOT / "extended_horizon_research_symbol_summary_latest.csv"

    symbol_summary = load_csv(
        symbol_summary_path,
        "extended-horizon symbol summary",
    )

    plan = decide_next_action(symbol_summary)

    write_outputs(plan)


if __name__ == "__main__":
    main()