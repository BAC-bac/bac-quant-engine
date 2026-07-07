"""
BACQE DUKASCOPY 66 - PIPELINE DECISION ENGINE

Purpose:
    Read the pipeline state registry and declarative pipeline definition,
    then decide the next required stage for each symbol.

This script does not execute heavy processing.
It only creates the next execution plan.
"""

from pathlib import Path
import yaml
import pandas as pd


PIPELINE_CONFIG_PATH = Path("config/dukascopy_pipeline_definition.yaml")


def load_pipeline_config() -> dict:
    with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if "dukascopy_pipeline" not in config:
        raise KeyError("Missing top-level key: dukascopy_pipeline")

    cfg = config["dukascopy_pipeline"]

    if not cfg.get("enabled", True):
        raise RuntimeError("dukascopy_pipeline.enabled is false.")

    return cfg


def load_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing state registry: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Pipeline state registry is empty.")

    return df


def decide_symbol_next_stage(row: pd.Series, stages: list[dict]) -> dict:
    symbol = row["symbol"]

    for stage in stages:
        stage_key = stage["stage_key"]
        status_column = stage["status_column"]
        complete_values = stage.get("complete_values", [])
        next_script_stage = stage.get("next_script_stage", "")
        priority = stage.get("priority", "medium")

        if status_column not in row.index:
            return {
                "symbol": symbol,
                "decision_status": "definition_error",
                "next_stage_key": stage_key,
                "next_script_stage": next_script_stage,
                "priority": "critical",
                "reason": f"Missing status column in registry: {status_column}",
            }

        current_value = str(row[status_column])

        if current_value not in complete_values:
            return {
                "symbol": symbol,
                "decision_status": "action_required",
                "next_stage_key": stage_key,
                "next_script_stage": next_script_stage,
                "priority": priority,
                "reason": (
                    f"{stage_key} incomplete: "
                    f"{status_column}={current_value}, "
                    f"expected one of {complete_values}"
                ),
            }

    return {
        "symbol": symbol,
        "decision_status": "complete",
        "next_stage_key": "none",
        "next_script_stage": "none",
        "priority": "complete",
        "reason": "All configured stages complete.",
    }


def build_execution_plan(registry: pd.DataFrame, stages: list[dict]) -> pd.DataFrame:
    rows = []

    for _, row in registry.iterrows():
        decision = decide_symbol_next_stage(row, stages)

        context_cols = [
            "start_date",
            "end_date",
            "raw_status",
            "processed_tick_status",
            "tick_bar_status",
            "tib_status",
            "engineered_feature_status",
            "horizon_feature_status",
        ]

        for col in context_cols:
            if col in row.index:
                decision[col] = row[col]

        rows.append(decision)

    plan = pd.DataFrame(rows)

    priority_order = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
        "complete": 99,
    }

    plan["priority_rank"] = plan["priority"].map(priority_order).fillna(50)

    plan = plan.sort_values(
        by=["priority_rank", "next_script_stage", "symbol"],
        ascending=[True, True, True],
    )

    return plan


def write_outputs(plan: pd.DataFrame, output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)

    plan_path = output_root / "dukascopy_pipeline_execution_plan_latest.csv"
    action_path = output_root / "dukascopy_pipeline_actions_latest.csv"
    report_path = output_root / "dukascopy_pipeline_decision_report_latest.txt"

    plan.to_csv(plan_path, index=False)

    actions = plan[plan["decision_status"] == "action_required"].copy()
    actions.to_csv(action_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 66 - PIPELINE DECISION ENGINE\n")
        f.write("=" * 90 + "\n\n")

        f.write("DECISION STATUS COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(plan["decision_status"].value_counts().to_string())
        f.write("\n\n")

        f.write("NEXT SCRIPT STAGE COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(plan["next_script_stage"].value_counts().to_string())
        f.write("\n\n")

        f.write("EXECUTION PLAN\n")
        f.write("-" * 90 + "\n")
        f.write(plan.to_string(index=False))
        f.write("\n")

    print("=" * 90)
    print("BACQE DUKASCOPY 66 - PIPELINE DECISION ENGINE")
    print("=" * 90)
    print(plan.to_string(index=False))
    print("-" * 90)
    print(f"Execution plan: {plan_path}")
    print(f"Actions only:   {action_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


def main() -> None:
    cfg = load_pipeline_config()

    registry_path = Path(cfg["state_registry_path"])
    output_root = Path(cfg["output_root"])
    stages = cfg["stages"]

    registry = load_registry(registry_path)
    plan = build_execution_plan(registry, stages)

    write_outputs(plan, output_root)


if __name__ == "__main__":
    main()