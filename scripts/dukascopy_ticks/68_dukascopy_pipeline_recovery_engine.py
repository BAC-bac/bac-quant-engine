"""
BACQE DUKASCOPY 68 - PIPELINE RECOVERY ENGINE

Purpose:
    Build a safe recovery plan from pipeline verification failures and
    decision-engine action requirements.

Reads:
    - Script 66 actions
    - Script 67 verification failures
    - Pipeline definition YAML

Outputs:
    - Recovery plan CSV
    - Recovery commands TXT
    - Recovery report TXT

This script does not execute recovery work.
It plans recovery work.
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


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path)

    if df.empty:
        return pd.DataFrame()

    return df


def build_stage_priority_map(stages: list[dict]) -> dict:
    return {
        str(stage["next_script_stage"]): {
            "stage_key": stage["stage_key"],
            "priority": stage.get("priority", "medium"),
        }
        for stage in stages
    }


def build_recovery_plan(
    actions: pd.DataFrame,
    failures: pd.DataFrame,
    stages: list[dict],
) -> pd.DataFrame:
    rows = []

    stage_map = build_stage_priority_map(stages)

    if not actions.empty:
        for _, row in actions.iterrows():
            next_stage = str(row.get("next_script_stage", "none"))

            if next_stage == "none":
                continue

            rows.append(
                {
                    "source": "decision_engine",
                    "symbol": row.get("symbol"),
                    "stage_key": row.get("next_stage_key"),
                    "script_stage": next_stage,
                    "priority": row.get("priority", stage_map.get(next_stage, {}).get("priority", "medium")),
                    "reason": row.get("reason"),
                    "recovery_status": "planned",
                }
            )

    if not failures.empty:
        for _, row in failures.iterrows():
            script_stage = str(row.get("next_script_stage", "none"))

            if script_stage == "none":
                continue

            rows.append(
                {
                    "source": "verification_engine",
                    "symbol": row.get("symbol"),
                    "stage_key": row.get("stage_key"),
                    "script_stage": script_stage,
                    "priority": row.get("priority", stage_map.get(script_stage, {}).get("priority", "medium")),
                    "reason": row.get("reason"),
                    "recovery_status": "planned",
                }
            )

    plan = pd.DataFrame(rows)

    if plan.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "symbol",
                "stage_key",
                "script_stage",
                "priority",
                "reason",
                "recovery_status",
                "recovery_command",
                "priority_rank",
            ]
        )

    plan = plan.drop_duplicates(
        subset=["symbol", "script_stage"],
        keep="first",
    ).copy()

    priority_order = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
        "complete": 99,
    }

    plan["priority_rank"] = plan["priority"].map(priority_order).fillna(50)

    plan["recovery_command"] = plan.apply(
        lambda row: (
            "python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py "
            f"--stages {row['script_stage']} --symbols {row['symbol']}"
        ),
        axis=1,
    )

    plan = plan.sort_values(
        by=["priority_rank", "script_stage", "symbol"],
        ascending=[True, True, True],
    )

    return plan


def write_outputs(plan: pd.DataFrame, output_root: Path) -> None:
    recovery_root = output_root.parent / "dukascopy_pipeline_recovery_engine"
    recovery_root.mkdir(parents=True, exist_ok=True)

    plan_path = recovery_root / "dukascopy_pipeline_recovery_plan_latest.csv"
    commands_path = recovery_root / "dukascopy_pipeline_recovery_commands_latest.txt"
    report_path = recovery_root / "dukascopy_pipeline_recovery_report_latest.txt"

    plan.to_csv(plan_path, index=False)

    with open(commands_path, "w", encoding="utf-8") as f:
        if plan.empty:
            f.write("# No recovery actions required.\n")
        else:
            for command in plan["recovery_command"].tolist():
                f.write(command + "\n")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 68 - PIPELINE RECOVERY ENGINE\n")
        f.write("=" * 90 + "\n\n")

        if plan.empty:
            f.write("RECOVERY STATUS: No recovery actions required.\n")
        else:
            f.write(f"RECOVERY STATUS: {len(plan)} action(s) planned.\n")

        f.write("\nRECOVERY PLAN\n")
        f.write("-" * 90 + "\n")
        f.write(plan.to_string(index=False))
        f.write("\n\n")

        f.write("RECOVERY COMMANDS\n")
        f.write("-" * 90 + "\n")
        if plan.empty:
            f.write("No recovery commands required.\n")
        else:
            for command in plan["recovery_command"].tolist():
                f.write(command + "\n")

    print("=" * 90)
    print("BACQE DUKASCOPY 68 - PIPELINE RECOVERY ENGINE")
    print("=" * 90)

    if plan.empty:
        print("[OK] No recovery actions required.")
    else:
        print(f"[PLAN] {len(plan)} recovery action(s) planned.")
        print(plan.to_string(index=False))

    print("-" * 90)
    print(f"Recovery plan:     {plan_path}")
    print(f"Recovery commands: {commands_path}")
    print(f"Report:            {report_path}")
    print("=" * 90)


def main() -> None:
    cfg = load_pipeline_config()

    output_root = Path(cfg["output_root"])

    actions_path = output_root / "dukascopy_pipeline_actions_latest.csv"

    verification_root = output_root.parent / "dukascopy_pipeline_verification_engine"
    failures_path = verification_root / "dukascopy_pipeline_verification_failures_latest.csv"

    actions = safe_read_csv(actions_path)
    failures = safe_read_csv(failures_path)

    plan = build_recovery_plan(
        actions=actions,
        failures=failures,
        stages=cfg["stages"],
    )

    write_outputs(
        plan=plan,
        output_root=output_root,
    )


if __name__ == "__main__":
    main()