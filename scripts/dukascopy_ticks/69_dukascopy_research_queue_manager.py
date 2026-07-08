"""
BACQE DUKASCOPY 69 - RESEARCH QUEUE MANAGER

Purpose:
    Build a prioritised queue of Dukascopy pipeline/research work.

Reads:
    - Script 65 state registry
    - Script 66 execution actions
    - Script 67 verification failures
    - Script 68 recovery plan

Outputs:
    - Research queue CSV
    - Priority queue CSV
    - Queue report TXT

This script does not execute work.
It prioritises work.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import yaml


PIPELINE_CONFIG_PATH = Path("config/dukascopy_pipeline_definition.yaml")


def load_pipeline_config() -> dict:
    with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if "dukascopy_pipeline" not in config:
        raise KeyError("Missing top-level key: dukascopy_pipeline")

    return config["dukascopy_pipeline"]


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path)

    if df.empty:
        return pd.DataFrame()

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


def add_queue_item(
    rows: list[dict],
    source: str,
    symbol: str,
    queue_type: str,
    priority: str,
    stage_key: str,
    script_stage: str,
    reason: str,
    command: str,
) -> None:
    rows.append(
        {
            "source": source,
            "symbol": symbol,
            "queue_type": queue_type,
            "priority": priority,
            "priority_rank": priority_rank(priority),
            "stage_key": stage_key,
            "script_stage": script_stage,
            "reason": reason,
            "command": command,
            "queue_status": "pending",
        }
    )


def build_queue(
    registry: pd.DataFrame,
    actions: pd.DataFrame,
    failures: pd.DataFrame,
    recovery: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    if not recovery.empty:
        for _, row in recovery.iterrows():
            add_queue_item(
                rows=rows,
                source="recovery_engine",
                symbol=row.get("symbol", "UNKNOWN"),
                queue_type="recovery",
                priority=row.get("priority", "critical"),
                stage_key=row.get("stage_key", "unknown"),
                script_stage=row.get("script_stage", "unknown"),
                reason=row.get("reason", "Recovery required."),
                command=row.get("recovery_command", ""),
            )

    if not failures.empty:
        for _, row in failures.iterrows():
            add_queue_item(
                rows=rows,
                source="verification_engine",
                symbol=row.get("symbol", "UNKNOWN"),
                queue_type="verification_failure",
                priority=row.get("priority", "critical"),
                stage_key=row.get("stage_key", "unknown"),
                script_stage=row.get("next_script_stage", "unknown"),
                reason=row.get("reason", "Verification failure."),
                command=(
                    "python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py "
                    f"--stages {row.get('next_script_stage', 'unknown')} "
                    f"--symbols {row.get('symbol', 'UNKNOWN')}"
                ),
            )

    if not actions.empty:
        for _, row in actions.iterrows():
            next_stage = str(row.get("next_script_stage", "none"))

            if next_stage == "none":
                continue

            add_queue_item(
                rows=rows,
                source="decision_engine",
                symbol=row.get("symbol", "UNKNOWN"),
                queue_type="pipeline_action",
                priority=row.get("priority", "medium"),
                stage_key=row.get("next_stage_key", "unknown"),
                script_stage=next_stage,
                reason=row.get("reason", "Pipeline action required."),
                command=(
                    "python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py "
                    f"--stages {next_stage} "
                    f"--symbols {row.get('symbol', 'UNKNOWN')}"
                ),
            )

    # If pipeline is complete, create research-readiness queue items.
    if actions.empty and failures.empty and recovery.empty and not registry.empty:
        for _, row in registry.iterrows():
            symbol = row["symbol"]

            add_queue_item(
                rows=rows,
                source="state_registry",
                symbol=symbol,
                queue_type="research_ready",
                priority="medium",
                stage_key="extended_horizon_research",
                script_stage="EH01",
                reason=(
                    "Configured ingestion pipeline is verified complete. "
                    "Symbol is ready for extended-horizon research processing."
                ),
                command=(
                    "python scripts/dukascopy_extended_horizons/"
                    f"01_build_extended_horizon_targets.py --symbol {symbol}"
                ),
            )

    queue = pd.DataFrame(rows)

    if queue.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "symbol",
                "queue_type",
                "priority",
                "priority_rank",
                "stage_key",
                "script_stage",
                "reason",
                "command",
                "queue_status",
            ]
        )

    queue = queue.drop_duplicates(
        subset=["symbol", "queue_type", "script_stage"],
        keep="first",
    ).copy()

    queue["queue_score"] = (
        (100 - queue["priority_rank"].fillna(50))
        + np.where(queue["queue_type"] == "recovery", 50, 0)
        + np.where(queue["queue_type"] == "verification_failure", 40, 0)
        + np.where(queue["queue_type"] == "pipeline_action", 30, 0)
        + np.where(queue["queue_type"] == "research_ready", 10, 0)
    )

    queue = queue.sort_values(
        by=["priority_rank", "queue_score", "symbol"],
        ascending=[True, False, True],
    )

    return queue


def build_summary(queue: pd.DataFrame) -> pd.DataFrame:
    if queue.empty:
        return pd.DataFrame()

    return (
        queue.groupby(["queue_type", "priority"], dropna=False)
        .agg(
            jobs=("symbol", "count"),
            symbols=("symbol", lambda x: ", ".join(sorted(set(x.astype(str))))),
        )
        .reset_index()
        .sort_values(by=["priority", "queue_type"])
    )


def write_outputs(queue: pd.DataFrame, summary: pd.DataFrame, output_root: Path) -> None:
    queue_root = output_root.parent / "dukascopy_research_queue_manager"
    queue_root.mkdir(parents=True, exist_ok=True)

    queue_path = queue_root / "dukascopy_research_queue_latest.csv"
    priority_path = queue_root / "dukascopy_research_priority_queue_latest.csv"
    summary_path = queue_root / "dukascopy_research_queue_summary_latest.csv"
    report_path = queue_root / "dukascopy_research_queue_report_latest.txt"
    commands_path = queue_root / "dukascopy_research_queue_commands_latest.txt"

    queue.to_csv(queue_path, index=False)

    priority_queue = queue[queue["queue_status"] == "pending"].copy()
    priority_queue.to_csv(priority_path, index=False)

    summary.to_csv(summary_path, index=False)

    with open(commands_path, "w", encoding="utf-8") as f:
        if priority_queue.empty:
            f.write("# No queued commands.\n")
        else:
            for command in priority_queue["command"].tolist():
                f.write(command + "\n")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 69 - RESEARCH QUEUE MANAGER\n")
        f.write("=" * 90 + "\n\n")

        if queue.empty:
            f.write("QUEUE STATUS: No work queued.\n")
        else:
            f.write(f"QUEUE STATUS: {len(queue)} job(s) queued.\n")

        f.write("\nQUEUE SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(summary.to_string(index=False) if not summary.empty else "No summary available.")
        f.write("\n\n")

        f.write("PRIORITY QUEUE\n")
        f.write("-" * 90 + "\n")
        f.write(priority_queue.to_string(index=False) if not priority_queue.empty else "No pending jobs.")
        f.write("\n\n")

        f.write("COMMANDS\n")
        f.write("-" * 90 + "\n")
        if priority_queue.empty:
            f.write("No queued commands.\n")
        else:
            for command in priority_queue["command"].tolist():
                f.write(command + "\n")

    print("=" * 90)
    print("BACQE DUKASCOPY 69 - RESEARCH QUEUE MANAGER")
    print("=" * 90)

    if queue.empty:
        print("[OK] No work queued.")
    else:
        print(f"[QUEUE] {len(queue)} job(s) queued.")
        print("-" * 90)
        print(summary.to_string(index=False) if not summary.empty else "No summary available.")
        print("-" * 90)
        print(priority_queue.to_string(index=False))

    print("-" * 90)
    print(f"Queue:          {queue_path}")
    print(f"Priority queue: {priority_path}")
    print(f"Summary:        {summary_path}")
    print(f"Commands:       {commands_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


def main() -> None:
    cfg = load_pipeline_config()

    output_root = Path(cfg["output_root"])

    registry_path = Path(cfg["state_registry_path"])
    actions_path = output_root / "dukascopy_pipeline_actions_latest.csv"

    verification_root = output_root.parent / "dukascopy_pipeline_verification_engine"
    failures_path = verification_root / "dukascopy_pipeline_verification_failures_latest.csv"

    recovery_root = output_root.parent / "dukascopy_pipeline_recovery_engine"
    recovery_path = recovery_root / "dukascopy_pipeline_recovery_plan_latest.csv"

    registry = safe_read_csv(registry_path)
    actions = safe_read_csv(actions_path)
    failures = safe_read_csv(failures_path)
    recovery = safe_read_csv(recovery_path)

    queue = build_queue(
        registry=registry,
        actions=actions,
        failures=failures,
        recovery=recovery,
    )

    summary = build_summary(queue)

    write_outputs(
        queue=queue,
        summary=summary,
        output_root=output_root,
    )


if __name__ == "__main__":
    main()