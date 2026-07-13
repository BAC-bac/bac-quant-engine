"""
BACQE DUKASCOPY EXTENDED HORIZONS 73
GLOBAL COHORT DECISION ENGINE

Purpose:
    Read the Script 72 global cohort registry and decide which global
    extended-horizon stage should run next.

Global dependency chain:
    EH11 - Cross-symbol transfer
    EH12 - Cross-year stability
    EH13 - Candidate registry

Rules:
    - EH12 cannot run until EH11 is current for the configured cohort.
    - EH13 cannot run until EH12 is current for the configured cohort.
    - Only the earliest incomplete or stale stage is scheduled.
    - Later stages are marked blocked.
    - No research processing is executed by this script.

Reads:
    config/dukascopy_research.yaml

    E:/Quant_Lab/data/analysis/dukascopy_extended_horizons/
        global_cohort_registry/
        extended_horizon_global_cohort_registry_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/dukascopy_extended_horizons/
        global_cohort_decision_engine/
"""

from __future__ import annotations

from pathlib import Path
import yaml
import pandas as pd


CONFIG_PATH = Path("config/dukascopy_research.yaml")

BASE_DIR = Path("E:/Quant_Lab")

EXTENDED_HORIZON_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
)

COHORT_REGISTRY_ROOT = (
    EXTENDED_HORIZON_ROOT
    / "global_cohort_registry"
)

REPORT_ROOT = (
    EXTENDED_HORIZON_ROOT
    / "global_cohort_decision_engine"
)


GLOBAL_STAGE_ORDER = ["EH11", "EH12", "EH13"]


GLOBAL_STAGE_DEFINITIONS = {
    "EH11": {
        "stage_name": "cross_symbol_transfer",
        "script_path": (
            "scripts/dukascopy_extended_horizons/"
            "11_extended_horizon_cross_symbol_transfer.py"
        ),
        "priority": "high",
    },
    "EH12": {
        "stage_name": "cross_year_stability",
        "script_path": (
            "scripts/dukascopy_extended_horizons/"
            "12_extended_horizon_cross_year_stability.py"
        ),
        "priority": "high",
    },
    "EH13": {
        "stage_name": "candidate_registry",
        "script_path": (
            "scripts/dukascopy_extended_horizons/"
            "13_extended_horizon_candidate_registry.py"
        ),
        "priority": "medium",
    },
}


COMPLETE_STATUS = "complete_for_current_cohort"


def load_research_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing research config: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "dukascopy_research" not in config:
        raise KeyError("Missing top-level key: dukascopy_research")

    return config["dukascopy_research"]


def configured_symbols(config: dict) -> list[str]:
    symbols = config.get("symbols", [])

    if not symbols:
        raise ValueError("No symbols configured in dukascopy_research.yaml")

    return sorted(
        {
            str(symbol).upper().strip()
            for symbol in symbols
            if str(symbol).strip()
        }
    )


def load_cohort_registry() -> pd.DataFrame:
    path = (
        COHORT_REGISTRY_ROOT
        / "extended_horizon_global_cohort_registry_latest.csv"
    )

    if not path.exists():
        raise FileNotFoundError(
            "Missing Script 72 global cohort registry. "
            f"Run Script 72 first:\n{path}"
        )

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"Global cohort registry is empty: {path}")

    required_columns = {
        "stage_key",
        "stage_name",
        "stage_status",
        "missing_symbols",
        "reason",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Global cohort registry is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    df["stage_key"] = df["stage_key"].astype(str).str.upper()
    df["stage_status"] = df["stage_status"].astype(str).str.strip()

    return df


def select_base_symbol(symbols: list[str]) -> str:
    """
    EURJPY remains the current discovery/base symbol.

    Future improvement:
        Move this to YAML under a global_research section.
    """

    preferred_base = "EURJPY"

    if preferred_base in symbols:
        return preferred_base

    return symbols[0]


def target_symbols(
    symbols: list[str],
    base_symbol: str,
) -> list[str]:
    return [
        symbol
        for symbol in symbols
        if symbol != base_symbol
    ]


def command_for_stage(
    stage_key: str,
    base_symbol: str,
    targets: list[str],
) -> str:
    definition = GLOBAL_STAGE_DEFINITIONS[stage_key]
    script_path = definition["script_path"]

    target_text = " ".join(targets)

    if stage_key == "EH11":
        return (
            f"python {script_path} "
            f"--base-symbol {base_symbol} "
            f"--symbols {target_text}"
        )

    if stage_key == "EH12":
        return (
            f"python {script_path} "
            f"--base-symbol {base_symbol} "
            f"--symbols {target_text}"
        )

    if stage_key == "EH13":
        return (
            f"python {script_path} "
            f"--base-symbol {base_symbol} "
            f"--symbols {target_text}"
        )

    return ""


def stage_status_lookup(
    cohort_registry: pd.DataFrame,
) -> dict[str, str]:
    return {
        str(row["stage_key"]): str(row["stage_status"])
        for _, row in cohort_registry.iterrows()
    }


def registry_row_lookup(
    cohort_registry: pd.DataFrame,
) -> dict[str, pd.Series]:
    return {
        str(row["stage_key"]): row
        for _, row in cohort_registry.iterrows()
    }


def find_first_incomplete_stage(
    statuses: dict[str, str],
) -> str | None:
    for stage_key in GLOBAL_STAGE_ORDER:
        if statuses.get(stage_key) != COMPLETE_STATUS:
            return stage_key

    return None


def priority_rank(priority: str) -> int:
    mapping = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
        "complete": 99,
        "blocked": 100,
    }

    return mapping.get(str(priority).lower(), 50)


def build_decision_plan(
    cohort_registry: pd.DataFrame,
    symbols: list[str],
) -> pd.DataFrame:
    statuses = stage_status_lookup(cohort_registry)
    registry_rows = registry_row_lookup(cohort_registry)

    base_symbol = select_base_symbol(symbols)
    targets = target_symbols(symbols, base_symbol)

    first_incomplete_stage = find_first_incomplete_stage(statuses)

    rows = []

    for stage_key in GLOBAL_STAGE_ORDER:
        definition = GLOBAL_STAGE_DEFINITIONS[stage_key]
        observed_status = statuses.get(stage_key, "missing")

        registry_row = registry_rows.get(stage_key)

        represented_symbols = ""
        missing_symbols = ""
        cohort_reason = "Stage missing from Script 72 registry."

        if registry_row is not None:
            represented_symbols = str(
                registry_row.get("represented_symbols", "")
            )

            missing_symbols = str(
                registry_row.get("missing_symbols", "")
            )

            cohort_reason = str(
                registry_row.get("reason", "")
            )

        if observed_status == COMPLETE_STATUS:
            decision_status = "complete"
            priority = "complete"
            command = ""
            reason = (
                f"{stage_key} is complete for the current configured cohort."
            )

        elif stage_key == first_incomplete_stage:
            decision_status = "action_required"
            priority = definition["priority"]
            command = command_for_stage(
                stage_key=stage_key,
                base_symbol=base_symbol,
                targets=targets,
            )

            reason = (
                f"{stage_key} is the earliest global stage that is not "
                f"complete for the current cohort. "
                f"Observed status: {observed_status}. "
                f"{cohort_reason}"
            )

        else:
            earlier_incomplete_stages = [
                earlier_stage
                for earlier_stage in GLOBAL_STAGE_ORDER
                if GLOBAL_STAGE_ORDER.index(earlier_stage)
                < GLOBAL_STAGE_ORDER.index(stage_key)
                and statuses.get(earlier_stage) != COMPLETE_STATUS
            ]

            if earlier_incomplete_stages:
                blocking_stage = earlier_incomplete_stages[0]

                decision_status = "blocked"
                priority = "blocked"
                command = ""
                reason = (
                    f"{stage_key} is blocked until {blocking_stage} is "
                    "complete for the current cohort."
                )
            else:
                decision_status = "action_required"
                priority = definition["priority"]
                command = command_for_stage(
                    stage_key=stage_key,
                    base_symbol=base_symbol,
                    targets=targets,
                )

                reason = (
                    f"{stage_key} requires execution for the current cohort. "
                    f"Observed status: {observed_status}."
                )

        rows.append(
            {
                "stage_key": stage_key,
                "stage_name": definition["stage_name"],
                "observed_stage_status": observed_status,
                "decision_status": decision_status,
                "priority": priority,
                "base_symbol": base_symbol,
                "target_symbols": ", ".join(targets),
                "configured_symbols": ", ".join(symbols),
                "represented_symbols": represented_symbols,
                "missing_symbols": missing_symbols,
                "reason": reason,
                "command": command,
                "priority_rank": priority_rank(priority),
            }
        )

    plan = pd.DataFrame(rows)

    return plan.sort_values(
        by=["priority_rank", "stage_key"],
        ascending=[True, True],
    )


def build_summary(plan: pd.DataFrame) -> pd.DataFrame:
    return (
        plan.groupby(
            [
                "decision_status",
                "priority",
            ],
            dropna=False,
        )
        .agg(
            stages=("stage_key", "count"),
            stage_keys=(
                "stage_key",
                lambda values: ", ".join(values.astype(str)),
            ),
        )
        .reset_index()
        .sort_values(
            by=["priority", "decision_status"],
            ascending=[True, True],
        )
    )


def write_outputs(
    plan: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    plan_path = (
        REPORT_ROOT
        / "extended_horizon_global_execution_plan_latest.csv"
    )

    actions_path = (
        REPORT_ROOT
        / "extended_horizon_global_actions_latest.csv"
    )

    commands_path = (
        REPORT_ROOT
        / "extended_horizon_global_commands_latest.txt"
    )

    summary_path = (
        REPORT_ROOT
        / "extended_horizon_global_decision_summary_latest.csv"
    )

    report_path = (
        REPORT_ROOT
        / "extended_horizon_global_decision_report_latest.txt"
    )

    plan.to_csv(plan_path, index=False)

    actions = plan[
        plan["decision_status"] == "action_required"
    ].copy()

    actions.to_csv(actions_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(commands_path, "w", encoding="utf-8") as file:
        if actions.empty:
            file.write("# No global cohort actions required.\n")
        else:
            for command in actions["command"]:
                if str(command).strip():
                    file.write(str(command).strip() + "\n")

    with open(report_path, "w", encoding="utf-8") as file:
        file.write(
            "BACQE DUKASCOPY EXTENDED HORIZONS 73\n"
            "GLOBAL COHORT DECISION ENGINE\n"
        )

        file.write("=" * 100 + "\n\n")

        file.write("DECISION SUMMARY\n")
        file.write("-" * 100 + "\n")
        file.write(summary.to_string(index=False))
        file.write("\n\n")

        file.write("GLOBAL EXECUTION PLAN\n")
        file.write("-" * 100 + "\n")
        file.write(plan.to_string(index=False))
        file.write("\n\n")

        file.write("ACTION COMMANDS\n")
        file.write("-" * 100 + "\n")

        if actions.empty:
            file.write("No actions required.\n")
        else:
            for command in actions["command"]:
                if str(command).strip():
                    file.write(str(command).strip() + "\n")

    print("=" * 100)
    print(
        "BACQE DUKASCOPY EXTENDED HORIZONS 73 "
        "- GLOBAL COHORT DECISION ENGINE"
    )
    print("=" * 100)

    print("DECISION SUMMARY")
    print("-" * 100)
    print(summary.to_string(index=False))

    print("-" * 100)
    print("GLOBAL EXECUTION PLAN")
    print("-" * 100)
    print(plan.to_string(index=False))

    print("-" * 100)

    if actions.empty:
        print("[COMPLETE] No global cohort actions required.")
    else:
        print(
            f"[ACTION REQUIRED] {len(actions)} global stage action(s)."
        )

        for command in actions["command"]:
            if str(command).strip():
                print(str(command).strip())

    print("-" * 100)
    print(f"Execution plan: {plan_path}")
    print(f"Actions:        {actions_path}")
    print(f"Commands:       {commands_path}")
    print(f"Summary:        {summary_path}")
    print(f"Report:         {report_path}")
    print("=" * 100)


def main() -> None:
    config = load_research_config()
    symbols = configured_symbols(config)

    cohort_registry = load_cohort_registry()

    plan = build_decision_plan(
        cohort_registry=cohort_registry,
        symbols=symbols,
    )

    summary = build_summary(plan)

    write_outputs(
        plan=plan,
        summary=summary,
    )


if __name__ == "__main__":
    main()