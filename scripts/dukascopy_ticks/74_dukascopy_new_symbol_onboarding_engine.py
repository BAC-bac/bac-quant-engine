"""
BACQE DUKASCOPY 74 - NEW SYMBOL ONBOARDING ENGINE

Purpose:
    Inspect each configured Dukascopy symbol and determine the exact
    ordered work required to onboard it through:

        Raw ticks
        Processed ticks
        Tick bars
        Tick imbalance bars
        Engineered features
        Horizon features
        Extended Horizons EH01-EH10
        Global Cohort EH11-EH13

This script does not execute processing.
It creates a safe, dependency-aware onboarding plan.

Reads:
    config/dukascopy_research.yaml

    Script 65:
        dukascopy_pipeline_state_registry_latest.csv

    Script 70:
        extended_horizon_research_symbol_summary_latest.csv

    Script 72:
        extended_horizon_global_cohort_registry_latest.csv

Outputs:
    onboarding_registry_latest.csv
    onboarding_actions_latest.csv
    onboarding_commands_latest.txt
    onboarding_report_latest.txt
"""

from __future__ import annotations

from pathlib import Path
import argparse
import yaml

from dukascopy_contract import get_symbol_metadata
import pandas as pd


CONFIG_PATH = Path("config/dukascopy_research.yaml")

BASE_DIR = Path("E:/Quant_Lab")

PIPELINE_STATE_PATH = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_pipeline_state_registry"
    / "dukascopy_pipeline_state_registry_latest.csv"
)

EH_SYMBOL_STATE_PATH = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "research_state_registry"
    / "extended_horizon_research_symbol_summary_latest.csv"
)

GLOBAL_COHORT_STATE_PATH = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "global_cohort_registry"
    / "extended_horizon_global_cohort_registry_latest.csv"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_symbol_onboarding"
)


INGESTION_STAGE_ORDER = [
    "RAW",
    "08",
    "09",
    "10",
    "23",
    "30",
]

EH_STAGE_ORDER = [
    "EH01",
    "EH02",
    "EH03",
    "EH04",
    "EH05",
    "EH06",
    "EH07",
    "EH08",
    "EH09",
    "EH10",
]


INGESTION_STAGE_DEFINITIONS = {
    "RAW": {
        "stage_name": "raw_ticks",
        "priority": "critical",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "07_download_dukascopy_date_range.py "
            "--symbol {symbol} "
            "--start-date {start_date} "
            "--end-date {end_date}"
        ),
    },
    "08": {
        "stage_name": "processed_ticks",
        "priority": "critical",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "08_normalise_dukascopy_date_range.py "
            "--symbol {symbol} "
            "--start-date {start_date} "
            "--end-date {end_date}"
        ),
    },
    "09": {
        "stage_name": "tick_bars",
        "priority": "high",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "09_build_dukascopy_tick_bars_date_range.py "
            "--symbol {symbol} "
            "--start-date {start_date} "
            "--end-date {end_date}"
        ),
    },
    "10": {
        "stage_name": "tick_imbalance_bars",
        "priority": "high",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "10_build_dukascopy_tibs_date_range.py "
            "--symbol {symbol} "
            "--start-date {start_date} "
            "--end-date {end_date}"
        ),
    },
    "23": {
        "stage_name": "engineered_features",
        "priority": "high",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "23_build_engineered_tick_features.py "
            "--symbol {symbol}"
        ),
    },
    "30": {
        "stage_name": "horizon_features",
        "priority": "medium",
        "command_template": (
            "python scripts/dukascopy_ticks/"
            "30_horizon_expansion_engine.py "
            "--symbol {symbol}"
        ),
    },
}


EH_STAGE_DEFINITIONS = {
    "EH01": {
        "stage_name": "extended_horizon_targets",
        "priority": "high",
        "script": "01_build_extended_horizon_targets.py",
    },
    "EH02": {
        "stage_name": "feature_discovery",
        "priority": "high",
        "script": "02_extended_horizon_feature_discovery.py",
    },
    "EH03": {
        "stage_name": "stability_engine",
        "priority": "high",
        "script": "03_extended_horizon_stability_engine.py",
    },
    "EH04": {
        "stage_name": "signal_validation",
        "priority": "high",
        "script": "04_extended_horizon_signal_validation.py",
    },
    "EH05": {
        "stage_name": "cost_survival",
        "priority": "medium",
        "script": "05_extended_horizon_cost_survival_engine.py",
    },
    "EH06": {
        "stage_name": "dynamic_cost",
        "priority": "medium",
        "script": "06_extended_horizon_dynamic_cost_engine.py",
    },
    "EH07": {
        "stage_name": "context_conditioning",
        "priority": "medium",
        "script": "07_extended_horizon_context_conditioning.py",
    },
    "EH08": {
        "stage_name": "regime_edge",
        "priority": "medium",
        "script": "08_extended_horizon_regime_edge_engine.py",
    },
    "EH09": {
        "stage_name": "regime_replay",
        "priority": "medium",
        "script": "09_extended_horizon_regime_replay_engine.py",
    },
    "EH10": {
        "stage_name": "monte_carlo",
        "priority": "medium",
        "script": "10_extended_horizon_monte_carlo_robustness.py",
    },
}


GLOBAL_STAGE_ORDER = ["EH11", "EH12", "EH13"]


PRIORITY_ORDER = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "complete": 99,
    "blocked": 100,
}


def load_yaml_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "dukascopy_research" not in config:
        raise KeyError("Missing top-level YAML key: dukascopy_research")

    return config["dukascopy_research"]


def load_csv_optional(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

    return df


def configured_symbols(config: dict) -> list[str]:
    symbols = config.get("symbols", [])

    if not symbols:
        raise ValueError("No configured Dukascopy symbols found.")

    return sorted({get_symbol_metadata(symbol).symbol for symbol in symbols if str(symbol).strip()})


def normalise_symbol_column(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "symbol" not in df.columns:
        return df

    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    return df


def ingestion_stage_statuses(
    symbol: str,
    pipeline_state: pd.DataFrame,
) -> dict[str, str]:
    defaults = {
        "RAW": "missing",
        "08": "missing",
        "09": "missing",
        "10": "missing",
        "23": "missing",
        "30": "missing",
    }

    if pipeline_state.empty:
        return defaults

    match = pipeline_state[pipeline_state["symbol"] == symbol]

    if match.empty:
        return defaults

    row = match.iloc[0]

    return {
        "RAW": str(row.get("raw_status", "missing")),
        "08": str(row.get("processed_tick_status", "missing")),
        "09": str(row.get("tick_bar_status", "missing")),
        "10": str(row.get("tib_status", "missing")),
        "23": str(row.get("engineered_feature_status", "missing")),
        "30": str(row.get("horizon_feature_status", "missing")),
    }


def ingestion_stage_complete(
    stage_key: str,
    observed_status: str,
) -> bool:
    accepted = {
        "RAW": {"complete_or_near_complete"},
        "08": {"certified_complete"},
        "09": {"present"},
        "10": {"present"},
        "23": {"partial", "complete_or_near_complete"},
        "30": {"partial", "complete_or_near_complete"},
    }

    return observed_status in accepted[stage_key]


def eh_symbol_state(
    symbol: str,
    eh_state: pd.DataFrame,
) -> tuple[str, str, float]:
    if eh_state.empty:
        return "not_started", "EH01", 0.0

    match = eh_state[eh_state["symbol"] == symbol]

    if match.empty:
        return "not_started", "EH01", 0.0

    row = match.iloc[0]

    return (
        str(row.get("research_status", "not_started")),
        str(row.get("next_missing_stage", "EH01")),
        float(row.get("completion_pct", 0.0)),
    )


def build_eh_command(
    symbol: str,
    stage_key: str,
) -> str:
    definition = EH_STAGE_DEFINITIONS[stage_key]

    return (
        "python scripts/dukascopy_extended_horizons/"
        f"{definition['script']} "
        f"--symbol {symbol}"
    )


def determine_symbol_action(
    symbol: str,
    start_date: str,
    end_date: str,
    pipeline_state: pd.DataFrame,
    eh_state: pd.DataFrame,
) -> dict:
    ingestion_statuses = ingestion_stage_statuses(
        symbol=symbol,
        pipeline_state=pipeline_state,
    )

    for stage_key in INGESTION_STAGE_ORDER:
        observed_status = ingestion_statuses[stage_key]

        if not ingestion_stage_complete(stage_key, observed_status):
            definition = INGESTION_STAGE_DEFINITIONS[stage_key]

            return {
                "symbol": symbol,
                "onboarding_status": "action_required",
                "layer": "ingestion",
                "next_stage": stage_key,
                "stage_name": definition["stage_name"],
                "priority": definition["priority"],
                "observed_status": observed_status,
                "reason": (
                    f"Ingestion stage {stage_key} is incomplete. "
                    f"Observed status: {observed_status}."
                ),
                "command": definition["command_template"].format(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                ),
            }

    research_status, next_eh_stage, completion_pct = eh_symbol_state(
        symbol=symbol,
        eh_state=eh_state,
    )

    if research_status != "complete":
        if next_eh_stage not in EH_STAGE_DEFINITIONS:
            return {
                "symbol": symbol,
                "onboarding_status": "definition_error",
                "layer": "extended_horizon",
                "next_stage": next_eh_stage,
                "stage_name": "unknown",
                "priority": "critical",
                "observed_status": research_status,
                "reason": (
                    f"No extended-horizon stage definition exists for "
                    f"{next_eh_stage}."
                ),
                "command": "",
            }

        definition = EH_STAGE_DEFINITIONS[next_eh_stage]

        return {
            "symbol": symbol,
            "onboarding_status": "action_required",
            "layer": "extended_horizon",
            "next_stage": next_eh_stage,
            "stage_name": definition["stage_name"],
            "priority": definition["priority"],
            "observed_status": research_status,
            "reason": (
                f"Extended-horizon research is {completion_pct:.2f}% complete. "
                f"Next missing stage: {next_eh_stage}."
            ),
            "command": build_eh_command(
                symbol=symbol,
                stage_key=next_eh_stage,
            ),
        }

    return {
        "symbol": symbol,
        "onboarding_status": "symbol_complete",
        "layer": "global_cohort",
        "next_stage": "GLOBAL",
        "stage_name": "global_cohort_refresh",
        "priority": "low",
        "observed_status": "eh01_eh10_complete",
        "reason": (
            "Symbol ingestion and EH01-EH10 are complete. "
            "Global cohort stages should be checked."
        ),
        "command": (
            "python scripts/dukascopy_ticks/"
            "72_extended_horizon_global_cohort_registry.py"
        ),
    }


def build_onboarding_registry(
    symbols: list[str],
    start_date: str,
    end_date: str,
    pipeline_state: pd.DataFrame,
    eh_state: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    for symbol in symbols:
        row = determine_symbol_action(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            pipeline_state=pipeline_state,
            eh_state=eh_state,
        )

        row["start_date"] = start_date
        row["end_date"] = end_date
        row["priority_rank"] = PRIORITY_ORDER.get(
            row["priority"],
            50,
        )

        rows.append(row)

    registry = pd.DataFrame(rows)

    return registry.sort_values(
        by=[
            "priority_rank",
            "layer",
            "next_stage",
            "symbol",
        ],
        ascending=[
            True,
            True,
            True,
            True,
        ],
    )


def add_global_cohort_action(
    registry: pd.DataFrame,
    global_state: pd.DataFrame,
) -> pd.DataFrame:
    if registry.empty:
        return registry

    symbol_work_remaining = registry[
        registry["onboarding_status"].isin(
            [
                "action_required",
                "definition_error",
            ]
        )
    ]

    if not symbol_work_remaining.empty:
        return registry

    if global_state.empty:
        global_row = {
            "symbol": "GLOBAL",
            "onboarding_status": "action_required",
            "layer": "global_cohort",
            "next_stage": "EH11",
            "stage_name": "cross_symbol_transfer",
            "priority": "high",
            "observed_status": "missing_registry",
            "reason": (
                "All symbols are complete through EH10, but the global "
                "cohort registry is missing."
            ),
            "command": (
                "python scripts/dukascopy_ticks/"
                "72_extended_horizon_global_cohort_registry.py"
            ),
            "start_date": "",
            "end_date": "",
            "priority_rank": PRIORITY_ORDER["high"],
        }

        return pd.concat(
            [
                registry,
                pd.DataFrame([global_row]),
            ],
            ignore_index=True,
        )

    incomplete_global = global_state[
        global_state["stage_status"]
        != "complete_for_current_cohort"
    ]

    if incomplete_global.empty:
        registry["onboarding_status"] = registry[
            "onboarding_status"
        ].replace(
            "symbol_complete",
            "fully_onboarded",
        )

        registry.loc[
            registry["onboarding_status"] == "fully_onboarded",
            "command",
        ] = ""

        registry.loc[
            registry["onboarding_status"] == "fully_onboarded",
            "reason",
        ] = (
            "Symbol ingestion, EH01-EH10 and global cohort stages are current."
        )

        return registry

    first_incomplete = incomplete_global.iloc[0]

    global_row = {
        "symbol": "GLOBAL",
        "onboarding_status": "action_required",
        "layer": "global_cohort",
        "next_stage": str(first_incomplete["stage_key"]),
        "stage_name": str(first_incomplete["stage_name"]),
        "priority": "high",
        "observed_status": str(
            first_incomplete["stage_status"]
        ),
        "reason": str(first_incomplete["reason"]),
        "command": (
            "python scripts/dukascopy_ticks/"
            "73_extended_horizon_global_cohort_decision_engine.py"
        ),
        "start_date": "",
        "end_date": "",
        "priority_rank": PRIORITY_ORDER["high"],
    }

    return pd.concat(
        [
            registry,
            pd.DataFrame([global_row]),
        ],
        ignore_index=True,
    )


def build_summary(
    registry: pd.DataFrame,
) -> pd.DataFrame:
    if registry.empty:
        return pd.DataFrame()

    return (
        registry.groupby(
            [
                "onboarding_status",
                "layer",
                "priority",
            ],
            dropna=False,
        )
        .agg(
            jobs=("symbol", "count"),
            symbols=(
                "symbol",
                lambda values: ", ".join(
                    sorted(
                        set(
                            values.astype(str)
                        )
                    )
                ),
            ),
        )
        .reset_index()
        .sort_values(
            by=[
                "priority",
                "layer",
                "onboarding_status",
            ],
        )
    )


def filter_requested_symbols(
    all_symbols: list[str],
    requested_symbols: list[str] | None,
) -> list[str]:
    if not requested_symbols:
        return all_symbols

    requested = {
        symbol.upper()
        for symbol in requested_symbols
    }

    unknown = requested - set(all_symbols)

    if unknown:
        raise ValueError(
            "Requested symbols are not present in YAML: "
            f"{sorted(unknown)}"
        )

    return [
        symbol
        for symbol in all_symbols
        if symbol in requested
    ]


def write_outputs(
    registry: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    registry_path = (
        REPORT_ROOT
        / "dukascopy_symbol_onboarding_registry_latest.csv"
    )

    actions_path = (
        REPORT_ROOT
        / "dukascopy_symbol_onboarding_actions_latest.csv"
    )

    commands_path = (
        REPORT_ROOT
        / "dukascopy_symbol_onboarding_commands_latest.txt"
    )

    summary_path = (
        REPORT_ROOT
        / "dukascopy_symbol_onboarding_summary_latest.csv"
    )

    report_path = (
        REPORT_ROOT
        / "dukascopy_symbol_onboarding_report_latest.txt"
    )

    registry.to_csv(
        registry_path,
        index=False,
    )

    actions = registry[
        registry["onboarding_status"].isin(
            [
                "action_required",
                "definition_error",
            ]
        )
    ].copy()

    actions.to_csv(
        actions_path,
        index=False,
    )

    summary.to_csv(
        summary_path,
        index=False,
    )

    with open(
        commands_path,
        "w",
        encoding="utf-8",
    ) as file:
        if actions.empty:
            file.write(
                "# No onboarding actions required.\n"
            )
        else:
            for command in actions["command"]:
                command = str(command).strip()

                if command:
                    file.write(command + "\n")

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 74 - NEW SYMBOL ONBOARDING ENGINE\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write("ONBOARDING SUMMARY\n")
        file.write("-" * 100 + "\n")
        file.write(
            summary.to_string(index=False)
            if not summary.empty
            else "No summary available."
        )
        file.write("\n\n")

        file.write("ONBOARDING REGISTRY\n")
        file.write("-" * 100 + "\n")
        file.write(registry.to_string(index=False))
        file.write("\n\n")

        file.write("ACTION COMMANDS\n")
        file.write("-" * 100 + "\n")

        if actions.empty:
            file.write("No onboarding actions required.\n")
        else:
            for command in actions["command"]:
                command = str(command).strip()

                if command:
                    file.write(command + "\n")

    print("=" * 100)
    print("BACQE DUKASCOPY 74 - NEW SYMBOL ONBOARDING ENGINE")
    print("=" * 100)

    print("ONBOARDING SUMMARY")
    print("-" * 100)

    print(
        summary.to_string(index=False)
        if not summary.empty
        else "No summary available."
    )

    print("-" * 100)
    print("ONBOARDING REGISTRY")
    print("-" * 100)
    print(registry.to_string(index=False))

    print("-" * 100)

    if actions.empty:
        print("[COMPLETE] No onboarding actions required.")
    else:
        print(
            f"[ACTION REQUIRED] {len(actions)} onboarding action(s)."
        )

        for command in actions["command"]:
            command = str(command).strip()

            if command:
                print(command)

    print("-" * 100)
    print(f"Registry: {registry_path}")
    print(f"Actions:  {actions_path}")
    print(f"Commands: {commands_path}")
    print(f"Summary:  {summary_path}")
    print(f"Report:   {report_path}")
    print("=" * 100)


def main(
    requested_symbols: list[str] | None,
) -> None:
    config = load_yaml_config()

    all_symbols = configured_symbols(config)

    symbols = filter_requested_symbols(
        all_symbols=all_symbols,
        requested_symbols=requested_symbols,
    )

    start_date = str(
        config["date_range"]["start"]
    )

    end_date = str(
        config["date_range"]["end"]
    )

    pipeline_state = normalise_symbol_column(
        load_csv_optional(PIPELINE_STATE_PATH)
    )

    eh_state = normalise_symbol_column(
        load_csv_optional(EH_SYMBOL_STATE_PATH)
    )

    global_state = load_csv_optional(
        GLOBAL_COHORT_STATE_PATH
    )

    registry = build_onboarding_registry(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        pipeline_state=pipeline_state,
        eh_state=eh_state,
    )

    registry = add_global_cohort_action(
        registry=registry,
        global_state=global_state,
    )

    registry = registry.sort_values(
        by=[
            "priority_rank",
            "layer",
            "next_stage",
            "symbol",
        ],
        ascending=True,
    )

    summary = build_summary(registry)

    write_outputs(
        registry=registry,
        summary=summary,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help=(
            "Optional symbol filter. "
            "Defaults to every symbol configured in YAML."
        ),
    )

    args = parser.parse_args()

    main(
        requested_symbols=args.symbols,
    )
