"""
BACQE DUKASCOPY EXTENDED HORIZONS 72
GLOBAL COHORT REGISTRY

Purpose:
    Determine whether the global extended-horizon research stages EH11-EH13
    represent the current symbol cohort configured in:

        config/dukascopy_research.yaml

The registry compares:

    1. Configured symbol universe
    2. EH01-EH10 symbol completion state from Script 70
    3. Symbols encoded in EH11-EH13 output filenames
    4. Symbols found inside output files where suitable columns exist

Global stages:
    EH11 - Cross-symbol transfer
    EH12 - Cross-year stability
    EH13 - Candidate registry

Possible statuses:
    complete_for_current_cohort
    stale_for_current_cohort
    blocked_symbol_stages_incomplete
    missing
    unreadable
"""

from __future__ import annotations

from pathlib import Path
import re
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

STATE_ROOT = EXTENDED_HORIZON_ROOT / "research_state_registry"

REPORT_ROOT = EXTENDED_HORIZON_ROOT / "global_cohort_registry"


GLOBAL_STAGE_DEFINITIONS = [
    {
        "stage_key": "EH11",
        "stage_name": "cross_symbol_transfer",
        "root": EXTENDED_HORIZON_ROOT / "cross_symbol_transfer",
        "filename_contains": "cross_symbol_transfer_ranked_latest.csv",
        "symbol_columns": [
            "base_symbol",
            "test_symbol",
            "source_symbol",
            "target_symbol",
            "symbol",
        ],
    },
    {
        "stage_key": "EH12",
        "stage_name": "cross_year_stability",
        "root": EXTENDED_HORIZON_ROOT / "cross_year_stability",
        "filename_contains": "cross_year_stability_ranked_latest.csv",
        "symbol_columns": [
            "base_symbol",
            "test_symbol",
            "source_symbol",
            "target_symbol",
            "symbol",
        ],
    },
    {
        "stage_key": "EH13",
        "stage_name": "candidate_registry",
        "root": EXTENDED_HORIZON_ROOT / "candidate_registry",
        "filename_contains": "candidate_registry_latest.csv",
        "symbol_columns": [
            "base_symbol",
            "test_symbol",
            "candidate_symbol",
            "source_symbol",
            "target_symbol",
            "symbol",
        ],
    },
]


KNOWN_SYMBOL_PATTERN = re.compile(r"[A-Z]{6}")


def load_yaml_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "dukascopy_research" not in config:
        raise KeyError("Missing top-level YAML key: dukascopy_research")

    return config["dukascopy_research"]


def configured_symbols(config: dict) -> list[str]:
    symbols = config.get("symbols", [])

    if not symbols:
        raise ValueError("No symbols configured in dukascopy_research.yaml")

    return sorted({str(symbol).upper() for symbol in symbols})


def load_symbol_summary() -> pd.DataFrame:
    path = STATE_ROOT / "extended_horizon_research_symbol_summary_latest.csv"

    if not path.exists():
        raise FileNotFoundError(
            "Missing Script 70 symbol summary. Run Script 70 first:\n"
            f"{path}"
        )

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"Script 70 symbol summary is empty: {path}")

    df["symbol"] = df["symbol"].astype(str).str.upper()

    return df


def completed_symbol_set(symbol_summary: pd.DataFrame) -> set[str]:
    completed = symbol_summary.loc[
        symbol_summary["research_status"].astype(str) == "complete",
        "symbol",
    ]

    return set(completed.astype(str).str.upper())


def find_stage_files(stage_definition: dict) -> list[Path]:
    root = stage_definition["root"]
    filename_contains = stage_definition["filename_contains"]

    if not root.exists():
        return []

    return sorted(
        path
        for path in root.rglob("*.csv")
        if filename_contains in path.name
    )


def extract_symbols_from_filename(
    path: Path,
    known_symbols: set[str],
) -> set[str]:
    name_upper = path.stem.upper()

    found = {
        symbol
        for symbol in known_symbols
        if symbol in name_upper
    }

    if found:
        return found

    generic_matches = set(KNOWN_SYMBOL_PATTERN.findall(name_upper))

    return generic_matches.intersection(known_symbols)


def extract_symbols_from_dataframe(
    path: Path,
    symbol_columns: list[str],
    known_symbols: set[str],
) -> tuple[set[str], int, str]:
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        return set(), 0, f"{type(exc).__name__}: {exc}"

    if df.empty:
        return set(), 0, ""

    found: set[str] = set()

    for column in symbol_columns:
        if column not in df.columns:
            continue

        values = (
            df[column]
            .dropna()
            .astype(str)
            .str.upper()
            .str.strip()
        )

        for value in values:
            if value in known_symbols:
                found.add(value)

    return found, len(df), ""


def inspect_stage_file(
    path: Path,
    stage_definition: dict,
    known_symbols: set[str],
) -> dict:
    filename_symbols = extract_symbols_from_filename(
        path=path,
        known_symbols=known_symbols,
    )

    data_symbols, rows, read_error = extract_symbols_from_dataframe(
        path=path,
        symbol_columns=stage_definition["symbol_columns"],
        known_symbols=known_symbols,
    )

    represented_symbols = filename_symbols.union(data_symbols)

    return {
        "stage_key": stage_definition["stage_key"],
        "stage_name": stage_definition["stage_name"],
        "file_name": path.name,
        "file_path": str(path),
        "rows": rows,
        "filename_symbols": ", ".join(sorted(filename_symbols)),
        "data_symbols": ", ".join(sorted(data_symbols)),
        "represented_symbols": ", ".join(sorted(represented_symbols)),
        "represented_symbol_count": len(represented_symbols),
        "read_status": "error" if read_error else "ok",
        "read_error": read_error,
    }


def build_file_registry(
    stage_definitions: list[dict],
    configured_symbol_set: set[str],
) -> pd.DataFrame:
    rows = []

    for stage_definition in stage_definitions:
        files = find_stage_files(stage_definition)

        for path in files:
            rows.append(
                inspect_stage_file(
                    path=path,
                    stage_definition=stage_definition,
                    known_symbols=configured_symbol_set,
                )
            )

    columns = [
        "stage_key",
        "stage_name",
        "file_name",
        "file_path",
        "rows",
        "filename_symbols",
        "data_symbols",
        "represented_symbols",
        "represented_symbol_count",
        "read_status",
        "read_error",
    ]

    return pd.DataFrame(rows, columns=columns)


def symbols_from_text(value: object) -> set[str]:
    if pd.isna(value):
        return set()

    text = str(value).strip()

    if not text:
        return set()

    return {
        token.strip().upper()
        for token in text.split(",")
        if token.strip()
    }


def determine_stage_status(
    files: pd.DataFrame,
    configured_symbol_set: set[str],
    completed_symbols: set[str],
) -> dict:
    configured_count = len(configured_symbol_set)

    symbol_stage_complete = configured_symbol_set.issubset(completed_symbols)

    if not symbol_stage_complete:
        missing_symbol_research = configured_symbol_set - completed_symbols

        return {
            "stage_status": "blocked_symbol_stages_incomplete",
            "represented_symbols": set(),
            "missing_symbols": configured_symbol_set,
            "extra_symbols": set(),
            "reason": (
                "EH01-EH10 are not complete for all configured symbols. "
                f"Missing: {sorted(missing_symbol_research)}"
            ),
        }

    if files.empty:
        return {
            "stage_status": "missing",
            "represented_symbols": set(),
            "missing_symbols": configured_symbol_set,
            "extra_symbols": set(),
            "reason": "No matching global-stage output files were found.",
        }

    readable_files = files[files["read_status"] == "ok"]

    if readable_files.empty:
        return {
            "stage_status": "unreadable",
            "represented_symbols": set(),
            "missing_symbols": configured_symbol_set,
            "extra_symbols": set(),
            "reason": "Matching files exist, but none could be read successfully.",
        }

    represented_symbols: set[str] = set()

    for value in readable_files["represented_symbols"]:
        represented_symbols.update(symbols_from_text(value))

    missing_symbols = configured_symbol_set - represented_symbols
    extra_symbols = represented_symbols - configured_symbol_set

    if not missing_symbols and configured_count > 0:
        stage_status = "complete_for_current_cohort"
        reason = "Global output represents every configured symbol."
    else:
        stage_status = "stale_for_current_cohort"
        reason = (
            "Global output does not represent the current configured cohort. "
            f"Missing symbols: {sorted(missing_symbols)}"
        )

    return {
        "stage_status": stage_status,
        "represented_symbols": represented_symbols,
        "missing_symbols": missing_symbols,
        "extra_symbols": extra_symbols,
        "reason": reason,
    }


def build_global_cohort_registry(
    stage_definitions: list[dict],
    file_registry: pd.DataFrame,
    configured_symbol_set: set[str],
    completed_symbols: set[str],
) -> pd.DataFrame:
    rows = []

    for stage_definition in stage_definitions:
        stage_key = stage_definition["stage_key"]

        if file_registry.empty:
            stage_files = pd.DataFrame()
        else:
            stage_files = file_registry[
                file_registry["stage_key"] == stage_key
            ].copy()

        result = determine_stage_status(
            files=stage_files,
            configured_symbol_set=configured_symbol_set,
            completed_symbols=completed_symbols,
        )

        rows.append(
            {
                "stage_key": stage_key,
                "stage_name": stage_definition["stage_name"],
                "stage_status": result["stage_status"],
                "files_found": len(stage_files),
                "configured_symbols": ", ".join(sorted(configured_symbol_set)),
                "configured_symbol_count": len(configured_symbol_set),
                "completed_eh01_eh10_symbols": ", ".join(
                    sorted(completed_symbols)
                ),
                "represented_symbols": ", ".join(
                    sorted(result["represented_symbols"])
                ),
                "represented_symbol_count": len(
                    result["represented_symbols"]
                ),
                "missing_symbols": ", ".join(
                    sorted(result["missing_symbols"])
                ),
                "missing_symbol_count": len(result["missing_symbols"]),
                "extra_symbols": ", ".join(
                    sorted(result["extra_symbols"])
                ),
                "reason": result["reason"],
            }
        )

    return pd.DataFrame(rows)


def build_cohort_summary(
    configured_symbol_set: set[str],
    completed_symbols: set[str],
) -> pd.DataFrame:
    rows = []

    for symbol in sorted(configured_symbol_set):
        rows.append(
            {
                "symbol": symbol,
                "configured": True,
                "eh01_eh10_complete": symbol in completed_symbols,
                "cohort_eligible": symbol in completed_symbols,
            }
        )

    return pd.DataFrame(rows)


def write_outputs(
    cohort_registry: pd.DataFrame,
    file_registry: pd.DataFrame,
    cohort_summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    cohort_path = (
        REPORT_ROOT
        / "extended_horizon_global_cohort_registry_latest.csv"
    )

    files_path = (
        REPORT_ROOT
        / "extended_horizon_global_cohort_file_registry_latest.csv"
    )

    summary_path = (
        REPORT_ROOT
        / "extended_horizon_global_cohort_symbol_summary_latest.csv"
    )

    report_path = (
        REPORT_ROOT
        / "extended_horizon_global_cohort_report_latest.txt"
    )

    cohort_registry.to_csv(cohort_path, index=False)
    file_registry.to_csv(files_path, index=False)
    cohort_summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as file:
        file.write(
            "BACQE DUKASCOPY EXTENDED HORIZONS 72\n"
            "GLOBAL COHORT REGISTRY\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write("GLOBAL STAGE STATUS\n")
        file.write("-" * 100 + "\n")
        file.write(cohort_registry.to_string(index=False))
        file.write("\n\n")

        file.write("COHORT SYMBOL ELIGIBILITY\n")
        file.write("-" * 100 + "\n")
        file.write(cohort_summary.to_string(index=False))
        file.write("\n\n")

        file.write("GLOBAL OUTPUT FILE INSPECTION\n")
        file.write("-" * 100 + "\n")

        if file_registry.empty:
            file.write("No matching global-stage files found.\n")
        else:
            file.write(file_registry.to_string(index=False))

    print("=" * 100)
    print(
        "BACQE DUKASCOPY EXTENDED HORIZONS 72 "
        "- GLOBAL COHORT REGISTRY"
    )
    print("=" * 100)

    print("GLOBAL STAGE STATUS")
    print("-" * 100)
    print(cohort_registry.to_string(index=False))

    print("-" * 100)
    print("COHORT SYMBOL ELIGIBILITY")
    print("-" * 100)
    print(cohort_summary.to_string(index=False))

    print("-" * 100)
    print(f"Cohort registry: {cohort_path}")
    print(f"File registry:   {files_path}")
    print(f"Symbol summary:  {summary_path}")
    print(f"Report:          {report_path}")
    print("=" * 100)


def main() -> None:
    config = load_yaml_config()

    symbols = configured_symbols(config)
    configured_symbol_set = set(symbols)

    symbol_summary = load_symbol_summary()
    completed_symbols = completed_symbol_set(symbol_summary)

    file_registry = build_file_registry(
        stage_definitions=GLOBAL_STAGE_DEFINITIONS,
        configured_symbol_set=configured_symbol_set,
    )

    cohort_registry = build_global_cohort_registry(
        stage_definitions=GLOBAL_STAGE_DEFINITIONS,
        file_registry=file_registry,
        configured_symbol_set=configured_symbol_set,
        completed_symbols=completed_symbols,
    )

    cohort_summary = build_cohort_summary(
        configured_symbol_set=configured_symbol_set,
        completed_symbols=completed_symbols,
    )

    write_outputs(
        cohort_registry=cohort_registry,
        file_registry=file_registry,
        cohort_summary=cohort_summary,
    )


if __name__ == "__main__":
    main()