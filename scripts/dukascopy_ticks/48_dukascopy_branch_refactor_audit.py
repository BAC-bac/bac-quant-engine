"""
BACQE DUKASCOPY 48 - BRANCH REFACTOR AUDIT

Purpose:
    Audit all Dukascopy scripts for hardcoded symbols, hardcoded paths,
    missing CLI arguments, and multi-symbol readiness.

This script is read-only.
"""

from pathlib import Path
import ast
import re
import pandas as pd


SCRIPT_DIR = Path("scripts/dukascopy_ticks")
OUTPUT_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_refactor_audit")

SYMBOL_PATTERNS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "EURGBP",
    "XAUUSD",
]

PATH_PATTERNS = [
    r"E:\Quant_Lab",
    "dukascopy_ticks",
    "dukascopy_engineered_features",
    "dukascopy_horizon_features",
    "dukascopy_feature_discovery",
    "dukascopy_feature_stability",
    "dukascopy_signal_validation",
    "dukascopy_candidate_replay",
    "dukascopy_cost_survival",
]

CLI_PATTERNS = [
    "argparse",
    "--symbol",
    "--start-date",
    "--end-date",
]

FUNCTION_PATTERNS = [
    "run_download",
    "run_normalisation",
    "run_feature_engineering",
    "run_horizon_expansion",
]


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "audit_tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def classify_script(filename: str) -> str:
    name = filename.lower()

    if "download" in name:
        return "download"
    if "normalise" in name or "normalize" in name:
        return "normalisation"
    if "tick_bars" in name or "tibs" in name or "imbalance" in name:
        return "bar_building"
    if "feature" in name or "horizon" in name:
        return "feature_pipeline"
    if "signal" in name:
        return "signal_research"
    if "replay" in name:
        return "replay"
    if "cost" in name:
        return "cost_validation"
    if "context" in name:
        return "context_research"
    if "oos" in name or "walk_forward" in name:
        return "validation"
    if "config" in name or "inventory" in name or "plan" in name or "retry" in name:
        return "utility"
    return "other"


def extract_assignments(tree: ast.AST) -> dict:
    assignments = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    try:
                        assignments[target.id] = ast.unparse(node.value)
                    except Exception:
                        assignments[target.id] = "<unparse_failed>"

    return assignments


def extract_functions(tree: ast.AST) -> list[str]:
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    ]


def audit_file(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="ignore")

    try:
        tree = ast.parse(text)
        parse_status = "ok"
        assignments = extract_assignments(tree)
        functions = extract_functions(tree)
    except Exception as exc:
        parse_status = f"parse_error: {exc}"
        assignments = {}
        functions = []

    symbol_hits = {
        symbol: len(re.findall(symbol, text))
        for symbol in SYMBOL_PATTERNS
    }

    path_hits = {
        pattern: text.count(pattern)
        for pattern in PATH_PATTERNS
    }

    cli_hits = {
        pattern: text.count(pattern)
        for pattern in CLI_PATTERNS
    }

    function_hits = {
        pattern: pattern in functions
        for pattern in FUNCTION_PATTERNS
    }

    has_argparse = cli_hits["argparse"] > 0
    has_symbol_arg = cli_hits["--symbol"] > 0
    has_start_arg = cli_hits["--start-date"] > 0
    has_end_arg = cli_hits["--end-date"] > 0

    hardcoded_symbol_count = sum(symbol_hits.values())
    hardcoded_path_count = sum(path_hits.values())

    default_symbol_assignment = assignments.get("SYMBOL") or assignments.get("DEFAULT_SYMBOL")
    has_run_function = any(name.startswith("run_") for name in functions)

    refactor_priority = "low"

    if hardcoded_symbol_count > 0 and not has_symbol_arg:
        refactor_priority = "high"
    elif hardcoded_symbol_count > 0 or hardcoded_path_count > 0:
        refactor_priority = "medium"

    if path.name.startswith(("42_", "43_", "44_", "45_", "46_", "47_")):
        if has_symbol_arg or "config" in path.name.lower():
            refactor_priority = "low"

    recommendation = []

    if hardcoded_symbol_count > 0:
        recommendation.append("remove_or_parameterise_hardcoded_symbols")

    if not has_symbol_arg:
        recommendation.append("add_symbol_argument_or_config_symbol")

    if hardcoded_path_count > 0:
        recommendation.append("verify_symbol_aware_output_paths")

    if not has_run_function:
        recommendation.append("expose_run_function")

    if not recommendation:
        recommendation.append("looks_multi_symbol_ready_or_low_risk")

    return {
        "script": path.name,
        "script_number": path.name.split("_", 1)[0],
        "category": classify_script(path.name),
        "parse_status": parse_status,
        "lines": text.count("\n") + 1,
        "functions": ", ".join(functions),
        "default_symbol_assignment": default_symbol_assignment,
        "hardcoded_symbol_count": hardcoded_symbol_count,
        "eurusd_hits": symbol_hits.get("EURUSD", 0),
        "gbpusd_hits": symbol_hits.get("GBPUSD", 0),
        "hardcoded_path_count": hardcoded_path_count,
        "has_argparse": has_argparse,
        "has_symbol_arg": has_symbol_arg,
        "has_start_date_arg": has_start_arg,
        "has_end_date_arg": has_end_arg,
        "has_run_function": has_run_function,
        "has_run_download": function_hits["run_download"],
        "has_run_normalisation": function_hits["run_normalisation"],
        "has_run_feature_engineering": function_hits["run_feature_engineering"],
        "has_run_horizon_expansion": function_hits["run_horizon_expansion"],
        "refactor_priority": refactor_priority,
        "recommendation": "; ".join(recommendation),
    }


def main() -> None:
    banner("BACQE DUKASCOPY 48 - BRANCH REFACTOR AUDIT")

    ensure_dirs()

    print(f"Script dir:  {SCRIPT_DIR}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    files = sorted(SCRIPT_DIR.glob("*.py"))

    if not files:
        print("[STOP] No scripts found.")
        return

    rows = []

    for path in files:
        rows.append(audit_file(path))

    audit = pd.DataFrame(rows)

    audit["script_number_numeric"] = pd.to_numeric(
        audit["script_number"].str.extract(r"(\d+)")[0],
        errors="coerce",
    )

    audit = audit.sort_values(
        ["script_number_numeric", "script"]
    ).drop(columns=["script_number_numeric"])

    summary = (
        audit.groupby(["category", "refactor_priority"], as_index=False)
        .agg(script_count=("script", "count"))
        .sort_values(["category", "refactor_priority"])
    )

    high_priority = audit[audit["refactor_priority"] == "high"].copy()
    medium_priority = audit[audit["refactor_priority"] == "medium"].copy()

    audit_path = OUTPUT_ROOT / "audit_tables" / "dukascopy_refactor_audit_latest.csv"
    summary_path = OUTPUT_ROOT / "audit_tables" / "dukascopy_refactor_summary_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "dukascopy_refactor_audit_report_latest.txt"

    audit.to_csv(audit_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY BRANCH REFACTOR AUDIT REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Scripts audited: {len(audit):,}\n")
        f.write(f"High priority: {len(high_priority):,}\n")
        f.write(f"Medium priority: {len(medium_priority):,}\n\n")

        f.write("Summary by Category / Priority\n")
        f.write("-" * 80 + "\n")
        f.write(summary.to_string(index=False))

        f.write("\n\nHigh Priority Scripts\n")
        f.write("-" * 80 + "\n")
        if high_priority.empty:
            f.write("None.\n")
        else:
            f.write(
                high_priority[
                    [
                        "script",
                        "category",
                        "hardcoded_symbol_count",
                        "eurusd_hits",
                        "has_symbol_arg",
                        "has_run_function",
                        "recommendation",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nMedium Priority Scripts\n")
        f.write("-" * 80 + "\n")
        if medium_priority.empty:
            f.write("None.\n")
        else:
            f.write(
                medium_priority[
                    [
                        "script",
                        "category",
                        "hardcoded_symbol_count",
                        "eurusd_hits",
                        "hardcoded_path_count",
                        "has_symbol_arg",
                        "recommendation",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"Audit table: {audit_path}\n")
        f.write(f"Summary: {summary_path}\n")

    print("SUMMARY")
    print("-" * 90)
    print(summary.to_string(index=False))
    print("-" * 90)
    print(f"High priority scripts:   {len(high_priority)}")
    print(f"Medium priority scripts: {len(medium_priority)}")
    print("=" * 90)
    print("[DONE] Refactor audit complete.")
    print(f"Audit:  {audit_path}")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()