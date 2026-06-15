"""
BACQE DUKASCOPY 52 - BATCH 03 SIGNAL REPLAY COST REFACTOR CHECK
"""

from pathlib import Path
import ast
import re
import pandas as pd


SCRIPT_DIR = Path("scripts/dukascopy_ticks")
OUTPUT_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_refactor_checks")

TARGET_SCRIPTS = [
    "25_signal_validation_engine.py",
    "26_signal_forensics_engine.py",
    "27_signal_filter_optimizer.py",
    "28_candidate_replay_engine.py",
    "29_cost_survival_engine.py",
    "32_horizon_candidate_replay.py",
    "33_horizon_cost_survival_engine.py",
]


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "check_tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def extract_functions(tree: ast.AST) -> list[str]:
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    ]


def check_script(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="ignore")

    try:
        tree = ast.parse(text)
        parse_status = "ok"
        functions = extract_functions(tree)
    except Exception as exc:
        parse_status = f"parse_error: {exc}"
        functions = []

    has_argparse = "argparse" in text
    has_symbol_arg = "--symbol" in text
    has_start_date_arg = "--start-date" in text
    has_end_date_arg = "--end-date" in text
    has_config = "dukascopy_research.yaml" in text or "CONFIG_PATH" in text

    has_run_function = any(fn.startswith("run_") for fn in functions)

    eurusd_hits = text.count("EURUSD")

    symbol_equals_eurusd = bool(
        re.search(
            r"^SYMBOL\s*=\s*['\"]EURUSD['\"]",
            text,
            flags=re.MULTILINE,
        )
    )

    default_symbol_ok = bool(
        re.search(
            r"^DEFAULT_SYMBOL\s*=\s*['\"]EURUSD['\"]",
            text,
            flags=re.MULTILINE,
        )
    )

    has_symbol_output_root = (
        "symbol={symbol}" in text
        or "symbol={SYMBOL}" in text
        or "f\"symbol={symbol}\"" in text
        or "f'symbol={symbol}'" in text
    )

    has_symbol_input_root = (
        "symbol={symbol}" in text
        or "symbol={SYMBOL}" in text
        or "f\"symbol={symbol}\"" in text
        or "f'symbol={symbol}'" in text
    )

    has_latest_outputs = "_latest" in text
    has_report_root = ("REPORT_ROOT" in text or "OUTPUT_ROOT" in text or "report_root" in text or "output_root" in text)

    blocking_issues = []

    if parse_status != "ok":
        blocking_issues.append("parse_error")

    if not has_symbol_arg:
        blocking_issues.append("missing_symbol_arg")

    if not has_run_function:
        blocking_issues.append("missing_run_function")

    if symbol_equals_eurusd:
        blocking_issues.append("hardcoded_SYMBOL_EURUSD")

    if not has_symbol_input_root:
        blocking_issues.append("missing_symbol_aware_input_root")

    if not has_symbol_output_root:
        blocking_issues.append("missing_symbol_aware_output_root")

    if not has_report_root:
        blocking_issues.append("missing_report_or_output_root")

    status = "pass" if not blocking_issues else "fail"

    return {
        "script": path.name,
        "exists": path.exists(),
        "parse_status": parse_status,
        "functions": ", ".join(functions),
        "has_argparse": has_argparse,
        "has_symbol_arg": has_symbol_arg,
        "has_start_date_arg": has_start_date_arg,
        "has_end_date_arg": has_end_date_arg,
        "has_config": has_config,
        "has_run_function": has_run_function,
        "eurusd_hits": eurusd_hits,
        "symbol_equals_eurusd": symbol_equals_eurusd,
        "default_symbol_ok": default_symbol_ok,
        "has_symbol_input_root": has_symbol_input_root,
        "has_symbol_output_root": has_symbol_output_root,
        "has_latest_outputs": has_latest_outputs,
        "has_report_root": has_report_root,
        "status": status,
        "blocking_issues": "; ".join(blocking_issues),
    }


def main() -> None:
    banner("BACQE DUKASCOPY 52 - BATCH 03 SIGNAL REPLAY COST REFACTOR CHECK")

    ensure_dirs()

    rows = []

    for script in TARGET_SCRIPTS:
        path = SCRIPT_DIR / script

        if not path.exists():
            rows.append({
                "script": script,
                "exists": False,
                "parse_status": "missing",
                "functions": "",
                "has_argparse": False,
                "has_symbol_arg": False,
                "has_start_date_arg": False,
                "has_end_date_arg": False,
                "has_config": False,
                "has_run_function": False,
                "eurusd_hits": 0,
                "symbol_equals_eurusd": False,
                "default_symbol_ok": False,
                "has_symbol_input_root": False,
                "has_symbol_output_root": False,
                "has_latest_outputs": False,
                "has_report_root": False,
                "status": "fail",
                "blocking_issues": "missing_script",
            })
            continue

        rows.append(check_script(path))

    results = pd.DataFrame(rows)

    output_csv = OUTPUT_ROOT / "check_tables" / "batch_03_signal_replay_cost_check_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "batch_03_signal_replay_cost_check_report_latest.txt"

    results.to_csv(output_csv, index=False)

    pass_count = (results["status"] == "pass").sum()
    fail_count = (results["status"] == "fail").sum()

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 52 - BATCH 03 SIGNAL REPLAY COST REFACTOR CHECK\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Scripts checked: {len(results)}\n")
        f.write(f"Pass: {pass_count}\n")
        f.write(f"Fail: {fail_count}\n\n")

        f.write("Results\n")
        f.write("-" * 80 + "\n")
        f.write(
            results[
                [
                    "script",
                    "status",
                    "has_symbol_arg",
                    "has_run_function",
                    "eurusd_hits",
                    "symbol_equals_eurusd",
                    "default_symbol_ok",
                    "has_symbol_input_root",
                    "has_symbol_output_root",
                    "blocking_issues",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"CSV: {output_csv}\n")

    print(results[
        [
            "script",
            "status",
            "has_symbol_arg",
            "has_run_function",
            "eurusd_hits",
            "symbol_equals_eurusd",
            "has_symbol_input_root",
            "has_symbol_output_root",
            "blocking_issues",
        ]
    ].to_string(index=False))

    print("=" * 90)
    if fail_count == 0:
        print("[PASS] Batch 03 signal replay cost scripts are symbol-safe.")
    else:
        print("[FAIL] Batch 03 signal replay cost still has issues to fix.")
    print(f"CSV:    {output_csv}")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()