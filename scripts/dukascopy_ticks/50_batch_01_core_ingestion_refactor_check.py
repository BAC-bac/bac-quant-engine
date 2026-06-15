"""
BACQE DUKASCOPY 50 - BATCH 01 CORE INGESTION REFACTOR CHECK

Purpose:
    Verify core ingestion scripts are symbol-safe before moving to feature pipeline refactoring.
"""

from pathlib import Path
import ast
import pandas as pd
import re


SCRIPT_DIR = Path("scripts/dukascopy_ticks")
OUTPUT_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_refactor_checks")

CORE_SCRIPTS = [
    "07_download_dukascopy_date_range.py",
    "08_normalise_dukascopy_date_range.py",
    "42_dukascopy_config_audit.py",
    "43_dukascopy_symbol_inventory.py",
    "44_dukascopy_symbol_download_plan.py",
    "45_dukascopy_batch_downloader.py",
    "46_dukascopy_trading_day_plan.py",
    "47_dukascopy_failed_hour_retry.py",
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
    symbol_equals_eurusd = bool(re.search(r"^SYMBOL\s*=\s*['\"]EURUSD['\"]", text, flags=re.MULTILINE, ))
    default_symbol_ok = 'DEFAULT_SYMBOL = "EURUSD"' in text or "DEFAULT_SYMBOL = 'EURUSD'" in text

    output_symbol_awareness = (
        'f"symbol={symbol}"' in text
        or "f'symbol={symbol}'" in text
        or "symbol=" in text
        or "row.symbol" in text
        or 'args.symbol' in text
    )

    blocking_issues = []

    if parse_status != "ok":
        blocking_issues.append("parse_error")

    if path.name in [
        "07_download_dukascopy_date_range.py",
        "08_normalise_dukascopy_date_range.py",
    ]:
        if not has_symbol_arg:
            blocking_issues.append("missing_symbol_arg")
        if not has_start_date_arg:
            blocking_issues.append("missing_start_date_arg")
        if not has_end_date_arg:
            blocking_issues.append("missing_end_date_arg")
        if not has_run_function:
            blocking_issues.append("missing_run_function")
        if symbol_equals_eurusd:
            blocking_issues.append("hardcoded_SYMBOL_EURUSD")

    if path.name in [
        "44_dukascopy_symbol_download_plan.py",
        "46_dukascopy_trading_day_plan.py",
        "47_dukascopy_failed_hour_retry.py",
    ]:
        if not has_symbol_arg:
            blocking_issues.append("missing_symbol_arg")

    if path.name in [
        "42_dukascopy_config_audit.py",
        "43_dukascopy_symbol_inventory.py",
        "45_dukascopy_batch_downloader.py",
    ]:
        if not has_config:
            blocking_issues.append("missing_config_usage")

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
        "output_symbol_awareness": output_symbol_awareness,
        "status": status,
        "blocking_issues": "; ".join(blocking_issues),
    }


def main() -> None:
    banner("BACQE DUKASCOPY 50 - BATCH 01 CORE INGESTION REFACTOR CHECK")

    ensure_dirs()

    print(f"Script dir:  {SCRIPT_DIR}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    rows = []

    for script in CORE_SCRIPTS:
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
                "output_symbol_awareness": False,
                "status": "fail",
                "blocking_issues": "missing_script",
            })
            continue

        rows.append(check_script(path))

    results = pd.DataFrame(rows)

    output_csv = OUTPUT_ROOT / "check_tables" / "batch_01_core_ingestion_check_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "batch_01_core_ingestion_check_report_latest.txt"

    results.to_csv(output_csv, index=False)

    pass_count = (results["status"] == "pass").sum()
    fail_count = (results["status"] == "fail").sum()

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY BATCH 01 CORE INGESTION REFACTOR CHECK\n")
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
                    "has_start_date_arg",
                    "has_end_date_arg",
                    "has_config",
                    "has_run_function",
                    "eurusd_hits",
                    "symbol_equals_eurusd",
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
            "has_start_date_arg",
            "has_end_date_arg",
            "has_config",
            "has_run_function",
            "blocking_issues",
        ]
    ].to_string(index=False))

    print("=" * 90)
    if fail_count == 0:
        print("[PASS] Batch 01 core ingestion scripts are symbol-safe.")
    else:
        print("[FAIL] Batch 01 still has issues to fix.")
    print(f"CSV:    {output_csv}")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()