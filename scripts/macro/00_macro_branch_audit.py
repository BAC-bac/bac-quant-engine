from pathlib import Path
import ast
import re
import subprocess
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

MACRO_SCRIPT_DIR = PROJECT_ROOT / "scripts" / "macro"
INFO_SCRIPT_DIR = PROJECT_ROOT / "scripts" / "information_data"
MACRO_DATA_DIR = PROJECT_ROOT / "macro_data"
REPORT_DIR = PROJECT_ROOT / "reports"

OUTPUT_CSV = REPORT_DIR / "macro_branch_audit_latest.csv"
OUTPUT_TXT = REPORT_DIR / "macro_branch_audit_latest.txt"
DEPENDENCY_CSV = REPORT_DIR / "macro_branch_dependency_health_latest.csv"


LEGACY_PATH_PATTERNS = [
    r"E:\\",
    r"BAC_Quant_Universe",
    r"MT5",
    r"FTMO",
]

PATH_ASSIGNMENT_PATTERNS = [
    r"([A-Z_]*(?:INPUT|OUTPUT|FILE|DIR|PRICE|SIGNALS|SUMMARY|LATEST)[A-Z_]*)\s*=\s*(.+)"
]


def get_git_info() -> dict:
    def run_git(args: list[str]) -> str:
        try:
            result = subprocess.run(
                ["git"] + args,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            return result.stdout.strip()
        except Exception as exc:
            return f"git_error: {exc}"

    return {
        "branch": run_git(["branch", "--show-current"]),
        "status": run_git(["status", "--short"]),
        "last_commit": run_git(["log", "-1", "--oneline"]),
    }


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1")
    except Exception as exc:
        return f"READ_ERROR: {exc}"


def detect_legacy_paths(text: str) -> list[str]:
    return [
        pattern
        for pattern in LEGACY_PATH_PATTERNS
        if re.search(pattern, text, flags=re.IGNORECASE)
    ]


def classify_script(name: str) -> str:
    try:
        number = int(name.split("_")[0])
    except ValueError:
        return "non_numbered"

    if number == 0:
        return "branch_audit"
    if number <= 4:
        return "debt_cleaning_and_visualisation"
    if number <= 12:
        return "sovereign_macro_scoring"
    if number <= 22:
        return "external_fiscal_weighted_scoring"
    if number <= 31:
        return "macro_fx_signals_and_watchlists"
    if number <= 36:
        return "real_rate_validation"
    if number <= 38:
        return "factor_decomposition_v6"

    return "unknown_future"


def safe_literal_eval(value: str):
    value = value.strip()

    try:
        return ast.literal_eval(value)
    except Exception:
        return None


def resolve_path_expression(expr: str) -> str | None:
    """
    Handles common patterns like:
    PROJECT_ROOT / "macro_data" / "processed" / "file.csv"
    Path(r"E:\\...")
    """

    expr = expr.strip()

    path_call_match = re.search(r"Path\((r?['\"].+?['\"])\)", expr)
    if path_call_match:
        raw = safe_literal_eval(path_call_match.group(1))
        return str(Path(raw)) if raw else None

    if "PROJECT_ROOT" in expr and "/" in expr:
        parts = re.findall(r"/\s*(r?['\"].+?['\"])", expr)
        clean_parts = []

        for part in parts:
            value = safe_literal_eval(part)
            if value:
                clean_parts.append(value)

        if clean_parts:
            return str(PROJECT_ROOT.joinpath(*clean_parts))

    direct_string = safe_literal_eval(expr)
    if isinstance(direct_string, str):
        return direct_string

    return None


def extract_path_assignments(text: str) -> list[dict]:
    rows = []

    for pattern in PATH_ASSIGNMENT_PATTERNS:
        matches = re.findall(pattern, text)

        for variable, expression in matches:
            resolved = resolve_path_expression(expression)

            if resolved is None:
                continue

            variable_upper = variable.upper()

            if any(token in variable_upper for token in ["INPUT", "SIGNALS", "PRICE"]):
                role = "input"
            elif any(token in variable_upper for token in ["OUTPUT", "SUMMARY", "LATEST"]):
                role = "output"
            elif "DIR" in variable_upper:
                role = "directory"
            else:
                role = "unknown"

            rows.append(
                {
                    "variable": variable,
                    "role": role,
                    "expression": expression.strip(),
                    "resolved_path": resolved,
                }
            )

    return rows


def path_exists(resolved_path: str) -> bool:
    try:
        return Path(resolved_path).exists()
    except Exception:
        return False


def audit_script(path: Path) -> tuple[dict, list[dict]]:
    text = read_text(path)
    legacy_hits = detect_legacy_paths(text)
    assignments = extract_path_assignments(text)

    dependency_rows = []

    for item in assignments:
        exists = path_exists(item["resolved_path"])

        dependency_rows.append(
            {
                "script_name": path.name,
                "script_path": str(path.relative_to(PROJECT_ROOT)),
                "script_group": classify_script(path.name),
                "variable": item["variable"],
                "role": item["role"],
                "exists": exists,
                "resolved_path": item["resolved_path"],
                "expression": item["expression"],
            }
        )

    input_count = sum(1 for item in assignments if item["role"] == "input")
    output_count = sum(1 for item in assignments if item["role"] == "output")
    missing_inputs = sum(
        1
        for row in dependency_rows
        if row["role"] in {"input", "directory"} and not row["exists"]
    )

    script_row = {
        "script_name": path.name,
        "script_path": str(path.relative_to(PROJECT_ROOT)),
        "size_bytes": path.stat().st_size,
        "modified": datetime.fromtimestamp(path.stat().st_mtime),
        "script_group": classify_script(path.name),
        "path_refs_count": len(assignments),
        "input_refs_count": input_count,
        "output_refs_count": output_count,
        "missing_input_or_dir_count": missing_inputs,
        "legacy_path_hits_count": len(legacy_hits),
        "legacy_path_hits": " | ".join(legacy_hits),
    }

    return script_row, dependency_rows


def inventory_files(root: Path) -> pd.DataFrame:
    rows = []

    if not root.exists():
        return pd.DataFrame()

    for path in root.rglob("*"):
        if path.is_file():
            rows.append(
                {
                    "path": str(path.relative_to(PROJECT_ROOT)),
                    "size_bytes": path.stat().st_size,
                    "modified": datetime.fromtimestamp(path.stat().st_mtime),
                    "suffix": path.suffix.lower(),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    git_info = get_git_info()

    macro_scripts = sorted(MACRO_SCRIPT_DIR.glob("*.py"))
    info_scripts = sorted(INFO_SCRIPT_DIR.glob("*.py")) if INFO_SCRIPT_DIR.exists() else []

    script_rows = []
    dependency_rows = []

    for script_path in macro_scripts:
        script_row, script_dependencies = audit_script(script_path)
        script_rows.append(script_row)
        dependency_rows.extend(script_dependencies)

    audit_df = pd.DataFrame(script_rows)
    dependency_df = pd.DataFrame(dependency_rows)
    macro_files_df = inventory_files(MACRO_DATA_DIR)

    audit_df.to_csv(OUTPUT_CSV, index=False)
    dependency_df.to_csv(DEPENDENCY_CSV, index=False)

    numbered_scripts = audit_df[audit_df["script_name"].str[:2].str.isnumeric()].copy()
    legacy_scripts = audit_df[audit_df["legacy_path_hits_count"] > 0].copy()
    missing_dependency_df = dependency_df[
        (dependency_df["role"].isin(["input", "directory"])) & (~dependency_df["exists"])
    ].copy()

    processed_files = (
        macro_files_df[
            macro_files_df["path"].str.contains(
                r"macro_data\\processed|macro_data/processed", regex=True
            )
        ]
        if not macro_files_df.empty
        else pd.DataFrame()
    )

    csv_like = (
        macro_files_df[macro_files_df["suffix"].isin([".csv", ".parquet", ".json", ".txt"])]
        if not macro_files_df.empty
        else pd.DataFrame()
    )

    with OUTPUT_TXT.open("w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("BACQE MACRO BRANCH AUDIT - ENHANCED HEALTH CHECK\n")
        f.write("=" * 90 + "\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Project root: {PROJECT_ROOT}\n")
        f.write(f"Git branch: {git_info['branch']}\n")
        f.write(f"Last commit: {git_info['last_commit']}\n")
        f.write(f"Git status short: {git_info['status'] or 'clean'}\n\n")

        f.write("-" * 90 + "\n")
        f.write("SCRIPT INVENTORY\n")
        f.write("-" * 90 + "\n")
        f.write(f"Macro scripts found: {len(macro_scripts)}\n")
        f.write(f"Information-data scripts found: {len(info_scripts)}\n")
        f.write(f"Numbered macro scripts found: {len(numbered_scripts)}\n")
        f.write(f"First macro script: {numbered_scripts['script_name'].min() if not numbered_scripts.empty else 'n/a'}\n")
        f.write(f"Last macro script: {numbered_scripts['script_name'].max() if not numbered_scripts.empty else 'n/a'}\n\n")

        f.write("Macro script groups:\n")
        if not numbered_scripts.empty:
            group_counts = numbered_scripts["script_group"].value_counts().sort_index()
            for group, count in group_counts.items():
                f.write(f"  {group}: {count}\n")
        f.write("\n")

        f.write("-" * 90 + "\n")
        f.write("LEGACY PATH CHECK\n")
        f.write("-" * 90 + "\n")
        f.write(f"Scripts with legacy path hits: {len(legacy_scripts)}\n")
        if not legacy_scripts.empty:
            for _, row in legacy_scripts.iterrows():
                f.write(f"  {row['script_name']} -> {row['legacy_path_hits']}\n")
        else:
            f.write("No legacy path patterns detected.\n")
        f.write("\n")

        f.write("-" * 90 + "\n")
        f.write("DEPENDENCY HEALTH\n")
        f.write("-" * 90 + "\n")
        f.write(f"Path references detected: {len(dependency_df)}\n")
        f.write(f"Missing input/directory references: {len(missing_dependency_df)}\n\n")

        if not missing_dependency_df.empty:
            f.write("Missing inputs/directories:\n")
            for _, row in missing_dependency_df.iterrows():
                f.write(f"  {row['script_name']} | {row['variable']} | {row['resolved_path']}\n")
        else:
            f.write("No missing input/directory references detected.\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("MACRO DATA INVENTORY\n")
        f.write("-" * 90 + "\n")
        f.write(f"Macro data files found: {len(macro_files_df)}\n")
        f.write(f"Processed files found: {len(processed_files)}\n")
        f.write(f"CSV/Parquet/JSON/TXT files found: {len(csv_like)}\n")

        if not macro_files_df.empty:
            f.write("\nFile types:\n")
            suffix_counts = macro_files_df["suffix"].value_counts().sort_index()
            for suffix, count in suffix_counts.items():
                f.write(f"  {suffix or '[no suffix]'}: {count}\n")

            f.write("\nLargest macro data files:\n")
            for _, row in macro_files_df.sort_values("size_bytes", ascending=False).head(15).iterrows():
                f.write(f"  {row['size_bytes']:>12}  {row['path']}\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("SCRIPT READINESS SUMMARY\n")
        f.write("-" * 90 + "\n")

        readiness = audit_df[
            [
                "script_name",
                "script_group",
                "input_refs_count",
                "output_refs_count",
                "missing_input_or_dir_count",
                "legacy_path_hits_count",
            ]
        ].copy()

        f.write(readiness.to_string(index=False))
        f.write("\n\n")

        f.write("=" * 90 + "\n")
        f.write("RECOMMENDED NEXT ACTIONS\n")
        f.write("=" * 90 + "\n")
        f.write("1. Copy or regenerate missing macro CSV/Parquet datasets.\n")
        f.write("2. Refactor scripts 27, 28, and 36 to remove legacy BAC_Quant_Universe D1 price paths.\n")
        f.write("3. Rerun the audit after datasets are restored.\n")
        f.write("4. Only continue to script 39 after scripts 01-38 are dependency-clean.\n")

    print("=" * 90)
    print("BACQE MACRO BRANCH ENHANCED AUDIT COMPLETE")
    print("=" * 90)
    print(f"CSV saved to: {OUTPUT_CSV}")
    print(f"Dependency CSV saved to: {DEPENDENCY_CSV}")
    print(f"TXT saved to: {OUTPUT_TXT}")
    print()
    print(f"Macro scripts found: {len(macro_scripts)}")
    print(f"Information-data scripts found: {len(info_scripts)}")
    print(f"Scripts with legacy path hits: {len(legacy_scripts)}")
    print(f"Missing input/directory references: {len(missing_dependency_df)}")
    print(f"Macro data files found: {len(macro_files_df)}")

    if not missing_dependency_df.empty:
        print("\nMissing input/directory references:")
        print(
            missing_dependency_df[
                ["script_name", "variable", "resolved_path"]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()