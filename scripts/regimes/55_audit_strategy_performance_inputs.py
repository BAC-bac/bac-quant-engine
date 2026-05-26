"""
BACQE REGIME ENGINE - 55 Audit Strategy Performance Inputs

Discovery/audit script for the Strategy Performance-by-Regime Layer.

It scans likely BACQE output folders for candidate strategy/backtest/performance files,
profiles their columns, and identifies whether they contain useful strategy metrics.

This script does NOT join or rank strategies yet.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")

SEARCH_ROOTS = [
    DATA_LAKE_ROOT / "data" / "analysis",
    DATA_LAKE_ROOT / "data" / "processed",
    DATA_LAKE_ROOT / "reports",
    PROJECT_ROOT / "outputs",
    PROJECT_ROOT / "reports",
    PROJECT_ROOT / "data",
]

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "strategy_performance_inputs"

OUTPUT_CSV = OUTPUT_ANALYSIS_DIR / "strategy_performance_input_audit_latest.csv"
OUTPUT_PARQUET = OUTPUT_ANALYSIS_DIR / "strategy_performance_input_audit_latest.parquet"
OUTPUT_REPORT = OUTPUT_REPORT_DIR / "strategy_performance_input_audit_latest.txt"

FILE_PATTERNS = [
    "*.csv",
    "*.parquet",
]

KEYWORDS = [
    "strategy",
    "backtest",
    "performance",
    "portfolio",
    "returns",
    "signals",
    "router",
    "decision",
    "stats",
    "metrics",
]

METRIC_KEYWORDS = [
    "return",
    "pnl",
    "profit",
    "profit_factor",
    "pf",
    "win_rate",
    "drawdown",
    "sharpe",
    "sortino",
    "calmar",
    "expectancy",
    "trades",
    "exposure",
    "accuracy",
    "precision",
    "recall",
    "f1",
]

IDENTIFIER_KEYWORDS = [
    "symbol",
    "timeframe",
    "strategy",
    "strategy_name",
    "model",
    "signal",
    "regime",
    "environment",
    "bar_type",
]


def file_is_candidate(path: Path) -> bool:
    lower = str(path).lower()
    return any(keyword in lower for keyword in KEYWORDS)


def discover_files() -> list[Path]:
    files = []

    for root in SEARCH_ROOTS:
        if not root.exists():
            continue

        for pattern in FILE_PATTERNS:
            for path in root.rglob(pattern):
                if file_is_candidate(path):
                    files.append(path)

    return sorted(set(files))


def read_sample(path: Path) -> tuple[str, pd.DataFrame | None, str | None]:
    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, low_memory=False, nrows=5000)
            return "success", df, None

        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
            if len(df) > 5000:
                df = df.head(5000)
            return "success", df, None

        return "unsupported", None, "Unsupported file extension."

    except Exception as exc:
        return "failed", None, str(exc)[:500]


def detect_columns(columns: list[str], keywords: list[str]) -> list[str]:
    detected = []

    for col in columns:
        lower = col.lower()
        if any(keyword in lower for keyword in keywords):
            detected.append(col)

    return detected


def classify_candidate_score(
    file_path: Path,
    columns: list[str],
    metric_cols: list[str],
    identifier_cols: list[str],
    row_count_sample: int,
) -> int:
    score = 0
    lower_path = str(file_path).lower()

    if "strategy" in lower_path:
        score += 20
    if "backtest" in lower_path:
        score += 20
    if "performance" in lower_path:
        score += 20
    if "stats" in lower_path or "metrics" in lower_path:
        score += 15
    if "router" in lower_path:
        score += 10

    score += min(len(metric_cols) * 5, 30)
    score += min(len(identifier_cols) * 3, 20)

    if row_count_sample > 0:
        score += 5

    return min(score, 100)


def classify_candidate_type(path: Path, metric_cols: list[str], identifier_cols: list[str]) -> str:
    lower = str(path).lower()

    if "router" in lower and "decision" in lower:
        return "strategy_router_decision_output"

    if "backtest" in lower:
        return "backtest_output"

    if "performance" in lower:
        return "performance_summary"

    if "stats" in lower or "metrics" in lower:
        return "metrics_output"

    if metric_cols and identifier_cols:
        return "possible_strategy_performance_file"

    if metric_cols:
        return "possible_metric_file"

    return "weak_candidate"


def audit_file(path: Path) -> dict:
    read_status, df, error = read_sample(path)

    record = {
        "file_path": str(path),
        "file_name": path.name,
        "parent_folder": str(path.parent),
        "extension": path.suffix.lower(),
        "file_size_mb": round(path.stat().st_size / (1024 * 1024), 6),
        "modified_time_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
        "read_status": read_status,
        "sample_rows": None,
        "sample_columns": None,
        "columns": None,
        "metric_columns_detected": None,
        "identifier_columns_detected": None,
        "candidate_type": None,
        "candidate_score": 0,
        "error_message": error,
        "audit_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    if read_status != "success" or df is None:
        return record

    columns = [str(col) for col in df.columns]
    metric_cols = detect_columns(columns, METRIC_KEYWORDS)
    identifier_cols = detect_columns(columns, IDENTIFIER_KEYWORDS)

    record["sample_rows"] = len(df)
    record["sample_columns"] = len(columns)
    record["columns"] = " | ".join(columns)
    record["metric_columns_detected"] = " | ".join(metric_cols)
    record["identifier_columns_detected"] = " | ".join(identifier_cols)
    record["candidate_type"] = classify_candidate_type(path, metric_cols, identifier_cols)
    record["candidate_score"] = classify_candidate_score(
        path,
        columns,
        metric_cols,
        identifier_cols,
        len(df),
    )

    return record


def build_report(audit: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 120)
    lines.append("BACQE STRATEGY PERFORMANCE INPUT AUDIT")
    lines.append("=" * 120)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append("-" * 120)

    lines.append("")
    lines.append("SEARCH ROOTS")
    lines.append("-" * 120)
    for root in SEARCH_ROOTS:
        lines.append(str(root))

    lines.append("")
    lines.append("AUDIT SUMMARY")
    lines.append("-" * 120)
    lines.append(f"Files audited: {len(audit):,}")

    if not audit.empty:
        lines.append("")
        lines.append("Read status counts:")
        lines.append(audit["read_status"].value_counts(dropna=False).to_string())

        lines.append("")
        lines.append("Candidate type counts:")
        lines.append(audit["candidate_type"].value_counts(dropna=False).to_string())

        lines.append("")
        lines.append("TOP CANDIDATE FILES")
        lines.append("-" * 120)

        display_cols = [
            "candidate_score",
            "candidate_type",
            "file_name",
            "sample_rows",
            "sample_columns",
            "metric_columns_detected",
            "identifier_columns_detected",
            "file_path",
        ]

        top = audit.sort_values("candidate_score", ascending=False).head(30)
        lines.append(top[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 120)
    lines.append("This audit identifies candidate strategy/backtest/performance files.")
    lines.append("High score means likely useful for the Strategy Performance-by-Regime layer.")
    lines.append("Script 56 will use this audit to build a cleaner strategy performance registry.")
    lines.append("=" * 120)

    return "\n".join(lines)


def main() -> None:
    print("=" * 120)
    print("BACQE REGIME ENGINE - 55 AUDIT STRATEGY PERFORMANCE INPUTS")
    print("=" * 120)

    files = discover_files()

    print(f"Candidate files discovered: {len(files):,}")
    print("-" * 120)

    records = []

    for i, path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] Auditing: {path}")
        records.append(audit_file(path))

    audit = pd.DataFrame(records)

    if not audit.empty:
        audit = audit.sort_values(
            ["candidate_score", "file_size_mb"],
            ascending=[False, False],
        ).reset_index(drop=True)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    audit.to_csv(OUTPUT_CSV, index=False)
    audit.to_parquet(OUTPUT_PARQUET, index=False)

    report = build_report(audit)
    OUTPUT_REPORT.write_text(report, encoding="utf-8")

    print("[DONE] Strategy performance input audit created.")
    print(f"CSV:     {OUTPUT_CSV}")
    print(f"Parquet: {OUTPUT_PARQUET}")
    print(f"Report:  {OUTPUT_REPORT}")
    print("-" * 120)

    print(report)
    print("=" * 120)


if __name__ == "__main__":
    main()