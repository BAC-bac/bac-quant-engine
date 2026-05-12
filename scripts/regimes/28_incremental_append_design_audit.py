"""
BACQE Script 28
Incremental Append Design Audit

Purpose:
- Audit full base vs recent regime files after sync verification
- Identify where recent files contain rows newer than full base
- Check schema compatibility, timestamp columns, duplicate risk, and append feasibility
- Produce an append design report before building any append executor

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

REGIME_PROCESSED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes"
LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

OUTPUT_DIR = LEDGER_DIR / "incremental_append_design"

WATCH_PAIRS = [
    {
        "plan_type": "feature_append",
        "full_stage": "features",
        "recent_stage": "recent_features",
        "full_dir": REGIME_PROCESSED_DIR / "features",
        "recent_dir": REGIME_PROCESSED_DIR / "recent" / "features",
    },
    {
        "plan_type": "classification_append",
        "full_stage": "classified",
        "recent_stage": "recent_classified",
        "full_dir": REGIME_PROCESSED_DIR / "classified",
        "recent_dir": REGIME_PROCESSED_DIR / "recent" / "classified",
    },
]

TIMESTAMP_CANDIDATES = [
    "timestamp",
    "time",
    "datetime",
    "date",
    "bar_time",
    "event_dt",
    "open_time",
]


def list_parquet_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return [p for p in folder.rglob("*.parquet") if p.is_file()]


def infer_key(path: Path, base_dir: Path) -> tuple[str, str, str]:
    """
    Expected:
    base_dir / broker / timeframe / SYMBOL_TIMEFRAME_stage.parquet
    """
    rel = path.relative_to(base_dir)
    parts = rel.parts

    broker = parts[0] if len(parts) >= 3 else None
    timeframe = parts[1] if len(parts) >= 3 else None
    symbol = path.stem.split("_")[0] if "_" in path.stem else path.stem

    return broker, timeframe, symbol


def inspect_parquet(path: Path) -> dict:
    result = {
        "exists": path.exists(),
        "rows": None,
        "size_mb": None,
        "columns": [],
        "timestamp_column": None,
        "min_timestamp": None,
        "max_timestamp": None,
        "error": None,
    }

    if not path.exists():
        return result

    result["size_mb"] = round(path.stat().st_size / (1024 * 1024), 4)

    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        result["rows"] = pf.metadata.num_rows
        result["columns"] = pf.schema.names

        cols_to_read = [c for c in TIMESTAMP_CANDIDATES if c in result["columns"]]

        if cols_to_read:
            ts_col = cols_to_read[0]
            df = pd.read_parquet(path, columns=[ts_col])
            converted = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

            if converted.notna().any():
                result["timestamp_column"] = ts_col
                result["min_timestamp"] = converted.min().isoformat()
                result["max_timestamp"] = converted.max().isoformat()

        else:
            # Fallback: small read to inspect index only.
            df = pd.read_parquet(path)
            if isinstance(df.index, pd.DatetimeIndex):
                result["timestamp_column"] = "index"
                result["min_timestamp"] = df.index.min().isoformat()
                result["max_timestamp"] = df.index.max().isoformat()

    except Exception as exc:
        result["error"] = str(exc)

    return result


def classify_append_feasibility(row) -> tuple[str, str]:
    issues = []

    if not row["full_exists"]:
        issues.append("full_missing")

    if not row["recent_exists"]:
        issues.append("recent_missing")

    if row["full_error"]:
        issues.append("full_read_error")

    if row["recent_error"]:
        issues.append("recent_read_error")

    if not row["schema_match"]:
        issues.append("schema_mismatch")

    if not row["timestamp_column_match"]:
        issues.append("timestamp_column_mismatch")

    if pd.isna(row["full_max_timestamp"]) or pd.isna(row["recent_max_timestamp"]):
        issues.append("missing_timestamp")

    if issues:
        return "blocked", "|".join(issues)

    full_max = pd.to_datetime(row["full_max_timestamp"], errors="coerce", utc=True)
    recent_max = pd.to_datetime(row["recent_max_timestamp"], errors="coerce", utc=True)

    if recent_max > full_max:
        return "append_candidate", "recent_newer_than_full"

    if recent_max == full_max:
        return "already_aligned", "recent_max_equals_full_max"

    return "recent_behind", "recent_max_older_than_full_max"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE INCREMENTAL APPEND DESIGN AUDIT")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    records = []

    for pair in WATCH_PAIRS:
        print(f"[SCAN] {pair['plan_type']}")

        full_files = list_parquet_files(pair["full_dir"])
        recent_files = list_parquet_files(pair["recent_dir"])

        full_map = {
            infer_key(path, pair["full_dir"]): path
            for path in full_files
        }

        recent_map = {
            infer_key(path, pair["recent_dir"]): path
            for path in recent_files
        }

        all_keys = sorted(set(full_map.keys()).union(set(recent_map.keys())))

        print(f"  Full files:   {len(full_files)}")
        print(f"  Recent files: {len(recent_files)}")
        print(f"  Keys checked: {len(all_keys)}")

        for broker, timeframe, symbol in all_keys:
            full_path = full_map.get((broker, timeframe, symbol))
            recent_path = recent_map.get((broker, timeframe, symbol))

            full_info = inspect_parquet(full_path) if full_path else {
                "exists": False,
                "rows": None,
                "size_mb": None,
                "columns": [],
                "timestamp_column": None,
                "min_timestamp": None,
                "max_timestamp": None,
                "error": None,
            }

            recent_info = inspect_parquet(recent_path) if recent_path else {
                "exists": False,
                "rows": None,
                "size_mb": None,
                "columns": [],
                "timestamp_column": None,
                "min_timestamp": None,
                "max_timestamp": None,
                "error": None,
            }

            schema_match = full_info["columns"] == recent_info["columns"]
            timestamp_column_match = full_info["timestamp_column"] == recent_info["timestamp_column"]

            row = {
                "plan_type": pair["plan_type"],
                "broker": broker,
                "timeframe": timeframe,
                "symbol": symbol,
                "full_stage": pair["full_stage"],
                "recent_stage": pair["recent_stage"],
                "full_path": str(full_path) if full_path else "",
                "recent_path": str(recent_path) if recent_path else "",
                "full_exists": full_info["exists"],
                "recent_exists": recent_info["exists"],
                "full_rows": full_info["rows"],
                "recent_rows": recent_info["rows"],
                "full_size_mb": full_info["size_mb"],
                "recent_size_mb": recent_info["size_mb"],
                "full_timestamp_column": full_info["timestamp_column"],
                "recent_timestamp_column": recent_info["timestamp_column"],
                "timestamp_column_match": timestamp_column_match,
                "full_min_timestamp": full_info["min_timestamp"],
                "full_max_timestamp": full_info["max_timestamp"],
                "recent_min_timestamp": recent_info["min_timestamp"],
                "recent_max_timestamp": recent_info["max_timestamp"],
                "schema_match": schema_match,
                "full_column_count": len(full_info["columns"]),
                "recent_column_count": len(recent_info["columns"]),
                "full_error": full_info["error"],
                "recent_error": recent_info["error"],
                "full_columns": "|".join(full_info["columns"]),
                "recent_columns": "|".join(recent_info["columns"]),
            }

            feasibility, reason = classify_append_feasibility(row)
            row["append_feasibility"] = feasibility
            row["append_reason"] = reason

            if feasibility == "append_candidate":
                full_max = pd.to_datetime(row["full_max_timestamp"], errors="coerce", utc=True)
                recent_max = pd.to_datetime(row["recent_max_timestamp"], errors="coerce", utc=True)
                row["gap_hours"] = round((recent_max - full_max).total_seconds() / 3600, 4)
            else:
                row["gap_hours"] = 0

            records.append(row)

    audit = pd.DataFrame(records)

    summary = (
        audit.groupby(
            ["plan_type", "broker", "timeframe", "append_feasibility", "append_reason"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            total_full_rows=("full_rows", "sum"),
            total_recent_rows=("recent_rows", "sum"),
            max_gap_hours=("gap_hours", "max"),
        )
        .reset_index()
        .sort_values(["append_feasibility", "timeframe", "candidates"], ascending=[True, True, False])
    )

    audit_latest = OUTPUT_DIR / "incremental_append_design_audit_latest.csv"
    audit_ts = OUTPUT_DIR / f"incremental_append_design_audit_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "incremental_append_design_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"incremental_append_design_summary_{run_ts}.csv"

    candidates_latest = OUTPUT_DIR / "incremental_append_candidates_latest.csv"
    candidates_ts = OUTPUT_DIR / f"incremental_append_candidates_{run_ts}.csv"

    blocked_latest = OUTPUT_DIR / "incremental_append_blocked_latest.csv"
    blocked_ts = OUTPUT_DIR / f"incremental_append_blocked_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"incremental_append_design_audit_{run_ts}.json"

    append_candidates = audit[audit["append_feasibility"].eq("append_candidate")].copy()
    blocked = audit[audit["append_feasibility"].eq("blocked")].copy()

    audit.to_csv(audit_latest, index=False)
    audit.to_csv(audit_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    append_candidates.to_csv(candidates_latest, index=False)
    append_candidates.to_csv(candidates_ts, index=False)

    blocked.to_csv(blocked_latest, index=False)
    blocked.to_csv(blocked_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "audit_latest": str(audit_latest),
        "summary_latest": str(summary_latest),
        "append_candidates_latest": str(candidates_latest),
        "blocked_latest": str(blocked_latest),
        "total_checked": int(len(audit)),
        "append_candidates": int(len(append_candidates)),
        "blocked": int(len(blocked)),
        "feasibility_counts": audit["append_feasibility"].value_counts().to_dict(),
        "next_recommended_step": (
            "If append candidates appear only after future recent refreshes, build Script 29 as "
            "a dry-run incremental append planner with overlap and duplicate checks."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Incremental append design audit complete.")
    print(f"Audit latest:      {audit_latest}")
    print(f"Summary latest:    {summary_latest}")
    print(f"Candidates latest: {candidates_latest}")
    print(f"Blocked latest:    {blocked_latest}")
    print(f"JSON report:       {json_path}")

    print("-" * 90)
    print("Append feasibility counts:")
    print(audit["append_feasibility"].value_counts().to_string())

    print("\nSummary preview:")
    print(summary.head(80).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()