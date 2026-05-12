"""
BACQE Script 22
Quarantined Naming Mismatch Inspector

Purpose:
- Inspect quarantined naming mismatch cases from Script 20
- Compare recent source files against similar existing full-base files
- Identify likely symbol parsing/name collision issues
- Produce a read-only resolution report

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"
BASE_CREATION_PLAN_DIR = LEDGER_DIR / "base_creation_plans"

QUARANTINE_LATEST = BASE_CREATION_PLAN_DIR / "missing_base_creation_quarantine_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "naming_mismatch_inspection"


def split_similar_files(value) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split("|") if item.strip()]


def path_exists(value) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    return Path(text).exists()


def read_schema(path_value) -> list[str]:
    if not path_exists(path_value):
        return []

    path = Path(str(path_value))

    try:
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq
            return pq.ParquetFile(path).schema.names

        if path.suffix.lower() == ".csv":
            return list(pd.read_csv(path, nrows=0).columns)
    except Exception:
        return []

    return []


def inspect_file_basic(path_value) -> dict:
    result = {
        "exists": False,
        "size_mb": None,
        "rows": None,
        "latest_timestamp": None,
        "timestamp_column": None,
        "columns": [],
        "error": None,
    }

    if not path_exists(path_value):
        return result

    path = Path(str(path_value))
    result["exists"] = True
    result["size_mb"] = round(path.stat().st_size / (1024 * 1024), 4)

    try:
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(path)
            result["rows"] = pf.metadata.num_rows
            df = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
            result["rows"] = len(df)
        else:
            return result

        result["columns"] = list(df.columns)

        datetime_candidates = [
            "timestamp",
            "time",
            "datetime",
            "date",
            "bar_time",
            "event_dt",
            "open_time",
        ]

        for col in datetime_candidates:
            if col in df.columns:
                converted = pd.to_datetime(df[col], errors="coerce", utc=True)
                if converted.notna().any():
                    result["latest_timestamp"] = converted.max().isoformat()
                    result["timestamp_column"] = col
                    break

        if result["latest_timestamp"] is None and isinstance(df.index, pd.DatetimeIndex):
            result["latest_timestamp"] = df.index.max().isoformat()
            result["timestamp_column"] = "index"

    except Exception as exc:
        result["error"] = str(exc)

    return result


def normalise_symbol(value) -> str:
    if pd.isna(value):
        return ""
    return (
        str(value)
        .upper()
        .replace("_", "")
        .replace("-", "")
        .replace(".", "")
        .replace("CASH", "")
        .replace("C", "")
        .strip()
    )


def likely_collision_reason(symbol: str, similar_files: list[str]) -> str:
    norm_symbol = normalise_symbol(symbol)

    if len(str(symbol)) <= 2:
        return "Very short symbol may collide with longer filenames."

    if "." in str(symbol):
        return "Symbol contains punctuation; parser may match partial cleaned names."

    for item in similar_files:
        stem = Path(item).stem
        norm_stem = normalise_symbol(stem)

        if norm_symbol and norm_symbol in norm_stem and norm_symbol != norm_stem:
            return "Symbol appears as substring of existing filename after normalisation."

    return "Potential partial string match from similarity search."


def suggested_resolution(row, source_info, best_match_path, best_match_info) -> str:
    symbol = str(row.get("symbol", ""))
    timeframe = str(row.get("timeframe", ""))
    source_exists = source_info.get("exists", False)
    best_exists = best_match_info.get("exists", False)

    if not source_exists:
        return "Source recent file missing. Regenerate recent layer or rerun Script 20."

    if not best_exists:
        return "No usable similar full file exists. Treat as genuine missing after manual review."

    if source_info.get("columns") == best_match_info.get("columns"):
        if len(symbol) <= 2:
            return (
                "Likely false naming collision caused by short symbol. "
                "Probably safe to treat as genuine missing if expected path is absent."
            )

        return (
            "Schema matches similar file. Review symbol naming; likely genuine missing file "
            "rather than true duplicate."
        )

    return (
        "Schema differs from similar file. Keep quarantined until parser/naming logic is reviewed."
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE QUARANTINED NAMING MISMATCH INSPECTOR")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Quarantine:   {QUARANTINE_LATEST}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    if not QUARANTINE_LATEST.exists():
        raise FileNotFoundError(f"Quarantine file not found: {QUARANTINE_LATEST}")

    quarantine_df = pd.read_csv(QUARANTINE_LATEST)

    print(f"Quarantined rows loaded: {len(quarantine_df)}")

    records = []

    for _, row in quarantine_df.iterrows():
        source_path = row.get("source_recent_path")
        destination_path = row.get("destination_full_path")
        similar_files = split_similar_files(row.get("similar_files"))

        source_info = inspect_file_basic(source_path)

        best_match_path = similar_files[0] if similar_files else ""
        best_match_info = inspect_file_basic(best_match_path) if best_match_path else {
            "exists": False,
            "size_mb": None,
            "rows": None,
            "latest_timestamp": None,
            "timestamp_column": None,
            "columns": [],
            "error": None,
        }

        schema_match_with_best = source_info.get("columns") == best_match_info.get("columns")

        reason = likely_collision_reason(row.get("symbol"), similar_files)

        resolution = suggested_resolution(
            row=row,
            source_info=source_info,
            best_match_path=best_match_path,
            best_match_info=best_match_info,
        )

        suggested_status = (
            "promote_to_genuine_missing_candidate"
            if "Probably safe" in resolution or "likely genuine missing" in resolution.lower()
            else "keep_quarantined"
        )

        records.append({
            "plan_type": row.get("plan_type"),
            "broker": row.get("broker"),
            "timeframe": row.get("timeframe"),
            "symbol": row.get("symbol"),
            "risk_level": row.get("risk_level"),
            "creation_action": row.get("creation_action"),
            "source_recent_path": source_path,
            "destination_full_path": destination_path,
            "destination_exists_now": row.get("destination_exists_now"),
            "source_exists": source_info.get("exists"),
            "source_rows": source_info.get("rows"),
            "source_size_mb": source_info.get("size_mb"),
            "source_latest_timestamp": source_info.get("latest_timestamp"),
            "source_timestamp_column": source_info.get("timestamp_column"),
            "similar_file_count": len(similar_files),
            "best_match_path": best_match_path,
            "best_match_exists": best_match_info.get("exists"),
            "best_match_rows": best_match_info.get("rows"),
            "best_match_size_mb": best_match_info.get("size_mb"),
            "best_match_latest_timestamp": best_match_info.get("latest_timestamp"),
            "schema_match_with_best": schema_match_with_best,
            "likely_collision_reason": reason,
            "suggested_resolution": resolution,
            "suggested_status": suggested_status,
            "source_columns": "|".join(source_info.get("columns", [])),
            "best_match_columns": "|".join(best_match_info.get("columns", [])),
            "all_similar_files": " | ".join(similar_files),
            "source_error": source_info.get("error"),
            "best_match_error": best_match_info.get("error"),
        })

    inspection_df = pd.DataFrame(records)

    summary = (
        inspection_df.groupby(
            ["plan_type", "broker", "timeframe", "suggested_status"],
            dropna=False,
        )
        .agg(
            cases=("symbol", "count"),
            schema_matches=("schema_match_with_best", "sum"),
            source_rows=("source_rows", "sum"),
        )
        .reset_index()
        .sort_values(["suggested_status", "cases"], ascending=[True, False])
    )

    inspection_latest = OUTPUT_DIR / "naming_mismatch_inspection_latest.csv"
    inspection_ts = OUTPUT_DIR / f"naming_mismatch_inspection_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "naming_mismatch_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"naming_mismatch_summary_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"naming_mismatch_inspection_{run_ts}.json"

    inspection_df.to_csv(inspection_latest, index=False)
    inspection_df.to_csv(inspection_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "quarantine_file": str(QUARANTINE_LATEST),
        "inspection_latest": str(inspection_latest),
        "summary_latest": str(summary_latest),
        "total_cases": int(len(inspection_df)),
        "suggested_status_counts": inspection_df["suggested_status"].value_counts().to_dict(),
        "next_recommended_step": (
            "Review inspection output. If cases are false naming collisions, improve the similarity "
            "logic in Script 19 and rebuild the missing-base investigation/plans."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Naming mismatch inspection complete.")
    print(f"Inspection latest: {inspection_latest}")
    print(f"Summary latest:    {summary_latest}")
    print(f"JSON report:       {json_path}")

    print("\nSuggested status counts:")
    print(inspection_df["suggested_status"].value_counts().to_string())

    print("\nSummary preview:")
    print(summary.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()