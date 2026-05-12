"""
BACQE Script 19
Missing Full Base Investigation

Purpose:
- Investigate Script 18 missing_full_create_candidate rows
- Identify exact broker/timeframe/symbol gaps
- Search for similar existing full files
- Detect likely naming mismatches
- Produce a creation review report

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
MERGE_PLAN_LATEST = LEDGER_DIR / "merge_plans" / "regime_incremental_merge_plan_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "missing_full_investigation"


FULL_STAGE_DIRS = {
    "feature_update": REGIME_PROCESSED_DIR / "features",
    "classification_update": REGIME_PROCESSED_DIR / "classified",
}


def normalise_symbol(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).upper().replace("_", "").replace("-", "").replace(".", "").strip()


def list_existing_files(base_dir: Path, broker: str, timeframe: str) -> list[Path]:
    folder = base_dir / str(broker) / str(timeframe)

    if not folder.exists():
        return []

    return [p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in {".parquet", ".csv"}]


def find_similar_files(symbol: str, existing_files: list[Path]) -> list[str]:
    norm_symbol = normalise_symbol(symbol)
    matches = []

    for path in existing_files:
        norm_name = normalise_symbol(path.stem)

        if norm_symbol and norm_symbol in norm_name:
            matches.append(str(path))
        elif norm_name and norm_name in norm_symbol:
            matches.append(str(path))

    return matches[:10]


def infer_expected_full_path(row) -> str:
    plan_type = row["plan_type"]
    broker = row["broker"]
    timeframe = row["timeframe"]
    symbol = row["symbol"]

    base_dir = FULL_STAGE_DIRS.get(plan_type)

    if base_dir is None:
        return ""

    if plan_type == "feature_update":
        filename = f"{symbol}_{timeframe}_features.parquet"
    elif plan_type == "classification_update":
        filename = f"{symbol}_{timeframe}_classified.parquet"
    else:
        filename = f"{symbol}_{timeframe}.parquet"

    return str(base_dir / str(broker) / str(timeframe) / filename)


def classify_missing_case(row, similar_files: list[str], expected_path: str) -> str:
    recent_exists = str(row.get("recent_file_exists", "")).lower() == "true"
    expected_exists = Path(expected_path).exists() if expected_path else False

    if expected_exists:
        return "expected_file_exists_but_ledger_missed"

    if similar_files:
        return "possible_naming_mismatch"

    if not recent_exists:
        return "recent_file_missing_now"

    return "genuine_missing_full_candidate"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE MISSING FULL BASE INVESTIGATION")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Merge plan:   {MERGE_PLAN_LATEST}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    if not MERGE_PLAN_LATEST.exists():
        raise FileNotFoundError(f"Missing merge plan: {MERGE_PLAN_LATEST}")

    merge_df = pd.read_csv(MERGE_PLAN_LATEST)

    missing_df = merge_df[
        merge_df["merge_category"].eq("missing_full_create_candidate")
    ].copy()

    print(f"Merge plan rows: {len(merge_df)}")
    print(f"Missing full candidates: {len(missing_df)}")

    records = []

    cache_existing_files = {}

    for _, row in missing_df.iterrows():
        plan_type = row["plan_type"]
        broker = row["broker"]
        timeframe = row["timeframe"]
        symbol = row["symbol"]

        base_dir = FULL_STAGE_DIRS.get(plan_type)

        cache_key = (plan_type, broker, timeframe)

        if cache_key not in cache_existing_files:
            cache_existing_files[cache_key] = list_existing_files(
                base_dir=base_dir,
                broker=broker,
                timeframe=timeframe,
            )

        existing_files = cache_existing_files[cache_key]

        similar_files = find_similar_files(symbol, existing_files)
        expected_path = infer_expected_full_path(row)
        missing_case = classify_missing_case(row, similar_files, expected_path)

        records.append({
            "plan_type": plan_type,
            "broker": broker,
            "timeframe": timeframe,
            "symbol": symbol,
            "missing_case": missing_case,
            "expected_full_path": expected_path,
            "expected_full_exists": Path(expected_path).exists() if expected_path else False,
            "recent_file_path": row.get("example_file_path_recent"),
            "recent_file_exists": row.get("recent_file_exists"),
            "latest_timestamp_recent": row.get("latest_timestamp_recent"),
            "total_rows_recent": row.get("total_rows_recent"),
            "total_size_mb_recent": row.get("total_size_mb_recent"),
            "similar_file_count": len(similar_files),
            "similar_files": " | ".join(similar_files),
        })

    investigation_df = pd.DataFrame(records)

    summary = (
        investigation_df.groupby(
            ["plan_type", "broker", "timeframe", "missing_case"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            total_recent_rows=("total_rows_recent", "sum"),
            total_recent_size_mb=("total_size_mb_recent", "sum"),
        )
        .reset_index()
        .sort_values(["missing_case", "candidates"], ascending=[True, False])
    )

    symbol_summary = (
        investigation_df.groupby(
            ["broker", "timeframe", "symbol"],
            dropna=False,
        )
        .agg(
            plan_types=("plan_type", lambda x: ",".join(sorted(set(map(str, x))))),
            missing_cases=("missing_case", lambda x: ",".join(sorted(set(map(str, x))))),
            recent_rows_total=("total_rows_recent", "sum"),
            recent_size_mb_total=("total_size_mb_recent", "sum"),
        )
        .reset_index()
        .sort_values(["timeframe", "symbol"])
    )

    investigation_latest = OUTPUT_DIR / "missing_full_base_investigation_latest.csv"
    investigation_ts = OUTPUT_DIR / f"missing_full_base_investigation_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "missing_full_base_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"missing_full_base_summary_{run_ts}.csv"

    symbol_summary_latest = OUTPUT_DIR / "missing_full_base_symbol_summary_latest.csv"
    symbol_summary_ts = OUTPUT_DIR / f"missing_full_base_symbol_summary_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"missing_full_base_investigation_{run_ts}.json"

    investigation_df.to_csv(investigation_latest, index=False)
    investigation_df.to_csv(investigation_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    symbol_summary.to_csv(symbol_summary_latest, index=False)
    symbol_summary.to_csv(symbol_summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "merge_plan": str(MERGE_PLAN_LATEST),
        "investigation_latest": str(investigation_latest),
        "summary_latest": str(summary_latest),
        "symbol_summary_latest": str(symbol_summary_latest),
        "total_missing_candidates": int(len(investigation_df)),
        "missing_case_counts": investigation_df["missing_case"].value_counts().to_dict(),
        "next_recommended_step": (
            "If most cases are genuine_missing_full_candidate, build Script 20 as a dry-run "
            "base-file creation planner. Do not write production files until backups and schema checks exist."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Missing full base investigation complete.")
    print(f"Investigation latest: {investigation_latest}")
    print(f"Summary latest:       {summary_latest}")
    print(f"Symbol summary:       {symbol_summary_latest}")
    print(f"JSON report:          {json_path}")

    print("\nMissing case counts:")
    print(investigation_df["missing_case"].value_counts().to_string())

    print("\nSummary preview:")
    print(summary.head(50).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()