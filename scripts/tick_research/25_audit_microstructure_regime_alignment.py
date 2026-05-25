"""
BACQE TICK RESEARCH - 25 Audit Microstructure / Regime Alignment

Phase 3 starting point.

Audits whether the GBPUSD microstructure feature store can be aligned with
BACQE regime-engine outputs.

This script does NOT merge yet.
It discovers candidate regime files and checks timestamp/date overlap.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
SYMBOL = "GBPUSD"
BROKER = "FTMO"

MICRO_FEATURE_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
    / f"{SYMBOL}_microstructure_feature_store_latest.parquet"
)

REGIME_SEARCH_ROOTS = [
    DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "recent",
    DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "classified" / BROKER,
    DATA_LAKE_ROOT / "data" / "analysis" / "regime_transitions" / BROKER,
    DATA_LAKE_ROOT / "data" / "analysis" / "regime_forecasts" / BROKER,
]

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

OUTPUT_CSV = OUTPUT_DIR / "microstructure_regime_alignment_audit_latest.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "microstructure_regime_alignment_audit_latest.parquet"


def load_microstructure_window() -> dict:
    if not MICRO_FEATURE_PATH.exists():
        raise FileNotFoundError(f"Microstructure feature store not found: {MICRO_FEATURE_PATH}")

    micro = pd.read_parquet(MICRO_FEATURE_PATH)

    micro["bar_start_time"] = pd.to_datetime(micro["bar_start_time"], errors="coerce", utc=True)
    micro["bar_end_time"] = pd.to_datetime(micro["bar_end_time"], errors="coerce", utc=True)

    return {
        "micro_rows": len(micro),
        "micro_columns": len(micro.columns),
        "micro_start": micro["bar_start_time"].min(),
        "micro_end": micro["bar_end_time"].max(),
        "micro_bar_types": "|".join(sorted(micro["bar_type"].dropna().astype(str).unique())),
    }


def find_candidate_regime_files() -> list[Path]:
    candidates = []

    patterns = [
        f"*{SYMBOL}*.parquet",
        f"*{SYMBOL}*.csv",
        f"*{SYMBOL.lower()}*.parquet",
        f"*{SYMBOL.lower()}*.csv",
    ]

    for root in REGIME_SEARCH_ROOTS:
        if not root.exists():
            continue

        for pattern in patterns:
            candidates.extend(root.rglob(pattern))

    unique_candidates = sorted(set(candidates))

    return unique_candidates


def guess_timestamp_columns(columns: list[str]) -> list[str]:
    timestamp_keywords = [
        "time",
        "datetime",
        "timestamp",
        "date",
        "bar_time",
        "open_time",
        "close_time",
    ]

    guesses = []

    for col in columns:
        lower = col.lower()
        if any(keyword in lower for keyword in timestamp_keywords):
            guesses.append(col)

    return guesses


def profile_candidate_file(file_path: Path, micro_window: dict) -> dict:
    record = {
        "file_path": str(file_path),
        "file_name": file_path.name,
        "parent_folder": str(file_path.parent),
        "extension": file_path.suffix.lower(),
        "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 4),
        "read_status": "unknown",
        "row_count": None,
        "column_count": None,
        "columns": None,
        "timestamp_column_candidates": None,
        "chosen_timestamp_column": None,
        "regime_start": None,
        "regime_end": None,
        "overlap_start": None,
        "overlap_end": None,
        "overlap_seconds": None,
        "overlap_days": None,
        "overlap_status": "unknown",
        "timeframes_detected": None,
        "regime_columns_detected": None,
        "error_message": None,
        "audit_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    try:
        if file_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(file_path)
        elif file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, low_memory=False)
        else:
            record["read_status"] = "skipped_unsupported"
            return record

        record["read_status"] = "success"
        record["row_count"] = len(df)
        record["column_count"] = len(df.columns)
        record["columns"] = "|".join(df.columns.astype(str))

        timestamp_candidates = guess_timestamp_columns(list(df.columns))
        record["timestamp_column_candidates"] = "|".join(timestamp_candidates)

        timeframe_cols = [col for col in df.columns if "timeframe" in col.lower() or col.lower() in {"tf", "period"}]
        if timeframe_cols:
            values = []
            for col in timeframe_cols:
                sample = df[col].dropna().astype(str).unique()[:20]
                values.extend(sample)
            record["timeframes_detected"] = "|".join(sorted(set(values)))

        regime_cols = [
            col for col in df.columns
            if "regime" in col.lower()
            or "trend_state" in col.lower()
            or "volatility_state" in col.lower()
            or "momentum_state" in col.lower()
            or "forecast" in col.lower()
        ]
        record["regime_columns_detected"] = "|".join(regime_cols)

        chosen_col = None
        best_non_null = 0

        for col in timestamp_candidates:
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            non_null = parsed.notna().sum()

            if non_null > best_non_null:
                best_non_null = non_null
                chosen_col = col

        if chosen_col is None or best_non_null == 0:
            record["overlap_status"] = "no_valid_timestamp_column"
            return record

        record["chosen_timestamp_column"] = chosen_col

        times = pd.to_datetime(df[chosen_col], errors="coerce", utc=True).dropna()

        if times.empty:
            record["overlap_status"] = "no_valid_timestamps"
            return record

        regime_start = times.min()
        regime_end = times.max()

        record["regime_start"] = regime_start.isoformat()
        record["regime_end"] = regime_end.isoformat()

        micro_start = micro_window["micro_start"]
        micro_end = micro_window["micro_end"]

        overlap_start = max(micro_start, regime_start)
        overlap_end = min(micro_end, regime_end)

        if overlap_start <= overlap_end:
            overlap_seconds = (overlap_end - overlap_start).total_seconds()
            record["overlap_start"] = overlap_start.isoformat()
            record["overlap_end"] = overlap_end.isoformat()
            record["overlap_seconds"] = round(overlap_seconds, 2)
            record["overlap_days"] = round(overlap_seconds / 86400, 4)
            record["overlap_status"] = "overlap"
        else:
            record["overlap_status"] = "no_overlap"

        return record

    except Exception as exc:
        record["read_status"] = "failed"
        record["error_message"] = str(exc)[:500]
        return record


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 25 AUDIT MICROSTRUCTURE / REGIME ALIGNMENT")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Micro feature store: {MICRO_FEATURE_PATH}")
    print("-" * 90)

    micro_window = load_microstructure_window()

    print("Microstructure window:")
    print(f"Rows:      {micro_window['micro_rows']:,}")
    print(f"Columns:   {micro_window['micro_columns']:,}")
    print(f"Start:     {micro_window['micro_start']}")
    print(f"End:       {micro_window['micro_end']}")
    print(f"Bar types: {micro_window['micro_bar_types']}")
    print("-" * 90)

    candidates = find_candidate_regime_files()

    print(f"Candidate regime files found: {len(candidates):,}")
    print("-" * 90)

    records = []

    if not candidates:
        print("[WARN] No candidate regime files found.")
    else:
        for i, file_path in enumerate(candidates, start=1):
            print(f"[{i}/{len(candidates)}] Auditing: {file_path}")
            record = profile_candidate_file(file_path, micro_window)
            records.append(record)

    audit = pd.DataFrame(records)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not audit.empty:
        audit = audit.sort_values(
            by=["overlap_status", "overlap_days", "row_count"],
            ascending=[True, False, False],
        ).reset_index(drop=True)

    audit.to_csv(OUTPUT_CSV, index=False)
    audit.to_parquet(OUTPUT_PARQUET, index=False)

    print("-" * 90)
    print("[DONE] Microstructure/regime alignment audit created.")
    print(f"CSV:     {OUTPUT_CSV}")
    print(f"Parquet: {OUTPUT_PARQUET}")
    print("-" * 90)

    if not audit.empty:
        display_cols = [
            "file_name",
            "read_status",
            "row_count",
            "chosen_timestamp_column",
            "regime_start",
            "regime_end",
            "overlap_status",
            "overlap_days",
            "timeframes_detected",
            "regime_columns_detected",
            "file_path",
        ]

        available_cols = [col for col in display_cols if col in audit.columns]

        print(audit[available_cols].head(30).to_string(index=False))
    else:
        print("No audit rows created.")

    print("=" * 90)


if __name__ == "__main__":
    main()