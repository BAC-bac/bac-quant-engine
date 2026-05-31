"""
BACQE TICK RESEARCH - 25 Audit Microstructure / Regime Alignment - Multi Symbol

Audits whether each symbol's microstructure feature store can be aligned with
BACQE regime-engine outputs.

This script does NOT merge yet.
It discovers candidate regime files and checks timestamp/date overlap.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

BROKER = "FTMO"

MICRO_FEATURE_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
)

REGIME_SEARCH_ROOTS = [
    DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "recent",
    DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "classified" / BROKER,
    DATA_LAKE_ROOT / "data" / "analysis" / "regime_transitions" / BROKER,
    DATA_LAKE_ROOT / "data" / "analysis" / "regime_forecasts" / BROKER,
]

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "microstructure_regime_alignment_audit"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "microstructure_regime_alignment_audit"
)


def get_micro_feature_path(symbol: str) -> Path:
    return (
        MICRO_FEATURE_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_feature_store_latest.parquet"
    )


def load_microstructure_window(symbol: str) -> dict:
    micro_path = get_micro_feature_path(symbol)

    if not micro_path.exists():
        raise FileNotFoundError(f"Microstructure feature store not found: {micro_path}")

    micro = pd.read_parquet(micro_path)

    micro["bar_start_time"] = pd.to_datetime(
        micro["bar_start_time"],
        errors="coerce",
        utc=True,
    )
    micro["bar_end_time"] = pd.to_datetime(
        micro["bar_end_time"],
        errors="coerce",
        utc=True,
    )

    return {
        "symbol": symbol,
        "micro_feature_path": str(micro_path),
        "micro_rows": len(micro),
        "micro_columns": len(micro.columns),
        "micro_start": micro["bar_start_time"].min(),
        "micro_end": micro["bar_end_time"].max(),
        "micro_bar_types": "|".join(
            sorted(micro["bar_type"].dropna().astype(str).unique())
        ),
    }


def find_candidate_regime_files(symbol: str) -> list[Path]:
    candidates = []

    patterns = [
        f"*{symbol}*.parquet",
        f"*{symbol}*.csv",
        f"*{symbol.lower()}*.parquet",
        f"*{symbol.lower()}*.csv",
    ]

    for root in REGIME_SEARCH_ROOTS:
        if not root.exists():
            continue

        for pattern in patterns:
            candidates.extend(root.rglob(pattern))

    return sorted(set(candidates))


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

    return [
        col for col in columns
        if any(keyword in col.lower() for keyword in timestamp_keywords)
    ]


def detect_timeframes(df: pd.DataFrame) -> str | None:
    timeframe_cols = [
        col for col in df.columns
        if "timeframe" in col.lower() or col.lower() in {"tf", "period"}
    ]

    if not timeframe_cols:
        return None

    values = []

    for col in timeframe_cols:
        sample = df[col].dropna().astype(str).unique()[:30]
        values.extend(sample)

    return "|".join(sorted(set(values))) if values else None


def detect_regime_columns(df: pd.DataFrame) -> str | None:
    regime_cols = [
        col for col in df.columns
        if "regime" in col.lower()
        or "trend_state" in col.lower()
        or "volatility_state" in col.lower()
        or "momentum_state" in col.lower()
        or "forecast" in col.lower()
        or "state" in col.lower()
    ]

    return "|".join(regime_cols) if regime_cols else None


def profile_candidate_file(
    symbol: str,
    file_path: Path,
    micro_window: dict,
) -> dict:
    record = {
        "symbol": symbol,
        "broker": BROKER,
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
        "micro_start": micro_window["micro_start"].isoformat()
        if pd.notna(micro_window["micro_start"])
        else None,
        "micro_end": micro_window["micro_end"].isoformat()
        if pd.notna(micro_window["micro_end"])
        else None,
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

        record["timeframes_detected"] = detect_timeframes(df)
        record["regime_columns_detected"] = detect_regime_columns(df)

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


def build_symbol_summary(
    symbol: str,
    micro_window: dict,
    audit: pd.DataFrame,
) -> pd.DataFrame:
    if audit.empty:
        overlap_files = 0
        best_overlap_days = 0
        candidate_files = 0
    else:
        candidate_files = len(audit)
        overlap_files = int((audit["overlap_status"] == "overlap").sum())
        best_overlap_days = pd.to_numeric(
            audit["overlap_days"],
            errors="coerce",
        ).max()

    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "broker": BROKER,
                "micro_rows": micro_window["micro_rows"],
                "micro_columns": micro_window["micro_columns"],
                "micro_start": micro_window["micro_start"].isoformat()
                if pd.notna(micro_window["micro_start"])
                else None,
                "micro_end": micro_window["micro_end"].isoformat()
                if pd.notna(micro_window["micro_end"])
                else None,
                "micro_bar_types": micro_window["micro_bar_types"],
                "candidate_regime_files": candidate_files,
                "overlap_files": overlap_files,
                "best_overlap_days": round(best_overlap_days, 4)
                if pd.notna(best_overlap_days)
                else 0,
                "alignment_status": "overlap_found"
                if overlap_files > 0
                else "no_overlap_found",
                "audit_time_utc": datetime.now(timezone.utc).isoformat(),
            }
        ]
    )


def build_report(
    symbol: str,
    micro_window: dict,
    audit: pd.DataFrame,
    summary: pd.DataFrame,
) -> str:
    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - MICROSTRUCTURE / REGIME ALIGNMENT AUDIT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Broker:          {BROKER}")
    lines.append(f"Feature store:   {micro_window['micro_feature_path']}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("MICROSTRUCTURE WINDOW")
    lines.append("-" * 90)
    lines.append(summary.to_string(index=False))
    lines.append("")
    lines.append("TOP CANDIDATE REGIME FILES")
    lines.append("-" * 90)

    if audit.empty:
        lines.append("No candidate files found.")
    else:
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

        lines.append(audit[available_cols].head(40).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This script only audits alignment. It does not merge datasets.")
    lines.append("Files with overlap_status='overlap' are candidates for Script 26 joins.")
    lines.append("Prefer files with strong overlap, clear timestamp columns, and useful regime/state columns.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    try:
        micro_window = load_microstructure_window(symbol)
    except FileNotFoundError as exc:
        print(f"[WARN] {exc}")
        return pd.DataFrame(), pd.DataFrame()

    print(f"[INFO] Micro rows:    {micro_window['micro_rows']:,}")
    print(f"[INFO] Micro columns: {micro_window['micro_columns']:,}")
    print(f"[INFO] Micro start:   {micro_window['micro_start']}")
    print(f"[INFO] Micro end:     {micro_window['micro_end']}")

    candidates = find_candidate_regime_files(symbol)

    print(f"[INFO] Candidate regime files found: {len(candidates):,}")

    records = []

    for i, file_path in enumerate(candidates, start=1):
        print(f"[{i}/{len(candidates)}] Auditing: {file_path}")
        records.append(
            profile_candidate_file(
                symbol=symbol,
                file_path=file_path,
                micro_window=micro_window,
            )
        )

    audit = pd.DataFrame(records)

    if not audit.empty:
        audit = audit.sort_values(
            by=["overlap_status", "overlap_days", "row_count"],
            ascending=[True, False, False],
        ).reset_index(drop=True)

    summary = build_symbol_summary(symbol, micro_window, audit)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    audit_csv = analysis_dir / f"{symbol}_microstructure_regime_alignment_audit_latest.csv"
    audit_parquet = analysis_dir / f"{symbol}_microstructure_regime_alignment_audit_latest.parquet"

    summary_csv = analysis_dir / f"{symbol}_microstructure_regime_alignment_summary_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_microstructure_regime_alignment_summary_latest.parquet"

    report_path = report_dir / f"{symbol}_microstructure_regime_alignment_audit_report_latest.txt"

    audit.to_csv(audit_csv, index=False)
    audit.to_parquet(audit_parquet, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(symbol, micro_window, audit, summary)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: audit CSV:   {audit_csv}")
    print(f"[DONE] {symbol}: summary CSV: {summary_csv}")
    print(f"[DONE] {symbol}: report:      {report_path}")

    return audit, summary


def save_master_outputs(
    audit_frames: list[pd.DataFrame],
    summary_frames: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_audit = (
        pd.concat(audit_frames, ignore_index=True)
        if audit_frames
        else pd.DataFrame()
    )

    master_summary = (
        pd.concat(summary_frames, ignore_index=True)
        if summary_frames
        else pd.DataFrame()
    )

    audit_csv = master_analysis_dir / "master_microstructure_regime_alignment_audit_latest.csv"
    audit_parquet = master_analysis_dir / "master_microstructure_regime_alignment_audit_latest.parquet"

    summary_csv = master_analysis_dir / "master_microstructure_regime_alignment_summary_latest.csv"
    summary_parquet = master_analysis_dir / "master_microstructure_regime_alignment_summary_latest.parquet"

    master_audit.to_csv(audit_csv, index=False)
    master_audit.to_parquet(audit_parquet, index=False)

    master_summary.to_csv(summary_csv, index=False)
    master_summary.to_parquet(summary_parquet, index=False)

    overlap_candidates = pd.DataFrame()

    if not master_audit.empty:
        overlap_candidates = master_audit[
            master_audit["overlap_status"] == "overlap"
        ].copy()

        if not overlap_candidates.empty:
            overlap_candidates = overlap_candidates.sort_values(
                ["symbol", "overlap_days", "row_count"],
                ascending=[True, False, False],
            ).reset_index(drop=True)

    overlap_csv = master_analysis_dir / "master_overlap_regime_candidates_latest.csv"
    overlap_candidates.to_csv(overlap_csv, index=False)

    report_path = master_report_dir / "master_microstructure_regime_alignment_audit_report_latest.txt"

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER MICROSTRUCTURE / REGIME ALIGNMENT AUDIT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "SYMBOL ALIGNMENT SUMMARY",
                "-" * 90,
                master_summary.to_string(index=False)
                if not master_summary.empty
                else "No summary rows generated.",
                "",
                "TOP OVERLAP CANDIDATES",
                "-" * 90,
                overlap_candidates[
                    [
                        "symbol",
                        "file_name",
                        "row_count",
                        "chosen_timestamp_column",
                        "regime_start",
                        "regime_end",
                        "overlap_days",
                        "timeframes_detected",
                        "regime_columns_detected",
                        "file_path",
                    ]
                ]
                .head(50)
                .to_string(index=False)
                if not overlap_candidates.empty
                else "No overlap candidates found.",
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "This script only audits alignment. It does not merge datasets.",
                "Use master_overlap_regime_candidates_latest.csv to guide Script 26.",
                "Script 26 should choose the safest overlapping regime source per symbol.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master microstructure/regime alignment audit created.")
    print(f"Master audit CSV:   {audit_csv}")
    print(f"Master summary CSV: {summary_csv}")
    print(f"Overlap candidates: {overlap_csv}")
    print(f"Master report:      {report_path}")

    if not master_summary.empty:
        print("-" * 90)
        print("SYMBOL ALIGNMENT SUMMARY")
        print(master_summary.to_string(index=False))

    if not overlap_candidates.empty:
        print("-" * 90)
        print("TOP OVERLAP CANDIDATES")
        display_cols = [
            "symbol",
            "file_name",
            "row_count",
            "chosen_timestamp_column",
            "regime_start",
            "regime_end",
            "overlap_days",
            "timeframes_detected",
            "regime_columns_detected",
        ]
        available_cols = [col for col in display_cols if col in overlap_candidates.columns]
        print(overlap_candidates[available_cols].head(30).to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 25 AUDIT MICROSTRUCTURE / REGIME ALIGNMENT - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:             {BROKER}")
    print(f"Micro feature root: {MICRO_FEATURE_ROOT}")
    print(f"Output analysis:    {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:     {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:            {SYMBOLS}")
    print("-" * 90)

    audit_frames = []
    summary_frames = []

    for symbol in SYMBOLS:
        audit, summary = process_symbol(symbol)

        if not audit.empty:
            audit_frames.append(audit)

        if not summary.empty:
            summary_frames.append(summary)

    if not summary_frames:
        print("[WARN] No alignment summaries created.")
        return

    save_master_outputs(
        audit_frames=audit_frames,
        summary_frames=summary_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol microstructure/regime alignment audit complete.")
    print(f"Symbols analysed: {len(summary_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()