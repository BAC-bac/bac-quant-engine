"""
BACQE TICK RESEARCH - 26 Build Microstructure / Regime Join - Multi Symbol

Joins each symbol's microstructure feature store to the most suitable
BACQE regime-engine output using an as-of merge.

Preferred regime timeframe order:
    M1 -> M5 -> M15 -> H1 -> H4 -> H8 -> H12 -> D1

This avoids simply choosing the file with the longest overlap, which can
incorrectly favour W1/MN1 files.

The merge is backward-looking only:
    each microstructure bar receives the latest known regime row
    at or before the microstructure bar_start_time.

This avoids lookahead bias.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import re
import numpy as np
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

PREFERRED_TIMEFRAMES = [
    "M1",
    "M5",
    "M15",
    "H1",
    "H4",
    "H8",
    "H12",
    "D1",
]

MAX_ALIGNMENT_GAP_BY_TIMEFRAME_MINUTES = {
    "M1": 3,
    "M5": 10,
    "M15": 30,
    "H1": 120,
    "H4": 480,
    "H8": 960,
    "H12": 1440,
    "D1": 2880,
}

MICRO_FEATURE_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
)

ALIGNMENT_AUDIT_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "microstructure_regime_alignment_audit"
)

OUTPUT_PROCESSED_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "regime_fusion"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "regime_fusion"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "regime_fusion"
)

REGIME_COLUMNS_TO_KEEP = [
    "time",
    "trend_state",
    "volatility_state",
    "momentum_state",
    "trend_strength_state",
    "composite_regime",
    "regime_confidence",
]


def get_micro_feature_path(symbol: str) -> Path:
    return (
        MICRO_FEATURE_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_feature_store_latest.parquet"
    )


def get_audit_path(symbol: str) -> Path:
    return (
        ALIGNMENT_AUDIT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regime_alignment_audit_latest.csv"
    )


def extract_timeframe_from_path(file_path: str) -> str | None:
    path = str(file_path).replace("\\", "/")

    for timeframe in PREFERRED_TIMEFRAMES + ["M2", "M3", "M10", "M30", "H2", "H3", "W1", "MN1"]:
        if f"/{timeframe}/" in path:
            return timeframe

    match = re.search(r"_([MHDW][N]?\d+)_", path)
    if match:
        return match.group(1)

    return None


def load_microstructure(symbol: str) -> pd.DataFrame:
    path = get_micro_feature_path(symbol)

    if not path.exists():
        raise FileNotFoundError(f"Microstructure feature store not found: {path}")

    micro = pd.read_parquet(path)

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

    micro = micro.dropna(subset=["bar_start_time"])
    micro = micro.sort_values("bar_start_time").reset_index(drop=True)

    return micro


def load_alignment_audit(symbol: str) -> pd.DataFrame:
    path = get_audit_path(symbol)

    if not path.exists():
        raise FileNotFoundError(f"Alignment audit not found: {path}")

    audit = pd.read_csv(path, low_memory=False)

    audit["detected_timeframe"] = audit["file_path"].apply(extract_timeframe_from_path)
    audit["overlap_days"] = pd.to_numeric(audit["overlap_days"], errors="coerce")
    audit["row_count"] = pd.to_numeric(audit["row_count"], errors="coerce")

    return audit


def choose_regime_file(symbol: str, audit: pd.DataFrame) -> dict:
    candidates = audit[
        (audit["overlap_status"] == "overlap")
        & (audit["read_status"] == "success")
        & (audit["regime_columns_detected"].notna())
        & (audit["detected_timeframe"].isin(PREFERRED_TIMEFRAMES))
    ].copy()

    if candidates.empty:
        raise ValueError(f"{symbol}: no suitable overlapping regime file found.")

    candidates = candidates[candidates["overlap_days"] >= 5].copy()

    if candidates.empty:
        raise ValueError(f"{symbol}: no suitable regime file with at least 5 days overlap found.")

    candidates["timeframe_priority"] = candidates["detected_timeframe"].apply(
        lambda tf: PREFERRED_TIMEFRAMES.index(tf)
        if tf in PREFERRED_TIMEFRAMES
        else 999
    )

    candidates["is_regime_file"] = candidates["file_name"].str.contains(
        "regime",
        case=False,
        na=False,
    )

    candidates = candidates.sort_values(
        [
            "timeframe_priority",
            "is_regime_file",
            "overlap_days",
            "row_count",
        ],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)

    chosen = candidates.iloc[0].to_dict()

    return chosen


def load_regimes(chosen: dict) -> tuple[pd.DataFrame, str]:
    regime_path = Path(chosen["file_path"])
    timeframe = str(chosen["detected_timeframe"])

    if not regime_path.exists():
        raise FileNotFoundError(f"Regime file not found: {regime_path}")

    if regime_path.suffix.lower() == ".parquet":
        regimes = pd.read_parquet(regime_path)
    elif regime_path.suffix.lower() == ".csv":
        regimes = pd.read_csv(regime_path, low_memory=False)
    else:
        raise ValueError(f"Unsupported regime file type: {regime_path}")

    timestamp_col = chosen.get("chosen_timestamp_column", None)

    if timestamp_col is None or pd.isna(timestamp_col) or timestamp_col not in regimes.columns:
        if "time" in regimes.columns:
            timestamp_col = "time"
        else:
            raise KeyError(f"No valid timestamp column found in regime file: {regime_path}")

    regimes[timestamp_col] = pd.to_datetime(
        regimes[timestamp_col],
        errors="coerce",
        utc=True,
    )

    regimes = (
        regimes.dropna(subset=[timestamp_col])
        .sort_values(timestamp_col)
        .reset_index(drop=True)
    )

    keep_cols = [col for col in REGIME_COLUMNS_TO_KEEP if col in regimes.columns]

    if timestamp_col not in keep_cols:
        keep_cols = [timestamp_col] + keep_cols

    regimes = regimes[keep_cols].copy()

    prefix = timeframe.lower()

    rename_map = {
        timestamp_col: f"regime_{prefix}_time",
        "trend_state": f"{prefix}_trend_state",
        "volatility_state": f"{prefix}_volatility_state",
        "momentum_state": f"{prefix}_momentum_state",
        "trend_strength_state": f"{prefix}_trend_strength_state",
        "composite_regime": f"{prefix}_composite_regime",
        "regime_confidence": f"{prefix}_regime_confidence",
    }

    regimes = regimes.rename(columns=rename_map)

    return regimes, timeframe


def merge_microstructure_with_regime(
    micro: pd.DataFrame,
    regimes: pd.DataFrame,
    timeframe: str,
) -> pd.DataFrame:
    prefix = timeframe.lower()
    regime_time_col = f"regime_{prefix}_time"

    tolerance_minutes = MAX_ALIGNMENT_GAP_BY_TIMEFRAME_MINUTES.get(timeframe, 30)

    micro_sorted = micro.sort_values("bar_start_time").reset_index(drop=True)
    regimes_sorted = regimes.sort_values(regime_time_col).reset_index(drop=True)

    fused = pd.merge_asof(
        micro_sorted,
        regimes_sorted,
        left_on="bar_start_time",
        right_on=regime_time_col,
        direction="backward",
        tolerance=pd.Timedelta(minutes=tolerance_minutes),
    )

    fused["selected_regime_timeframe"] = timeframe
    fused["selected_regime_tolerance_minutes"] = tolerance_minutes

    fused["regime_alignment_gap_seconds"] = (
        fused["bar_start_time"] - fused[regime_time_col]
    ).dt.total_seconds()

    fused["regime_alignment_gap_minutes"] = (
        fused["regime_alignment_gap_seconds"] / 60
    )

    fused["has_selected_regime"] = fused[regime_time_col].notna()
    fused["fusion_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return fused


def build_alignment_summary(
    symbol: str,
    fused: pd.DataFrame,
    chosen: dict,
) -> pd.DataFrame:
    records = []

    for bar_type, group in fused.groupby("bar_type", dropna=False):
        records.append(
            {
                "symbol": symbol,
                "selected_timeframe": chosen.get("detected_timeframe"),
                "selected_file_name": chosen.get("file_name"),
                "selected_file_path": chosen.get("file_path"),
                "bar_type": bar_type,
                "rows": len(group),
                "matched_rows": int(group["has_selected_regime"].sum()),
                "matched_pct": round(group["has_selected_regime"].mean() * 100, 6),
                "avg_alignment_gap_minutes": round(
                    group["regime_alignment_gap_minutes"].mean(),
                    6,
                ),
                "max_alignment_gap_minutes": round(
                    group["regime_alignment_gap_minutes"].max(),
                    6,
                ),
                "first_bar_time": group["bar_start_time"].min(),
                "last_bar_time": group["bar_start_time"].max(),
                "summary_time_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    return pd.DataFrame(records).sort_values("bar_type").reset_index(drop=True)


def build_fusion_summary(
    symbol: str,
    fused: pd.DataFrame,
    timeframe: str,
) -> pd.DataFrame:
    prefix = timeframe.lower()

    group_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        f"{prefix}_composite_regime",
        f"{prefix}_trend_state",
        f"{prefix}_volatility_state",
        f"{prefix}_momentum_state",
    ]

    existing_group_cols = [col for col in group_cols if col in fused.columns]

    summary = (
        fused.groupby(existing_group_cols, dropna=False)
        .agg(
            rows=("bar_type", "count"),
            matched_regime_pct=("has_selected_regime", "mean"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
            avg_alignment_gap_minutes=("regime_alignment_gap_minutes", "mean"),
            target_up_h1_pct=("target_up_h1", "mean"),
            target_direction_persist_h1_pct=("target_direction_persist_h1", "mean"),
            target_direction_flip_h1_pct=("target_direction_flip_h1", "mean"),
        )
        .reset_index()
    )

    confidence_col = f"{prefix}_regime_confidence"
    if confidence_col in fused.columns:
        confidence_summary = (
            fused.groupby(existing_group_cols, dropna=False)
            .agg(avg_regime_confidence=(confidence_col, "mean"))
            .reset_index()
        )
        summary = summary.merge(confidence_summary, on=existing_group_cols, how="left")

    for col in [
        "matched_regime_pct",
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]:
        if col in summary.columns:
            summary[col] = summary[col] * 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["selected_timeframe"] = timeframe
    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = summary.sort_values(
        ["symbol", "bar_type", "rows"],
        ascending=[True, True, False],
    ).reset_index(drop=True)

    return summary


def build_symbol_report(
    symbol: str,
    chosen: dict,
    alignment_summary: pd.DataFrame,
    fusion_summary: pd.DataFrame,
) -> str:
    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - MICROSTRUCTURE / REGIME JOIN REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC:     {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Selected timeframe:  {chosen.get('detected_timeframe')}")
    lines.append(f"Selected file:       {chosen.get('file_path')}")
    lines.append(f"Overlap days:        {chosen.get('overlap_days')}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("ALIGNMENT SUMMARY")
    lines.append("-" * 90)
    lines.append(alignment_summary.to_string(index=False))

    lines.append("")
    lines.append("FUSION SUMMARY PREVIEW")
    lines.append("-" * 90)
    lines.append(fusion_summary.head(40).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This merge uses merge_asof with direction='backward' to avoid lookahead bias.")
    lines.append("The selected regime source is chosen by preferred timeframe order, not longest overlap.")
    lines.append("Script 27 can now analyse microstructure behaviour by selected regime state.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    micro = load_microstructure(symbol)
    audit = load_alignment_audit(symbol)
    chosen = choose_regime_file(symbol, audit)

    print(f"[INFO] Micro rows:        {len(micro):,}")
    print(f"[INFO] Selected file:     {chosen['file_name']}")
    print(f"[INFO] Selected timeframe:{chosen['detected_timeframe']}")
    print(f"[INFO] Overlap days:      {chosen.get('overlap_days')}")

    regimes, timeframe = load_regimes(chosen)

    print(f"[INFO] Regime rows:       {len(regimes):,}")

    fused = merge_microstructure_with_regime(micro, regimes, timeframe)
    alignment_summary = build_alignment_summary(symbol, fused, chosen)
    fusion_summary = build_fusion_summary(symbol, fused, timeframe)

    processed_dir = OUTPUT_PROCESSED_ROOT / f"symbol={symbol}"
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    processed_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    fused_parquet = processed_dir / f"{symbol}_microstructure_regime_fusion_latest.parquet"
    fused_csv = processed_dir / f"{symbol}_microstructure_regime_fusion_latest.csv"

    alignment_csv = analysis_dir / f"{symbol}_microstructure_regime_alignment_summary_latest.csv"
    alignment_parquet = analysis_dir / f"{symbol}_microstructure_regime_alignment_summary_latest.parquet"

    fusion_csv = analysis_dir / f"{symbol}_microstructure_regime_fusion_summary_latest.csv"
    fusion_parquet = analysis_dir / f"{symbol}_microstructure_regime_fusion_summary_latest.parquet"

    chosen_json = analysis_dir / f"{symbol}_selected_regime_source_latest.json"

    report_path = report_dir / f"{symbol}_microstructure_regime_join_report_latest.txt"

    fused.to_parquet(fused_parquet, index=False)
    fused.to_csv(fused_csv, index=False)

    alignment_summary.to_csv(alignment_csv, index=False)
    alignment_summary.to_parquet(alignment_parquet, index=False)

    fusion_summary.to_csv(fusion_csv, index=False)
    fusion_summary.to_parquet(fusion_parquet, index=False)

    with open(chosen_json, "w", encoding="utf-8") as f:
        json.dump(chosen, f, indent=4, default=str)

    report = build_symbol_report(
        symbol=symbol,
        chosen=chosen,
        alignment_summary=alignment_summary,
        fusion_summary=fusion_summary,
    )
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: fused CSV:      {fused_csv}")
    print(f"[DONE] {symbol}: alignment CSV:  {alignment_csv}")
    print(f"[DONE] {symbol}: fusion CSV:     {fusion_csv}")
    print(f"[DONE] {symbol}: selected JSON:  {chosen_json}")
    print(f"[DONE] {symbol}: report:         {report_path}")

    return fused, alignment_summary, fusion_summary


def save_master_outputs(
    fused_frames: list[pd.DataFrame],
    alignment_frames: list[pd.DataFrame],
    fusion_frames: list[pd.DataFrame],
) -> None:
    master_processed_dir = OUTPUT_PROCESSED_ROOT / "_master"
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_processed_dir.mkdir(parents=True, exist_ok=True)
    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_fused = pd.concat(fused_frames, ignore_index=True)
    master_alignment = pd.concat(alignment_frames, ignore_index=True)
    master_fusion = pd.concat(fusion_frames, ignore_index=True)

    fused_parquet = master_processed_dir / "master_microstructure_regime_fusion_latest.parquet"
    fused_csv = master_processed_dir / "master_microstructure_regime_fusion_latest.csv"

    alignment_csv = master_analysis_dir / "master_microstructure_regime_alignment_summary_latest.csv"
    alignment_parquet = master_analysis_dir / "master_microstructure_regime_alignment_summary_latest.parquet"

    fusion_csv = master_analysis_dir / "master_microstructure_regime_fusion_summary_latest.csv"
    fusion_parquet = master_analysis_dir / "master_microstructure_regime_fusion_summary_latest.parquet"

    master_fused.to_parquet(fused_parquet, index=False)
    master_fused.to_csv(fused_csv, index=False)

    master_alignment.to_csv(alignment_csv, index=False)
    master_alignment.to_parquet(alignment_parquet, index=False)

    master_fusion.to_csv(fusion_csv, index=False)
    master_fusion.to_parquet(fusion_parquet, index=False)

    report_path = master_report_dir / "master_microstructure_regime_join_report_latest.txt"

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER MICROSTRUCTURE / REGIME JOIN REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "MASTER ALIGNMENT SUMMARY",
                "-" * 90,
                master_alignment.to_string(index=False),
                "",
                "MASTER FUSION SUMMARY PREVIEW",
                "-" * 90,
                master_fusion.head(80).to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master microstructure/regime fusion created.")
    print(f"Master fused CSV:      {fused_csv}")
    print(f"Master alignment CSV:  {alignment_csv}")
    print(f"Master fusion CSV:     {fusion_csv}")
    print(f"Master report:         {report_path}")

    print("-" * 90)
    print("MASTER ALIGNMENT SUMMARY")
    print(master_alignment.to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 26 BUILD MICROSTRUCTURE / REGIME JOIN - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:                  {BROKER}")
    print(f"Preferred timeframes:    {PREFERRED_TIMEFRAMES}")
    print(f"Micro feature root:      {MICRO_FEATURE_ROOT}")
    print(f"Alignment audit root:    {ALIGNMENT_AUDIT_ROOT}")
    print(f"Output processed root:   {OUTPUT_PROCESSED_ROOT}")
    print(f"Output analysis root:    {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:      {OUTPUT_REPORT_ROOT}")
    print("-" * 90)

    fused_frames = []
    alignment_frames = []
    fusion_frames = []

    for symbol in SYMBOLS:
        try:
            fused, alignment_summary, fusion_summary = process_symbol(symbol)

            if not fused.empty:
                fused_frames.append(fused)

            if not alignment_summary.empty:
                alignment_frames.append(alignment_summary)

            if not fusion_summary.empty:
                fusion_frames.append(fusion_summary)

        except Exception as exc:
            print(f"[ERROR] {symbol}: {exc}")

    if not fused_frames:
        print("[WARN] No fused datasets created.")
        return

    save_master_outputs(
        fused_frames=fused_frames,
        alignment_frames=alignment_frames,
        fusion_frames=fusion_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol microstructure/regime join complete.")
    print(f"Symbols fused: {len(fused_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()