"""
BACQE Script 34
Regime Transition Matrix Builder

Purpose:
- Build regime transition analytics from classified regime outputs
- Calculate regime -> next regime transition counts and probabilities
- Calculate regime persistence / duration statistics
- Produce symbol/timeframe-level transition summaries

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

CLASSIFIED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "classified"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_transition_intelligence"

MAX_FILES = None  # Set to an integer for testing, e.g. 20


TIMESTAMP_CANDIDATES = [
    "timestamp",
    "time",
    "datetime",
    "date",
    "bar_time",
    "open_time",
]

REGIME_CANDIDATES = [
    "composite_regime",
    "regime",
    "regime_state",
    "regime_class",
    "regime_name",
    "regime_label",
    "classified_regime",
    "market_regime",
    "final_regime",
    "primary_regime",
    "market_state",
    "state",
]


def list_classified_files() -> list[Path]:
    files = [p for p in CLASSIFIED_DIR.rglob("*.parquet") if p.is_file()]
    return sorted(files)


def infer_metadata(path: Path) -> dict:
    try:
        rel = path.relative_to(CLASSIFIED_DIR)
        parts = rel.parts

        broker = parts[0] if len(parts) >= 3 else "unknown"
        timeframe = parts[1] if len(parts) >= 3 else "unknown"

        stem = path.stem
        symbol = stem

        if stem.endswith("_classified"):
            symbol = stem.replace("_classified", "")

        suffix = f"_{timeframe}"
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]

        return {
            "broker": broker,
            "timeframe": timeframe,
            "symbol": symbol,
        }

    except Exception:
        return {
            "broker": "unknown",
            "timeframe": "unknown",
            "symbol": path.stem,
        }


def find_first_existing(columns: list[str], candidates: list[str]) -> str | None:
    for col in candidates:
        if col in columns:
            return col
    return None


def load_classified_file(path: Path) -> tuple[pd.DataFrame | None, dict]:
    meta = infer_metadata(path)

    info = {
        **meta,
        "file_path": str(path),
        "status": "ok",
        "error": None,
        "rows": 0,
        "timestamp_col": None,
        "regime_col": None,
    }

    try:
        df = pd.read_parquet(path)
        info["rows"] = len(df)

        timestamp_col = find_first_existing(list(df.columns), TIMESTAMP_CANDIDATES)
        regime_col = find_first_existing(list(df.columns), REGIME_CANDIDATES)

        if timestamp_col is None:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index().rename(columns={"index": "timestamp"})
                timestamp_col = "timestamp"
            else:
                info["status"] = "skipped"
                info["error"] = "no_timestamp_column"
                return None, info

        if regime_col is None:
            info["status"] = "skipped"
            info["error"] = "no_regime_column"
            return None, info

        df = df[[timestamp_col, regime_col]].copy()
        df = df.rename(columns={timestamp_col: "timestamp", regime_col: "regime"})

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["regime"] = df["regime"].astype(str)

        df = df.dropna(subset=["timestamp"])
        df = df[df["regime"].notna()]
        df = df.sort_values("timestamp").reset_index(drop=True)

        info["timestamp_col"] = timestamp_col
        info["regime_col"] = regime_col
        info["clean_rows"] = len(df)

        if df.empty:
            info["status"] = "skipped"
            info["error"] = "empty_after_cleaning"
            return None, info

        for key, value in meta.items():
            df[key] = value

        return df, info

    except Exception as exc:
        info["status"] = "error"
        info["error"] = str(exc)
        return None, info


def build_transitions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp").copy()

    df["next_timestamp"] = df["timestamp"].shift(-1)
    df["next_regime"] = df["regime"].shift(-1)

    transitions = df.dropna(subset=["next_regime"]).copy()
    transitions["transition"] = transitions["regime"] + " -> " + transitions["next_regime"]
    transitions["is_regime_change"] = transitions["regime"] != transitions["next_regime"]

    return transitions


def build_duration_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp").copy()
    df["regime_change"] = df["regime"].ne(df["regime"].shift())
    df["segment_id"] = df["regime_change"].cumsum()

    segments = (
        df.groupby(["broker", "timeframe", "symbol", "segment_id", "regime"], dropna=False)
        .agg(
            segment_start=("timestamp", "min"),
            segment_end=("timestamp", "max"),
            bars=("timestamp", "count"),
        )
        .reset_index()
    )

    return segments


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME TRANSITION MATRIX BUILDER")
    print("=" * 90)
    print(f"Project root:    {PROJECT_ROOT}")
    print(f"Classified dir:  {CLASSIFIED_DIR}")
    print(f"Output dir:      {OUTPUT_DIR}")
    print("-" * 90)

    files = list_classified_files()
    if MAX_FILES is not None:
        files = files[:MAX_FILES]

    print(f"Classified files found: {len(files)}")

    all_transitions = []
    all_segments = []
    file_records = []

    for idx, path in enumerate(files, start=1):
        if idx % 250 == 0 or idx == 1:
            print(f"[LOAD] {idx}/{len(files)}: {path}")

        df, info = load_classified_file(path)
        file_records.append(info)

        if df is None or df.empty:
            continue

        transitions = build_transitions(df)
        segments = build_duration_stats(df)

        all_transitions.append(transitions)
        all_segments.append(segments)

    file_audit = pd.DataFrame(file_records)

    if all_transitions:
        transitions_df = pd.concat(all_transitions, ignore_index=True)
    else:
        transitions_df = pd.DataFrame()

    if all_segments:
        segments_df = pd.concat(all_segments, ignore_index=True)
    else:
        segments_df = pd.DataFrame()

    if transitions_df.empty:
        print("[WARN] No transition data created.")
        return

    transition_counts = (
        transitions_df.groupby(
            ["broker", "timeframe", "symbol", "regime", "next_regime"],
            dropna=False,
        )
        .agg(
            transition_count=("transition", "count"),
            regime_changes=("is_regime_change", "sum"),
        )
        .reset_index()
    )

    totals = (
        transition_counts.groupby(["broker", "timeframe", "symbol", "regime"], dropna=False)
        .agg(total_from_regime=("transition_count", "sum"))
        .reset_index()
    )

    transition_matrix = transition_counts.merge(
        totals,
        on=["broker", "timeframe", "symbol", "regime"],
        how="left",
    )

    transition_matrix["transition_probability"] = (
        transition_matrix["transition_count"] / transition_matrix["total_from_regime"]
    ).round(6)

    transition_summary = (
        transitions_df.groupby(["broker", "timeframe", "symbol"], dropna=False)
        .agg(
            total_transitions=("transition", "count"),
            regime_change_count=("is_regime_change", "sum"),
            unique_regimes=("regime", "nunique"),
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )

    transition_summary["regime_change_rate"] = (
        transition_summary["regime_change_count"] / transition_summary["total_transitions"]
    ).round(6)

    duration_summary = (
        segments_df.groupby(["broker", "timeframe", "symbol", "regime"], dropna=False)
        .agg(
            segments=("segment_id", "count"),
            avg_segment_bars=("bars", "mean"),
            median_segment_bars=("bars", "median"),
            max_segment_bars=("bars", "max"),
            min_segment_bars=("bars", "min"),
        )
        .reset_index()
    )

    duration_summary["avg_segment_bars"] = duration_summary["avg_segment_bars"].round(3)
    duration_summary["median_segment_bars"] = duration_summary["median_segment_bars"].round(3)

    global_transition_matrix = (
        transitions_df.groupby(["broker", "timeframe", "regime", "next_regime"], dropna=False)
        .agg(
            transition_count=("transition", "count"),
            regime_changes=("is_regime_change", "sum"),
        )
        .reset_index()
    )

    global_totals = (
        global_transition_matrix.groupby(["broker", "timeframe", "regime"], dropna=False)
        .agg(total_from_regime=("transition_count", "sum"))
        .reset_index()
    )

    global_transition_matrix = global_transition_matrix.merge(
        global_totals,
        on=["broker", "timeframe", "regime"],
        how="left",
    )

    global_transition_matrix["transition_probability"] = (
        global_transition_matrix["transition_count"] / global_transition_matrix["total_from_regime"]
    ).round(6)

    output_paths = {
        "file_audit_latest": OUTPUT_DIR / "regime_transition_file_audit_latest.csv",
        "transition_matrix_latest": OUTPUT_DIR / "regime_transition_matrix_latest.csv",
        "global_transition_matrix_latest": OUTPUT_DIR / "regime_global_transition_matrix_latest.csv",
        "transition_summary_latest": OUTPUT_DIR / "regime_transition_summary_latest.csv",
        "duration_summary_latest": OUTPUT_DIR / "regime_duration_summary_latest.csv",
        "segments_latest": OUTPUT_DIR / "regime_segments_latest.csv",
    }

    timestamped_paths = {
        name.replace("_latest", f"_{run_ts}"): path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for name, path in output_paths.items()
    }

    file_audit.to_csv(output_paths["file_audit_latest"], index=False)
    transition_matrix.to_csv(output_paths["transition_matrix_latest"], index=False)
    global_transition_matrix.to_csv(output_paths["global_transition_matrix_latest"], index=False)
    transition_summary.to_csv(output_paths["transition_summary_latest"], index=False)
    duration_summary.to_csv(output_paths["duration_summary_latest"], index=False)
    segments_df.to_csv(output_paths["segments_latest"], index=False)

    file_audit.to_csv(timestamped_paths["file_audit_" + run_ts], index=False)
    transition_matrix.to_csv(timestamped_paths["transition_matrix_" + run_ts], index=False)
    global_transition_matrix.to_csv(timestamped_paths["global_transition_matrix_" + run_ts], index=False)
    transition_summary.to_csv(timestamped_paths["transition_summary_" + run_ts], index=False)
    duration_summary.to_csv(timestamped_paths["duration_summary_" + run_ts], index=False)
    segments_df.to_csv(timestamped_paths["segments_" + run_ts], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "classified_files_found": len(files),
        "files_ok": int(file_audit["status"].eq("ok").sum()),
        "files_skipped": int(file_audit["status"].eq("skipped").sum()),
        "files_error": int(file_audit["status"].eq("error").sum()),
        "transition_rows": int(len(transitions_df)),
        "transition_matrix_rows": int(len(transition_matrix)),
        "global_transition_matrix_rows": int(len(global_transition_matrix)),
        "segment_rows": int(len(segments_df)),
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect global transition matrix and duration summary. "
            "Next script can identify high-instability regimes and transition risk scores."
        ),
    }

    json_latest = OUTPUT_DIR / "regime_transition_matrix_builder_latest.json"
    json_ts = OUTPUT_DIR / f"regime_transition_matrix_builder_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Regime transition analytics created.")
    print(f"Files OK:             {payload['files_ok']}")
    print(f"Files skipped:        {payload['files_skipped']}")
    print(f"Files error:          {payload['files_error']}")
    print(f"Transition rows:      {payload['transition_rows']:,}")
    print(f"Transition matrix:    {output_paths['transition_matrix_latest']}")
    print(f"Global matrix:        {output_paths['global_transition_matrix_latest']}")
    print(f"Duration summary:     {output_paths['duration_summary_latest']}")
    print(f"JSON summary:         {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()