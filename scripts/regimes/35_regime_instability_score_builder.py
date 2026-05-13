"""
BACQE Script 35
Regime Instability Score Builder

Purpose:
- Build instability scores from regime transition intelligence
- Score broker/timeframe/symbol/regime combinations by:
  - probability of leaving current regime
  - regime change rate
  - average/median/max duration
  - transition frequency
- Produce ranked instability outputs

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

TRANSITION_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_transition_intelligence"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_instability_intelligence"

TRANSITION_MATRIX = TRANSITION_DIR / "regime_transition_matrix_latest.csv"
TRANSITION_SUMMARY = TRANSITION_DIR / "regime_transition_summary_latest.csv"
DURATION_SUMMARY = TRANSITION_DIR / "regime_duration_summary_latest.csv"
GLOBAL_TRANSITION_MATRIX = TRANSITION_DIR / "regime_global_transition_matrix_latest.csv"


def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    return pd.read_csv(path)


def minmax(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").fillna(0)

    min_v = values.min()
    max_v = values.max()

    if max_v == min_v:
        return pd.Series(0.0, index=series.index)

    return (values - min_v) / (max_v - min_v)


def build_symbol_regime_instability(
    transition_matrix: pd.DataFrame,
    transition_summary: pd.DataFrame,
    duration_summary: pd.DataFrame,
) -> pd.DataFrame:

    tm = transition_matrix.copy()

    tm["transition_count"] = pd.to_numeric(tm["transition_count"], errors="coerce").fillna(0)
    tm["transition_probability"] = pd.to_numeric(tm["transition_probability"], errors="coerce").fillna(0)

    stay = tm[tm["regime"].astype(str).eq(tm["next_regime"].astype(str))].copy()

    stay = stay.rename(columns={
        "transition_probability": "stay_probability",
        "transition_count": "stay_transition_count",
    })

    stay = stay[
        [
            "broker",
            "timeframe",
            "symbol",
            "regime",
            "stay_probability",
            "stay_transition_count",
            "total_from_regime",
        ]
    ]

    base = duration_summary.merge(
        stay,
        on=["broker", "timeframe", "symbol", "regime"],
        how="left",
    )

    base["stay_probability"] = pd.to_numeric(base["stay_probability"], errors="coerce").fillna(0)
    base["leave_probability"] = 1 - base["stay_probability"]

    ts = transition_summary[
        [
            "broker",
            "timeframe",
            "symbol",
            "total_transitions",
            "regime_change_count",
            "unique_regimes",
            "regime_change_rate",
            "first_timestamp",
            "last_timestamp",
        ]
    ].copy()

    base = base.merge(
        ts,
        on=["broker", "timeframe", "symbol"],
        how="left",
    )

    numeric_cols = [
        "segments",
        "avg_segment_bars",
        "median_segment_bars",
        "max_segment_bars",
        "min_segment_bars",
        "total_from_regime",
        "total_transitions",
        "regime_change_count",
        "unique_regimes",
        "regime_change_rate",
    ]

    for col in numeric_cols:
        if col in base.columns:
            base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0)

    base["duration_stability_score"] = minmax(base["avg_segment_bars"])
    base["duration_instability_component"] = 1 - base["duration_stability_score"]

    base["leave_probability_component"] = minmax(base["leave_probability"])
    base["regime_change_rate_component"] = minmax(base["regime_change_rate"])
    base["segment_frequency_component"] = minmax(base["segments"])

    base["instability_score"] = (
        0.40 * base["leave_probability_component"]
        + 0.30 * base["regime_change_rate_component"]
        + 0.20 * base["duration_instability_component"]
        + 0.10 * base["segment_frequency_component"]
    ).round(6)

    base["stability_score"] = (1 - base["instability_score"]).round(6)

    base["instability_band"] = pd.cut(
        base["instability_score"],
        bins=[-0.001, 0.25, 0.50, 0.75, 1.001],
        labels=["low", "moderate", "high", "extreme"],
    ).astype(str)

    return base.sort_values("instability_score", ascending=False).reset_index(drop=True)


def build_global_regime_instability(global_transition_matrix: pd.DataFrame) -> pd.DataFrame:
    gtm = global_transition_matrix.copy()

    gtm["transition_count"] = pd.to_numeric(gtm["transition_count"], errors="coerce").fillna(0)
    gtm["transition_probability"] = pd.to_numeric(gtm["transition_probability"], errors="coerce").fillna(0)

    stay = gtm[gtm["regime"].astype(str).eq(gtm["next_regime"].astype(str))].copy()

    stay = stay.rename(columns={
        "transition_probability": "global_stay_probability",
        "transition_count": "global_stay_transition_count",
    })

    stay = stay[
        [
            "broker",
            "timeframe",
            "regime",
            "global_stay_probability",
            "global_stay_transition_count",
            "total_from_regime",
        ]
    ]

    stay["global_leave_probability"] = 1 - pd.to_numeric(
        stay["global_stay_probability"],
        errors="coerce",
    ).fillna(0)

    stay["global_instability_score"] = minmax(stay["global_leave_probability"]).round(6)

    stay["global_instability_band"] = pd.cut(
        stay["global_instability_score"],
        bins=[-0.001, 0.25, 0.50, 0.75, 1.001],
        labels=["low", "moderate", "high", "extreme"],
    ).astype(str)

    return stay.sort_values(
        ["global_instability_score", "timeframe", "regime"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def build_symbol_timeframe_summary(symbol_regime_instability: pd.DataFrame) -> pd.DataFrame:
    summary = (
        symbol_regime_instability.groupby(["broker", "timeframe", "symbol"], dropna=False)
        .agg(
            regimes_scored=("regime", "count"),
            avg_instability_score=("instability_score", "mean"),
            max_instability_score=("instability_score", "max"),
            avg_leave_probability=("leave_probability", "mean"),
            max_leave_probability=("leave_probability", "max"),
            avg_regime_change_rate=("regime_change_rate", "mean"),
            max_segments=("segments", "max"),
            avg_segment_bars=("avg_segment_bars", "mean"),
            min_avg_segment_bars=("avg_segment_bars", "min"),
            unique_regimes=("unique_regimes", "max"),
            total_transitions=("total_transitions", "max"),
            regime_change_count=("regime_change_count", "max"),
        )
        .reset_index()
    )

    for col in [
        "avg_instability_score",
        "max_instability_score",
        "avg_leave_probability",
        "max_leave_probability",
        "avg_regime_change_rate",
        "avg_segment_bars",
        "min_avg_segment_bars",
    ]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    summary["instability_band"] = pd.cut(
        summary["avg_instability_score"],
        bins=[-0.001, 0.25, 0.50, 0.75, 1.001],
        labels=["low", "moderate", "high", "extreme"],
    ).astype(str)

    return summary.sort_values(
        ["avg_instability_score", "max_instability_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME INSTABILITY SCORE BUILDER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input dir:    {TRANSITION_DIR}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    transition_matrix = read_csv_required(TRANSITION_MATRIX)
    transition_summary = read_csv_required(TRANSITION_SUMMARY)
    duration_summary = read_csv_required(DURATION_SUMMARY)
    global_transition_matrix = read_csv_required(GLOBAL_TRANSITION_MATRIX)

    print(f"Transition matrix rows:        {len(transition_matrix):,}")
    print(f"Transition summary rows:       {len(transition_summary):,}")
    print(f"Duration summary rows:         {len(duration_summary):,}")
    print(f"Global transition matrix rows: {len(global_transition_matrix):,}")

    symbol_regime_instability = build_symbol_regime_instability(
        transition_matrix=transition_matrix,
        transition_summary=transition_summary,
        duration_summary=duration_summary,
    )

    global_regime_instability = build_global_regime_instability(global_transition_matrix)

    symbol_timeframe_summary = build_symbol_timeframe_summary(symbol_regime_instability)

    top_unstable = symbol_regime_instability.head(500).copy()
    top_stable = symbol_regime_instability.sort_values("instability_score", ascending=True).head(500).copy()

    output_paths = {
        "symbol_regime_latest": OUTPUT_DIR / "regime_symbol_regime_instability_latest.csv",
        "symbol_timeframe_latest": OUTPUT_DIR / "regime_symbol_timeframe_instability_latest.csv",
        "global_regime_latest": OUTPUT_DIR / "regime_global_instability_latest.csv",
        "top_unstable_latest": OUTPUT_DIR / "regime_top_unstable_latest.csv",
        "top_stable_latest": OUTPUT_DIR / "regime_top_stable_latest.csv",
    }

    timestamped_paths = {
        name: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for name, path in output_paths.items()
    }

    symbol_regime_instability.to_csv(output_paths["symbol_regime_latest"], index=False)
    symbol_timeframe_summary.to_csv(output_paths["symbol_timeframe_latest"], index=False)
    global_regime_instability.to_csv(output_paths["global_regime_latest"], index=False)
    top_unstable.to_csv(output_paths["top_unstable_latest"], index=False)
    top_stable.to_csv(output_paths["top_stable_latest"], index=False)

    symbol_regime_instability.to_csv(timestamped_paths["symbol_regime_latest"], index=False)
    symbol_timeframe_summary.to_csv(timestamped_paths["symbol_timeframe_latest"], index=False)
    global_regime_instability.to_csv(timestamped_paths["global_regime_latest"], index=False)
    top_unstable.to_csv(timestamped_paths["top_unstable_latest"], index=False)
    top_stable.to_csv(timestamped_paths["top_stable_latest"], index=False)

    band_counts = symbol_regime_instability["instability_band"].value_counts().to_dict()
    timeframe_summary = (
        symbol_timeframe_summary.groupby(["broker", "timeframe", "instability_band"], dropna=False)
        .agg(symbols=("symbol", "count"))
        .reset_index()
    )

    timeframe_summary_latest = OUTPUT_DIR / "regime_instability_timeframe_band_summary_latest.csv"
    timeframe_summary_ts = OUTPUT_DIR / f"regime_instability_timeframe_band_summary_{run_ts}.csv"

    timeframe_summary.to_csv(timeframe_summary_latest, index=False)
    timeframe_summary.to_csv(timeframe_summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "symbol_regime_rows": int(len(symbol_regime_instability)),
        "symbol_timeframe_rows": int(len(symbol_timeframe_summary)),
        "global_regime_rows": int(len(global_regime_instability)),
        "band_counts": band_counts,
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect top unstable and top stable regime outputs. "
            "Next script can build regime risk scores or transition-risk alerts."
        ),
    }

    json_latest = OUTPUT_DIR / "regime_instability_score_builder_latest.json"
    json_ts = OUTPUT_DIR / f"regime_instability_score_builder_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Regime instability scores created.")
    print(f"Symbol-regime rows:      {len(symbol_regime_instability):,}")
    print(f"Symbol-timeframe rows:   {len(symbol_timeframe_summary):,}")
    print(f"Global-regime rows:      {len(global_regime_instability):,}")
    print(f"Top unstable:            {output_paths['top_unstable_latest']}")
    print(f"Top stable:              {output_paths['top_stable_latest']}")
    print(f"Symbol-timeframe scores: {output_paths['symbol_timeframe_latest']}")
    print(f"Global scores:           {output_paths['global_regime_latest']}")
    print(f"JSON summary:            {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()