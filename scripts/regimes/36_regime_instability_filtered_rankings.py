"""
BACQE Script 36
Regime Instability Filtered Rankings

Purpose:
- Build cleaner, more robust instability rankings from Script 35
- Reduce small-sample distortion
- Separate tactical, swing, position, and long-horizon timeframes
- Produce filtered unstable/stable rankings and regime-family summaries

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_instability_intelligence"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_instability_rankings"

SYMBOL_REGIME_FILE = INPUT_DIR / "regime_symbol_regime_instability_latest.csv"
SYMBOL_TIMEFRAME_FILE = INPUT_DIR / "regime_symbol_timeframe_instability_latest.csv"
GLOBAL_INSTABILITY_FILE = INPUT_DIR / "regime_global_instability_latest.csv"


TACTICAL_TIMEFRAMES = {"M1", "M2", "M3", "M5", "M10", "M15", "M30"}
INTRADAY_SWING_TIMEFRAMES = {"H1", "H2", "H3", "H4", "H8", "H12"}
POSITION_TIMEFRAMES = {"D1", "W1"}
LONG_HORIZON_TIMEFRAMES = {"MN1"}


MIN_TOTAL_FROM_REGIME = 100
MIN_SEGMENTS = 10
MIN_TOTAL_TRANSITIONS = 500
MIN_AVG_SEGMENT_BARS = 1.5


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def classify_timeframe_group(timeframe: str) -> str:
    tf = str(timeframe).upper()

    if tf in TACTICAL_TIMEFRAMES:
        return "tactical_intraday"
    if tf in INTRADAY_SWING_TIMEFRAMES:
        return "intraday_swing"
    if tf in POSITION_TIMEFRAMES:
        return "position"
    if tf in LONG_HORIZON_TIMEFRAMES:
        return "long_horizon"

    return "unknown"


def regime_family(regime: str) -> str:
    r = str(regime).lower()

    if "volatile_transition" in r:
        return "volatile_transition"
    if "volatile_range" in r:
        return "volatile_range"
    if "transition" in r:
        return "transition"
    if "range" in r:
        return "range"
    if "bull_trend_high_vol" in r:
        return "bull_trend_high_vol"
    if "bear_trend_high_vol" in r:
        return "bear_trend_high_vol"
    if "bull_trend_normal_vol" in r:
        return "bull_trend_normal_vol"
    if "bear_trend_normal_vol" in r:
        return "bear_trend_normal_vol"
    if "bull_trend_low_vol" in r:
        return "bull_trend_low_vol"
    if "bear_trend_low_vol" in r:
        return "bear_trend_low_vol"
    if "bull" in r:
        return "bull_other"
    if "bear" in r:
        return "bear_other"

    return "other"


def add_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_cols = [
        "instability_score",
        "stability_score",
        "leave_probability",
        "stay_probability",
        "segments",
        "avg_segment_bars",
        "median_segment_bars",
        "max_segment_bars",
        "min_segment_bars",
        "total_from_regime",
        "total_transitions",
        "regime_change_count",
        "regime_change_rate",
        "unique_regimes",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    out["timeframe_group"] = out["timeframe"].apply(classify_timeframe_group)
    out["regime_family"] = out["regime"].apply(regime_family)

    out["passes_sample_filter"] = (
        (out["total_from_regime"] >= MIN_TOTAL_FROM_REGIME)
        & (out["segments"] >= MIN_SEGMENTS)
        & (out["total_transitions"] >= MIN_TOTAL_TRANSITIONS)
        & (out["avg_segment_bars"] >= MIN_AVG_SEGMENT_BARS)
    )

    out["sample_quality"] = "weak"
    out.loc[out["passes_sample_filter"], "sample_quality"] = "robust"

    out.loc[
        (out["total_from_regime"] >= MIN_TOTAL_FROM_REGIME)
        & (out["segments"] >= MIN_SEGMENTS)
        & (out["avg_segment_bars"] < MIN_AVG_SEGMENT_BARS),
        "sample_quality",
    ] = "short_duration_bias"

    out.loc[
        (out["total_from_regime"] < MIN_TOTAL_FROM_REGIME)
        | (out["segments"] < MIN_SEGMENTS),
        "sample_quality",
    ] = "small_sample"

    out["filtered_instability_rank"] = (
        out[out["passes_sample_filter"]]
        ["instability_score"]
        .rank(method="dense", ascending=False)
    )

    return out


def build_group_rankings(filtered: pd.DataFrame) -> dict[str, pd.DataFrame]:
    robust = filtered[filtered["passes_sample_filter"]].copy()

    outputs = {}

    outputs["robust_top_unstable"] = robust.sort_values(
        ["instability_score", "leave_probability", "regime_change_rate"],
        ascending=[False, False, False],
    ).head(1000)

    outputs["robust_top_stable"] = robust.sort_values(
        ["instability_score", "stay_probability", "avg_segment_bars"],
        ascending=[True, False, False],
    ).head(1000)

    for group in [
        "tactical_intraday",
        "intraday_swing",
        "position",
        "long_horizon",
    ]:
        subset = robust[robust["timeframe_group"].eq(group)].copy()

        outputs[f"{group}_unstable"] = subset.sort_values(
            ["instability_score", "leave_probability"],
            ascending=[False, False],
        ).head(500)

        outputs[f"{group}_stable"] = subset.sort_values(
            ["instability_score", "stay_probability"],
            ascending=[True, False],
        ).head(500)

    outputs["weak_sample_review"] = filtered[
        ~filtered["passes_sample_filter"]
    ].sort_values(
        ["instability_score", "leave_probability"],
        ascending=[False, False],
    ).head(1000)

    return outputs


def build_summaries(filtered: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    robust = filtered[filtered["passes_sample_filter"]].copy()

    timeframe_summary = (
        robust.groupby(["broker", "timeframe", "timeframe_group"], dropna=False)
        .agg(
            robust_regime_rows=("regime", "count"),
            avg_instability_score=("instability_score", "mean"),
            median_instability_score=("instability_score", "median"),
            max_instability_score=("instability_score", "max"),
            avg_leave_probability=("leave_probability", "mean"),
            avg_regime_change_rate=("regime_change_rate", "mean"),
            avg_segment_bars=("avg_segment_bars", "mean"),
            total_segments=("segments", "sum"),
            total_from_regime=("total_from_regime", "sum"),
        )
        .reset_index()
    )

    regime_family_summary = (
        robust.groupby(["broker", "timeframe_group", "regime_family"], dropna=False)
        .agg(
            robust_regime_rows=("regime", "count"),
            avg_instability_score=("instability_score", "mean"),
            median_instability_score=("instability_score", "median"),
            max_instability_score=("instability_score", "max"),
            avg_leave_probability=("leave_probability", "mean"),
            avg_stay_probability=("stay_probability", "mean"),
            avg_segment_bars=("avg_segment_bars", "mean"),
            total_segments=("segments", "sum"),
            total_from_regime=("total_from_regime", "sum"),
        )
        .reset_index()
    )

    sample_quality_summary = (
        filtered.groupby(["timeframe_group", "sample_quality"], dropna=False)
        .agg(
            rows=("regime", "count"),
            avg_instability_score=("instability_score", "mean"),
            avg_leave_probability=("leave_probability", "mean"),
            avg_segment_bars=("avg_segment_bars", "mean"),
        )
        .reset_index()
    )

    for frame in [timeframe_summary, regime_family_summary, sample_quality_summary]:
        for col in frame.columns:
            if col.startswith("avg_") or col.startswith("median_") or col.startswith("max_"):
                frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0).round(6)

    return timeframe_summary, regime_family_summary, sample_quality_summary


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME INSTABILITY FILTERED RANKINGS")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input dir:    {INPUT_DIR}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    symbol_regime = read_required(SYMBOL_REGIME_FILE)
    symbol_timeframe = read_required(SYMBOL_TIMEFRAME_FILE)
    global_instability = read_required(GLOBAL_INSTABILITY_FILE)

    print(f"Symbol-regime rows loaded:    {len(symbol_regime):,}")
    print(f"Symbol-timeframe rows loaded: {len(symbol_timeframe):,}")
    print(f"Global instability rows:      {len(global_instability):,}")

    filtered = add_filters(symbol_regime)

    robust_count = int(filtered["passes_sample_filter"].sum())
    weak_count = int((~filtered["passes_sample_filter"]).sum())

    rankings = build_group_rankings(filtered)

    timeframe_summary, regime_family_summary, sample_quality_summary = build_summaries(filtered)

    filtered_latest = OUTPUT_DIR / "regime_instability_filtered_all_latest.csv"
    filtered_ts = OUTPUT_DIR / f"regime_instability_filtered_all_{run_ts}.csv"

    timeframe_summary_latest = OUTPUT_DIR / "regime_instability_filtered_timeframe_summary_latest.csv"
    timeframe_summary_ts = OUTPUT_DIR / f"regime_instability_filtered_timeframe_summary_{run_ts}.csv"

    regime_family_summary_latest = OUTPUT_DIR / "regime_instability_filtered_regime_family_summary_latest.csv"
    regime_family_summary_ts = OUTPUT_DIR / f"regime_instability_filtered_regime_family_summary_{run_ts}.csv"

    sample_quality_latest = OUTPUT_DIR / "regime_instability_sample_quality_summary_latest.csv"
    sample_quality_ts = OUTPUT_DIR / f"regime_instability_sample_quality_summary_{run_ts}.csv"

    filtered.to_csv(filtered_latest, index=False)
    filtered.to_csv(filtered_ts, index=False)

    timeframe_summary.to_csv(timeframe_summary_latest, index=False)
    timeframe_summary.to_csv(timeframe_summary_ts, index=False)

    regime_family_summary.to_csv(regime_family_summary_latest, index=False)
    regime_family_summary.to_csv(regime_family_summary_ts, index=False)

    sample_quality_summary.to_csv(sample_quality_latest, index=False)
    sample_quality_summary.to_csv(sample_quality_ts, index=False)

    ranking_paths = {}

    for name, df in rankings.items():
        latest_path = OUTPUT_DIR / f"regime_instability_{name}_latest.csv"
        ts_path = OUTPUT_DIR / f"regime_instability_{name}_{run_ts}.csv"

        df.to_csv(latest_path, index=False)
        df.to_csv(ts_path, index=False)

        ranking_paths[name] = str(latest_path)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input_symbol_regime_rows": int(len(symbol_regime)),
        "filtered_rows": int(len(filtered)),
        "robust_rows": robust_count,
        "weak_rows": weak_count,
        "minimum_total_from_regime": MIN_TOTAL_FROM_REGIME,
        "minimum_segments": MIN_SEGMENTS,
        "minimum_total_transitions": MIN_TOTAL_TRANSITIONS,
        "minimum_avg_segment_bars": MIN_AVG_SEGMENT_BARS,
        "output_dir": str(OUTPUT_DIR),
        "ranking_paths": ranking_paths,
        "next_recommended_step": (
            "Inspect robust unstable/stable rankings. "
            "Next script can convert filtered instability into regime risk scores or live alerts."
        ),
    }

    json_latest = OUTPUT_DIR / "regime_instability_filtered_rankings_latest.json"
    json_ts = OUTPUT_DIR / f"regime_instability_filtered_rankings_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Filtered instability rankings created.")
    print(f"Filtered rows:       {len(filtered):,}")
    print(f"Robust rows:         {robust_count:,}")
    print(f"Weak/sample rows:    {weak_count:,}")
    print(f"Filtered all:        {filtered_latest}")
    print(f"Top robust unstable: {ranking_paths['robust_top_unstable']}")
    print(f"Top robust stable:   {ranking_paths['robust_top_stable']}")
    print(f"Family summary:      {regime_family_summary_latest}")
    print(f"JSON summary:        {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()