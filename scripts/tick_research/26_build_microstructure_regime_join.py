"""
BACQE TICK RESEARCH - 26 Build Microstructure / Regime Join

Joins GBPUSD microstructure feature store to BACQE M15 regime states.

Uses an as-of merge:
    each microstructure bar receives the latest known M15 regime row
    at or before the microstructure bar_start_time.

This avoids lookahead bias.

Outputs:
    E:/Quant_Lab/data/processed/tick_research/regime_fusion/GBPUSD_microstructure_m15_regime_fusion_latest.parquet
    E:/Quant_Lab/data/analysis/tick_research/microstructure_m15_regime_fusion_summary_latest.csv
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
SYMBOL = "GBPUSD"
BROKER = "FTMO"
TIMEFRAME = "M15"

MICRO_FEATURE_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
    / f"{SYMBOL}_microstructure_feature_store_latest.parquet"
)

REGIME_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "regimes"
    / "classified"
    / BROKER
    / TIMEFRAME
    / f"{SYMBOL}_{TIMEFRAME}_regimes.parquet"
)

OUTPUT_PROCESSED_DIR = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "regime_fusion"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

MAX_ALIGNMENT_GAP_MINUTES = 30


REGIME_COLUMNS_TO_KEEP = [
    "time",
    "trend_state",
    "volatility_state",
    "momentum_state",
    "trend_strength_state",
    "composite_regime",
    "regime_confidence",
]


def load_microstructure() -> pd.DataFrame:
    if not MICRO_FEATURE_PATH.exists():
        raise FileNotFoundError(f"Microstructure feature store not found: {MICRO_FEATURE_PATH}")

    micro = pd.read_parquet(MICRO_FEATURE_PATH)

    micro["bar_start_time"] = pd.to_datetime(micro["bar_start_time"], errors="coerce", utc=True)
    micro["bar_end_time"] = pd.to_datetime(micro["bar_end_time"], errors="coerce", utc=True)

    micro = micro.dropna(subset=["bar_start_time"])
    micro = micro.sort_values("bar_start_time").reset_index(drop=True)

    return micro


def load_regimes() -> pd.DataFrame:
    if not REGIME_PATH.exists():
        raise FileNotFoundError(f"Regime file not found: {REGIME_PATH}")

    regimes = pd.read_parquet(REGIME_PATH)

    if "time" not in regimes.columns:
        raise KeyError(f"Expected 'time' column not found in regime file: {REGIME_PATH}")

    regimes["time"] = pd.to_datetime(regimes["time"], errors="coerce", utc=True)

    regimes = regimes.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    keep_cols = [col for col in REGIME_COLUMNS_TO_KEEP if col in regimes.columns]
    regimes = regimes[keep_cols].copy()

    rename_map = {
        "time": "regime_m15_time",
        "trend_state": "m15_trend_state",
        "volatility_state": "m15_volatility_state",
        "momentum_state": "m15_momentum_state",
        "trend_strength_state": "m15_trend_strength_state",
        "composite_regime": "m15_composite_regime",
        "regime_confidence": "m15_regime_confidence",
    }

    regimes = regimes.rename(columns=rename_map)

    return regimes


def merge_microstructure_with_regime(micro: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    micro_sorted = micro.sort_values("bar_start_time").reset_index(drop=True)
    regimes_sorted = regimes.sort_values("regime_m15_time").reset_index(drop=True)

    fused = pd.merge_asof(
        micro_sorted,
        regimes_sorted,
        left_on="bar_start_time",
        right_on="regime_m15_time",
        direction="backward",
        tolerance=pd.Timedelta(minutes=MAX_ALIGNMENT_GAP_MINUTES),
    )

    fused["regime_alignment_gap_seconds"] = (
        fused["bar_start_time"] - fused["regime_m15_time"]
    ).dt.total_seconds()

    fused["regime_alignment_gap_minutes"] = fused["regime_alignment_gap_seconds"] / 60

    fused["has_m15_regime"] = fused["regime_m15_time"].notna()

    fused["fusion_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return fused


def build_summary(fused: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fused.groupby(
            [
                "bar_type",
                "bar_family",
                "m15_composite_regime",
                "m15_trend_state",
                "m15_volatility_state",
                "m15_momentum_state",
            ],
            dropna=False,
        )
        .agg(
            rows=("bar_type", "count"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
            avg_regime_confidence=("m15_regime_confidence", "mean"),
            avg_alignment_gap_minutes=("regime_alignment_gap_minutes", "mean"),
            target_up_h1_pct=("target_up_h1", "mean"),
            target_direction_persist_h1_pct=("target_direction_persist_h1", "mean"),
            target_direction_flip_h1_pct=("target_direction_flip_h1", "mean"),
        )
        .reset_index()
    )

    for col in [
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]:
        if col in summary.columns:
            summary[col] = summary[col] * 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = summary.sort_values(["bar_type", "rows"], ascending=[True, False]).reset_index(drop=True)

    return summary


def build_alignment_summary(fused: pd.DataFrame) -> pd.DataFrame:
    records = []

    for bar_type, group in fused.groupby("bar_type", dropna=False):
        records.append(
            {
                "bar_type": bar_type,
                "rows": len(group),
                "matched_rows": int(group["has_m15_regime"].sum()),
                "matched_pct": round(group["has_m15_regime"].mean() * 100, 6),
                "avg_alignment_gap_minutes": round(group["regime_alignment_gap_minutes"].mean(), 6),
                "max_alignment_gap_minutes": round(group["regime_alignment_gap_minutes"].max(), 6),
                "first_bar_time": group["bar_start_time"].min(),
                "last_bar_time": group["bar_start_time"].max(),
                "summary_time_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    return pd.DataFrame(records).sort_values("bar_type").reset_index(drop=True)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 26 BUILD MICROSTRUCTURE / M15 REGIME JOIN")
    print("=" * 90)
    print(f"Micro feature store: {MICRO_FEATURE_PATH}")
    print(f"Regime file:         {REGIME_PATH}")
    print("-" * 90)

    micro = load_microstructure()
    regimes = load_regimes()

    print(f"Micro rows:   {len(micro):,}")
    print(f"Micro start:  {micro['bar_start_time'].min()}")
    print(f"Micro end:    {micro['bar_start_time'].max()}")
    print("-" * 90)
    print(f"Regime rows:  {len(regimes):,}")
    print(f"Regime start: {regimes['regime_m15_time'].min()}")
    print(f"Regime end:   {regimes['regime_m15_time'].max()}")
    print("-" * 90)

    fused = merge_microstructure_with_regime(micro, regimes)

    summary = build_summary(fused)
    alignment_summary = build_alignment_summary(fused)

    OUTPUT_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    fused_parquet = OUTPUT_PROCESSED_DIR / f"{SYMBOL}_microstructure_m15_regime_fusion_latest.parquet"
    fused_csv = OUTPUT_PROCESSED_DIR / f"{SYMBOL}_microstructure_m15_regime_fusion_latest.csv"

    summary_csv = OUTPUT_ANALYSIS_DIR / "microstructure_m15_regime_fusion_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "microstructure_m15_regime_fusion_summary_latest.parquet"

    alignment_csv = OUTPUT_ANALYSIS_DIR / "microstructure_m15_regime_alignment_summary_latest.csv"
    alignment_parquet = OUTPUT_ANALYSIS_DIR / "microstructure_m15_regime_alignment_summary_latest.parquet"

    fused.to_parquet(fused_parquet, index=False)
    fused.to_csv(fused_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    alignment_summary.to_csv(alignment_csv, index=False)
    alignment_summary.to_parquet(alignment_parquet, index=False)

    print("[DONE] Microstructure / M15 regime fusion created.")
    print(f"Fused Parquet:     {fused_parquet}")
    print(f"Fused CSV:         {fused_csv}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"Summary Parquet:   {summary_parquet}")
    print(f"Alignment CSV:     {alignment_csv}")
    print(f"Alignment Parquet: {alignment_parquet}")
    print("-" * 90)

    print("ALIGNMENT SUMMARY")
    print(alignment_summary.to_string(index=False))
    print("-" * 90)

    display_cols = [
        "bar_type",
        "bar_family",
        "m15_composite_regime",
        "m15_trend_state",
        "m15_volatility_state",
        "rows",
        "avg_abs_return",
        "return_std",
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "avg_regime_confidence",
    ]

    available_cols = [col for col in display_cols if col in summary.columns]

    print("FUSION SUMMARY PREVIEW")
    print(summary[available_cols].head(40).to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()