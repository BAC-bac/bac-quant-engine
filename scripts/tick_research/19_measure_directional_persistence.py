"""
BACQE TICK RESEARCH - 19 Measure Directional Persistence

Measures microstructure momentum / directional persistence across bar types,
microstructure regimes, and sessions.

Input:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/GBPUSD_microstructure_regimes_latest.parquet

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/directional_persistence_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/directional_persistence_latest.parquet
    E:/Quant_Lab/reports/tick_research/directional_persistence/directional_persistence_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "microstructure_regimes"
    / f"{SYMBOL}_microstructure_regimes_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "directional_persistence"

HORIZONS = [1, 2, 3, 5]
MIN_OBSERVATIONS = 30


def classify_session(hour_utc: int) -> str:
    if 0 <= hour_utc < 7:
        return "asia_overnight"
    if 7 <= hour_utc < 12:
        return "london_morning"
    if 12 <= hour_utc < 16:
        return "london_new_york_overlap"
    if 16 <= hour_utc < 21:
        return "new_york_afternoon"
    return "late_us_rollover"


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(data["bar_start_time"], errors="coerce", utc=True)
    data = data.dropna(subset=["bar_start_time"]).copy()

    data["hour_utc"] = data["bar_start_time"].dt.hour
    data["session_utc"] = data["hour_utc"].apply(classify_session)

    data["return"] = pd.to_numeric(data["return"], errors="coerce")

    data["direction"] = pd.to_numeric(data["direction"], errors="coerce").fillna(0).astype(int)

    data = data.sort_values(["bar_type", "bar_start_time"]).reset_index(drop=True)

    return data


def add_forward_direction_labels(data: pd.DataFrame) -> pd.DataFrame:
    labelled = data.copy()

    for horizon in HORIZONS:
        labelled[f"future_return_h{horizon}"] = (
            labelled.groupby("bar_type")["return"].shift(-horizon)
        )

        labelled[f"future_direction_h{horizon}"] = 0
        labelled.loc[labelled[f"future_return_h{horizon}"] > 0, f"future_direction_h{horizon}"] = 1
        labelled.loc[labelled[f"future_return_h{horizon}"] < 0, f"future_direction_h{horizon}"] = -1

        labelled[f"persist_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == labelled["direction"])
        )

        labelled[f"flip_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == -labelled["direction"])
        )

        labelled[f"future_abs_return_h{horizon}"] = labelled[f"future_return_h{horizon}"].abs()

    return labelled


def summarise_group(labelled: pd.DataFrame, group_cols: list[str], level_name: str) -> pd.DataFrame:
    records = []

    grouped = labelled.groupby(group_cols, dropna=False)

    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)

        base = {
            "summary_level": level_name,
            "observations_total": len(group),
            "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        for col, value in zip(group_cols, keys):
            base[col] = value

        current_directional = group[group["direction"] != 0].copy()
        base["directional_observations"] = len(current_directional)

        for horizon in HORIZONS:
            valid = current_directional.dropna(subset=[f"future_return_h{horizon}"]).copy()

            obs = len(valid)

            base[f"obs_h{horizon}"] = obs

            if obs == 0:
                base[f"persist_pct_h{horizon}"] = np.nan
                base[f"flip_pct_h{horizon}"] = np.nan
                base[f"future_avg_return_h{horizon}"] = np.nan
                base[f"future_avg_abs_return_h{horizon}"] = np.nan
                base[f"persistence_edge_h{horizon}"] = np.nan
                continue

            base[f"persist_pct_h{horizon}"] = valid[f"persist_h{horizon}"].mean() * 100
            base[f"flip_pct_h{horizon}"] = valid[f"flip_h{horizon}"].mean() * 100
            base[f"future_avg_return_h{horizon}"] = valid[f"future_return_h{horizon}"].mean()
            base[f"future_avg_abs_return_h{horizon}"] = valid[f"future_abs_return_h{horizon}"].mean()
            base[f"persistence_edge_h{horizon}"] = (
                base[f"persist_pct_h{horizon}"] - base[f"flip_pct_h{horizon}"]
            )

        records.append(base)

    summary = pd.DataFrame(records)

    for horizon in HORIZONS:
        summary[f"sample_quality_h{horizon}"] = "low_sample"
        summary.loc[summary[f"obs_h{horizon}"] >= MIN_OBSERVATIONS, f"sample_quality_h{horizon}"] = "usable"
        summary.loc[summary[f"obs_h{horizon}"] >= 100, f"sample_quality_h{horizon}"] = "stronger"

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary


def build_report(summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    bar_level = summary[summary["summary_level"] == "bar_type"].copy()

    bar_display_cols = [
        "bar_type",
        "bar_family",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "persist_pct_h2",
        "flip_pct_h2",
        "persistence_edge_h2",
        "persist_pct_h3",
        "persistence_edge_h3",
        "persist_pct_h5",
        "persistence_edge_h5",
    ]

    available_bar_cols = [col for col in bar_display_cols if col in bar_level.columns]

    strongest = summary.copy()
    if "persistence_edge_h1" in strongest.columns:
        strongest = strongest.sort_values("persistence_edge_h1", ascending=False, na_position="last")

    strongest_cols = [
        "summary_level",
        "bar_type",
        "bar_family",
        "microstructure_regime",
        "session_utc",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "sample_quality_h1",
    ]

    available_strong_cols = [col for col in strongest_cols if col in strongest.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - DIRECTIONAL PERSISTENCE REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Horizons:        {HORIZONS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("BAR-LEVEL DIRECTIONAL PERSISTENCE")
    lines.append("-" * 90)

    if bar_level.empty:
        lines.append("No bar-level rows found.")
    else:
        lines.append(bar_level[available_bar_cols].to_string(index=False))

    lines.append("")
    lines.append("STRONGEST H1 PERSISTENCE EDGES")
    lines.append("-" * 90)
    lines.append(strongest[available_strong_cols].head(30).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("persist_pct measures whether current direction continues after N bars.")
    lines.append("flip_pct measures whether current direction reverses after N bars.")
    lines.append("persistence_edge = persist_pct - flip_pct.")
    lines.append("Positive persistence_edge suggests microstructure momentum.")
    lines.append("Negative persistence_edge suggests mean-reversion / reversal pressure.")
    lines.append("Small samples can be misleading; use sample_quality fields.")
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 19 MEASURE DIRECTIONAL PERSISTENCE")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Microstructure regimes file not found: {INPUT_PATH}")

    regimes = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded: {len(regimes):,}")

    data = prepare_data(regimes)
    labelled = add_forward_direction_labels(data)

    summaries = []

    summaries.append(
        summarise_group(
            labelled,
            group_cols=["bar_type", "bar_family"],
            level_name="bar_type",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=["bar_type", "bar_family", "microstructure_regime"],
            level_name="bar_type_regime",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=["bar_type", "bar_family", "session_utc"],
            level_name="bar_type_session",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=["bar_type", "bar_family", "microstructure_regime", "session_utc"],
            level_name="bar_type_regime_session",
        )
    )

    summary = pd.concat(summaries, ignore_index=True)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "directional_persistence_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "directional_persistence_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "directional_persistence_report_latest.txt"

    summary.to_csv(csv_path, index=False)
    summary.to_parquet(parquet_path, index=False)

    report = build_report(summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Directional persistence analysis created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = [
        "summary_level",
        "bar_type",
        "bar_family",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "persist_pct_h2",
        "persistence_edge_h2",
        "persist_pct_h3",
        "persistence_edge_h3",
        "persist_pct_h5",
        "persistence_edge_h5",
    ]

    available_display_cols = [col for col in display_cols if col in summary.columns]

    print(
        summary[summary["summary_level"] == "bar_type"][available_display_cols]
        .to_string(index=False)
    )
    print("=" * 90)


if __name__ == "__main__":
    main()