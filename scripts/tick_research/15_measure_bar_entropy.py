"""
BACQE TICK RESEARCH - 15 Measure Bar Entropy

Measures directional entropy and transition entropy across fixed tick bars
and tick imbalance bars.

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/bar_entropy_analysis_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/bar_entropy_analysis_latest.parquet
    E:/Quant_Lab/reports/tick_research/bar_entropy/bar_entropy_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import math
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_BAR_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "tick_bars"
    / f"symbol={SYMBOL}"
)

IMBALANCE_BAR_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "tick_imbalance_bars"
    / f"symbol={SYMBOL}"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "bar_entropy"

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]


def shannon_entropy(values: pd.Series) -> float:
    clean = values.dropna()

    if clean.empty:
        return np.nan

    probs = clean.value_counts(normalize=True)

    entropy = -sum(p * math.log2(p) for p in probs if p > 0)

    return entropy


def normalized_entropy(values: pd.Series) -> float:
    clean = values.dropna()

    if clean.empty:
        return np.nan

    unique_count = clean.nunique(dropna=True)

    if unique_count <= 1:
        return 0.0

    entropy = shannon_entropy(clean)
    max_entropy = math.log2(unique_count)

    return entropy / max_entropy if max_entropy > 0 else np.nan


def transition_entropy(values: pd.Series) -> float:
    clean = values.dropna().astype(int)

    if len(clean) < 2:
        return np.nan

    transitions = clean.astype(str).shift(1) + "->" + clean.astype(str)
    transitions = transitions.dropna()

    return shannon_entropy(transitions)


def transition_matrix_summary(values: pd.Series) -> dict:
    clean = values.dropna().astype(int)

    if len(clean) < 2:
        return {
            "same_direction_pct": np.nan,
            "direction_flip_pct": np.nan,
            "up_after_up_pct": np.nan,
            "down_after_down_pct": np.nan,
        }

    prev = clean.shift(1)
    curr = clean

    valid = pd.DataFrame({"prev": prev, "curr": curr}).dropna()

    if valid.empty:
        return {
            "same_direction_pct": np.nan,
            "direction_flip_pct": np.nan,
            "up_after_up_pct": np.nan,
            "down_after_down_pct": np.nan,
        }

    same = (valid["prev"] == valid["curr"]).mean() * 100
    flip = (valid["prev"] != valid["curr"]).mean() * 100

    prev_up = valid[valid["prev"] == 1]
    prev_down = valid[valid["prev"] == -1]

    up_after_up = (prev_up["curr"] == 1).mean() * 100 if len(prev_up) else np.nan
    down_after_down = (prev_down["curr"] == -1).mean() * 100 if len(prev_down) else np.nan

    return {
        "same_direction_pct": same,
        "direction_flip_pct": flip,
        "up_after_up_pct": up_after_up,
        "down_after_down_pct": down_after_down,
    }


def load_tick_bars(tick_size: int) -> pd.DataFrame:
    path = (
        TICK_BAR_ROOT
        / f"tick_size={tick_size}"
        / f"{SYMBOL}_tick_bars_{tick_size}_latest.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(f"Tick bar file not found: {path}")

    bars = pd.read_parquet(path)
    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)

    return bars


def load_imbalance_bars(threshold: int) -> pd.DataFrame:
    path = (
        IMBALANCE_BAR_ROOT
        / f"imbalance_threshold={threshold}"
        / f"{SYMBOL}_tick_imbalance_bars_{threshold}_latest.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(f"Imbalance bar file not found: {path}")

    bars = pd.read_parquet(path)
    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)

    return bars


def analyse_entropy(bars: pd.DataFrame) -> dict:
    bars = bars.copy()

    returns = pd.to_numeric(bars["return"], errors="coerce").replace([np.inf, -np.inf], np.nan)

    return_sign = pd.Series(0, index=bars.index, dtype="int64")
    return_sign.loc[returns > 0] = 1
    return_sign.loc[returns < 0] = -1

    direction = pd.to_numeric(bars["direction"], errors="coerce")

    direction_transitions = transition_matrix_summary(direction)
    return_sign_transitions = transition_matrix_summary(return_sign)

    result = {
        "symbol": SYMBOL,
        "broker": BROKER,
        "bar_family": bars["bar_family"].iloc[0],
        "bar_type": bars["bar_type"].iloc[0],
        "bar_parameter": bars["bar_parameter"].iloc[0],
        "bar_count": len(bars),
        "first_bar_time": bars["bar_start_time"].min(),
        "last_bar_time": bars["bar_end_time"].max(),
        "direction_entropy": shannon_entropy(direction),
        "direction_entropy_normalized": normalized_entropy(direction),
        "return_sign_entropy": shannon_entropy(return_sign),
        "return_sign_entropy_normalized": normalized_entropy(return_sign),
        "direction_transition_entropy": transition_entropy(direction),
        "return_sign_transition_entropy": transition_entropy(return_sign),
        "up_pct": (direction == 1).mean() * 100,
        "down_pct": (direction == -1).mean() * 100,
        "flat_pct": (direction == 0).mean() * 100,
        "return_up_pct": (return_sign == 1).mean() * 100,
        "return_down_pct": (return_sign == -1).mean() * 100,
        "return_flat_pct": (return_sign == 0).mean() * 100,
        "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    result.update({f"direction_{k}": v for k, v in direction_transitions.items()})
    result.update({f"return_sign_{k}": v for k, v in return_sign_transitions.items()})

    return result


def build_report(analysis: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "bar_type",
        "bar_family",
        "bar_count",
        "direction_entropy_normalized",
        "return_sign_entropy_normalized",
        "direction_transition_entropy",
        "return_sign_transition_entropy",
        "direction_same_direction_pct",
        "direction_flip_pct",
        "return_sign_same_direction_pct",
        "return_sign_direction_flip_pct",
    ]

    available_cols = [col for col in display_cols if col in analysis.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - BAR ENTROPY REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Symbol:          {SYMBOL}")
    lines.append(f"Broker:          {BROKER}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("ENTROPY SUMMARY")
    lines.append("-" * 90)
    lines.append(analysis[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("Entropy measures randomness or uncertainty in direction/return signs.")
    lines.append("Normalized entropy near 1 suggests near-maximum directional randomness.")
    lines.append("Lower entropy suggests more structure, persistence, or imbalance.")
    lines.append("Transition entropy measures randomness in movement from one state to the next.")
    lines.append("Same-direction percentage measures simple persistence.")
    lines.append("Flip percentage measures alternating behaviour.")
    lines.append("")
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 15 MEASURE BAR ENTROPY")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Broker: {BROKER}")
    print("-" * 90)

    records = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(tick_size)
        records.append(analyse_entropy(bars))
        print(f"[DONE] Analysed tick bars: {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(threshold)
        records.append(analyse_entropy(bars))
        print(f"[DONE] Analysed imbalance bars: {threshold} | bars={len(bars):,}")

    analysis = pd.DataFrame(records)

    numeric_cols = analysis.select_dtypes(include=["float", "int"]).columns
    analysis[numeric_cols] = analysis[numeric_cols].round(8)

    order = {
        "tick_100": 1,
        "tick_250": 2,
        "tick_500": 3,
        "tick_1000": 4,
        "imbalance_25": 5,
        "imbalance_50": 6,
        "imbalance_100": 7,
        "imbalance_200": 8,
    }

    analysis["sort_order"] = analysis["bar_type"].map(order).fillna(999)
    analysis = analysis.sort_values("sort_order").drop(columns=["sort_order"]).reset_index(drop=True)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "bar_entropy_analysis_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "bar_entropy_analysis_latest.parquet"
    report_path = OUTPUT_REPORT_DIR / "bar_entropy_report_latest.txt"

    analysis.to_csv(csv_path, index=False)
    analysis.to_parquet(parquet_path, index=False)

    report = build_report(analysis)
    report_path.write_text(report, encoding="utf-8")

    print("-" * 90)
    print("[DONE] Bar entropy analysis created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = ["bar_type", "bar_count", "direction_entropy_normalized", "return_sign_entropy_normalized",
        "direction_transition_entropy", "direction_same_direction_pct", "direction_direction_flip_pct",
        "return_sign_same_direction_pct", "return_sign_direction_flip_pct", ]

    available_display_cols = [col for col in display_cols if col in analysis.columns]

    print(analysis[available_display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()