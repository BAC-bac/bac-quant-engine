"""
BACQE MICROSTRUCTURE 33 - REGIME EDGE STABILITY

Purpose:
    Evaluate which liquidity regimes show the most stable predictive behaviour.

Inputs:
    liquidity_regime_research/
        microstructure_liquidity_regime_research_latest.csv
        microstructure_liquidity_regime_summary_latest.csv
        microstructure_liquidity_transition_summary_latest.csv

Outputs:
    regime_edge_stability/
        microstructure_regime_edge_stability_latest.csv
        microstructure_regime_edge_stability_summary_latest.csv
        microstructure_regime_edge_stability_latest.json
        microstructure_regime_edge_stability_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def safe_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def classify_stability(row: pd.Series) -> str:
    stability_score = row.get("stability_score", 0)
    total_rows = row.get("total_rows", 0)
    regime_count = row.get("regime_count", 0)

    if stability_score >= 85 and total_rows >= 500 and regime_count >= 3:
        return "high_stability"

    if stability_score >= 70 and total_rows >= 250:
        return "research_stability"

    if stability_score >= 55:
        return "watch_stability"

    return "unstable_or_low_sample"


def build_edge_stability(summary_df: pd.DataFrame) -> pd.DataFrame:
    work = summary_df.copy()

    numeric_cols = [
        "total_rows",
        "avg_positive_return_rate",
        "avg_abs_correlation",
        "max_abs_correlation",
        "avg_correlation_sample_size",
        "regime_score",
    ]

    work = safe_numeric(work, numeric_cols)

    group_cols = [
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
    ]

    grouped = (
        work
        .groupby(group_cols, dropna=False)
        .agg(
            regime_count=("liquidity_regime", "nunique"),
            regimes=("liquidity_regime", lambda s: ",".join(sorted(s.dropna().unique()))),
            total_rows=("total_rows", "sum"),
            avg_rows=("total_rows", "mean"),
            min_rows=("total_rows", "min"),
            max_rows=("total_rows", "max"),
            avg_positive_return_rate=("avg_positive_return_rate", "mean"),
            min_positive_return_rate=("avg_positive_return_rate", "min"),
            max_positive_return_rate=("avg_positive_return_rate", "max"),
            std_positive_return_rate=("avg_positive_return_rate", "std"),
            avg_abs_correlation=("avg_abs_correlation", "mean"),
            min_abs_correlation=("avg_abs_correlation", "min"),
            max_abs_correlation=("avg_abs_correlation", "max"),
            std_abs_correlation=("avg_abs_correlation", "std"),
            avg_regime_score=("regime_score", "mean"),
            max_regime_score=("regime_score", "max"),
            regimes_above_score_70=("regime_score", lambda s: int((s >= 70).sum())),
            regimes_above_score_85=("regime_score", lambda s: int((s >= 85).sum())),
        )
        .reset_index()
    )

    grouped["positive_rate_range"] = (
        grouped["max_positive_return_rate"] - grouped["min_positive_return_rate"]
    )

    grouped["correlation_range"] = (
        grouped["max_abs_correlation"] - grouped["min_abs_correlation"]
    )

    grouped["regime_hit_rate_70"] = grouped["regimes_above_score_70"] / grouped["regime_count"]
    grouped["regime_hit_rate_85"] = grouped["regimes_above_score_85"] / grouped["regime_count"]

    grouped["stability_score"] = (
        grouped["avg_abs_correlation"].fillna(0) * 220
        + grouped["regime_hit_rate_70"].fillna(0) * 25
        + grouped["regime_hit_rate_85"].fillna(0) * 20
        + np.log1p(grouped["total_rows"].fillna(0)) * 3
        - grouped["std_abs_correlation"].fillna(0) * 40
        - grouped["std_positive_return_rate"].fillna(0) * 30
    ).clip(0, 100).round(2)

    grouped["stability_label"] = grouped.apply(classify_stability, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "stability_score",
            "avg_abs_correlation",
            "regime_hit_rate_70",
            "total_rows",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["stability_rank"] = grouped.index + 1

    return grouped


def build_regime_level_stability(summary_df: pd.DataFrame) -> pd.DataFrame:
    work = summary_df.copy()

    numeric_cols = [
        "total_rows",
        "avg_positive_return_rate",
        "avg_abs_correlation",
        "max_abs_correlation",
        "regime_score",
    ]

    work = safe_numeric(work, numeric_cols)

    grouped = (
        work
        .groupby(["liquidity_regime"], dropna=False)
        .agg(
            observations=("liquidity_regime", "count"),
            symbols=("symbol", "nunique"),
            bar_types=("bar_type", "nunique"),
            targets=("target", "nunique"),
            spread_features=("spread_feature", "nunique"),
            total_rows=("total_rows", "sum"),
            avg_rows=("total_rows", "mean"),
            avg_positive_return_rate=("avg_positive_return_rate", "mean"),
            std_positive_return_rate=("avg_positive_return_rate", "std"),
            avg_abs_correlation=("avg_abs_correlation", "mean"),
            max_abs_correlation=("max_abs_correlation", "max"),
            std_abs_correlation=("avg_abs_correlation", "std"),
            avg_regime_score=("regime_score", "mean"),
            max_regime_score=("regime_score", "max"),
            score_70_count=("regime_score", lambda s: int((s >= 70).sum())),
            score_85_count=("regime_score", lambda s: int((s >= 85).sum())),
        )
        .reset_index()
    )

    grouped["score_70_rate"] = grouped["score_70_count"] / grouped["observations"]
    grouped["score_85_rate"] = grouped["score_85_count"] / grouped["observations"]

    grouped["regime_stability_score"] = (
        grouped["avg_abs_correlation"].fillna(0) * 250
        + grouped["score_70_rate"].fillna(0) * 25
        + grouped["score_85_rate"].fillna(0) * 20
        + np.log1p(grouped["total_rows"].fillna(0)) * 2
        - grouped["std_abs_correlation"].fillna(0) * 35
        - grouped["std_positive_return_rate"].fillna(0) * 20
    ).clip(0, 100).round(2)

    grouped = grouped.sort_values(
        [
            "regime_stability_score",
            "avg_abs_correlation",
            "score_70_rate",
            "total_rows",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["regime_rank"] = grouped.index + 1

    return grouped


def build_transition_stability(transition_df: pd.DataFrame) -> pd.DataFrame:
    work = transition_df.copy()

    numeric_cols = [
        "tight_liquidity",
        "normal_liquidity",
        "wide_liquidity",
        "extreme_wide_liquidity",
        "tight_to_extreme_delta",
        "normal_to_wide_delta",
        "absolute_tight_to_extreme_delta",
        "absolute_normal_to_wide_delta",
        "transition_score",
    ]

    work = safe_numeric(work, numeric_cols)

    group_cols = [
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
    ]

    grouped = (
        work
        .groupby(group_cols, dropna=False)
        .agg(
            transition_count=("transition_score", "count"),
            avg_transition_score=("transition_score", "mean"),
            max_transition_score=("transition_score", "max"),
            avg_tight_to_extreme_delta=("tight_to_extreme_delta", "mean"),
            max_abs_tight_to_extreme_delta=("absolute_tight_to_extreme_delta", "max"),
            avg_normal_to_wide_delta=("normal_to_wide_delta", "mean"),
            max_abs_normal_to_wide_delta=("absolute_normal_to_wide_delta", "max"),
            transitions_above_50=("transition_score", lambda s: int((s >= 50).sum())),
            transitions_above_75=("transition_score", lambda s: int((s >= 75).sum())),
        )
        .reset_index()
    )

    grouped["transition_hit_rate_50"] = grouped["transitions_above_50"] / grouped["transition_count"]
    grouped["transition_hit_rate_75"] = grouped["transitions_above_75"] / grouped["transition_count"]

    grouped["transition_stability_score"] = (
        grouped["avg_transition_score"].fillna(0) * 0.5
        + grouped["max_transition_score"].fillna(0) * 0.25
        + grouped["transition_hit_rate_50"].fillna(0) * 15
        + grouped["transition_hit_rate_75"].fillna(0) * 15
    ).clip(0, 100).round(2)

    grouped = grouped.sort_values(
        [
            "transition_stability_score",
            "avg_transition_score",
            "max_transition_score",
        ],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    grouped["transition_stability_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 33 - REGIME EDGE STABILITY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    liquidity_dir = analysis_dir / "liquidity_regime_research"

    regime_summary_path = liquidity_dir / "microstructure_liquidity_regime_summary_latest.csv"
    transition_path = liquidity_dir / "microstructure_liquidity_transition_summary_latest.csv"

    report_dir = analysis_dir / "regime_edge_stability"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Regime summary: {regime_summary_path}")
    print(f"Transition:     {transition_path}")
    print(f"Report dir:     {report_dir}")
    print("-" * 90)

    if not regime_summary_path.exists():
        raise FileNotFoundError(
            f"Missing liquidity regime summary: {regime_summary_path}. Run script 32 first."
        )

    if not transition_path.exists():
        raise FileNotFoundError(
            f"Missing liquidity transition summary: {transition_path}. Run script 32 first."
        )

    regime_summary_df = pd.read_csv(regime_summary_path)
    transition_df = pd.read_csv(transition_path)

    edge_stability_df = build_edge_stability(regime_summary_df)
    regime_level_df = build_regime_level_stability(regime_summary_df)
    transition_stability_df = build_transition_stability(transition_df)

    edge_csv = report_dir / "microstructure_regime_edge_stability_latest.csv"
    regime_csv = report_dir / "microstructure_regime_level_stability_latest.csv"
    transition_csv = report_dir / "microstructure_transition_stability_latest.csv"

    json_path = report_dir / "microstructure_regime_edge_stability_latest.json"
    txt_path = report_dir / "microstructure_regime_edge_stability_latest.txt"

    edge_stability_df.to_csv(edge_csv, index=False)
    regime_level_df.to_csv(regime_csv, index=False)
    transition_stability_df.to_csv(transition_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "edge_rows": len(edge_stability_df),
        "regime_rows": len(regime_level_df),
        "transition_rows": len(transition_stability_df),
        "top_edge_stability": edge_stability_df.head(50).to_dict(orient="records"),
        "regime_level_stability": regime_level_df.to_dict(orient="records"),
        "top_transition_stability": transition_stability_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    label_counts = edge_stability_df["stability_label"].value_counts(dropna=False).to_dict()

    edge_display_cols = [
        "stability_rank",
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
        "regime_count",
        "total_rows",
        "avg_positive_return_rate",
        "positive_rate_range",
        "avg_abs_correlation",
        "correlation_range",
        "regime_hit_rate_70",
        "regime_hit_rate_85",
        "stability_score",
        "stability_label",
    ]

    regime_display_cols = [
        "regime_rank",
        "liquidity_regime",
        "observations",
        "symbols",
        "bar_types",
        "targets",
        "total_rows",
        "avg_positive_return_rate",
        "avg_abs_correlation",
        "score_70_rate",
        "score_85_rate",
        "regime_stability_score",
    ]

    transition_display_cols = [
        "transition_stability_rank",
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
        "transition_count",
        "avg_transition_score",
        "max_transition_score",
        "avg_tight_to_extreme_delta",
        "max_abs_tight_to_extreme_delta",
        "transition_hit_rate_50",
        "transition_hit_rate_75",
        "transition_stability_score",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE REGIME EDGE STABILITY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Regime summary rows:      {len(regime_summary_df):,}")
    lines.append(f"Transition rows:          {len(transition_df):,}")
    lines.append(f"Edge stability rows:      {len(edge_stability_df):,}")
    lines.append(f"Regime-level rows:        {len(regime_level_df):,}")
    lines.append(f"Transition stability rows:{len(transition_stability_df):,}")
    lines.append("")
    lines.append(f"Stability label counts: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP EDGE STABILITY")
    lines.append("-" * 90)
    lines.append(edge_stability_df[edge_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("REGIME LEVEL STABILITY")
    lines.append("-" * 90)
    lines.append(regime_level_df[regime_display_cols].to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP TRANSITION STABILITY")
    lines.append("-" * 90)
    lines.append(transition_stability_df[transition_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Regime summary rows:       {len(regime_summary_df):,}")
    print(f"Transition rows:           {len(transition_df):,}")
    print(f"Edge stability rows:       {len(edge_stability_df):,}")
    print(f"Regime-level rows:         {len(regime_level_df):,}")
    print(f"Transition stability rows: {len(transition_stability_df):,}")
    print(f"Stability label counts:    {label_counts}")
    print("-" * 90)
    print("[REGIME LEVEL STABILITY]")
    print(regime_level_df[regime_display_cols].to_string(index=False))
    print("-" * 90)
    print("[DONE] Regime edge stability complete.")
    print(f"Edge CSV:       {edge_csv}")
    print(f"Regime CSV:     {regime_csv}")
    print(f"Transition CSV: {transition_csv}")
    print(f"JSON output:    {json_path}")
    print(f"TXT output:     {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()