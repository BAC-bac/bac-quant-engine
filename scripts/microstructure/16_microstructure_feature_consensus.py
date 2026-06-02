"""
BACQE MICROSTRUCTURE 16 - MICROSTRUCTURE FEATURE CONSENSUS

Purpose:
    Identify robust recurring microstructure features across:

        - symbols
        - bar types
        - target horizons
        - datasets
        - signal strengths

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_leaderboard/
        microstructure_feature_leaderboard_latest.csv

    E:/Quant_Lab/data/analysis/microstructure/feature_leaderboard/
        microstructure_feature_symbol_leaderboard_latest.csv

    E:/Quant_Lab/data/analysis/microstructure/predictive_audit/
        microstructure_predictive_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_consensus/
        microstructure_feature_consensus_latest.csv
        microstructure_feature_consensus_latest.json
        microstructure_feature_consensus_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

WATCHLIST_LEVELS = {
    "weak_watchlist",
    "research_watchlist",
    "strong_watchlist",
}


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def consensus_label(row: pd.Series) -> str:
    score = row.get("consensus_score", 0)
    symbols = row.get("symbol_count", 0)
    bar_types = row.get("bar_type_count", 0)
    targets = row.get("target_count", 0)
    strong = row.get("strong_watchlist_count", 0)
    datasets = row.get("dataset_count", 0)

    if score >= 85 and symbols >= 4 and targets >= 2 and datasets >= 8:
        return "high_consensus"

    if score >= 75 and symbols >= 3 and targets >= 2:
        return "strong_consensus"

    if score >= 65 and symbols >= 2 and targets >= 2:
        return "moderate_consensus"

    if strong >= 2:
        return "narrow_strong_signal"

    if score >= 50:
        return "weak_consensus"

    return "low_consensus"


def research_priority(row: pd.Series) -> str:
    label = row.get("consensus_label", "")
    family = row.get("feature_family", "")

    if label == "high_consensus" and family in {"imbalance", "activity", "return_volatility"}:
        return "priority_microstructure_research"

    if label == "high_consensus":
        return "priority_baseline_research"

    if label == "strong_consensus" and family in {"imbalance", "activity"}:
        return "strong_microstructure_research"

    if label in {"strong_consensus", "moderate_consensus"}:
        return "research_candidate"

    if label == "narrow_strong_signal":
        return "single_market_deep_dive"

    return "watch_only"


def build_consensus_score(row: pd.Series) -> float:
    score = 0.0

    watchlist_count = row.get("watchlist_count", 0)
    strong_count = row.get("strong_watchlist_count", 0)
    research_count = row.get("research_watchlist_count", 0)

    avg_abs_corr = row.get("avg_abs_correlation", 0)
    max_abs_corr = row.get("max_abs_correlation", 0)

    symbol_count = row.get("symbol_count", 0)
    bar_type_count = row.get("bar_type_count", 0)
    target_count = row.get("target_count", 0)
    dataset_count = row.get("dataset_count", 0)

    avg_sample_size = row.get("avg_sample_size", 0)

    score += min(symbol_count * 8, 24)
    score += min(bar_type_count * 7, 21)
    score += min(target_count * 6, 18)
    score += min(dataset_count * 1.5, 18)

    score += min(strong_count * 2.0, 20)
    score += min(research_count * 0.6, 12)
    score += min(watchlist_count * 0.15, 8)

    score += min(avg_abs_corr * 180, 12)
    score += min(max_abs_corr * 60, 8)

    if avg_sample_size >= 5000:
        score += 10
    elif avg_sample_size >= 1000:
        score += 7
    elif avg_sample_size >= 500:
        score += 4
    elif avg_sample_size < 100:
        score -= 10

    return round(max(0, min(100, score)), 2)


def build_consensus_from_audit(audit_df: pd.DataFrame) -> pd.DataFrame:
    watchlist_df = audit_df[
        audit_df["signal_strength"].isin(WATCHLIST_LEVELS)
    ].copy()

    grouped = (
        watchlist_df
        .groupby(["feature_name", "feature_family"], dropna=False)
        .agg(
            watchlist_count=("signal_strength", "count"),
            strong_watchlist_count=("signal_strength", lambda s: int((s == "strong_watchlist").sum())),
            research_watchlist_count=("signal_strength", lambda s: int((s == "research_watchlist").sum())),
            weak_watchlist_count=("signal_strength", lambda s: int((s == "weak_watchlist").sum())),
            avg_correlation=("correlation", "mean"),
            avg_abs_correlation=("abs_correlation", "mean"),
            max_abs_correlation=("abs_correlation", "max"),
            median_abs_correlation=("abs_correlation", "median"),
            avg_sample_size=("sample_size", "mean"),
            max_sample_size=("sample_size", "max"),
            symbol_count=("symbol", "nunique"),
            symbols=("symbol", lambda s: ",".join(sorted(s.dropna().unique()))),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            dataset_count=("file_path", "nunique"),
            best_symbol=("symbol", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_bar_type=("bar_type", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_target=("target", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
        )
        .reset_index()
    )

    grouped["consensus_score"] = grouped.apply(build_consensus_score, axis=1)
    grouped["consensus_label"] = grouped.apply(consensus_label, axis=1)
    grouped["research_priority"] = grouped.apply(research_priority, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "consensus_score",
            "symbol_count",
            "bar_type_count",
            "target_count",
            "strong_watchlist_count",
            "avg_abs_correlation",
        ],
        ascending=[False, False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["consensus_rank"] = grouped.index + 1

    return grouped


def build_symbol_consensus(audit_df: pd.DataFrame) -> pd.DataFrame:
    watchlist_df = audit_df[
        audit_df["signal_strength"].isin(WATCHLIST_LEVELS)
    ].copy()

    grouped = (
        watchlist_df
        .groupby(["feature_name", "feature_family", "symbol"], dropna=False)
        .agg(
            watchlist_count=("signal_strength", "count"),
            strong_watchlist_count=("signal_strength", lambda s: int((s == "strong_watchlist").sum())),
            research_watchlist_count=("signal_strength", lambda s: int((s == "research_watchlist").sum())),
            weak_watchlist_count=("signal_strength", lambda s: int((s == "weak_watchlist").sum())),
            avg_correlation=("correlation", "mean"),
            avg_abs_correlation=("abs_correlation", "mean"),
            max_abs_correlation=("abs_correlation", "max"),
            median_abs_correlation=("abs_correlation", "median"),
            avg_sample_size=("sample_size", "mean"),
            max_sample_size=("sample_size", "max"),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            dataset_count=("file_path", "nunique"),
            best_bar_type=("bar_type", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_target=("target", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
        )
        .reset_index()
    )

    grouped["symbol_count"] = 1
    grouped["consensus_score"] = grouped.apply(build_consensus_score, axis=1)
    grouped["consensus_label"] = grouped.apply(consensus_label, axis=1)
    grouped["research_priority"] = grouped.apply(research_priority, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "consensus_score",
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["symbol_consensus_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 16 - MICROSTRUCTURE FEATURE CONSENSUS")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    predictive_audit_path = (
        analysis_dir
        / "predictive_audit"
        / "microstructure_predictive_audit_latest.csv"
    )

    report_dir = analysis_dir / "feature_consensus"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Predictive audit: {predictive_audit_path}")
    print(f"Report dir:       {report_dir}")
    print("-" * 90)

    if not predictive_audit_path.exists():
        raise FileNotFoundError(
            f"Missing predictive audit file: {predictive_audit_path}. "
            "Run script 13 first."
        )

    audit_df = pd.read_csv(predictive_audit_path)

    watchlist_df = audit_df[
        audit_df["signal_strength"].isin(WATCHLIST_LEVELS)
    ].copy()

    print(f"Audit rows:      {len(audit_df):,}")
    print(f"Watchlist rows:  {len(watchlist_df):,}")
    print("-" * 90)

    if watchlist_df.empty:
        raise RuntimeError("No watchlist rows available for consensus analysis.")

    consensus_df = build_consensus_from_audit(audit_df)
    symbol_consensus_df = build_symbol_consensus(audit_df)

    csv_path = report_dir / "microstructure_feature_consensus_latest.csv"
    json_path = report_dir / "microstructure_feature_consensus_latest.json"
    txt_path = report_dir / "microstructure_feature_consensus_latest.txt"

    csv_symbol_path = report_dir / "microstructure_symbol_feature_consensus_latest.csv"
    json_symbol_path = report_dir / "microstructure_symbol_feature_consensus_latest.json"

    consensus_df.to_csv(csv_path, index=False)
    symbol_consensus_df.to_csv(csv_symbol_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(consensus_df.to_dict(orient="records"), f, indent=2, default=str)

    with open(json_symbol_path, "w", encoding="utf-8") as f:
        json.dump(symbol_consensus_df.to_dict(orient="records"), f, indent=2, default=str)

    consensus_counts = consensus_df["consensus_label"].value_counts(dropna=False).to_dict()
    priority_counts = consensus_df["research_priority"].value_counts(dropna=False).to_dict()
    family_counts = consensus_df["feature_family"].value_counts(dropna=False).to_dict()

    top_consensus = consensus_df.head(30)[
        [
            "consensus_rank",
            "feature_name",
            "feature_family",
            "watchlist_count",
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
            "max_abs_correlation",
            "symbol_count",
            "bar_type_count",
            "target_count",
            "dataset_count",
            "best_symbol",
            "best_bar_type",
            "best_target",
            "consensus_score",
            "consensus_label",
            "research_priority",
        ]
    ]

    top_symbol_consensus = symbol_consensus_df.head(30)[
        [
            "symbol_consensus_rank",
            "feature_name",
            "feature_family",
            "symbol",
            "watchlist_count",
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
            "max_abs_correlation",
            "bar_type_count",
            "target_count",
            "dataset_count",
            "best_bar_type",
            "best_target",
            "consensus_score",
            "consensus_label",
            "research_priority",
        ]
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE CONSENSUS REPORT")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Audit rows:       {len(audit_df):,}")
    lines.append(f"Watchlist rows:   {len(watchlist_df):,}")
    lines.append(f"Consensus rows:   {len(consensus_df):,}")
    lines.append(f"Symbol consensus rows: {len(symbol_consensus_df):,}")
    lines.append("")
    lines.append(f"Consensus label counts: {consensus_counts}")
    lines.append(f"Research priority counts: {priority_counts}")
    lines.append(f"Feature family counts: {family_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 GLOBAL FEATURE CONSENSUS")
    lines.append("-" * 90)
    lines.append(top_consensus.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 SYMBOL FEATURE CONSENSUS")
    lines.append("-" * 90)
    lines.append(top_symbol_consensus.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Consensus rows:         {len(consensus_df):,}")
    print(f"Symbol consensus rows:  {len(symbol_consensus_df):,}")
    print(f"Consensus label counts: {consensus_counts}")
    print(f"Research priorities:    {priority_counts}")
    print(f"Feature family counts:  {family_counts}")
    print("-" * 90)
    print("[TOP 15 GLOBAL CONSENSUS FEATURES]")
    print(top_consensus.head(15).to_string(index=False))
    print("-" * 90)
    print("[DONE] Microstructure feature consensus complete.")
    print(f"Global CSV:        {csv_path}")
    print(f"Global JSON:       {json_path}")
    print(f"Symbol CSV:        {csv_symbol_path}")
    print(f"Symbol JSON:       {json_symbol_path}")
    print(f"TXT output:        {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()