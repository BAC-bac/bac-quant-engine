"""
BACQE MICROSTRUCTURE 15 - MICROSTRUCTURE FEATURE LEADERBOARD

Purpose:
    Create a ranked leaderboard of recurring predictive microstructure features.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/predictive_audit/
        microstructure_predictive_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_leaderboard/
        microstructure_feature_leaderboard_latest.csv
        microstructure_feature_leaderboard_latest.json
        microstructure_feature_leaderboard_latest.txt
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


def actionability_label(row: pd.Series) -> str:
    score = row.get("leaderboard_score", 0)
    strong = row.get("strong_watchlist_count", 0)
    research = row.get("research_watchlist_count", 0)
    symbols = row.get("symbol_count", 0)
    targets = row.get("target_count", 0)

    if score >= 85 and strong >= 5 and symbols >= 2 and targets >= 2:
        return "priority_feature"

    if score >= 75 and strong >= 3:
        return "strong_feature"

    if score >= 65 and research >= 5:
        return "research_feature"

    if score >= 50:
        return "watch_feature"

    return "low_priority"


def build_leaderboard_score(row: pd.Series) -> float:
    """
    Transparent scoring system.

    Rewards:
        - repeated watchlist appearances
        - strong watchlist count
        - average absolute correlation
        - max absolute correlation
        - cross-symbol presence
        - cross-target presence
        - cross-bar-type presence

    Penalises:
        - very small sample size
    """
    score = 0.0

    watchlist_count = row.get("watchlist_count", 0)
    strong_count = row.get("strong_watchlist_count", 0)
    research_count = row.get("research_watchlist_count", 0)
    weak_count = row.get("weak_watchlist_count", 0)

    avg_abs_corr = row.get("avg_abs_correlation", 0)
    max_abs_corr = row.get("max_abs_correlation", 0)
    avg_sample_size = row.get("avg_sample_size", 0)

    symbol_count = row.get("symbol_count", 0)
    target_count = row.get("target_count", 0)
    bar_type_count = row.get("bar_type_count", 0)
    dataset_count = row.get("dataset_count", 0)

    score += min(watchlist_count * 0.3, 20)
    score += min(strong_count * 2.0, 25)
    score += min(research_count * 0.8, 15)
    score += min(weak_count * 0.15, 5)

    score += min(avg_abs_corr * 250, 15)
    score += min(max_abs_corr * 100, 10)

    score += min(symbol_count * 2.5, 10)
    score += min(target_count * 2.0, 6)
    score += min(bar_type_count * 2.0, 6)
    score += min(dataset_count * 0.5, 8)

    if avg_sample_size < 100:
        score -= 15
    elif avg_sample_size < 250:
        score -= 8
    elif avg_sample_size < 500:
        score -= 3

    return round(max(0, min(100, score)), 2)


def summarise_feature_leaderboard(watchlist_df: pd.DataFrame) -> pd.DataFrame:
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

    grouped["leaderboard_score"] = grouped.apply(build_leaderboard_score, axis=1)
    grouped["actionability"] = grouped.apply(actionability_label, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "leaderboard_score",
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
            "watchlist_count",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["leaderboard_rank"] = grouped.index + 1

    return grouped


def summarise_feature_symbol_leaderboard(watchlist_df: pd.DataFrame) -> pd.DataFrame:
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
    grouped["leaderboard_score"] = grouped.apply(build_leaderboard_score, axis=1)
    grouped["actionability"] = grouped.apply(actionability_label, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "leaderboard_score",
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
            "watchlist_count",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["leaderboard_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 15 - MICROSTRUCTURE FEATURE LEADERBOARD")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    predictive_audit_path = (
        analysis_dir
        / "predictive_audit"
        / "microstructure_predictive_audit_latest.csv"
    )

    report_dir = analysis_dir / "feature_leaderboard"
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
        raise RuntimeError("No watchlist rows available for leaderboard.")

    feature_leaderboard = summarise_feature_leaderboard(watchlist_df)
    feature_symbol_leaderboard = summarise_feature_symbol_leaderboard(watchlist_df)

    csv_path = report_dir / "microstructure_feature_leaderboard_latest.csv"
    json_path = report_dir / "microstructure_feature_leaderboard_latest.json"
    txt_path = report_dir / "microstructure_feature_leaderboard_latest.txt"

    csv_symbol_path = report_dir / "microstructure_feature_symbol_leaderboard_latest.csv"
    json_symbol_path = report_dir / "microstructure_feature_symbol_leaderboard_latest.json"

    feature_leaderboard.to_csv(csv_path, index=False)
    feature_symbol_leaderboard.to_csv(csv_symbol_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(feature_leaderboard.to_dict(orient="records"), f, indent=2, default=str)

    with open(json_symbol_path, "w", encoding="utf-8") as f:
        json.dump(feature_symbol_leaderboard.to_dict(orient="records"), f, indent=2, default=str)

    actionability_counts = feature_leaderboard["actionability"].value_counts(dropna=False).to_dict()
    family_counts = feature_leaderboard["feature_family"].value_counts(dropna=False).to_dict()

    top_features = feature_leaderboard.head(30)[
        [
            "leaderboard_rank",
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
            "best_symbol",
            "best_bar_type",
            "best_target",
            "leaderboard_score",
            "actionability",
        ]
    ]

    top_feature_symbols = feature_symbol_leaderboard.head(30)[
        [
            "leaderboard_rank",
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
            "best_bar_type",
            "best_target",
            "leaderboard_score",
            "actionability",
        ]
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE LEADERBOARD")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Audit rows:        {len(audit_df):,}")
    lines.append(f"Watchlist rows:    {len(watchlist_df):,}")
    lines.append(f"Feature rows:      {len(feature_leaderboard):,}")
    lines.append(f"Feature/symbol rows: {len(feature_symbol_leaderboard):,}")
    lines.append(f"Actionability counts: {actionability_counts}")
    lines.append(f"Feature family counts: {family_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 FEATURE LEADERBOARD")
    lines.append("-" * 90)
    lines.append(top_features.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 FEATURE/SYMBOL LEADERBOARD")
    lines.append("-" * 90)
    lines.append(top_feature_symbols.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Feature rows:          {len(feature_leaderboard):,}")
    print(f"Feature/symbol rows:   {len(feature_symbol_leaderboard):,}")
    print(f"Actionability counts:  {actionability_counts}")
    print(f"Feature family counts: {family_counts}")
    print("-" * 90)
    print("[TOP 15 FEATURES]")
    print(top_features.head(15).to_string(index=False))
    print("-" * 90)
    print("[DONE] Microstructure feature leaderboard complete.")
    print(f"Feature CSV:        {csv_path}")
    print(f"Feature JSON:       {json_path}")
    print(f"Feature/Symbol CSV: {csv_symbol_path}")
    print(f"Feature/Symbol JSON:{json_symbol_path}")
    print(f"TXT output:         {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()