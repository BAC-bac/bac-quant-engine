"""
BACQE MICROSTRUCTURE 21 - RESEARCH MASTER SHORTLIST

Purpose:
    Consolidate the strongest microstructure research candidates into one
    master shortlist.

Inputs:
    feature_leaderboard/
    feature_consensus/
    validated_feature_summary/
    combination_summary/

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/research_master_shortlist/
        microstructure_research_master_shortlist_features_latest.csv
        microstructure_research_master_shortlist_combinations_latest.csv
        microstructure_research_master_shortlist_latest.json
        microstructure_research_master_shortlist_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
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


def safe_read_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing required file: {path}")
        return pd.DataFrame()

    return pd.read_csv(path)


def normalise_feature_name(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "feature_name" in df.columns:
        df["feature_name"] = df["feature_name"].astype(str)
    return df


def build_master_feature_shortlist(
    leaderboard_df: pd.DataFrame,
    consensus_df: pd.DataFrame,
    validated_df: pd.DataFrame,
) -> pd.DataFrame:
    leaderboard_df = normalise_feature_name(leaderboard_df)
    consensus_df = normalise_feature_name(consensus_df)
    validated_df = normalise_feature_name(validated_df)

    base_cols = [
        "feature_name",
        "feature_family",
        "leaderboard_rank",
        "leaderboard_score",
        "actionability",
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
    ]

    available_base_cols = [c for c in base_cols if c in leaderboard_df.columns]
    master = leaderboard_df[available_base_cols].copy()

    consensus_cols = [
        "feature_name",
        "consensus_rank",
        "consensus_score",
        "consensus_label",
        "research_priority",
    ]

    available_consensus_cols = [c for c in consensus_cols if c in consensus_df.columns]

    master = master.merge(
        consensus_df[available_consensus_cols],
        on="feature_name",
        how="left",
        suffixes=("", "_consensus"),
    )

    validated_cols = [
        "feature_name",
        "research_rank",
        "research_score",
        "research_tier",
        "validated_relationship_count",
        "validated_count",
        "partially_validated_count",
        "avg_test_abs_correlation",
        "max_test_abs_correlation",
        "avg_correlation_decay_ratio",
    ]

    available_validated_cols = [c for c in validated_cols if c in validated_df.columns]

    master = master.merge(
        validated_df[available_validated_cols],
        on="feature_name",
        how="left",
        suffixes=("", "_validated"),
    )

    for col in [
        "leaderboard_score",
        "consensus_score",
        "research_score",
        "validated_count",
        "partially_validated_count",
        "symbol_count",
        "bar_type_count",
        "target_count",
        "dataset_count",
        "avg_test_abs_correlation",
        "avg_correlation_decay_ratio",
    ]:
        if col not in master.columns:
            master[col] = 0
        master[col] = pd.to_numeric(master[col], errors="coerce").fillna(0)

    master["master_score"] = (
        master["leaderboard_score"] * 0.25
        + master["consensus_score"] * 0.30
        + master["research_score"] * 0.35
        + master["validated_count"].clip(upper=50) * 0.20
        + master["symbol_count"] * 1.5
        + master["bar_type_count"] * 1.0
        + master["target_count"] * 1.0
    )

    master["master_score"] = master["master_score"].clip(0, 100).round(2)

    def master_tier(row: pd.Series) -> str:
        score = row["master_score"]
        family = row.get("feature_family", "")
        validated = row.get("validated_count", 0)
        symbols = row.get("symbol_count", 0)

        if score >= 90 and validated >= 20 and symbols >= 4:
            return "tier_1_core_candidate"

        if score >= 80 and family in {"imbalance", "activity", "return_volatility"}:
            return "tier_2_microstructure_candidate"

        if score >= 75:
            return "tier_2_baseline_candidate"

        if score >= 65:
            return "tier_3_research_candidate"

        if score >= 50:
            return "tier_4_watch_candidate"

        return "tier_5_low_priority"

    master["master_tier"] = master.apply(master_tier, axis=1)
    master["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    master = master.sort_values(
        [
            "master_score",
            "validated_count",
            "avg_test_abs_correlation",
            "symbol_count",
            "target_count",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    master["master_rank"] = master.index + 1

    return master


def build_master_combination_shortlist(pair_summary_df: pd.DataFrame) -> pd.DataFrame:
    if pair_summary_df.empty:
        return pd.DataFrame()

    df = pair_summary_df.copy()

    for col in [
        "combo_score",
        "strong_improvement_count",
        "research_improvement_count",
        "improvement_count",
        "avg_correlation_improvement",
        "max_correlation_improvement",
        "avg_combination_abs_corr",
        "symbol_count",
        "bar_type_count",
        "target_count",
    ]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["master_combo_score"] = (
        df["combo_score"] * 0.55
        + df["strong_improvement_count"].clip(upper=5) * 6
        + df["research_improvement_count"].clip(upper=10) * 2
        + df["avg_correlation_improvement"] * 200
        + df["max_correlation_improvement"] * 120
        + df["symbol_count"] * 2
        + df["bar_type_count"] * 1.5
        + df["target_count"] * 1.5
    )

    df["master_combo_score"] = df["master_combo_score"].clip(0, 100).round(2)

    def combo_master_tier(row: pd.Series) -> str:
        score = row["master_combo_score"]
        strong = row.get("strong_improvement_count", 0)
        research = row.get("research_improvement_count", 0)

        if score >= 85 and strong >= 1:
            return "tier_1_combo_candidate"

        if score >= 75 and research >= 3:
            return "tier_2_combo_candidate"

        if score >= 65:
            return "tier_3_combo_research"

        if score >= 50:
            return "tier_4_combo_watch"

        return "tier_5_combo_low_priority"

    df["master_combo_tier"] = df.apply(combo_master_tier, axis=1)
    df["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    df = df.sort_values(
        [
            "master_combo_score",
            "strong_improvement_count",
            "research_improvement_count",
            "max_correlation_improvement",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    df["master_combo_rank"] = df.index + 1

    return df


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 21 - RESEARCH MASTER SHORTLIST")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    report_dir = analysis_dir / "research_master_shortlist"
    report_dir.mkdir(parents=True, exist_ok=True)

    leaderboard_path = (
        analysis_dir
        / "feature_leaderboard"
        / "microstructure_feature_leaderboard_latest.csv"
    )

    consensus_path = (
        analysis_dir
        / "feature_consensus"
        / "microstructure_feature_consensus_latest.csv"
    )

    validated_path = (
        analysis_dir
        / "validated_feature_summary"
        / "microstructure_validated_feature_summary_latest.csv"
    )

    pair_summary_path = (
        analysis_dir
        / "combination_summary"
        / "microstructure_combination_pair_summary_latest.csv"
    )

    print(f"Leaderboard:   {leaderboard_path}")
    print(f"Consensus:     {consensus_path}")
    print(f"Validated:     {validated_path}")
    print(f"Combinations:  {pair_summary_path}")
    print(f"Report dir:    {report_dir}")
    print("-" * 90)

    leaderboard_df = safe_read_csv(leaderboard_path)
    consensus_df = safe_read_csv(consensus_path)
    validated_df = safe_read_csv(validated_path)
    pair_summary_df = safe_read_csv(pair_summary_path, required=False)

    feature_shortlist = build_master_feature_shortlist(
        leaderboard_df=leaderboard_df,
        consensus_df=consensus_df,
        validated_df=validated_df,
    )

    combo_shortlist = build_master_combination_shortlist(pair_summary_df)

    feature_csv = report_dir / "microstructure_research_master_shortlist_features_latest.csv"
    combo_csv = report_dir / "microstructure_research_master_shortlist_combinations_latest.csv"
    json_path = report_dir / "microstructure_research_master_shortlist_latest.json"
    txt_path = report_dir / "microstructure_research_master_shortlist_latest.txt"

    feature_shortlist.to_csv(feature_csv, index=False)

    if not combo_shortlist.empty:
        combo_shortlist.to_csv(combo_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "feature_rows": len(feature_shortlist),
        "combination_rows": len(combo_shortlist),
        "top_features": feature_shortlist.head(50).to_dict(orient="records"),
        "top_combinations": combo_shortlist.head(50).to_dict(orient="records") if not combo_shortlist.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    feature_tier_counts = feature_shortlist["master_tier"].value_counts(dropna=False).to_dict()
    feature_family_counts = feature_shortlist["feature_family"].value_counts(dropna=False).to_dict()

    if not combo_shortlist.empty:
        combo_tier_counts = combo_shortlist["master_combo_tier"].value_counts(dropna=False).to_dict()
    else:
        combo_tier_counts = {}

    top_features = feature_shortlist.head(30)
    top_combos = combo_shortlist.head(20) if not combo_shortlist.empty else pd.DataFrame()

    feature_display_cols = [
        "master_rank",
        "feature_name",
        "feature_family",
        "master_score",
        "master_tier",
        "leaderboard_score",
        "consensus_score",
        "research_score",
        "validated_count",
        "partially_validated_count",
        "avg_test_abs_correlation",
        "avg_correlation_decay_ratio",
        "symbol_count",
        "bar_type_count",
        "target_count",
        "best_symbol",
        "best_bar_type",
        "best_target",
    ]

    available_feature_display_cols = [
        c for c in feature_display_cols if c in top_features.columns
    ]

    combo_display_cols = [
        "master_combo_rank",
        "feature_a",
        "feature_a_family",
        "feature_b",
        "feature_b_family",
        "combination_method",
        "master_combo_score",
        "master_combo_tier",
        "strong_improvement_count",
        "research_improvement_count",
        "avg_correlation_improvement",
        "max_correlation_improvement",
        "symbol_count",
        "bar_type_count",
        "target_count",
        "best_symbol",
        "best_bar_type",
        "best_target",
    ]

    available_combo_display_cols = [
        c for c in combo_display_cols if not top_combos.empty and c in top_combos.columns
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE RESEARCH MASTER SHORTLIST")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Feature candidates:     {len(feature_shortlist):,}")
    lines.append(f"Combination candidates: {len(combo_shortlist):,}")
    lines.append("")
    lines.append(f"Feature tier counts:   {feature_tier_counts}")
    lines.append(f"Feature family counts: {feature_family_counts}")
    lines.append(f"Combo tier counts:     {combo_tier_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 MASTER FEATURE SHORTLIST")
    lines.append("-" * 90)
    lines.append(top_features[available_feature_display_cols].to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 20 MASTER COMBINATION SHORTLIST")
    lines.append("-" * 90)

    if top_combos.empty:
        lines.append("No combination shortlist available.")
    else:
        lines.append(top_combos[available_combo_display_cols].to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Feature candidates:     {len(feature_shortlist):,}")
    print(f"Combination candidates: {len(combo_shortlist):,}")
    print(f"Feature tier counts:    {feature_tier_counts}")
    print(f"Feature family counts:  {feature_family_counts}")
    print(f"Combo tier counts:      {combo_tier_counts}")
    print("-" * 90)
    print("[TOP 15 FEATURES]")
    print(top_features[available_feature_display_cols].head(15).to_string(index=False))
    print("-" * 90)
    print("[DONE] Research master shortlist complete.")
    print(f"Feature CSV:      {feature_csv}")
    print(f"Combination CSV:  {combo_csv}")
    print(f"JSON output:      {json_path}")
    print(f"TXT output:       {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()