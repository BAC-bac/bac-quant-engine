"""
BACQE MICROSTRUCTURE 31 - FEATURE FAMILY PLAYBOOK

Purpose:
    Convert Script 30 feature importance results into a practical
    microstructure feature/family research playbook.
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


def assign_family_research_theme(feature_family: str) -> str:
    themes = {
        "spread": "liquidity_state",
        "price_ohlc": "event_price_formation",
        "return_volatility": "event_momentum_volatility",
        "range": "event_range_expansion",
        "activity": "market_activity_timing",
        "imbalance": "order_flow_pressure",
    }
    return themes.get(feature_family, "other_microstructure_signal")


def assign_playbook_priority(row: pd.Series) -> str:
    score = row.get("importance_score", 0)
    family = row.get("feature_family", "")
    avg_importance = row.get("avg_importance", 0)
    best_rank = row.get("best_rank", 99)

    if family == "spread" and avg_importance >= 0.03:
        return "priority_liquidity_research"

    if score >= 54:
        return "priority_feature_research"

    if best_rank <= 3 and avg_importance >= 0.03:
        return "strong_feature_research"

    if score >= 50:
        return "research_candidate"

    return "watchlist_feature"


def build_research_note(row: pd.Series) -> str:
    feature = row.get("feature_name", "UNKNOWN")
    family = row.get("feature_family", "UNKNOWN")
    theme = row.get("research_theme", "UNKNOWN")
    avg_imp = row.get("avg_importance", 0)
    max_imp = row.get("max_importance", 0)
    appearances = row.get("appearances", 0)
    symbols = row.get("symbols", "")

    return (
        f"{feature} belongs to the {family} family and maps to the "
        f"{theme} research theme. It appeared {appearances} times across "
        f"the importance research, with average importance {avg_imp:.4f} "
        f"and max importance {max_imp:.4f}. Symbols represented: {symbols}."
    )


def assign_next_step(row: pd.Series) -> str:
    family = row.get("feature_family", "")
    priority = row.get("playbook_priority", "")

    if priority == "priority_liquidity_research":
        return "Build a dedicated spread/liquidity research script and test spread state regimes."

    if family == "price_ohlc":
        return "Test whether event-price features remain predictive after de-trending and normalisation."

    if family == "return_volatility":
        return "Build momentum/volatility interaction features and compare against current return_mean features."

    if family == "range":
        return "Investigate whether range expansion predicts continuation or mean reversion by symbol."

    if family == "activity":
        return "Test duration/activity timing features across liquid and less-liquid sessions."

    if family == "imbalance":
        return "Revisit raw imbalance features and test whether alternative thresholds expose stronger order-flow signals."

    return "Keep feature on the research watchlist."


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 31 - FEATURE FAMILY PLAYBOOK")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    importance_summary_path = (
        analysis_dir
        / "feature_importance_research"
        / "microstructure_feature_importance_summary_latest.csv"
    )

    family_summary_path = (
        analysis_dir
        / "feature_importance_research"
        / "microstructure_feature_importance_family_latest.csv"
    )

    report_dir = analysis_dir / "feature_family_playbook"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Importance summary: {importance_summary_path}")
    print(f"Family summary:     {family_summary_path}")
    print(f"Report dir:         {report_dir}")
    print("-" * 90)

    if not importance_summary_path.exists():
        raise FileNotFoundError(
            f"Missing importance summary: {importance_summary_path}. Run script 30 first."
        )

    if not family_summary_path.exists():
        raise FileNotFoundError(
            f"Missing family summary: {family_summary_path}. Run script 30 first."
        )

    feature_df = pd.read_csv(importance_summary_path)
    family_df = pd.read_csv(family_summary_path)

    for col in [
        "importance_score",
        "avg_importance",
        "max_importance",
        "appearances",
        "avg_rank",
        "best_rank",
    ]:
        if col in feature_df.columns:
            feature_df[col] = pd.to_numeric(feature_df[col], errors="coerce")

    feature_df["research_theme"] = feature_df["feature_family"].apply(assign_family_research_theme)
    feature_df["playbook_priority"] = feature_df.apply(assign_playbook_priority, axis=1)
    feature_df["research_note"] = feature_df.apply(build_research_note, axis=1)
    feature_df["recommended_next_step"] = feature_df.apply(assign_next_step, axis=1)
    feature_df["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    feature_df = feature_df.sort_values(
        [
            "importance_score",
            "avg_importance",
            "max_importance",
            "appearances",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    feature_df["playbook_rank"] = feature_df.index + 1

    family_df["research_theme"] = family_df["feature_family"].apply(assign_family_research_theme)
    family_df["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    feature_csv = report_dir / "microstructure_feature_family_playbook_latest.csv"
    family_csv = report_dir / "microstructure_feature_family_summary_playbook_latest.csv"
    json_path = report_dir / "microstructure_feature_family_playbook_latest.json"
    txt_path = report_dir / "microstructure_feature_family_playbook_latest.txt"

    feature_df.to_csv(feature_csv, index=False)
    family_df.to_csv(family_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "feature_rows": len(feature_df),
        "family_rows": len(family_df),
        "top_features": feature_df.head(50).to_dict(orient="records"),
        "families": family_df.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    priority_counts = feature_df["playbook_priority"].value_counts(dropna=False).to_dict()
    family_counts = feature_df["feature_family"].value_counts(dropna=False).to_dict()
    theme_counts = feature_df["research_theme"].value_counts(dropna=False).to_dict()

    display_cols = [
        "playbook_rank",
        "feature_name",
        "feature_family",
        "research_theme",
        "playbook_priority",
        "importance_score",
        "avg_importance",
        "max_importance",
        "appearances",
        "avg_rank",
        "best_rank",
        "symbol_count",
        "bar_type_count",
        "target_count",
        "model_count",
        "recommended_next_step",
    ]

    available_display_cols = [c for c in display_cols if c in feature_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE FAMILY PLAYBOOK")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Feature rows: {len(feature_df):,}")
    lines.append(f"Family rows:  {len(family_df):,}")
    lines.append("")
    lines.append(f"Priority counts: {priority_counts}")
    lines.append(f"Family counts:   {family_counts}")
    lines.append(f"Theme counts:    {theme_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("FEATURE FAMILY SUMMARY")
    lines.append("-" * 90)
    lines.append(family_df.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP FEATURE PLAYBOOK")
    lines.append("-" * 90)
    lines.append(feature_df[available_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("RESEARCH NOTES")
    lines.append("-" * 90)

    for _, row in feature_df.head(20).iterrows():
        lines.append(f"[{row['playbook_rank']}] {row['feature_name']} / {row['feature_family']}")
        lines.append(f"    {row['research_note']}")
        lines.append(f"    Next: {row['recommended_next_step']}")
        lines.append("")

    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Feature rows: {len(feature_df):,}")
    print(f"Family rows:  {len(family_df):,}")
    print(f"Priority counts: {priority_counts}")
    print(f"Family counts:   {family_counts}")
    print(f"Theme counts:    {theme_counts}")
    print("-" * 90)
    print("[TOP 20 FEATURE FAMILY PLAYBOOK]")
    print(feature_df[available_display_cols].head(20).to_string(index=False))
    print("-" * 90)
    print("[DONE] Feature family playbook complete.")
    print(f"Feature CSV: {feature_csv}")
    print(f"Family CSV:  {family_csv}")
    print(f"JSON output: {json_path}")
    print(f"TXT output:  {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()