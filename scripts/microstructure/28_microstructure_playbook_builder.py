"""
BACQE MICROSTRUCTURE 28 - PLAYBOOK BUILDER

Purpose:
    Build a permanent research playbook from the best model catalogue.

Inputs:
    best_model_catalogue/
        microstructure_best_model_catalogue_latest.csv

Outputs:
    playbooks/
        microstructure_playbook_latest.csv
        microstructure_playbook_latest.json
        microstructure_playbook_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

PLAYBOOK_TIERS = {
    "tier_1_playbook_candidate",
    "tier_2_research_candidate",
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


def classify_playbook_priority(row: pd.Series) -> str:
    catalogue_tier = row.get("catalogue_tier", "")
    roc_auc = row.get("roc_auc", 0)
    bal_acc = row.get("balanced_accuracy", 0)
    test_rows = row.get("test_rows", 0)
    symbol = row.get("symbol", "")
    bar_type = row.get("bar_type", "")
    target = row.get("target", "")

    if (
        catalogue_tier == "tier_1_playbook_candidate"
        and roc_auc >= 0.60
        and bal_acc >= 0.56
        and test_rows >= 250
    ):
        return "priority_playbook"

    if (
        bar_type == "tick_imbalance_bars"
        and target == "forward_return_5"
        and roc_auc >= 0.65
    ):
        return "microstructure_deep_dive"

    if symbol == "EURUSD" and roc_auc >= 0.60:
        return "eurusd_core_research"

    if catalogue_tier == "tier_2_research_candidate":
        return "research_playbook"

    return "watch_playbook"


def build_research_note(row: pd.Series) -> str:
    symbol = row.get("symbol", "UNKNOWN")
    bar_type = row.get("bar_type", "UNKNOWN")
    parameter = row.get("parameter", "UNKNOWN")
    target = row.get("target", "UNKNOWN")
    model_name = row.get("model_name", "UNKNOWN")
    model_family = row.get("model_family", "UNKNOWN")
    roc_auc = row.get("roc_auc", None)
    bal_acc = row.get("balanced_accuracy", None)
    precision = row.get("precision", None)
    recall = row.get("recall", None)
    test_rows = row.get("test_rows", None)

    return (
        f"{symbol} using {bar_type} ({parameter}) showed a promising "
        f"{target} directional model with {model_name} ({model_family}). "
        f"Test rows={test_rows}, ROC-AUC={roc_auc:.4f}, "
        f"balanced accuracy={bal_acc:.4f}, precision={precision:.4f}, "
        f"recall={recall:.4f}. This should be treated as a research candidate, "
        f"not a deployable trading signal."
    )


def assign_next_step(row: pd.Series) -> str:
    priority = row.get("playbook_priority", "")
    bar_type = row.get("bar_type", "")
    target = row.get("target", "")

    if priority == "priority_playbook":
        return "Run walk-forward validation and transaction-cost-aware backtest."

    if priority == "microstructure_deep_dive":
        return "Inspect tick imbalance regime behaviour and feature importance."

    if priority == "eurusd_core_research":
        return "Build EURUSD-focused validation across additional tick history."

    if target == "forward_return_5":
        return "Compare 3-bar and 5-bar horizon stability before modelling further."

    if bar_type == "volume_bars":
        return "Validate whether proxy-volume bars hold up with larger sample sizes."

    return "Keep on watchlist until more tick history is available."


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 28 - PLAYBOOK BUILDER")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    catalogue_path = (
        analysis_dir
        / "best_model_catalogue"
        / "microstructure_best_model_catalogue_latest.csv"
    )

    report_dir = analysis_dir / "playbooks"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Catalogue:  {catalogue_path}")
    print(f"Report dir: {report_dir}")
    print("-" * 90)

    if not catalogue_path.exists():
        raise FileNotFoundError(
            f"Missing best model catalogue: {catalogue_path}. "
            "Run script 27 first."
        )

    catalogue_df = pd.read_csv(catalogue_path)

    playbook_df = catalogue_df[
        catalogue_df["catalogue_tier"].isin(PLAYBOOK_TIERS)
    ].copy()

    if playbook_df.empty:
        raise RuntimeError("No Tier 1 or Tier 2 playbook candidates found.")

    numeric_cols = [
        "accuracy",
        "balanced_accuracy",
        "precision",
        "recall",
        "roc_auc",
        "model_score",
        "catalogue_score",
        "test_rows",
    ]

    for col in numeric_cols:
        if col in playbook_df.columns:
            playbook_df[col] = pd.to_numeric(playbook_df[col], errors="coerce")

    playbook_df["playbook_priority"] = playbook_df.apply(classify_playbook_priority, axis=1)
    playbook_df["research_note"] = playbook_df.apply(build_research_note, axis=1)
    playbook_df["recommended_next_step"] = playbook_df.apply(assign_next_step, axis=1)
    playbook_df["playbook_created_at_utc"] = datetime.now(timezone.utc).isoformat()

    playbook_df = playbook_df.sort_values(
        [
            "catalogue_score",
            "roc_auc",
            "balanced_accuracy",
            "test_rows",
        ],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    playbook_df["playbook_rank"] = playbook_df.index + 1

    output_cols = [
        "playbook_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "model_family",
        "model_label",
        "catalogue_tier",
        "playbook_priority",
        "test_rows",
        "accuracy",
        "balanced_accuracy",
        "precision",
        "recall",
        "roc_auc",
        "model_score",
        "catalogue_score",
        "research_focus",
        "research_note",
        "recommended_next_step",
        "dataset_file",
        "playbook_created_at_utc",
    ]

    available_output_cols = [c for c in output_cols if c in playbook_df.columns]
    playbook_df = playbook_df[available_output_cols].copy()

    csv_path = report_dir / "microstructure_playbook_latest.csv"
    json_path = report_dir / "microstructure_playbook_latest.json"
    txt_path = report_dir / "microstructure_playbook_latest.txt"

    playbook_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(playbook_df.to_dict(orient="records"), f, indent=2, default=str)

    priority_counts = playbook_df["playbook_priority"].value_counts(dropna=False).to_dict()
    symbol_counts = playbook_df["symbol"].value_counts(dropna=False).to_dict()
    bar_type_counts = playbook_df["bar_type"].value_counts(dropna=False).to_dict()
    target_counts = playbook_df["target"].value_counts(dropna=False).to_dict()
    model_counts = playbook_df["model_name"].value_counts(dropna=False).to_dict()

    display_cols = [
        "playbook_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "model_family",
        "playbook_priority",
        "test_rows",
        "balanced_accuracy",
        "roc_auc",
        "catalogue_score",
        "recommended_next_step",
    ]

    available_display_cols = [c for c in display_cols if c in playbook_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE PLAYBOOK")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Catalogue rows: {len(catalogue_df):,}")
    lines.append(f"Playbook rows:  {len(playbook_df):,}")
    lines.append("")
    lines.append(f"Priority counts: {priority_counts}")
    lines.append(f"Symbol counts:   {symbol_counts}")
    lines.append(f"Bar type counts: {bar_type_counts}")
    lines.append(f"Target counts:   {target_counts}")
    lines.append(f"Model counts:    {model_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP PLAYBOOK CANDIDATES")
    lines.append("-" * 90)
    lines.append(playbook_df[available_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("RESEARCH NOTES")
    lines.append("-" * 90)

    for _, row in playbook_df.head(20).iterrows():
        lines.append(f"[{row['playbook_rank']}] {row['research_focus']}")
        lines.append(f"    {row['research_note']}")
        lines.append(f"    Next: {row['recommended_next_step']}")
        lines.append("")

    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Catalogue rows: {len(catalogue_df):,}")
    print(f"Playbook rows:  {len(playbook_df):,}")
    print(f"Priority counts: {priority_counts}")
    print(f"Symbol counts:   {symbol_counts}")
    print(f"Bar type counts: {bar_type_counts}")
    print(f"Target counts:   {target_counts}")
    print(f"Model counts:    {model_counts}")
    print("-" * 90)
    print("[TOP 20 PLAYBOOK ROWS]")
    print(playbook_df[available_display_cols].head(20).to_string(index=False))
    print("-" * 90)
    print("[DONE] Microstructure playbook build complete.")
    print(f"CSV output:  {csv_path}")
    print(f"JSON output: {json_path}")
    print(f"TXT output:  {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()