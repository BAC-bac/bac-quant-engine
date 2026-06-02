"""
BACQE MICROSTRUCTURE 14 - MICROSTRUCTURE WATCHLIST SUMMARY

Purpose:
    Summarise the predictive audit watchlist from Script 13.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/predictive_audit/
        microstructure_predictive_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/watchlist_summary/
        microstructure_watchlist_summary_latest.csv
        microstructure_watchlist_summary_latest.json
        microstructure_watchlist_summary_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

WATCHLIST_LEVELS = [
    "weak_watchlist",
    "research_watchlist",
    "strong_watchlist",
]


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


def classify_actionability(row: pd.Series) -> str:
    strong_count = row.get("strong_watchlist_count", 0)
    research_count = row.get("research_watchlist_count", 0)
    weak_count = row.get("weak_watchlist_count", 0)
    avg_abs_corr = row.get("avg_abs_correlation", 0)
    max_abs_corr = row.get("max_abs_correlation", 0)
    dataset_count = row.get("dataset_count", 0)
    symbol_count = row.get("symbol_count", 0)

    if strong_count >= 5 and dataset_count >= 3 and avg_abs_corr >= 0.075:
        return "priority_research"

    if strong_count >= 2 and max_abs_corr >= 0.10:
        return "strong_research"

    if research_count >= 5 and dataset_count >= 3:
        return "broad_research"

    if symbol_count >= 3 and research_count >= 3:
        return "cross_symbol_research"

    if weak_count >= 5:
        return "low_priority_research"

    return "watch_only"


def summarise_by_group(watchlist_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        watchlist_df
        .groupby(group_cols, dropna=False)
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
            bar_type_count=("bar_type", "nunique"),
            target_count=("target", "nunique"),
            dataset_count=("file_path", "nunique"),
        )
        .reset_index()
    )

    grouped["actionability"] = grouped.apply(classify_actionability, axis=1)

    grouped = grouped.sort_values(
        [
            "strong_watchlist_count",
            "research_watchlist_count",
            "avg_abs_correlation",
            "watchlist_count",
        ],
        ascending=[False, False, False, False],
    )

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 14 - MICROSTRUCTURE WATCHLIST SUMMARY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    predictive_audit_path = (
        analysis_dir
        / "predictive_audit"
        / "microstructure_predictive_audit_latest.csv"
    )

    report_dir = analysis_dir / "watchlist_summary"
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
        raise RuntimeError("No watchlist rows found.")

    feature_summary = summarise_by_group(
        watchlist_df,
        ["feature_name", "feature_family"],
    )

    family_summary = summarise_by_group(
        watchlist_df,
        ["feature_family"],
    )

    symbol_summary = summarise_by_group(
        watchlist_df,
        ["symbol"],
    )

    bar_type_summary = summarise_by_group(
        watchlist_df,
        ["bar_type"],
    )

    target_summary = summarise_by_group(
        watchlist_df,
        ["target"],
    )

    symbol_bar_type_summary = summarise_by_group(
        watchlist_df,
        ["symbol", "bar_type"],
    )

    feature_symbol_summary = summarise_by_group(
        watchlist_df,
        ["feature_name", "feature_family", "symbol"],
    )

    csv_main_path = report_dir / "microstructure_watchlist_summary_latest.csv"
    csv_feature_path = report_dir / "microstructure_watchlist_by_feature_latest.csv"
    csv_family_path = report_dir / "microstructure_watchlist_by_family_latest.csv"
    csv_symbol_path = report_dir / "microstructure_watchlist_by_symbol_latest.csv"
    csv_bar_type_path = report_dir / "microstructure_watchlist_by_bar_type_latest.csv"
    csv_target_path = report_dir / "microstructure_watchlist_by_target_latest.csv"
    csv_symbol_bar_type_path = report_dir / "microstructure_watchlist_by_symbol_bar_type_latest.csv"
    csv_feature_symbol_path = report_dir / "microstructure_watchlist_by_feature_symbol_latest.csv"
    json_path = report_dir / "microstructure_watchlist_summary_latest.json"
    txt_path = report_dir / "microstructure_watchlist_summary_latest.txt"

    feature_summary.to_csv(csv_feature_path, index=False)
    family_summary.to_csv(csv_family_path, index=False)
    symbol_summary.to_csv(csv_symbol_path, index=False)
    bar_type_summary.to_csv(csv_bar_type_path, index=False)
    target_summary.to_csv(csv_target_path, index=False)
    symbol_bar_type_summary.to_csv(csv_symbol_bar_type_path, index=False)
    feature_symbol_summary.to_csv(csv_feature_symbol_path, index=False)

    combined_summary = pd.concat(
        [
            feature_summary.assign(summary_type="feature"),
            family_summary.assign(summary_type="family"),
            symbol_summary.assign(summary_type="symbol"),
            bar_type_summary.assign(summary_type="bar_type"),
            target_summary.assign(summary_type="target"),
            symbol_bar_type_summary.assign(summary_type="symbol_bar_type"),
            feature_symbol_summary.assign(summary_type="feature_symbol"),
        ],
        ignore_index=True,
        sort=False,
    )

    combined_summary.to_csv(csv_main_path, index=False)

    output_payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "audit_rows": len(audit_df),
        "watchlist_rows": len(watchlist_df),
        "signal_counts": watchlist_df["signal_strength"].value_counts(dropna=False).to_dict(),
        "top_features": feature_summary.head(50).to_dict(orient="records"),
        "top_families": family_summary.to_dict(orient="records"),
        "top_symbols": symbol_summary.to_dict(orient="records"),
        "top_bar_types": bar_type_summary.to_dict(orient="records"),
        "top_targets": target_summary.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2, default=str)

    top_features = feature_summary.head(25)
    top_feature_symbol = feature_symbol_summary.head(25)
    top_family = family_summary.head(10)
    top_symbol = symbol_summary.head(10)
    top_bar_type = bar_type_summary.head(10)
    top_target = target_summary.head(10)

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE WATCHLIST SUMMARY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Predictive audit rows: {len(audit_df):,}")
    lines.append(f"Watchlist rows:        {len(watchlist_df):,}")
    lines.append(f"Signal counts:         {watchlist_df['signal_strength'].value_counts(dropna=False).to_dict()}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP FEATURE FAMILIES")
    lines.append("-" * 90)
    lines.append(top_family.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP SYMBOLS")
    lines.append("-" * 90)
    lines.append(top_symbol.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP BAR TYPES")
    lines.append("-" * 90)
    lines.append(top_bar_type.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP TARGET HORIZONS")
    lines.append("-" * 90)
    lines.append(top_target.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 25 RECURRING FEATURES")
    lines.append("-" * 90)
    lines.append(top_features.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 25 FEATURE/SYMBOL COMBINATIONS")
    lines.append("-" * 90)
    lines.append(top_feature_symbol.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Watchlist rows: {len(watchlist_df):,}")
    print(f"Signal counts:  {watchlist_df['signal_strength'].value_counts(dropna=False).to_dict()}")
    print("-" * 90)

    print("[TOP FEATURE FAMILIES]")
    print(top_family.to_string(index=False))
    print("-" * 90)

    print("[TOP SYMBOLS]")
    print(top_symbol.to_string(index=False))
    print("-" * 90)

    print("[TOP BAR TYPES]")
    print(top_bar_type.to_string(index=False))
    print("-" * 90)

    print("[TOP TARGETS]")
    print(top_target.to_string(index=False))
    print("-" * 90)

    print("[DONE] Microstructure watchlist summary complete.")
    print(f"Combined CSV:        {csv_main_path}")
    print(f"Feature CSV:         {csv_feature_path}")
    print(f"Family CSV:          {csv_family_path}")
    print(f"Symbol CSV:          {csv_symbol_path}")
    print(f"Bar type CSV:        {csv_bar_type_path}")
    print(f"Target CSV:          {csv_target_path}")
    print(f"Symbol/bar type CSV: {csv_symbol_bar_type_path}")
    print(f"Feature/symbol CSV:  {csv_feature_symbol_path}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()