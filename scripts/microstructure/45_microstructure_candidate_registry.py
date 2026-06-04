"""
BACQE MICROSTRUCTURE 45 - CANDIDATE REGISTRY

Purpose:
    Build a consolidated registry of microstructure trade-rule candidates.

Inputs:
    signal_filter_optimizer/
        microstructure_signal_filter_optimizer_summary_latest.csv

    signal_forensics/
        microstructure_signal_forensics_health_summary_latest.csv

    cost_stress_test/
        microstructure_cost_stress_test_survival_latest.csv

    monte_carlo_stability/
        microstructure_monte_carlo_summary_latest.csv

Outputs:
    candidate_registry/
        microstructure_candidate_registry_latest.csv
        microstructure_candidate_registry_latest.json
        microstructure_candidate_registry_latest.txt
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


def make_candidate_key_from_filter(row: pd.Series) -> str:
    return (
        f"{row.get('filter_name')}|"
        f"{row.get('cost_per_trade')}"
    )


def classify_registry_candidate(row: pd.Series) -> str:
    forensic_label = row.get("forensic_health_label")
    trade_count = row.get("trade_count", 0)
    win_rate = row.get("net_win_rate", np.nan)
    profit_factor = row.get("net_profit_factor", np.nan)
    max_drawdown = row.get("max_drawdown", np.nan)
    unique_dates = row.get("unique_dates", 0)

    if forensic_label == "audit_problem":
        return "reject_audit_problem"

    if forensic_label in {"date_concentrated", "highly_date_concentrated"}:
        return "watch_concentrated"

    if pd.isna(win_rate) or pd.isna(profit_factor):
        return "insufficient_metrics"

    if trade_count >= 300 and unique_dates >= 4 and win_rate >= 0.70 and profit_factor >= 3.0:
        return "primary_research_candidate"

    if trade_count >= 100 and unique_dates >= 3 and win_rate >= 0.60 and profit_factor >= 1.5:
        return "secondary_research_candidate"

    if trade_count >= 30 and win_rate >= 0.55 and profit_factor >= 1.1:
        return "watchlist_candidate"

    return "deprioritised"


def build_registry_score(row: pd.Series) -> float:
    trade_count = row.get("trade_count", 0)
    unique_dates = row.get("unique_dates", 0)
    win_rate = row.get("net_win_rate", 0.5)
    avg_return = row.get("net_avg_return", 0)
    total_return = row.get("net_total_return", 0)
    profit_factor = row.get("net_profit_factor", 0)
    max_drawdown = row.get("max_drawdown", 0)
    date_concentration = row.get("date_concentration_ratio", 1)
    hour_concentration = row.get("hour_concentration_ratio", 1)

    forensic_label = row.get("forensic_health_label", "")

    score = (
        (win_rate - 0.5) * 250
        + min(profit_factor, 5) * 10
        + np.log1p(max(trade_count, 0)) * 4
        + np.log1p(max(unique_dates, 0)) * 8
        + total_return * 8000
        + avg_return * 20000
        + max_drawdown * 3000
        - date_concentration * 20
        - hour_concentration * 10
    )

    if forensic_label == "forensic_pass":
        score += 20
    elif forensic_label == "date_concentrated":
        score -= 35
    elif forensic_label == "highly_date_concentrated":
        score -= 45
    elif forensic_label == "audit_problem":
        score -= 100

    return round(float(np.clip(score, 0, 100)), 2)


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 45 - CANDIDATE REGISTRY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    filter_summary_path = (
        analysis_dir
        / "signal_filter_optimizer"
        / "microstructure_signal_filter_optimizer_summary_latest.csv"
    )

    forensic_health_path = (
        analysis_dir
        / "signal_forensics"
        / "microstructure_signal_forensics_health_summary_latest.csv"
    )

    cost_survival_path = (
        analysis_dir
        / "cost_stress_test"
        / "microstructure_cost_stress_test_survival_latest.csv"
    )

    mc_summary_path = (
        analysis_dir
        / "monte_carlo_stability"
        / "microstructure_monte_carlo_summary_latest.csv"
    )

    report_dir = analysis_dir / "candidate_registry"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Filter summary:   {filter_summary_path}")
    print(f"Forensic health:  {forensic_health_path}")
    print(f"Cost survival:    {cost_survival_path}")
    print(f"Monte Carlo:      {mc_summary_path}")
    print(f"Report dir:       {report_dir}")
    print("-" * 90)

    if not filter_summary_path.exists():
        raise FileNotFoundError(f"Missing filter summary: {filter_summary_path}")

    if not forensic_health_path.exists():
        raise FileNotFoundError(f"Missing forensic health summary: {forensic_health_path}")

    filter_df = pd.read_csv(filter_summary_path)
    forensic_df = pd.read_csv(forensic_health_path)

    registry_df = filter_df.copy()

    merge_cols = ["filter_name", "cost_per_trade"]

    forensic_cols = [
        "filter_name",
        "cost_per_trade",
        "unique_dates",
        "unique_hours",
        "unique_weekdays",
        "unique_candidates",
        "date_concentration_ratio",
        "hour_concentration_ratio",
        "duplicate_trade_rows",
        "return_direction_mismatch_rows",
        "forensic_health_label",
    ]

    forensic_cols = [c for c in forensic_cols if c in forensic_df.columns]

    registry_df = registry_df.merge(
        forensic_df[forensic_cols],
        on=merge_cols,
        how="left",
        suffixes=("", "_forensic"),
    )

    numeric_cols = [
        "trade_count",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "unique_dates",
        "unique_hours",
        "unique_weekdays",
        "unique_candidates",
        "date_concentration_ratio",
        "hour_concentration_ratio",
        "duplicate_trade_rows",
        "return_direction_mismatch_rows",
    ]

    for col in numeric_cols:
        if col in registry_df.columns:
            registry_df[col] = pd.to_numeric(registry_df[col], errors="coerce")

    registry_df["candidate_key"] = registry_df.apply(make_candidate_key_from_filter, axis=1)
    registry_df["registry_label"] = registry_df.apply(classify_registry_candidate, axis=1)
    registry_df["registry_score"] = registry_df.apply(build_registry_score, axis=1)

    label_rank = {
        "primary_research_candidate": 1,
        "secondary_research_candidate": 2,
        "watchlist_candidate": 3,
        "watch_concentrated": 4,
        "deprioritised": 5,
        "insufficient_metrics": 6,
        "reject_audit_problem": 7,
    }

    registry_df["label_rank"] = registry_df["registry_label"].map(label_rank).fillna(99)
    registry_df["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    registry_df = registry_df.sort_values(
        [
            "label_rank",
            "registry_score",
            "trade_count",
            "net_profit_factor",
            "net_win_rate",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    registry_df["registry_rank"] = registry_df.index + 1

    csv_path = report_dir / "microstructure_candidate_registry_latest.csv"
    json_path = report_dir / "microstructure_candidate_registry_latest.json"
    txt_path = report_dir / "microstructure_candidate_registry_latest.txt"

    registry_df.to_csv(csv_path, index=False)

    label_counts = registry_df["registry_label"].value_counts(dropna=False).to_dict()
    forensic_counts = registry_df["forensic_health_label"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "registry_rows": len(registry_df),
        "label_counts": label_counts,
        "forensic_counts": forensic_counts,
        "top_registry": registry_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    display_cols = [
        "registry_rank",
        "filter_name",
        "cost_per_trade",
        "trade_count",
        "symbols",
        "sessions",
        "weekdays",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "unique_dates",
        "unique_hours",
        "unique_weekdays",
        "date_concentration_ratio",
        "hour_concentration_ratio",
        "forensic_health_label",
        "registry_score",
        "registry_label",
    ]

    available_display_cols = [c for c in display_cols if c in registry_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE CANDIDATE REGISTRY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Registry rows: {len(registry_df):,}")
    lines.append("")
    lines.append(f"Registry labels: {label_counts}")
    lines.append(f"Forensic labels: {forensic_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP CANDIDATE REGISTRY")
    lines.append("-" * 90)
    lines.append(registry_df[available_display_cols].head(60).to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Candidate registry complete.")
    print(f"Registry rows:    {len(registry_df):,}")
    print(f"Registry labels:  {label_counts}")
    print(f"Forensic labels:  {forensic_counts}")
    print(f"CSV output:       {csv_path}")
    print(f"JSON output:      {json_path}")
    print(f"TXT output:       {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()