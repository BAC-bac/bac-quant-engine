"""
BACQE MICROSTRUCTURE 47 - CANDIDATE VALIDATION REVIEW

Purpose:
    Validate microstructure candidate registry findings before treating them
    as credible research candidates.

Important:
    This is NOT a trading strategy.
    This is a conservative research validation layer.
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


def safe_read_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"{label} file is empty: {path}")

    return df


def classify_validation(row: pd.Series) -> str:
    trade_count = row.get("trade_count", 0)
    win_rate = row.get("net_win_rate", np.nan)
    profit_factor = row.get("net_profit_factor", np.nan)
    forensic_label = row.get("forensic_health_label", "")
    registry_label = row.get("registry_label", "")
    date_conc = row.get("date_concentration_ratio", np.nan)
    hour_conc = row.get("hour_concentration_ratio", np.nan)
    max_loss_streak = row.get("max_loss_streak", np.nan)

    if forensic_label != "forensic_pass":
        return "reject_concentration_risk"

    if trade_count < 50:
        return "reject_low_sample"

    if pd.isna(win_rate) or pd.isna(profit_factor):
        return "investigate_missing_metrics"

    if date_conc >= 0.75:
        return "reject_date_concentrated"

    if hour_conc >= 0.80:
        return "investigate_hour_concentrated"

    if profit_factor >= 20:
        return "investigate_too_good_to_trust"

    if trade_count >= 250 and win_rate >= 0.65 and profit_factor >= 2.0:
        return "validation_pass_primary"

    if trade_count >= 100 and win_rate >= 0.58 and profit_factor >= 1.5:
        return "validation_pass_secondary"

    if "watchlist" in str(registry_label) and trade_count >= 30:
        return "watchlist_only"

    return "no_validation_edge"


def classify_risk_flag(row: pd.Series) -> str:
    flags = []

    if row.get("forensic_health_label") != "forensic_pass":
        flags.append("forensic_warning")

    if row.get("trade_count", 0) < 100:
        flags.append("low_sample")

    pf = row.get("net_profit_factor", np.nan)
    if pd.notna(pf) and pf >= 20:
        flags.append("extreme_profit_factor")

    dc = row.get("date_concentration_ratio", np.nan)
    if pd.notna(dc) and dc >= 0.75:
        flags.append("date_concentration")

    hc = row.get("hour_concentration_ratio", np.nan)
    if pd.notna(hc) and hc >= 0.80:
        flags.append("hour_concentration")

    if row.get("unique_dates", 0) < 5:
        flags.append("few_unique_dates")

    if row.get("unique_sessions", 0) < 2:
        flags.append("single_session")

    return ",".join(flags) if flags else "clean"


def build_validation_summary(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    numeric_cols = [
        "trade_count",
        "net_win_rate",
        "net_avg_return",
        "net_median_return",
        "net_total_return",
        "net_std_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "date_concentration_ratio",
        "hour_concentration_ratio",
        "unique_dates",
        "unique_hours",
        "unique_sessions",
        "registry_score",
        "registry_rank",
    ]

    for col in numeric_cols:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    work["validated_at_utc"] = datetime.now(timezone.utc).isoformat()
    work["validation_label"] = work.apply(classify_validation, axis=1)
    work["risk_flags"] = work.apply(classify_risk_flag, axis=1)

    priority_map = {
        "validation_pass_primary": 1,
        "validation_pass_secondary": 2,
        "investigate_too_good_to_trust": 3,
        "watchlist_only": 4,
        "investigate_hour_concentrated": 5,
        "investigate_missing_metrics": 6,
        "reject_concentration_risk": 7,
        "reject_date_concentrated": 8,
        "reject_low_sample": 9,
        "no_validation_edge": 10,
    }

    work["validation_rank_group"] = work["validation_label"].map(priority_map).fillna(99)

    sort_cols = [
        "validation_rank_group",
        "registry_rank",
        "trade_count",
        "net_profit_factor",
    ]

    sort_cols = [col for col in sort_cols if col in work.columns]

    work = work.sort_values(
        by=sort_cols,
        ascending=[True, True, False, False][: len(sort_cols)],
    ).reset_index(drop=True)

    work["validation_rank"] = np.arange(1, len(work) + 1)

    output_cols = [
        "validation_rank",
        "validation_label",
        "risk_flags",
        "filter_name",
        "symbols",
        "trade_count",
        "net_win_rate",
        "net_avg_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "unique_dates",
        "unique_sessions",
        "date_concentration_ratio",
        "hour_concentration_ratio",
        "registry_label",
        "registry_score",
        "registry_rank",
        "forensic_health_label",
        "cost_per_trade",
        "sessions",
        "weekdays",
        "threshold_pairs",
        "first_signal_time",
        "last_signal_time",
        "validated_at_utc",
    ]

    output_cols = [col for col in output_cols if col in work.columns]

    return work[output_cols]


def write_outputs(report_dir: Path, validation_df: pd.DataFrame, source_path: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)

    latest_csv = report_dir / "microstructure_candidate_validation_review_latest.csv"
    latest_json = report_dir / "microstructure_candidate_validation_review_latest.json"
    latest_txt = report_dir / "microstructure_candidate_validation_review_latest.txt"

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamped_csv = report_dir / f"microstructure_candidate_validation_review_{timestamp}.csv"

    validation_df.to_csv(latest_csv, index=False)
    validation_df.to_csv(timestamped_csv, index=False)

    summary = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(source_path),
        "rows": int(len(validation_df)),
        "validation_label_counts": validation_df["validation_label"].value_counts(dropna=False).to_dict(),
        "risk_flag_counts": validation_df["risk_flags"].value_counts(dropna=False).to_dict(),
        "top_candidates": validation_df.head(10).to_dict(orient="records"),
        "latest_csv": str(latest_csv),
        "timestamped_csv": str(timestamped_csv),
    }

    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    with open(latest_txt, "w", encoding="utf-8") as f:
        f.write("BACQE MICROSTRUCTURE 47 - CANDIDATE VALIDATION REVIEW\n")
        f.write("=" * 90 + "\n")
        f.write(f"Checked at UTC: {summary['checked_at_utc']}\n")
        f.write(f"Source file:     {summary['source_file']}\n")
        f.write(f"Rows:            {summary['rows']}\n")
        f.write("-" * 90 + "\n")
        f.write("Validation labels:\n")
        for key, value in summary["validation_label_counts"].items():
            f.write(f"  {key}: {value}\n")
        f.write("-" * 90 + "\n")
        f.write("Risk flags:\n")
        for key, value in summary["risk_flag_counts"].items():
            f.write(f"  {key}: {value}\n")
        f.write("-" * 90 + "\n")
        f.write("Top 10 candidates:\n")
        f.write(validation_df.head(10).to_string(index=False))
        f.write("\n")
        f.write("=" * 90 + "\n")

    print(f"CSV output:        {latest_csv}")
    print(f"Timestamped CSV:  {timestamped_csv}")
    print(f"JSON output:       {latest_json}")
    print(f"TXT output:        {latest_txt}")


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 47 - CANDIDATE VALIDATION REVIEW")

    cfg = load_config()
    micro_cfg = cfg["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    candidate_registry_path = (
        analysis_dir
        / "candidate_registry"
        / "microstructure_candidate_registry_latest.csv"
    )

    report_dir = analysis_dir / "candidate_validation_review"

    print(f"Candidate registry: {candidate_registry_path}")
    print(f"Report dir:         {report_dir}")
    print("-" * 90)

    registry_df = safe_read_csv(candidate_registry_path, "Candidate registry")

    validation_df = build_validation_summary(registry_df)

    print(f"Registry rows:     {len(registry_df)}")
    print(f"Validation rows:   {len(validation_df)}")
    print("-" * 90)

    print("Validation labels:")
    print(validation_df["validation_label"].value_counts(dropna=False).to_string())
    print("-" * 90)

    print("Risk flags:")
    print(validation_df["risk_flags"].value_counts(dropna=False).to_string())
    print("-" * 90)

    print("Top validation candidates:")
    display_cols = [
        "validation_rank",
        "validation_label",
        "risk_flags",
        "filter_name",
        "symbols",
        "trade_count",
        "net_win_rate",
        "net_profit_factor",
        "forensic_health_label",
    ]
    display_cols = [col for col in display_cols if col in validation_df.columns]

    print(validation_df[display_cols].head(15).to_string(index=False))
    print("-" * 90)

    write_outputs(report_dir, validation_df, candidate_registry_path)

    print_header("[DONE] Candidate validation review complete.")


if __name__ == "__main__":
    main()