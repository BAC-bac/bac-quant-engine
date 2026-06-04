"""
BACQE MICROSTRUCTURE 43 - SIGNAL FILTER OPTIMIZER

Purpose:
    Combine discoveries from threshold, cost, time-of-day, and weekday analysis
    into candidate microstructure trade-rule specifications.

Inputs:
    cost_stress_test/
        microstructure_cost_stress_test_trades_latest.csv

Outputs:
    signal_filter_optimizer/
        microstructure_signal_filter_optimizer_trades_latest.csv
        microstructure_signal_filter_optimizer_summary_latest.csv
        microstructure_signal_filter_optimizer_latest.json
        microstructure_signal_filter_optimizer_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

LONDON_TZ = "Europe/London"

SELECTED_LABELS = {
    "threshold_signal_strong",
    "threshold_signal_research",
}

FILTER_SETS = [
    {
        "filter_name": "eurusd_asia_thu_fri_core",
        "symbols": ["EURUSD"],
        "sessions": ["asia_late_overnight"],
        "weekdays": ["Thursday", "Friday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_10", "avg_spread_mean_5"],
        "targets": ["forward_return_3", "forward_return_5"],
        "max_cost": 0.00015,
    },
    {
        "filter_name": "eurusd_asia_thursday_only",
        "symbols": ["EURUSD"],
        "sessions": ["asia_late_overnight"],
        "weekdays": ["Thursday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_10", "avg_spread_mean_5"],
        "targets": ["forward_return_3", "forward_return_5"],
        "max_cost": 0.00015,
    },
    {
        "filter_name": "eurusd_asia_friday_only",
        "symbols": ["EURUSD"],
        "sessions": ["asia_late_overnight"],
        "weekdays": ["Friday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_10", "avg_spread_mean_5"],
        "targets": ["forward_return_3", "forward_return_5"],
        "max_cost": 0.00015,
    },
    {
        "filter_name": "eurusd_pre_new_york_candidate",
        "symbols": ["EURUSD"],
        "sessions": ["pre_new_york"],
        "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_10", "avg_spread_mean_5"],
        "targets": ["forward_return_3", "forward_return_5"],
        "max_cost": 0.00015,
    },
    {
        "filter_name": "eurusd_exclude_bad_days",
        "symbols": ["EURUSD"],
        "sessions": ["asia_late_overnight", "pre_new_york", "london_mid_morning"],
        "weekdays": ["Tuesday", "Thursday", "Friday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_10", "avg_spread_mean_5"],
        "targets": ["forward_return_3", "forward_return_5"],
        "max_cost": 0.00015,
    },
    {
        "filter_name": "gbpusd_asia_thu_fri_watchlist",
        "symbols": ["GBPUSD"],
        "sessions": ["asia_late_overnight"],
        "weekdays": ["Thursday", "Friday"],
        "parameters": ["imbalance_threshold_25"],
        "spread_features": ["avg_spread_mean_5"],
        "targets": ["forward_return_3"],
        "max_cost": 0.00005,
    },
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


def assign_session(hour: int) -> str:
    if 0 <= hour < 6:
        return "asia_late_overnight"
    if 6 <= hour < 8:
        return "pre_london"
    if 8 <= hour < 10:
        return "london_open"
    if 10 <= hour < 12:
        return "london_mid_morning"
    if 12 <= hour < 13:
        return "pre_new_york"
    if 13 <= hour < 15:
        return "new_york_open"
    if 15 <= hour < 17:
        return "london_new_york_overlap"
    if 17 <= hour < 21:
        return "new_york_afternoon"
    return "late_us_close"


def calculate_streaks(win_series: pd.Series) -> tuple[int, int]:
    max_win_streak = 0
    max_loss_streak = 0
    current_win = 0
    current_loss = 0

    for value in win_series.fillna(False):
        if bool(value):
            current_win += 1
            current_loss = 0
        else:
            current_loss += 1
            current_win = 0

        max_win_streak = max(max_win_streak, current_win)
        max_loss_streak = max(max_loss_streak, current_loss)

    return max_win_streak, max_loss_streak


def prepare_trades(cost_trades_df: pd.DataFrame) -> pd.DataFrame:
    df = cost_trades_df.copy()

    df = df[
        (df["status"] == "ok")
        & (df["threshold_signal_label"].isin(SELECTED_LABELS))
    ].copy()

    df["end_time_utc"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["end_time_utc"]).copy()

    df["end_time_london"] = df["end_time_utc"].dt.tz_convert(LONDON_TZ)
    df["weekday"] = df["end_time_london"].dt.day_name()
    df["london_hour"] = df["end_time_london"].dt.hour
    df["session"] = df["london_hour"].apply(assign_session)

    df["net_signed_return"] = pd.to_numeric(df["net_signed_return"], errors="coerce")
    df["gross_signed_return"] = pd.to_numeric(df["gross_signed_return"], errors="coerce")
    df["cost_per_trade"] = pd.to_numeric(df["cost_per_trade"], errors="coerce")
    df["net_win_flag"] = df["net_signed_return"] > 0

    return df


def apply_filter(df: pd.DataFrame, filter_spec: dict) -> pd.DataFrame:
    work = df.copy()

    work = work[work["symbol"].isin(filter_spec["symbols"])]
    work = work[work["session"].isin(filter_spec["sessions"])]
    work = work[work["weekday"].isin(filter_spec["weekdays"])]
    work = work[work["parameter"].isin(filter_spec["parameters"])]
    work = work[work["spread_feature"].isin(filter_spec["spread_features"])]
    work = work[work["target"].isin(filter_spec["targets"])]
    work = work[work["cost_per_trade"] <= filter_spec["max_cost"]]

    work = work.copy()
    work["filter_name"] = filter_spec["filter_name"]
    work["filter_symbols"] = ",".join(filter_spec["symbols"])
    work["filter_sessions"] = ",".join(filter_spec["sessions"])
    work["filter_weekdays"] = ",".join(filter_spec["weekdays"])
    work["filter_parameters"] = ",".join(filter_spec["parameters"])
    work["filter_spread_features"] = ",".join(filter_spec["spread_features"])
    work["filter_targets"] = ",".join(filter_spec["targets"])
    work["filter_max_cost"] = filter_spec["max_cost"]

    return work


def build_filter_summary(filtered_trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "filter_name",
        "cost_per_trade",
    ]

    records = []

    for keys, group in filtered_trades_df.groupby(group_cols, dropna=False):
        group = group.sort_values("end_time_london").reset_index(drop=True)

        gross_profit = group.loc[group["net_signed_return"] > 0, "net_signed_return"].sum()
        gross_loss = group.loc[group["net_signed_return"] < 0, "net_signed_return"].sum()

        profit_factor = None
        if gross_loss < 0:
            profit_factor = float(gross_profit / abs(gross_loss))

        avg_return = group["net_signed_return"].mean()
        std_return = group["net_signed_return"].std()

        sharpe_like = None
        if std_return and std_return > 0:
            sharpe_like = float(avg_return / std_return)

        cumulative = group["net_signed_return"].fillna(0).cumsum()
        running_max = cumulative.cummax()
        drawdown = cumulative - running_max

        max_win_streak, max_loss_streak = calculate_streaks(group["net_win_flag"])

        record = dict(zip(group_cols, keys))
        record.update(
            {
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "trade_count": len(group),
                "unique_symbols": int(group["symbol"].nunique()),
                "symbols": ",".join(sorted(group["symbol"].dropna().unique())),
                "unique_sessions": int(group["session"].nunique()),
                "sessions": ",".join(sorted(group["session"].dropna().unique())),
                "unique_weekdays": int(group["weekday"].nunique()),
                "weekdays": ",".join(sorted(group["weekday"].dropna().unique())),
                "unique_candidates": int(group["dataset_file"].nunique()),
                "unique_threshold_pairs": int(group["threshold_pair"].nunique()),
                "threshold_pairs": ",".join(sorted(group["threshold_pair"].dropna().astype(str).unique())),
                "long_trades": int((group["signal_direction"] == 1).sum()),
                "short_trades": int((group["signal_direction"] == -1).sum()),
                "net_win_rate": float(group["net_win_flag"].mean()),
                "net_avg_return": float(avg_return),
                "net_median_return": float(group["net_signed_return"].median()),
                "net_total_return": float(group["net_signed_return"].sum()),
                "net_std_return": float(std_return),
                "net_profit_factor": profit_factor,
                "net_sharpe_like": sharpe_like,
                "max_drawdown": float(drawdown.min()),
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "first_signal_time": str(group["end_time_london"].min()),
                "last_signal_time": str(group["end_time_london"].max()),
            }
        )

        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary["filter_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
        + summary["max_drawdown"].fillna(0) * 5000
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["trade_count"] < 30:
            return "low_sample"

        if (
            row["net_win_rate"] >= 0.65
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.50
        ):
            return "filter_strong_candidate"

        if (
            row["net_win_rate"] >= 0.58
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.20
        ):
            return "filter_research_candidate"

        if row["net_win_rate"] >= 0.53 and row["net_avg_return"] > 0:
            return "filter_weak_candidate"

        return "filter_failed"

    summary["filter_label"] = summary.apply(label, axis=1)

    label_rank = {
        "filter_strong_candidate": 1,
        "filter_research_candidate": 2,
        "filter_weak_candidate": 3,
        "filter_failed": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["filter_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "filter_score",
            "net_win_rate",
            "net_profit_factor",
            "trade_count",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["filter_rank"] = summary.index + 1

    return summary


def build_filter_audit(filtered_trades_df: pd.DataFrame) -> pd.DataFrame:
    audit_cols = [
        "filter_name",
        "cost_per_trade",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "trade_number",
        "end_time",
        "end_time_london",
        "weekday",
        "session",
        "liquidity_regime",
        "signal_direction",
        "forward_return",
        "gross_signed_return",
        "net_signed_return",
        "net_win_flag",
        "dataset_file",
    ]

    available_cols = [c for c in audit_cols if c in filtered_trades_df.columns]
    audit_df = filtered_trades_df[available_cols].copy()

    duplicate_key_cols = [
        "filter_name",
        "cost_per_trade",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "trade_number",
        "end_time",
    ]

    available_key_cols = [c for c in duplicate_key_cols if c in audit_df.columns]

    audit_df["duplicate_trade_key"] = audit_df.duplicated(
        subset=available_key_cols,
        keep=False,
    )

    audit_df["return_direction_check"] = np.where(
        audit_df["signal_direction"] != 0,
        audit_df["gross_signed_return"] == audit_df["forward_return"] * audit_df["signal_direction"],
        False,
    )

    audit_df["audit_warning"] = ""

    audit_df.loc[audit_df["duplicate_trade_key"], "audit_warning"] += "duplicate_trade_key;"
    audit_df.loc[~audit_df["return_direction_check"], "audit_warning"] += "return_direction_mismatch;"

    audit_df = audit_df.sort_values(
        [
            "filter_name",
            "cost_per_trade",
            "symbol",
            "parameter",
            "spread_feature",
            "target",
            "threshold_pair",
            "end_time_london",
            "trade_number",
        ],
        ascending=True,
        na_position="last",
    ).reset_index(drop=True)

    audit_df["audit_row"] = audit_df.index + 1

    return audit_df


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 43 - SIGNAL FILTER OPTIMIZER")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    cost_trades_path = (
        analysis_dir
        / "cost_stress_test"
        / "microstructure_cost_stress_test_trades_latest.csv"
    )

    report_dir = analysis_dir / "signal_filter_optimizer"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cost trades: {cost_trades_path}")
    print(f"Report dir:  {report_dir}")
    print(f"Filters:     {len(FILTER_SETS)}")
    print("-" * 90)

    if not cost_trades_path.exists():
        raise FileNotFoundError(
            f"Missing cost trades file: {cost_trades_path}. Run script 39 first."
        )

    cost_trades_df = pd.read_csv(cost_trades_path)
    prepared_df = prepare_trades(cost_trades_df)

    filtered_frames = []

    for spec in FILTER_SETS:
        filtered = apply_filter(prepared_df, spec)
        filtered_frames.append(filtered)

        print(
            f"[FILTER] {spec['filter_name']:<35} "
            f"rows={len(filtered):,}"
        )

    filtered_trades_df = pd.concat(filtered_frames, ignore_index=True, sort=False)

    if filtered_trades_df.empty:
        raise RuntimeError("No trades survived the filter optimizer.")

    summary_df = build_filter_summary(filtered_trades_df)
    audit_df = build_filter_audit(filtered_trades_df)

    trades_csv = report_dir / "microstructure_signal_filter_optimizer_trades_latest.csv"
    summary_csv = report_dir / "microstructure_signal_filter_optimizer_summary_latest.csv"
    json_path = report_dir / "microstructure_signal_filter_optimizer_latest.json"
    txt_path = report_dir / "microstructure_signal_filter_optimizer_latest.txt"
    audit_csv = report_dir / "microstructure_signal_filter_optimizer_audit_latest.csv"

    filtered_trades_df.to_csv(trades_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    audit_df.to_csv(audit_csv, index=False)

    label_counts = summary_df["filter_label"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(cost_trades_df),
        "prepared_rows": len(prepared_df),
        "filtered_rows": len(filtered_trades_df),
        "summary_rows": len(summary_df),
        "filters": FILTER_SETS,
        "label_counts": label_counts,
        "top_summary": summary_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    display_cols = [
        "filter_rank",
        "filter_name",
        "cost_per_trade",
        "trade_count",
        "symbols",
        "sessions",
        "weekdays",
        "threshold_pairs",
        "long_trades",
        "short_trades",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "filter_score",
        "filter_label",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL FILTER OPTIMIZER")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Input rows:     {len(cost_trades_df):,}")
    lines.append(f"Prepared rows:  {len(prepared_df):,}")
    lines.append(f"Filtered rows:  {len(filtered_trades_df):,}")
    lines.append(f"Summary rows:   {len(summary_df):,}")
    lines.append(f"Audit rows:    {len(audit_df):,}")
    lines.append(f"Audit warnings: {audit_df['audit_warning'].value_counts(dropna=False).to_dict()}")
    lines.append(f"Duplicate trade rows: {int(audit_df['duplicate_trade_key'].sum()):,}")
    lines.append(f"Return direction mismatch rows: {int((~audit_df['return_direction_check']).sum()):,}")
    lines.append("")
    lines.append(f"Filter labels: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("FILTER OPTIMIZER SUMMARY")
    lines.append("-" * 90)
    lines.append(summary_df[display_cols].to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal filter optimizer complete.")
    print(f"Input rows:    {len(cost_trades_df):,}")
    print(f"Prepared rows: {len(prepared_df):,}")
    print(f"Filtered rows: {len(filtered_trades_df):,}")
    print(f"Summary rows:  {len(summary_df):,}")
    print(f"Audit CSV:     {audit_csv}")
    print(f"Audit warnings:{audit_df['audit_warning'].value_counts(dropna=False).to_dict()}")
    print(f"Duplicates:    {int(audit_df['duplicate_trade_key'].sum()):,}")
    print(f"Return mismatches: {int((~audit_df['return_direction_check']).sum()):,}")
    print(f"Labels:        {label_counts}")
    print(f"Trades CSV:    {trades_csv}")
    print(f"Summary CSV:   {summary_csv}")
    print(f"JSON output:   {json_path}")
    print(f"TXT output:    {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()