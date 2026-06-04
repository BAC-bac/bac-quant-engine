"""
BACQE MICROSTRUCTURE 44 - SIGNAL FORENSICS

Purpose:
    Forensically inspect the most interesting filtered microstructure signals.

Focus:
    - Are trades unique?
    - Are trades clustered into a tiny number of dates?
    - Which hours drive performance?
    - Which liquidity regimes drive performance?
    - Are suspicious 100% win-rate filters genuinely broad or concentrated?

Inputs:
    signal_filter_optimizer/
        microstructure_signal_filter_optimizer_trades_latest.csv

Outputs:
    signal_forensics/
        microstructure_signal_forensics_trades_latest.csv
        microstructure_signal_forensics_date_summary_latest.csv
        microstructure_signal_forensics_hour_summary_latest.csv
        microstructure_signal_forensics_regime_summary_latest.csv
        microstructure_signal_forensics_gap_summary_latest.csv
        microstructure_signal_forensics_latest.json
        microstructure_signal_forensics_latest.txt
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

FOCUS_FILTERS = [
    "eurusd_asia_thursday_only",
    "eurusd_asia_thu_fri_core",
    "eurusd_asia_friday_only",
    "eurusd_pre_new_york_candidate",
    "eurusd_exclude_bad_days",
    "gbpusd_asia_thu_fri_watchlist",
]

FOCUS_COST_LEVELS = [
    0.00000,
    0.00002,
    0.00005,
    0.00010,
    0.00015,
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


def safe_profit_factor(returns: pd.Series) -> float | None:
    gross_profit = returns[returns > 0].sum()
    gross_loss = returns[returns < 0].sum()

    if gross_loss < 0:
        return float(gross_profit / abs(gross_loss))

    return None


def summarise_group(group: pd.DataFrame) -> dict:
    returns = pd.to_numeric(group["net_signed_return"], errors="coerce").dropna()
    wins = returns > 0

    avg_return = returns.mean()
    std_return = returns.std()

    sharpe_like = None
    if std_return and std_return > 0:
        sharpe_like = float(avg_return / std_return)

    cumulative = returns.fillna(0).cumsum()
    running_max = cumulative.cummax()
    drawdown = cumulative - running_max

    max_win_streak, max_loss_streak = calculate_streaks(wins)

    return {
        "trade_count": int(len(returns)),
        "long_trades": int((group["signal_direction"] == 1).sum()) if "signal_direction" in group.columns else None,
        "short_trades": int((group["signal_direction"] == -1).sum()) if "signal_direction" in group.columns else None,
        "win_rate": float(wins.mean()) if len(wins) else None,
        "avg_return": float(avg_return) if pd.notna(avg_return) else None,
        "median_return": float(returns.median()) if len(returns) else None,
        "total_return": float(returns.sum()) if len(returns) else None,
        "std_return": float(std_return) if pd.notna(std_return) else None,
        "profit_factor": safe_profit_factor(returns),
        "sharpe_like": sharpe_like,
        "max_drawdown": float(drawdown.min()) if len(drawdown) else None,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
    }


def prepare_forensics_trades(trades_df: pd.DataFrame) -> pd.DataFrame:
    df = trades_df.copy()

    df = df[
        df["filter_name"].isin(FOCUS_FILTERS)
        & df["cost_per_trade"].isin(FOCUS_COST_LEVELS)
    ].copy()

    if df.empty:
        return df

    if "end_time_london" in df.columns:
        df["end_time_london"] = pd.to_datetime(df["end_time_london"], utc=True, errors="coerce")
    elif "end_time" in df.columns:
        df["end_time_london"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce").dt.tz_convert(LONDON_TZ)

    df = df.dropna(subset=["end_time_london"]).copy()

    df["london_date"] = df["end_time_london"].dt.date.astype(str)
    df["london_hour"] = df["end_time_london"].dt.hour
    df["london_minute"] = df["end_time_london"].dt.minute
    df["weekday"] = df["end_time_london"].dt.day_name()

    df["net_signed_return"] = pd.to_numeric(df["net_signed_return"], errors="coerce")
    df["gross_signed_return"] = pd.to_numeric(df["gross_signed_return"], errors="coerce")
    df["forward_return"] = pd.to_numeric(df["forward_return"], errors="coerce")
    df["cost_per_trade"] = pd.to_numeric(df["cost_per_trade"], errors="coerce")
    df["net_win_flag"] = df["net_signed_return"] > 0

    duplicate_cols = [
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

    available_duplicate_cols = [c for c in duplicate_cols if c in df.columns]

    df["duplicate_trade_key"] = df.duplicated(
        subset=available_duplicate_cols,
        keep=False,
    )

    df["return_direction_check"] = np.where(
        df["signal_direction"] != 0,
        np.isclose(
            df["gross_signed_return"],
            df["forward_return"] * df["signal_direction"],
            equal_nan=False,
        ),
        False,
    )

    df = df.sort_values(
        [
            "filter_name",
            "cost_per_trade",
            "end_time_london",
            "symbol",
            "parameter",
            "spread_feature",
            "target",
            "threshold_pair",
        ],
        ascending=True,
        na_position="last",
    ).reset_index(drop=True)

    df["forensic_trade_row"] = df.index + 1

    return df


def build_date_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["filter_name", "cost_per_trade", "london_date", "weekday"]

    records = []

    for keys, group in df.groupby(group_cols, dropna=False):
        record = dict(zip(group_cols, keys))
        record.update(summarise_group(group))
        record["unique_hours"] = int(group["london_hour"].nunique())
        record["unique_candidates"] = int(group["dataset_file"].nunique()) if "dataset_file" in group.columns else None
        record["first_signal_time"] = str(group["end_time_london"].min())
        record["last_signal_time"] = str(group["end_time_london"].max())
        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary = summary.sort_values(
        ["filter_name", "cost_per_trade", "london_date"],
        ascending=True,
    ).reset_index(drop=True)

    summary["date_summary_rank"] = summary.index + 1

    return summary


def build_hour_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["filter_name", "cost_per_trade", "london_hour"]

    records = []

    for keys, group in df.groupby(group_cols, dropna=False):
        record = dict(zip(group_cols, keys))
        record.update(summarise_group(group))
        record["unique_dates"] = int(group["london_date"].nunique())
        record["unique_weekdays"] = int(group["weekday"].nunique())
        record["unique_candidates"] = int(group["dataset_file"].nunique()) if "dataset_file" in group.columns else None
        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary = summary.sort_values(
        [
            "filter_name",
            "cost_per_trade",
            "london_hour",
        ],
        ascending=True,
    ).reset_index(drop=True)

    summary["hour_summary_rank"] = summary.index + 1

    return summary


def build_regime_summary(df: pd.DataFrame) -> pd.DataFrame:
    regime_col = "liquidity_regime"

    if regime_col not in df.columns:
        return pd.DataFrame()

    group_cols = ["filter_name", "cost_per_trade", regime_col]

    records = []

    for keys, group in df.groupby(group_cols, dropna=False):
        record = dict(zip(group_cols, keys))
        record.update(summarise_group(group))
        record["unique_dates"] = int(group["london_date"].nunique())
        record["unique_hours"] = int(group["london_hour"].nunique())
        record["unique_weekdays"] = int(group["weekday"].nunique())
        record["unique_candidates"] = int(group["dataset_file"].nunique()) if "dataset_file" in group.columns else None
        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary = summary.sort_values(
        [
            "filter_name",
            "cost_per_trade",
            regime_col,
        ],
        ascending=True,
    ).reset_index(drop=True)

    summary["regime_summary_rank"] = summary.index + 1

    return summary


def build_gap_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["filter_name", "cost_per_trade"]

    records = []

    for keys, group in df.groupby(group_cols, dropna=False):
        group = group.sort_values("end_time_london").reset_index(drop=True)

        gaps = group["end_time_london"].diff().dt.total_seconds() / 60.0
        gaps = gaps.dropna()

        record = dict(zip(group_cols, keys))
        record.update(
            {
                "trade_count": len(group),
                "unique_dates": int(group["london_date"].nunique()),
                "unique_hours": int(group["london_hour"].nunique()),
                "unique_weekdays": int(group["weekday"].nunique()),
                "first_signal_time": str(group["end_time_london"].min()),
                "last_signal_time": str(group["end_time_london"].max()),
                "avg_gap_minutes": float(gaps.mean()) if not gaps.empty else None,
                "median_gap_minutes": float(gaps.median()) if not gaps.empty else None,
                "min_gap_minutes": float(gaps.min()) if not gaps.empty else None,
                "max_gap_minutes": float(gaps.max()) if not gaps.empty else None,
                "same_timestamp_duplicates": int(group["end_time_london"].duplicated(keep=False).sum()),
                "duplicate_trade_rows": int(group["duplicate_trade_key"].sum()) if "duplicate_trade_key" in group.columns else None,
                "return_direction_mismatch_rows": int((~group["return_direction_check"]).sum()) if "return_direction_check" in group.columns else None,
            }
        )

        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary = summary.sort_values(
        ["filter_name", "cost_per_trade"],
        ascending=True,
    ).reset_index(drop=True)

    summary["gap_summary_rank"] = summary.index + 1

    return summary


def build_filter_health_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["filter_name", "cost_per_trade"]

    records = []

    for keys, group in df.groupby(group_cols, dropna=False):
        record = dict(zip(group_cols, keys))
        base_summary = summarise_group(group)

        record.update(base_summary)
        record.update(
            {
                "unique_dates": int(group["london_date"].nunique()),
                "unique_hours": int(group["london_hour"].nunique()),
                "unique_weekdays": int(group["weekday"].nunique()),
                "unique_candidates": int(group["dataset_file"].nunique()) if "dataset_file" in group.columns else None,
                "duplicate_trade_rows": int(group["duplicate_trade_key"].sum()),
                "return_direction_mismatch_rows": int((~group["return_direction_check"]).sum()),
                "max_trades_single_date": int(group.groupby("london_date").size().max()),
                "max_trades_single_hour": int(group.groupby("london_hour").size().max()),
            }
        )

        record["date_concentration_ratio"] = (
            record["max_trades_single_date"] / record["trade_count"]
            if record["trade_count"] else None
        )

        record["hour_concentration_ratio"] = (
            record["max_trades_single_hour"] / record["trade_count"]
            if record["trade_count"] else None
        )

        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    def health_label(row: pd.Series) -> str:
        if row["duplicate_trade_rows"] > 0 or row["return_direction_mismatch_rows"] > 0:
            return "audit_problem"

        if row["trade_count"] < 30:
            return "low_sample"

        if row["unique_dates"] <= 2:
            return "date_concentrated"

        if row["date_concentration_ratio"] >= 0.70:
            return "highly_date_concentrated"

        if row["hour_concentration_ratio"] >= 0.70:
            return "highly_hour_concentrated"

        return "forensic_pass"

    summary["forensic_health_label"] = summary.apply(health_label, axis=1)

    label_rank = {
        "forensic_pass": 1,
        "date_concentrated": 2,
        "highly_date_concentrated": 3,
        "highly_hour_concentrated": 4,
        "low_sample": 5,
        "audit_problem": 6,
    }

    summary["label_rank"] = summary["forensic_health_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "trade_count",
            "win_rate",
            "profit_factor",
        ],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["filter_health_rank"] = summary.index + 1

    return summary


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 44 - SIGNAL FORENSICS")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    filter_trades_path = (
        analysis_dir
        / "signal_filter_optimizer"
        / "microstructure_signal_filter_optimizer_trades_latest.csv"
    )

    report_dir = analysis_dir / "signal_forensics"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Filter trades: {filter_trades_path}")
    print(f"Report dir:    {report_dir}")
    print(f"Focus filters: {FOCUS_FILTERS}")
    print("-" * 90)

    if not filter_trades_path.exists():
        raise FileNotFoundError(
            f"Missing filter optimizer trades file: {filter_trades_path}. Run script 43 first."
        )

    raw_trades_df = pd.read_csv(filter_trades_path)
    forensic_trades_df = prepare_forensics_trades(raw_trades_df)

    if forensic_trades_df.empty:
        raise RuntimeError("No forensic trades available.")

    date_summary_df = build_date_summary(forensic_trades_df)
    hour_summary_df = build_hour_summary(forensic_trades_df)
    regime_summary_df = build_regime_summary(forensic_trades_df)
    gap_summary_df = build_gap_summary(forensic_trades_df)
    health_summary_df = build_filter_health_summary(forensic_trades_df)

    trades_csv = report_dir / "microstructure_signal_forensics_trades_latest.csv"
    date_csv = report_dir / "microstructure_signal_forensics_date_summary_latest.csv"
    hour_csv = report_dir / "microstructure_signal_forensics_hour_summary_latest.csv"
    regime_csv = report_dir / "microstructure_signal_forensics_regime_summary_latest.csv"
    gap_csv = report_dir / "microstructure_signal_forensics_gap_summary_latest.csv"
    health_csv = report_dir / "microstructure_signal_forensics_health_summary_latest.csv"
    json_path = report_dir / "microstructure_signal_forensics_latest.json"
    txt_path = report_dir / "microstructure_signal_forensics_latest.txt"

    forensic_trades_df.to_csv(trades_csv, index=False)
    date_summary_df.to_csv(date_csv, index=False)
    hour_summary_df.to_csv(hour_csv, index=False)
    regime_summary_df.to_csv(regime_csv, index=False)
    gap_summary_df.to_csv(gap_csv, index=False)
    health_summary_df.to_csv(health_csv, index=False)

    health_counts = health_summary_df["forensic_health_label"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_rows": len(raw_trades_df),
        "forensic_rows": len(forensic_trades_df),
        "date_summary_rows": len(date_summary_df),
        "hour_summary_rows": len(hour_summary_df),
        "regime_summary_rows": len(regime_summary_df),
        "gap_summary_rows": len(gap_summary_df),
        "health_summary_rows": len(health_summary_df),
        "health_counts": health_counts,
        "focus_filters": FOCUS_FILTERS,
        "focus_cost_levels": FOCUS_COST_LEVELS,
        "top_health_summary": health_summary_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    health_cols = [
        "filter_health_rank",
        "filter_name",
        "cost_per_trade",
        "trade_count",
        "unique_dates",
        "unique_hours",
        "unique_weekdays",
        "unique_candidates",
        "win_rate",
        "avg_return",
        "total_return",
        "profit_factor",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "max_trades_single_date",
        "date_concentration_ratio",
        "max_trades_single_hour",
        "hour_concentration_ratio",
        "duplicate_trade_rows",
        "return_direction_mismatch_rows",
        "forensic_health_label",
    ]

    date_cols = [
        "date_summary_rank",
        "filter_name",
        "cost_per_trade",
        "london_date",
        "weekday",
        "trade_count",
        "unique_hours",
        "unique_candidates",
        "win_rate",
        "avg_return",
        "total_return",
        "profit_factor",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
    ]

    hour_cols = [
        "hour_summary_rank",
        "filter_name",
        "cost_per_trade",
        "london_hour",
        "trade_count",
        "unique_dates",
        "unique_weekdays",
        "unique_candidates",
        "win_rate",
        "avg_return",
        "total_return",
        "profit_factor",
        "max_drawdown",
    ]

    regime_cols = [
        "regime_summary_rank",
        "filter_name",
        "cost_per_trade",
        "liquidity_regime",
        "trade_count",
        "unique_dates",
        "unique_hours",
        "unique_weekdays",
        "unique_candidates",
        "win_rate",
        "avg_return",
        "total_return",
        "profit_factor",
        "max_drawdown",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL FORENSICS")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Raw rows:             {len(raw_trades_df):,}")
    lines.append(f"Forensic rows:        {len(forensic_trades_df):,}")
    lines.append(f"Date summary rows:    {len(date_summary_df):,}")
    lines.append(f"Hour summary rows:    {len(hour_summary_df):,}")
    lines.append(f"Regime summary rows:  {len(regime_summary_df):,}")
    lines.append(f"Gap summary rows:     {len(gap_summary_df):,}")
    lines.append(f"Health summary rows:  {len(health_summary_df):,}")
    lines.append("")
    lines.append(f"Health labels: {health_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("FILTER HEALTH SUMMARY")
    lines.append("-" * 90)
    lines.append(health_summary_df[health_cols].to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("DATE CONCENTRATION SUMMARY")
    lines.append("-" * 90)
    lines.append(date_summary_df[date_cols].head(80).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("HOUR CONCENTRATION SUMMARY")
    lines.append("-" * 90)
    lines.append(hour_summary_df[hour_cols].head(80).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("REGIME SUMMARY")
    lines.append("-" * 90)

    if regime_summary_df.empty:
        lines.append("No liquidity regime column available.")
    else:
        lines.append(regime_summary_df[regime_cols].head(80).to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal forensics complete.")
    print(f"Raw rows:            {len(raw_trades_df):,}")
    print(f"Forensic rows:       {len(forensic_trades_df):,}")
    print(f"Date summary rows:   {len(date_summary_df):,}")
    print(f"Hour summary rows:   {len(hour_summary_df):,}")
    print(f"Regime summary rows: {len(regime_summary_df):,}")
    print(f"Gap summary rows:    {len(gap_summary_df):,}")
    print(f"Health rows:         {len(health_summary_df):,}")
    print(f"Health labels:       {health_counts}")
    print(f"Trades CSV:          {trades_csv}")
    print(f"Date CSV:            {date_csv}")
    print(f"Hour CSV:            {hour_csv}")
    print(f"Regime CSV:          {regime_csv}")
    print(f"Gap CSV:             {gap_csv}")
    print(f"Health CSV:          {health_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()