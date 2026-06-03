"""
BACQE MICROSTRUCTURE 41 - TIME OF DAY ANALYSIS

Purpose:
    Analyse whether cost-adjusted microstructure signal performance is concentrated
    around specific trading sessions such as London open, New York open, or overlap.

Inputs:
    cost_stress_test/
        microstructure_cost_stress_test_trades_latest.csv

Outputs:
    time_of_day_analysis/
        microstructure_time_of_day_trades_latest.csv
        microstructure_time_of_day_summary_latest.csv
        microstructure_time_of_day_session_summary_latest.csv
        microstructure_time_of_day_latest.json
        microstructure_time_of_day_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SELECTED_COST_LEVELS = [0.0, 0.00002, 0.00005, 0.00010, 0.00015]

SELECTED_LABELS = {
    "threshold_signal_strong",
    "threshold_signal_research",
}

LONDON_TZ = "Europe/London"


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
    """
    London-time trading session buckets.

    These are deliberately practical research buckets, not exact exchange sessions.
    """
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


def assign_session_priority(session: str) -> int:
    priority = {
        "london_open": 1,
        "new_york_open": 2,
        "london_new_york_overlap": 3,
        "pre_new_york": 4,
        "london_mid_morning": 5,
        "pre_london": 6,
        "new_york_afternoon": 7,
        "asia_late_overnight": 8,
        "late_us_close": 9,
    }
    return priority.get(session, 99)


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


def enrich_time_of_day(cost_trades_df: pd.DataFrame) -> pd.DataFrame:
    df = cost_trades_df.copy()

    df = df[
        (df["status"] == "ok")
        & (df["cost_per_trade"].isin(SELECTED_COST_LEVELS))
        & (df["threshold_signal_label"].isin(SELECTED_LABELS))
    ].copy()

    if df.empty:
        return df

    df["end_time_utc"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["end_time_utc"]).copy()

    df["end_time_london"] = df["end_time_utc"].dt.tz_convert(LONDON_TZ)
    df["london_date"] = df["end_time_london"].dt.date.astype(str)
    df["london_hour"] = df["end_time_london"].dt.hour
    df["london_minute"] = df["end_time_london"].dt.minute
    df["london_weekday"] = df["end_time_london"].dt.day_name()

    df["session"] = df["london_hour"].apply(assign_session)
    df["session_priority"] = df["session"].apply(assign_session_priority)

    df["gross_signed_return"] = pd.to_numeric(df["gross_signed_return"], errors="coerce")
    df["net_signed_return"] = pd.to_numeric(df["net_signed_return"], errors="coerce")

    df["net_win_flag"] = df["net_signed_return"] > 0

    return df


def build_time_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "session",
    ]

    records = []

    for keys, group in trades_df.groupby(group_cols, dropna=False):
        group = group.sort_values("end_time_utc").reset_index(drop=True)

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

        max_win_streak, max_loss_streak = calculate_streaks(group["net_win_flag"])

        cumulative = group["net_signed_return"].fillna(0).cumsum()
        running_max = cumulative.cummax()
        drawdown = cumulative - running_max

        record = dict(zip(group_cols, keys))
        record.update(
            {
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "trade_count": len(group),
                "long_trades": int((group["signal_direction"] == 1).sum()),
                "short_trades": int((group["signal_direction"] == -1).sum()),
                "net_win_rate": float(group["net_win_flag"].mean()),
                "net_avg_return": float(avg_return),
                "net_median_return": float(group["net_signed_return"].median()),
                "net_total_return": float(group["net_signed_return"].sum()),
                "net_std_return": float(std_return),
                "net_profit_factor": profit_factor,
                "net_sharpe_like": sharpe_like,
                "session_max_drawdown": float(drawdown.min()),
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "first_signal_time": str(group["end_time_london"].min()),
                "last_signal_time": str(group["end_time_london"].max()),
                "unique_days": int(group["london_date"].nunique()),
                "session_priority": assign_session_priority(keys[-1]),
            }
        )

        records.append(record)

    summary = pd.DataFrame(records)

    if summary.empty:
        return summary

    summary["time_of_day_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
        + summary["session_max_drawdown"].fillna(0) * 5000
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["trade_count"] < 20:
            return "low_sample"

        if (
            row["net_win_rate"] >= 0.60
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.30
        ):
            return "time_window_strong"

        if (
            row["net_win_rate"] >= 0.55
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.10
        ):
            return "time_window_research"

        if row["net_win_rate"] >= 0.52 and row["net_avg_return"] > 0:
            return "time_window_weak"

        return "time_window_failed"

    summary["time_window_label"] = summary.apply(label, axis=1)

    label_rank = {
        "time_window_strong": 1,
        "time_window_research": 2,
        "time_window_weak": 3,
        "time_window_failed": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["time_window_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "time_of_day_score",
            "net_win_rate",
            "net_profit_factor",
            "trade_count",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["time_of_day_rank"] = summary.index + 1

    return summary


def build_session_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["session", "cost_per_trade"]

    session_summary = (
        trades_df
        .groupby(group_cols, dropna=False)
        .agg(
            trade_count=("net_signed_return", "count"),
            symbols=("symbol", "nunique"),
            candidates=("dataset_file", "nunique"),
            net_win_rate=("net_win_flag", "mean"),
            net_avg_return=("net_signed_return", "mean"),
            net_median_return=("net_signed_return", "median"),
            net_total_return=("net_signed_return", "sum"),
            net_std_return=("net_signed_return", "std"),
            unique_days=("london_date", "nunique"),
        )
        .reset_index()
    )

    gross_profit = (
        trades_df[trades_df["net_signed_return"] > 0]
        .groupby(group_cols, dropna=False)["net_signed_return"]
        .sum()
        .rename("gross_profit")
    )

    gross_loss = (
        trades_df[trades_df["net_signed_return"] < 0]
        .groupby(group_cols, dropna=False)["net_signed_return"]
        .sum()
        .rename("gross_loss")
    )

    session_summary = session_summary.merge(gross_profit, on=group_cols, how="left")
    session_summary = session_summary.merge(gross_loss, on=group_cols, how="left")

    session_summary["gross_profit"] = session_summary["gross_profit"].fillna(0)
    session_summary["gross_loss"] = session_summary["gross_loss"].fillna(0)

    session_summary["net_profit_factor"] = np.where(
        session_summary["gross_loss"] < 0,
        session_summary["gross_profit"] / session_summary["gross_loss"].abs(),
        np.nan,
    )

    session_summary["net_sharpe_like"] = np.where(
        session_summary["net_std_return"] > 0,
        session_summary["net_avg_return"] / session_summary["net_std_return"],
        np.nan,
    )

    session_summary["session_priority"] = session_summary["session"].apply(assign_session_priority)

    session_summary["session_score"] = (
        (session_summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + session_summary["net_profit_factor"].fillna(0).clip(0, 5) * 10
        + session_summary["net_sharpe_like"].fillna(0) * 25
        + np.log1p(session_summary["trade_count"].fillna(0)) * 3
        + session_summary["net_total_return"].fillna(0) * 10000
    ).clip(0, 100).round(2)

    session_summary = session_summary.sort_values(
        ["session_score", "net_win_rate", "net_profit_factor", "trade_count"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    session_summary["session_rank"] = session_summary.index + 1

    return session_summary


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 41 - TIME OF DAY ANALYSIS")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    cost_trades_path = (
        analysis_dir
        / "cost_stress_test"
        / "microstructure_cost_stress_test_trades_latest.csv"
    )

    report_dir = analysis_dir / "time_of_day_analysis"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cost trades: {cost_trades_path}")
    print(f"Report dir:  {report_dir}")
    print(f"Timezone:    {LONDON_TZ}")
    print("-" * 90)

    if not cost_trades_path.exists():
        raise FileNotFoundError(
            f"Missing cost trades file: {cost_trades_path}. Run script 39 first."
        )

    cost_trades_df = pd.read_csv(cost_trades_path)

    tod_trades_df = enrich_time_of_day(cost_trades_df)

    if tod_trades_df.empty:
        raise RuntimeError("No eligible time-of-day trades found.")

    tod_summary_df = build_time_summary(tod_trades_df)
    session_summary_df = build_session_summary(tod_trades_df)

    trades_csv = report_dir / "microstructure_time_of_day_trades_latest.csv"
    summary_csv = report_dir / "microstructure_time_of_day_summary_latest.csv"
    session_csv = report_dir / "microstructure_time_of_day_session_summary_latest.csv"
    json_path = report_dir / "microstructure_time_of_day_latest.json"
    txt_path = report_dir / "microstructure_time_of_day_latest.txt"

    tod_trades_df.to_csv(trades_csv, index=False)
    tod_summary_df.to_csv(summary_csv, index=False)
    session_summary_df.to_csv(session_csv, index=False)

    label_counts = tod_summary_df["time_window_label"].value_counts(dropna=False).to_dict()
    session_counts = tod_trades_df["session"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(cost_trades_df),
        "eligible_rows": len(tod_trades_df),
        "summary_rows": len(tod_summary_df),
        "session_rows": len(session_summary_df),
        "timezone": LONDON_TZ,
        "selected_cost_levels": SELECTED_COST_LEVELS,
        "label_counts": label_counts,
        "session_counts": session_counts,
        "top_time_windows": tod_summary_df.head(50).to_dict(orient="records"),
        "session_summary": session_summary_df.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    summary_cols = [
        "time_of_day_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "session",
        "trade_count",
        "unique_days",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "session_max_drawdown",
        "time_of_day_score",
        "time_window_label",
    ]

    session_cols = [
        "session_rank",
        "session",
        "cost_per_trade",
        "trade_count",
        "symbols",
        "candidates",
        "unique_days",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "session_score",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE TIME OF DAY ANALYSIS")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Input rows:      {len(cost_trades_df):,}")
    lines.append(f"Eligible rows:   {len(tod_trades_df):,}")
    lines.append(f"Summary rows:    {len(tod_summary_df):,}")
    lines.append(f"Session rows:    {len(session_summary_df):,}")
    lines.append(f"Timezone:        {LONDON_TZ}")
    lines.append("")
    lines.append(f"Time-window labels: {label_counts}")
    lines.append(f"Session counts:     {session_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP TIME OF DAY WINDOWS")
    lines.append("-" * 90)
    lines.append(tod_summary_df[summary_cols].head(60).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("SESSION SUMMARY")
    lines.append("-" * 90)
    lines.append(session_summary_df[session_cols].to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Time-of-day analysis complete.")
    print(f"Input rows:    {len(cost_trades_df):,}")
    print(f"Eligible rows: {len(tod_trades_df):,}")
    print(f"Summary rows:  {len(tod_summary_df):,}")
    print(f"Session rows:  {len(session_summary_df):,}")
    print(f"Labels:        {label_counts}")
    print(f"Sessions:      {session_counts}")
    print(f"Trades CSV:    {trades_csv}")
    print(f"Summary CSV:   {summary_csv}")
    print(f"Session CSV:   {session_csv}")
    print(f"JSON output:   {json_path}")
    print(f"TXT output:    {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()