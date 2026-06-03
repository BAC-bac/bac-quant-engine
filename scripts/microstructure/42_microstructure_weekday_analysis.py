"""
BACQE MICROSTRUCTURE 42 - WEEKDAY ANALYSIS

Purpose:
    Analyse whether cost-adjusted microstructure signal performance is concentrated
    on specific weekdays.

Inputs:
    time_of_day_analysis/
        microstructure_time_of_day_trades_latest.csv

Outputs:
    weekday_analysis/
        microstructure_weekday_trades_latest.csv
        microstructure_weekday_summary_latest.csv
        microstructure_weekday_session_summary_latest.csv
        microstructure_weekday_latest.json
        microstructure_weekday_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

WEEKDAY_ORDER = {
    "Monday": 1,
    "Tuesday": 2,
    "Wednesday": 3,
    "Thursday": 4,
    "Friday": 5,
    "Saturday": 6,
    "Sunday": 7,
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


def prepare_weekday_trades(tod_trades_df: pd.DataFrame) -> pd.DataFrame:
    df = tod_trades_df.copy()

    if "end_time_london" in df.columns:
        df["end_time_london"] = pd.to_datetime(df["end_time_london"], utc=True, errors="coerce")
    elif "end_time_utc" in df.columns:
        df["end_time_london"] = pd.to_datetime(df["end_time_utc"], utc=True, errors="coerce").dt.tz_convert("Europe/London")
    else:
        df["end_time_london"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce").dt.tz_convert("Europe/London")

    df = df.dropna(subset=["end_time_london"]).copy()

    df["weekday"] = df["end_time_london"].dt.day_name()
    df["weekday_number"] = df["weekday"].map(WEEKDAY_ORDER).fillna(99).astype(int)

    df["net_signed_return"] = pd.to_numeric(df["net_signed_return"], errors="coerce")
    df["gross_signed_return"] = pd.to_numeric(df["gross_signed_return"], errors="coerce")
    df["net_win_flag"] = df["net_signed_return"] > 0

    df = df[df["weekday"].isin(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])].copy()

    return df


def build_weekday_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "session",
        "weekday",
    ]

    records = []

    for keys, group in trades_df.groupby(group_cols, dropna=False):
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
                "weekday_number": WEEKDAY_ORDER.get(keys[-1], 99),
                "trade_count": len(group),
                "unique_dates": int(group["london_date"].nunique()) if "london_date" in group.columns else None,
                "long_trades": int((group["signal_direction"] == 1).sum()),
                "short_trades": int((group["signal_direction"] == -1).sum()),
                "net_win_rate": float(group["net_win_flag"].mean()),
                "net_avg_return": float(avg_return),
                "net_median_return": float(group["net_signed_return"].median()),
                "net_total_return": float(group["net_signed_return"].sum()),
                "net_std_return": float(std_return),
                "net_profit_factor": profit_factor,
                "net_sharpe_like": sharpe_like,
                "weekday_max_drawdown": float(drawdown.min()),
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

    summary["weekday_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
        + summary["weekday_max_drawdown"].fillna(0) * 5000
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["trade_count"] < 10:
            return "low_sample"

        if (
            row["net_win_rate"] >= 0.65
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.50
        ):
            return "weekday_strong"

        if (
            row["net_win_rate"] >= 0.58
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.20
        ):
            return "weekday_research"

        if row["net_win_rate"] >= 0.53 and row["net_avg_return"] > 0:
            return "weekday_weak"

        return "weekday_failed"

    summary["weekday_label"] = summary.apply(label, axis=1)

    label_rank = {
        "weekday_strong": 1,
        "weekday_research": 2,
        "weekday_weak": 3,
        "weekday_failed": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["weekday_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "weekday_score",
            "net_win_rate",
            "net_profit_factor",
            "trade_count",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["weekday_rank"] = summary.index + 1

    return summary


def build_weekday_session_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["weekday", "session", "cost_per_trade"]

    summary = (
        trades_df
        .groupby(group_cols, dropna=False)
        .agg(
            trade_count=("net_signed_return", "count"),
            symbols=("symbol", "nunique"),
            candidates=("dataset_file", "nunique"),
            unique_dates=("london_date", "nunique"),
            net_win_rate=("net_win_flag", "mean"),
            net_avg_return=("net_signed_return", "mean"),
            net_median_return=("net_signed_return", "median"),
            net_total_return=("net_signed_return", "sum"),
            net_std_return=("net_signed_return", "std"),
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

    summary = summary.merge(gross_profit, on=group_cols, how="left")
    summary = summary.merge(gross_loss, on=group_cols, how="left")

    summary["gross_profit"] = summary["gross_profit"].fillna(0)
    summary["gross_loss"] = summary["gross_loss"].fillna(0)

    summary["net_profit_factor"] = np.where(
        summary["gross_loss"] < 0,
        summary["gross_profit"] / summary["gross_loss"].abs(),
        np.nan,
    )

    summary["net_sharpe_like"] = np.where(
        summary["net_std_return"] > 0,
        summary["net_avg_return"] / summary["net_std_return"],
        np.nan,
    )

    summary["weekday_number"] = summary["weekday"].map(WEEKDAY_ORDER).fillna(99).astype(int)

    summary["weekday_session_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
    ).clip(0, 100).round(2)

    summary = summary.sort_values(
        [
            "weekday_session_score",
            "net_win_rate",
            "net_profit_factor",
            "trade_count",
        ],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["weekday_session_rank"] = summary.index + 1

    return summary


def build_weekday_overall_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["weekday", "cost_per_trade"]

    summary = (
        trades_df
        .groupby(group_cols, dropna=False)
        .agg(
            trade_count=("net_signed_return", "count"),
            symbols=("symbol", "nunique"),
            sessions=("session", "nunique"),
            candidates=("dataset_file", "nunique"),
            unique_dates=("london_date", "nunique"),
            net_win_rate=("net_win_flag", "mean"),
            net_avg_return=("net_signed_return", "mean"),
            net_median_return=("net_signed_return", "median"),
            net_total_return=("net_signed_return", "sum"),
            net_std_return=("net_signed_return", "std"),
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

    summary = summary.merge(gross_profit, on=group_cols, how="left")
    summary = summary.merge(gross_loss, on=group_cols, how="left")

    summary["gross_profit"] = summary["gross_profit"].fillna(0)
    summary["gross_loss"] = summary["gross_loss"].fillna(0)

    summary["net_profit_factor"] = np.where(
        summary["gross_loss"] < 0,
        summary["gross_profit"] / summary["gross_loss"].abs(),
        np.nan,
    )

    summary["net_sharpe_like"] = np.where(
        summary["net_std_return"] > 0,
        summary["net_avg_return"] / summary["net_std_return"],
        np.nan,
    )

    summary["weekday_number"] = summary["weekday"].map(WEEKDAY_ORDER).fillna(99).astype(int)

    summary["weekday_overall_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
    ).clip(0, 100).round(2)

    summary = summary.sort_values(
        [
            "weekday_overall_score",
            "net_win_rate",
            "net_profit_factor",
            "trade_count",
        ],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["weekday_overall_rank"] = summary.index + 1

    return summary


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 42 - WEEKDAY ANALYSIS")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    time_trades_path = (
        analysis_dir
        / "time_of_day_analysis"
        / "microstructure_time_of_day_trades_latest.csv"
    )

    report_dir = analysis_dir / "weekday_analysis"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Time-of-day trades: {time_trades_path}")
    print(f"Report dir:         {report_dir}")
    print("-" * 90)

    if not time_trades_path.exists():
        raise FileNotFoundError(
            f"Missing time-of-day trades file: {time_trades_path}. Run script 41 first."
        )

    tod_trades_df = pd.read_csv(time_trades_path)

    weekday_trades_df = prepare_weekday_trades(tod_trades_df)

    if weekday_trades_df.empty:
        raise RuntimeError("No weekday trades available after filtering.")

    weekday_summary_df = build_weekday_summary(weekday_trades_df)
    weekday_session_df = build_weekday_session_summary(weekday_trades_df)
    weekday_overall_df = build_weekday_overall_summary(weekday_trades_df)

    trades_csv = report_dir / "microstructure_weekday_trades_latest.csv"
    summary_csv = report_dir / "microstructure_weekday_summary_latest.csv"
    session_csv = report_dir / "microstructure_weekday_session_summary_latest.csv"
    overall_csv = report_dir / "microstructure_weekday_overall_summary_latest.csv"
    json_path = report_dir / "microstructure_weekday_latest.json"
    txt_path = report_dir / "microstructure_weekday_latest.txt"

    weekday_trades_df.to_csv(trades_csv, index=False)
    weekday_summary_df.to_csv(summary_csv, index=False)
    weekday_session_df.to_csv(session_csv, index=False)
    weekday_overall_df.to_csv(overall_csv, index=False)

    label_counts = weekday_summary_df["weekday_label"].value_counts(dropna=False).to_dict()
    weekday_counts = weekday_trades_df["weekday"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(tod_trades_df),
        "weekday_rows": len(weekday_trades_df),
        "weekday_summary_rows": len(weekday_summary_df),
        "weekday_session_rows": len(weekday_session_df),
        "weekday_overall_rows": len(weekday_overall_df),
        "weekday_counts": weekday_counts,
        "label_counts": label_counts,
        "top_weekday_windows": weekday_summary_df.head(50).to_dict(orient="records"),
        "weekday_session_summary": weekday_session_df.head(50).to_dict(orient="records"),
        "weekday_overall_summary": weekday_overall_df.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    summary_cols = [
        "weekday_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "session",
        "weekday",
        "trade_count",
        "unique_dates",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "weekday_max_drawdown",
        "weekday_score",
        "weekday_label",
    ]

    weekday_session_cols = [
        "weekday_session_rank",
        "weekday",
        "session",
        "cost_per_trade",
        "trade_count",
        "symbols",
        "candidates",
        "unique_dates",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "weekday_session_score",
    ]

    weekday_overall_cols = [
        "weekday_overall_rank",
        "weekday",
        "cost_per_trade",
        "trade_count",
        "symbols",
        "sessions",
        "candidates",
        "unique_dates",
        "net_win_rate",
        "net_avg_return",
        "net_total_return",
        "net_profit_factor",
        "net_sharpe_like",
        "weekday_overall_score",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE WEEKDAY ANALYSIS")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Input rows:             {len(tod_trades_df):,}")
    lines.append(f"Weekday rows:           {len(weekday_trades_df):,}")
    lines.append(f"Weekday summary rows:   {len(weekday_summary_df):,}")
    lines.append(f"Weekday session rows:   {len(weekday_session_df):,}")
    lines.append(f"Weekday overall rows:   {len(weekday_overall_df):,}")
    lines.append("")
    lines.append(f"Weekday counts: {weekday_counts}")
    lines.append(f"Weekday labels: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP WEEKDAY WINDOWS")
    lines.append("-" * 90)
    lines.append(weekday_summary_df[summary_cols].head(60).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("WEEKDAY SESSION SUMMARY")
    lines.append("-" * 90)
    lines.append(weekday_session_df[weekday_session_cols].head(60).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("WEEKDAY OVERALL SUMMARY")
    lines.append("-" * 90)
    lines.append(weekday_overall_df[weekday_overall_cols].to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Weekday analysis complete.")
    print(f"Input rows:           {len(tod_trades_df):,}")
    print(f"Weekday rows:         {len(weekday_trades_df):,}")
    print(f"Weekday summary rows: {len(weekday_summary_df):,}")
    print(f"Session rows:         {len(weekday_session_df):,}")
    print(f"Overall rows:         {len(weekday_overall_df):,}")
    print(f"Weekday counts:       {weekday_counts}")
    print(f"Labels:               {label_counts}")
    print(f"Trades CSV:           {trades_csv}")
    print(f"Summary CSV:          {summary_csv}")
    print(f"Session CSV:          {session_csv}")
    print(f"Overall CSV:          {overall_csv}")
    print(f"JSON output:          {json_path}")
    print(f"TXT output:           {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()