"""
BACQE MICROSTRUCTURE 46 - LIVE CANDIDATE MONITOR

Purpose:
    Track Candidate Registry signals as forward / out-of-sample evidence.

Important:
    This script does NOT place trades.
    It monitors candidate performance as new data appears.

Inputs:
    candidate_registry/
        microstructure_candidate_registry_latest.csv

    signal_filter_optimizer/
        microstructure_signal_filter_optimizer_trades_latest.csv

Outputs:
    live_candidate_monitor/
        microstructure_live_candidate_monitor_latest.csv
        microstructure_live_candidate_monitor_ledger.csv
        microstructure_live_candidate_monitor_latest.json
        microstructure_live_candidate_monitor_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SELECTED_REGISTRY_LABELS = {
    "primary_research_candidate",
    "secondary_research_candidate",
    "watchlist_candidate",
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


def summarise_returns(group: pd.DataFrame) -> dict:
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


def prepare_filter_trades(filter_trades_df: pd.DataFrame) -> pd.DataFrame:
    df = filter_trades_df.copy()

    df["end_time_utc"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["end_time_utc"]).copy()

    df["end_time_london"] = df["end_time_utc"].dt.tz_convert(LONDON_TZ)
    df["london_date"] = df["end_time_london"].dt.date.astype(str)
    df["weekday"] = df["end_time_london"].dt.day_name()
    df["london_hour"] = df["end_time_london"].dt.hour

    df["net_signed_return"] = pd.to_numeric(df["net_signed_return"], errors="coerce")
    df["cost_per_trade"] = pd.to_numeric(df["cost_per_trade"], errors="coerce")
    df["net_win_flag"] = df["net_signed_return"] > 0

    return df


def build_candidate_monitor(registry_df: pd.DataFrame, trades_df: pd.DataFrame) -> pd.DataFrame:
    selected_registry = registry_df[
        registry_df["registry_label"].isin(SELECTED_REGISTRY_LABELS)
    ].copy()

    records = []

    for _, candidate in selected_registry.iterrows():
        filter_name = candidate["filter_name"]
        cost_per_trade = candidate["cost_per_trade"]

        candidate_trades = trades_df[
            (trades_df["filter_name"] == filter_name)
            & (np.isclose(trades_df["cost_per_trade"], cost_per_trade))
        ].copy()

        record = {
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "candidate_key": candidate.get("candidate_key"),
            "registry_rank": candidate.get("registry_rank"),
            "filter_name": filter_name,
            "cost_per_trade": cost_per_trade,
            "registry_label": candidate.get("registry_label"),
            "registry_score": candidate.get("registry_score"),
            "forensic_health_label": candidate.get("forensic_health_label"),
            "registry_trade_count": candidate.get("trade_count"),
            "registry_win_rate": candidate.get("net_win_rate"),
            "registry_profit_factor": candidate.get("net_profit_factor"),
            "registry_total_return": candidate.get("net_total_return"),
            "registry_max_drawdown": candidate.get("max_drawdown"),
            "monitor_status": "unknown",
            "latest_trade_time": None,
            "latest_trade_date": None,
            "monitored_trade_count": 0,
            "monitored_unique_dates": 0,
            "monitored_unique_hours": 0,
            "monitored_win_rate": None,
            "monitored_avg_return": None,
            "monitored_total_return": None,
            "monitored_profit_factor": None,
            "monitored_sharpe_like": None,
            "monitored_max_drawdown": None,
            "monitored_max_win_streak": None,
            "monitored_max_loss_streak": None,
            "performance_delta_win_rate": None,
            "performance_delta_profit_factor": None,
            "performance_delta_total_return": None,
            "oos_candidate_label": "not_evaluated",
        }

        if candidate_trades.empty:
            record["monitor_status"] = "no_matching_trades"
            records.append(record)
            continue

        summary = summarise_returns(candidate_trades)

        record.update(
            {
                "monitor_status": "ok",
                "latest_trade_time": str(candidate_trades["end_time_london"].max()),
                "latest_trade_date": str(candidate_trades["london_date"].max()),
                "monitored_trade_count": summary["trade_count"],
                "monitored_unique_dates": int(candidate_trades["london_date"].nunique()),
                "monitored_unique_hours": int(candidate_trades["london_hour"].nunique()),
                "monitored_win_rate": summary["win_rate"],
                "monitored_avg_return": summary["avg_return"],
                "monitored_total_return": summary["total_return"],
                "monitored_profit_factor": summary["profit_factor"],
                "monitored_sharpe_like": summary["sharpe_like"],
                "monitored_max_drawdown": summary["max_drawdown"],
                "monitored_max_win_streak": summary["max_win_streak"],
                "monitored_max_loss_streak": summary["max_loss_streak"],
            }
        )

        if pd.notna(record["registry_win_rate"]) and pd.notna(record["monitored_win_rate"]):
            record["performance_delta_win_rate"] = (
                record["monitored_win_rate"] - record["registry_win_rate"]
            )

        if pd.notna(record["registry_profit_factor"]) and pd.notna(record["monitored_profit_factor"]):
            record["performance_delta_profit_factor"] = (
                record["monitored_profit_factor"] - record["registry_profit_factor"]
            )

        if pd.notna(record["registry_total_return"]) and pd.notna(record["monitored_total_return"]):
            record["performance_delta_total_return"] = (
                record["monitored_total_return"] - record["registry_total_return"]
            )

        record["oos_candidate_label"] = classify_oos_candidate(record)

        records.append(record)

    monitor_df = pd.DataFrame(records)

    label_rank = {
        "oos_tracking_ok": 1,
        "oos_watch_small_sample": 2,
        "oos_degraded": 3,
        "oos_failed": 4,
        "not_evaluated": 5,
    }

    monitor_df["oos_label_rank"] = monitor_df["oos_candidate_label"].map(label_rank).fillna(99)

    monitor_df = monitor_df.sort_values(
        [
            "oos_label_rank",
            "registry_rank",
            "monitored_trade_count",
            "monitored_win_rate",
        ],
        ascending=[True, True, False, False],
        na_position="last",
    ).reset_index(drop=True)

    monitor_df["monitor_rank"] = monitor_df.index + 1

    return monitor_df


def classify_oos_candidate(record: dict) -> str:
    trade_count = record.get("monitored_trade_count", 0)
    win_rate = record.get("monitored_win_rate")
    profit_factor = record.get("monitored_profit_factor")
    total_return = record.get("monitored_total_return")
    registry_win_rate = record.get("registry_win_rate")
    registry_profit_factor = record.get("registry_profit_factor")

    if record.get("monitor_status") != "ok":
        return "not_evaluated"

    if trade_count < 30:
        return "oos_watch_small_sample"

    if pd.isna(win_rate) or pd.isna(profit_factor):
        return "not_evaluated"

    if total_return is not None and total_return <= 0:
        return "oos_failed"

    win_rate_drop = 0
    if registry_win_rate is not None and pd.notna(registry_win_rate):
        win_rate_drop = registry_win_rate - win_rate

    pf_drop = 0
    if registry_profit_factor is not None and pd.notna(registry_profit_factor):
        pf_drop = registry_profit_factor - profit_factor

    if win_rate >= 0.55 and profit_factor >= 1.2 and win_rate_drop <= 0.15:
        return "oos_tracking_ok"

    if win_rate >= 0.50 and profit_factor >= 1.0:
        return "oos_degraded"

    return "oos_failed"


def update_ledger(monitor_df: pd.DataFrame, ledger_path: Path) -> pd.DataFrame:
    monitor_snapshot = monitor_df.copy()
    monitor_snapshot["ledger_run_id"] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if ledger_path.exists():
        existing = pd.read_csv(ledger_path)
        ledger_df = pd.concat([existing, monitor_snapshot], ignore_index=True, sort=False)
    else:
        ledger_df = monitor_snapshot

    ledger_df.to_csv(ledger_path, index=False)

    return ledger_df


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 46 - LIVE CANDIDATE MONITOR")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    registry_path = (
        analysis_dir
        / "candidate_registry"
        / "microstructure_candidate_registry_latest.csv"
    )

    filter_trades_path = (
        analysis_dir
        / "signal_filter_optimizer"
        / "microstructure_signal_filter_optimizer_trades_latest.csv"
    )

    report_dir = analysis_dir / "live_candidate_monitor"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Candidate registry: {registry_path}")
    print(f"Filter trades:      {filter_trades_path}")
    print(f"Report dir:         {report_dir}")
    print("-" * 90)

    if not registry_path.exists():
        raise FileNotFoundError(
            f"Missing candidate registry: {registry_path}. Run script 45 first."
        )

    if not filter_trades_path.exists():
        raise FileNotFoundError(
            f"Missing filter trades: {filter_trades_path}. Run script 43 first."
        )

    registry_df = pd.read_csv(registry_path)
    raw_trades_df = pd.read_csv(filter_trades_path)
    trades_df = prepare_filter_trades(raw_trades_df)

    monitor_df = build_candidate_monitor(registry_df, trades_df)

    latest_csv = report_dir / "microstructure_live_candidate_monitor_latest.csv"
    ledger_csv = report_dir / "microstructure_live_candidate_monitor_ledger.csv"
    json_path = report_dir / "microstructure_live_candidate_monitor_latest.json"
    txt_path = report_dir / "microstructure_live_candidate_monitor_latest.txt"

    monitor_df.to_csv(latest_csv, index=False)
    ledger_df = update_ledger(monitor_df, ledger_csv)

    label_counts = monitor_df["oos_candidate_label"].value_counts(dropna=False).to_dict()
    status_counts = monitor_df["monitor_status"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "registry_rows": len(registry_df),
        "selected_registry_rows": int(registry_df["registry_label"].isin(SELECTED_REGISTRY_LABELS).sum()),
        "raw_trade_rows": len(raw_trades_df),
        "monitor_rows": len(monitor_df),
        "ledger_rows": len(ledger_df),
        "label_counts": label_counts,
        "status_counts": status_counts,
        "top_monitor": monitor_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    display_cols = [
        "monitor_rank",
        "registry_rank",
        "filter_name",
        "cost_per_trade",
        "registry_label",
        "forensic_health_label",
        "registry_trade_count",
        "registry_win_rate",
        "registry_profit_factor",
        "monitored_trade_count",
        "monitored_unique_dates",
        "monitored_win_rate",
        "monitored_total_return",
        "monitored_profit_factor",
        "monitored_max_drawdown",
        "performance_delta_win_rate",
        "performance_delta_profit_factor",
        "latest_trade_date",
        "oos_candidate_label",
    ]

    available_display_cols = [c for c in display_cols if c in monitor_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE LIVE CANDIDATE MONITOR")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Registry rows:          {len(registry_df):,}")
    lines.append(f"Selected registry rows: {int(registry_df['registry_label'].isin(SELECTED_REGISTRY_LABELS).sum()):,}")
    lines.append(f"Raw trade rows:         {len(raw_trades_df):,}")
    lines.append(f"Monitor rows:           {len(monitor_df):,}")
    lines.append(f"Ledger rows:            {len(ledger_df):,}")
    lines.append("")
    lines.append(f"Monitor statuses: {status_counts}")
    lines.append(f"OOS labels:       {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("LIVE CANDIDATE MONITOR")
    lines.append("-" * 90)
    lines.append(monitor_df[available_display_cols].to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Live candidate monitor complete.")
    print(f"Registry rows:  {len(registry_df):,}")
    print(f"Monitor rows:   {len(monitor_df):,}")
    print(f"Ledger rows:    {len(ledger_df):,}")
    print(f"Statuses:       {status_counts}")
    print(f"OOS labels:     {label_counts}")
    print(f"Latest CSV:     {latest_csv}")
    print(f"Ledger CSV:     {ledger_csv}")
    print(f"JSON output:    {json_path}")
    print(f"TXT output:     {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()