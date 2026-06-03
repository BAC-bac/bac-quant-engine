"""
BACQE MICROSTRUCTURE 39 - COST STRESS TEST

Purpose:
    Apply simple per-trade friction costs to Script 38 replay trades.

Input:
    signal_replay/
        microstructure_signal_replay_trades_latest.csv

Outputs:
    cost_stress_test/
        microstructure_cost_stress_test_trades_latest.csv
        microstructure_cost_stress_test_summary_latest.csv
        microstructure_cost_stress_test_latest.json
        microstructure_cost_stress_test_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

COST_LEVELS = [
    0.00000,
    0.00002,
    0.00005,
    0.00010,
    0.00015,
    0.00020,
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


def apply_costs(trades_df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    ok_df = trades_df[trades_df["status"] == "ok"].copy()

    for cost in COST_LEVELS:
        work = ok_df.copy()

        work["cost_per_trade"] = cost
        work["gross_signed_return"] = pd.to_numeric(work["signed_return"], errors="coerce")
        work["net_signed_return"] = work["gross_signed_return"] - cost

        work["gross_win_flag"] = work["gross_signed_return"] > 0
        work["net_win_flag"] = work["net_signed_return"] > 0

        work["net_cumulative_return"] = (
            work.groupby(
                [
                    "symbol",
                    "bar_type",
                    "parameter",
                    "spread_feature",
                    "target",
                    "threshold_pair",
                ],
                dropna=False,
            )["net_signed_return"]
            .cumsum()
        )

        work["net_running_max"] = (
            work.groupby(
                [
                    "symbol",
                    "bar_type",
                    "parameter",
                    "spread_feature",
                    "target",
                    "threshold_pair",
                ],
                dropna=False,
            )["net_cumulative_return"]
            .cummax()
        )

        work["net_drawdown"] = work["net_cumulative_return"] - work["net_running_max"]

        frames.append(work)

    return pd.concat(frames, ignore_index=True, sort=False)


def build_cost_summary(cost_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "threshold_signal_label",
        "cost_per_trade",
        "dataset_file",
    ]

    records = []

    for keys, group in cost_df.groupby(group_cols, dropna=False):
        group = group.sort_values("trade_number").reset_index(drop=True)

        gross_profit = group.loc[group["gross_signed_return"] > 0, "gross_signed_return"].sum()
        gross_loss = group.loc[group["gross_signed_return"] < 0, "gross_signed_return"].sum()

        net_profit = group.loc[group["net_signed_return"] > 0, "net_signed_return"].sum()
        net_loss = group.loc[group["net_signed_return"] < 0, "net_signed_return"].sum()

        gross_profit_factor = None
        if gross_loss < 0:
            gross_profit_factor = float(gross_profit / abs(gross_loss))

        net_profit_factor = None
        if net_loss < 0:
            net_profit_factor = float(net_profit / abs(net_loss))

        gross_mean = group["gross_signed_return"].mean()
        gross_std = group["gross_signed_return"].std()

        net_mean = group["net_signed_return"].mean()
        net_std = group["net_signed_return"].std()

        gross_sharpe_like = None
        if gross_std and gross_std > 0:
            gross_sharpe_like = float(gross_mean / gross_std)

        net_sharpe_like = None
        if net_std and net_std > 0:
            net_sharpe_like = float(net_mean / net_std)

        max_net_win_streak, max_net_loss_streak = calculate_streaks(group["net_win_flag"])

        record = dict(zip(group_cols, keys))
        record.update(
            {
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "trade_count": len(group),
                "long_trades": int((group["signal_direction"] == 1).sum()),
                "short_trades": int((group["signal_direction"] == -1).sum()),
                "gross_win_rate": float(group["gross_win_flag"].mean()),
                "net_win_rate": float(group["net_win_flag"].mean()),
                "gross_avg_return": float(gross_mean),
                "net_avg_return": float(net_mean),
                "gross_total_return": float(group["gross_signed_return"].sum()),
                "net_total_return": float(group["net_signed_return"].sum()),
                "gross_profit_factor": gross_profit_factor,
                "net_profit_factor": net_profit_factor,
                "gross_sharpe_like": gross_sharpe_like,
                "net_sharpe_like": net_sharpe_like,
                "gross_max_drawdown": float(group["drawdown"].min()),
                "net_max_drawdown": float(group["net_drawdown"].min()),
                "max_net_win_streak": max_net_win_streak,
                "max_net_loss_streak": max_net_loss_streak,
                "cost_drag": float(group["gross_signed_return"].sum() - group["net_signed_return"].sum()),
            }
        )

        records.append(record)

    summary = pd.DataFrame(records)

    summary["cost_survival_score"] = (
        (summary["net_win_rate"].fillna(0.5) - 0.5) * 300
        + summary["net_profit_factor"].fillna(0).clip(0, 5) * 12
        + summary["net_sharpe_like"].fillna(0) * 30
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["net_total_return"].fillna(0) * 10000
        + summary["net_max_drawdown"].fillna(0) * 5000
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["trade_count"] < 50:
            return "low_sample"

        if (
            row["net_win_rate"] >= 0.58
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.25
        ):
            return "cost_resilient_strong"

        if (
            row["net_win_rate"] >= 0.54
            and row["net_avg_return"] > 0
            and pd.notna(row["net_profit_factor"])
            and row["net_profit_factor"] >= 1.10
        ):
            return "cost_resilient_research"

        if row["net_win_rate"] >= 0.51 and row["net_avg_return"] > 0:
            return "cost_resilient_weak"

        return "cost_failed"

    summary["cost_resilience_label"] = summary.apply(label, axis=1)

    label_rank = {
        "cost_resilient_strong": 1,
        "cost_resilient_research": 2,
        "cost_resilient_weak": 3,
        "cost_failed": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["cost_resilience_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "cost_per_trade",
            "cost_survival_score",
            "net_win_rate",
            "net_profit_factor",
        ],
        ascending=[True, True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["cost_test_rank"] = summary.index + 1

    return summary


def build_candidate_survival(summary_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "threshold_signal_label",
        "dataset_file",
    ]

    survival = (
        summary_df
        .groupby(group_cols, dropna=False)
        .agg(
            tested_cost_levels=("cost_per_trade", "count"),
            max_survived_cost=("cost_per_trade", lambda s: float(
                summary_df.loc[s.index][
                    summary_df.loc[s.index]["cost_resilience_label"].isin(
                        [
                            "cost_resilient_strong",
                            "cost_resilient_research",
                            "cost_resilient_weak",
                        ]
                    )
                ]["cost_per_trade"].max()
            ) if not summary_df.loc[s.index][
                summary_df.loc[s.index]["cost_resilience_label"].isin(
                    [
                        "cost_resilient_strong",
                        "cost_resilient_research",
                        "cost_resilient_weak",
                    ]
                )
            ].empty else 0.0),
            strong_cost_levels=("cost_resilience_label", lambda s: int((s == "cost_resilient_strong").sum())),
            research_cost_levels=("cost_resilience_label", lambda s: int((s == "cost_resilient_research").sum())),
            weak_cost_levels=("cost_resilience_label", lambda s: int((s == "cost_resilient_weak").sum())),
            failed_cost_levels=("cost_resilience_label", lambda s: int((s == "cost_failed").sum())),
            min_net_total_return=("net_total_return", "min"),
            max_net_total_return=("net_total_return", "max"),
            avg_net_total_return=("net_total_return", "mean"),
            min_net_profit_factor=("net_profit_factor", "min"),
            max_net_profit_factor=("net_profit_factor", "max"),
            avg_net_profit_factor=("net_profit_factor", "mean"),
            min_net_win_rate=("net_win_rate", "min"),
            max_net_win_rate=("net_win_rate", "max"),
            avg_net_win_rate=("net_win_rate", "mean"),
        )
        .reset_index()
    )

    survival["survival_score"] = (
        survival["max_survived_cost"].fillna(0) * 200000
        + survival["strong_cost_levels"] * 10
        + survival["research_cost_levels"] * 6
        + survival["weak_cost_levels"] * 3
        + survival["avg_net_profit_factor"].fillna(0).clip(0, 5) * 5
        + survival["avg_net_win_rate"].fillna(0.5) * 20
    ).clip(0, 100).round(2)

    survival = survival.sort_values(
        [
            "max_survived_cost",
            "strong_cost_levels",
            "research_cost_levels",
            "survival_score",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    survival["survival_rank"] = survival.index + 1

    return survival


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 39 - COST STRESS TEST")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    replay_trades_path = (
        analysis_dir
        / "signal_replay"
        / "microstructure_signal_replay_trades_latest.csv"
    )

    report_dir = analysis_dir / "cost_stress_test"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Replay trades: {replay_trades_path}")
    print(f"Report dir:    {report_dir}")
    print(f"Cost levels:   {COST_LEVELS}")
    print("-" * 90)

    if not replay_trades_path.exists():
        raise FileNotFoundError(
            f"Missing replay trades file: {replay_trades_path}. Run script 38 first."
        )

    trades_df = pd.read_csv(replay_trades_path)

    if trades_df.empty:
        raise RuntimeError("Replay trades file is empty.")

    cost_trades_df = apply_costs(trades_df)
    summary_df = build_cost_summary(cost_trades_df)
    survival_df = build_candidate_survival(summary_df)

    cost_trades_csv = report_dir / "microstructure_cost_stress_test_trades_latest.csv"
    summary_csv = report_dir / "microstructure_cost_stress_test_summary_latest.csv"
    survival_csv = report_dir / "microstructure_cost_stress_test_survival_latest.csv"
    json_path = report_dir / "microstructure_cost_stress_test_latest.json"
    txt_path = report_dir / "microstructure_cost_stress_test_latest.txt"

    cost_trades_df.to_csv(cost_trades_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    survival_df.to_csv(survival_csv, index=False)

    label_counts = summary_df["cost_resilience_label"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_trade_rows": len(trades_df),
        "cost_trade_rows": len(cost_trades_df),
        "summary_rows": len(summary_df),
        "survival_rows": len(survival_df),
        "cost_levels": COST_LEVELS,
        "label_counts": label_counts,
        "top_summary": summary_df.head(50).to_dict(orient="records"),
        "top_survival": survival_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    summary_display_cols = [
        "cost_test_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "trade_count",
        "gross_win_rate",
        "net_win_rate",
        "gross_total_return",
        "net_total_return",
        "gross_profit_factor",
        "net_profit_factor",
        "net_max_drawdown",
        "cost_survival_score",
        "cost_resilience_label",
    ]

    survival_display_cols = [
        "survival_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "max_survived_cost",
        "strong_cost_levels",
        "research_cost_levels",
        "weak_cost_levels",
        "failed_cost_levels",
        "avg_net_total_return",
        "avg_net_profit_factor",
        "avg_net_win_rate",
        "survival_score",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE COST STRESS TEST")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Input trade rows: {len(trades_df):,}")
    lines.append(f"Cost trade rows:  {len(cost_trades_df):,}")
    lines.append(f"Summary rows:     {len(summary_df):,}")
    lines.append(f"Survival rows:    {len(survival_df):,}")
    lines.append(f"Cost levels:      {COST_LEVELS}")
    lines.append("")
    lines.append(f"Cost labels: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP COST STRESS SUMMARY")
    lines.append("-" * 90)
    lines.append(summary_df[summary_display_cols].head(50).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP COST SURVIVAL SUMMARY")
    lines.append("-" * 90)
    lines.append(survival_df[survival_display_cols].head(50).to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Cost stress test complete.")
    print(f"Input trade rows: {len(trades_df):,}")
    print(f"Cost trade rows:  {len(cost_trades_df):,}")
    print(f"Summary rows:     {len(summary_df):,}")
    print(f"Survival rows:    {len(survival_df):,}")
    print(f"Cost labels:      {label_counts}")
    print(f"Trades CSV:       {cost_trades_csv}")
    print(f"Summary CSV:      {summary_csv}")
    print(f"Survival CSV:     {survival_csv}")
    print(f"JSON output:      {json_path}")
    print(f"TXT output:       {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()