"""
BACQE MICROSTRUCTURE 38 - SIGNAL REPLAY ENGINE

Purpose:
    Replay threshold-optimised microstructure signals chronologically.

Input:
    signal_threshold_optimization/
        microstructure_signal_threshold_optimization_summary_latest.csv

Outputs:
    signal_replay/
        microstructure_signal_replay_trades_latest.csv
        microstructure_signal_replay_equity_curve_latest.csv
        microstructure_signal_replay_summary_latest.csv
        microstructure_signal_replay_latest.json
        microstructure_signal_replay_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SELECTED_LABELS = {
    "threshold_signal_strong",
    "threshold_signal_research",
}

REGIME_LABELS = [
    "tight_liquidity",
    "normal_liquidity",
    "wide_liquidity",
    "extreme_wide_liquidity",
]

MIN_REGIME_ROWS = 20


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


def assign_liquidity_regimes(df: pd.DataFrame, spread_col: str) -> pd.Series:
    spread = pd.to_numeric(df[spread_col], errors="coerce")

    q25 = spread.quantile(0.25)
    q75 = spread.quantile(0.75)
    q90 = spread.quantile(0.90)

    regimes = pd.Series(index=df.index, dtype="object")
    regimes[spread <= q25] = "tight_liquidity"
    regimes[(spread > q25) & (spread <= q75)] = "normal_liquidity"
    regimes[(spread > q75) & (spread <= q90)] = "wide_liquidity"
    regimes[spread > q90] = "extreme_wide_liquidity"
    regimes[spread.isna()] = "unknown_liquidity"

    return regimes


def infer_regime_signal_map(
    train_df: pd.DataFrame,
    target: str,
    long_threshold: float,
    short_threshold: float,
) -> dict:
    regime_map = {}

    for regime in REGIME_LABELS:
        regime_df = train_df[train_df["liquidity_regime"] == regime].copy()

        if len(regime_df) < MIN_REGIME_ROWS:
            regime_map[regime] = 0
            continue

        positive_rate = float((regime_df[target] > 0).mean())

        if positive_rate > long_threshold:
            regime_map[regime] = 1
        elif positive_rate < short_threshold:
            regime_map[regime] = -1
        else:
            regime_map[regime] = 0

    return regime_map


def parse_threshold_pair(pair: str) -> tuple[float, float]:
    long_s, short_s = str(pair).split("_")
    return float(long_s), float(short_s)


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


def replay_signal_candidate(row: pd.Series) -> pd.DataFrame:
    dataset_file = Path(row["dataset_file"])
    target = row["target"]
    spread_feature = row["spread_feature"]
    threshold_pair = row["threshold_pair"]

    long_threshold, short_threshold = parse_threshold_pair(threshold_pair)

    base_cols = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "threshold_rank": row.get("threshold_rank"),
        "symbol": row.get("symbol"),
        "bar_type": row.get("bar_type"),
        "parameter": row.get("parameter"),
        "spread_feature": spread_feature,
        "target": target,
        "threshold_pair": threshold_pair,
        "long_threshold": long_threshold,
        "short_threshold": short_threshold,
        "threshold_signal_label": row.get("threshold_signal_label"),
        "threshold_score": row.get("threshold_score"),
        "dataset_file": str(dataset_file),
    }

    if not dataset_file.exists():
        return pd.DataFrame([{**base_cols, "status": "missing_dataset", "error": "Dataset missing."}])

    try:
        df = pd.read_parquet(dataset_file)
    except Exception as exc:
        return pd.DataFrame([{**base_cols, "status": "failed_read", "error": str(exc)}])

    if df.empty:
        return pd.DataFrame([{**base_cols, "status": "empty_dataset", "error": "Dataset empty."}])

    if spread_feature not in df.columns:
        return pd.DataFrame([{**base_cols, "status": "missing_spread_feature", "error": f"Missing {spread_feature}"}])

    if target not in df.columns:
        return pd.DataFrame([{**base_cols, "status": "missing_target", "error": f"Missing {target}"}])

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)
    else:
        df["end_time"] = pd.RangeIndex(start=0, stop=len(df), step=1)

    df[target] = pd.to_numeric(df[target], errors="coerce")
    df = df.dropna(subset=[target]).reset_index(drop=True)

    df["liquidity_regime"] = assign_liquidity_regimes(df, spread_feature)

    train_end = int(len(df) * 0.50)
    train_df = df.iloc[:train_end].copy()
    replay_df = df.iloc[train_end:].copy()

    regime_map = infer_regime_signal_map(
        train_df=train_df,
        target=target,
        long_threshold=long_threshold,
        short_threshold=short_threshold,
    )

    replay_df["signal_direction"] = (
        replay_df["liquidity_regime"].map(regime_map).fillna(0).astype(int)
    )

    replay_df = replay_df[replay_df["signal_direction"] != 0].copy()

    if replay_df.empty:
        return pd.DataFrame([{**base_cols, "status": "no_signals", "error": "No active signals generated."}])

    replay_df["forward_return"] = pd.to_numeric(replay_df[target], errors="coerce")
    replay_df["signed_return"] = replay_df["forward_return"] * replay_df["signal_direction"]
    replay_df["win_flag"] = replay_df["signed_return"] > 0
    replay_df["cumulative_return"] = replay_df["signed_return"].fillna(0).cumsum()
    replay_df["running_max"] = replay_df["cumulative_return"].cummax()
    replay_df["drawdown"] = replay_df["cumulative_return"] - replay_df["running_max"]

    replay_df["regime_signal_map"] = json.dumps(regime_map)
    replay_df["trade_number"] = np.arange(1, len(replay_df) + 1)
    replay_df["status"] = "ok"
    replay_df["error"] = None

    for key, value in base_cols.items():
        replay_df[key] = value

    output_cols = [
        "checked_at_utc",
        "threshold_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "long_threshold",
        "short_threshold",
        "threshold_signal_label",
        "threshold_score",
        "trade_number",
        "end_time",
        "liquidity_regime",
        "signal_direction",
        "forward_return",
        "signed_return",
        "win_flag",
        "cumulative_return",
        "running_max",
        "drawdown",
        "regime_signal_map",
        "dataset_file",
        "status",
        "error",
    ]

    return replay_df[output_cols].copy()


def build_replay_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = trades_df[trades_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

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

    summary_records = []

    for keys, group in ok_df.groupby(group_cols, dropna=False):
        group = group.sort_values("end_time").reset_index(drop=True)

        wins = group["win_flag"]
        max_win_streak, max_loss_streak = calculate_streaks(wins)

        gross_profit = group.loc[group["signed_return"] > 0, "signed_return"].sum()
        gross_loss = group.loc[group["signed_return"] < 0, "signed_return"].sum()

        profit_factor = None
        if gross_loss < 0:
            profit_factor = float(gross_profit / abs(gross_loss))

        mean_ret = group["signed_return"].mean()
        std_ret = group["signed_return"].std()

        sharpe_like = None
        if std_ret and std_ret > 0:
            sharpe_like = float(mean_ret / std_ret)

        record = dict(zip(group_cols, keys))
        record.update(
            {
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "trade_count": len(group),
                "long_trades": int((group["signal_direction"] == 1).sum()),
                "short_trades": int((group["signal_direction"] == -1).sum()),
                "win_count": int(wins.sum()),
                "loss_count": int((~wins).sum()),
                "win_rate": float(wins.mean()),
                "avg_signed_return": float(mean_ret),
                "median_signed_return": float(group["signed_return"].median()),
                "total_signed_return": float(group["signed_return"].sum()),
                "std_signed_return": float(std_ret),
                "sharpe_like": sharpe_like,
                "gross_profit": float(gross_profit),
                "gross_loss": float(gross_loss),
                "profit_factor": profit_factor,
                "max_drawdown": float(group["drawdown"].min()),
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "first_signal_time": str(group["end_time"].min()),
                "last_signal_time": str(group["end_time"].max()),
                "active_regimes": ",".join(sorted(group["liquidity_regime"].dropna().unique())),
            }
        )

        summary_records.append(record)

    summary = pd.DataFrame(summary_records)

    summary["replay_score"] = (
        (summary["win_rate"].fillna(0.5) - 0.5) * 300
        + summary["profit_factor"].fillna(0).clip(0, 5) * 10
        + summary["sharpe_like"].fillna(0) * 25
        + np.log1p(summary["trade_count"].fillna(0)) * 3
        + summary["total_signed_return"].fillna(0) * 10000
        + summary["max_drawdown"].fillna(0) * 5000
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["trade_count"] < 50:
            return "low_sample"
        if row["win_rate"] >= 0.58 and row["avg_signed_return"] > 0 and row["profit_factor"] and row["profit_factor"] >= 1.25:
            return "strong_replay_candidate"
        if row["win_rate"] >= 0.54 and row["avg_signed_return"] > 0 and row["profit_factor"] and row["profit_factor"] >= 1.10:
            return "research_replay_candidate"
        if row["win_rate"] >= 0.51 and row["avg_signed_return"] > 0:
            return "weak_replay_candidate"
        return "no_replay_edge"

    summary["replay_label"] = summary.apply(label, axis=1)

    label_rank = {
        "strong_replay_candidate": 1,
        "research_replay_candidate": 2,
        "weak_replay_candidate": 3,
        "no_replay_edge": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["replay_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        ["label_rank", "replay_score", "win_rate", "profit_factor", "trade_count"],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["replay_rank"] = summary.index + 1

    return summary


def build_combined_equity_curve(trades_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = trades_df[trades_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    ok_df["end_time_sort"] = pd.to_datetime(ok_df["end_time"], utc=True, errors="coerce")
    ok_df = ok_df.sort_values(["end_time_sort", "symbol", "parameter"]).reset_index(drop=True)

    equity = ok_df[
        [
            "end_time",
            "symbol",
            "bar_type",
            "parameter",
            "spread_feature",
            "target",
            "threshold_pair",
            "signal_direction",
            "signed_return",
            "win_flag",
        ]
    ].copy()

    equity["combined_trade_number"] = np.arange(1, len(equity) + 1)
    equity["combined_equity"] = equity["signed_return"].fillna(0).cumsum()
    equity["combined_running_max"] = equity["combined_equity"].cummax()
    equity["combined_drawdown"] = equity["combined_equity"] - equity["combined_running_max"]

    return equity


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 38 - SIGNAL REPLAY ENGINE")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    threshold_summary_path = (
        analysis_dir
        / "signal_threshold_optimization"
        / "microstructure_signal_threshold_optimization_summary_latest.csv"
    )

    report_dir = analysis_dir / "signal_replay"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Threshold summary: {threshold_summary_path}")
    print(f"Report dir:        {report_dir}")
    print("-" * 90)

    if not threshold_summary_path.exists():
        raise FileNotFoundError(
            f"Missing threshold summary: {threshold_summary_path}. Run script 37 first."
        )

    threshold_df = pd.read_csv(threshold_summary_path)

    candidates_df = threshold_df[
        threshold_df["threshold_signal_label"].isin(SELECTED_LABELS)
    ].copy()

    candidates_df = candidates_df.sort_values(
        ["label_rank", "threshold_score", "avg_hit_rate", "total_signal_rows"],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    print(f"Threshold rows:      {len(threshold_df):,}")
    print(f"Replay candidates:   {len(candidates_df):,}")
    print("-" * 90)

    if candidates_df.empty:
        raise RuntimeError("No strong/research threshold candidates available for replay.")

    trade_frames = []

    for idx, row in candidates_df.iterrows():
        replay_df = replay_signal_candidate(row)
        trade_frames.append(replay_df)

        ok_rows = int((replay_df["status"] == "ok").sum()) if "status" in replay_df.columns else 0

        print(
            f"[REPLAY] {idx + 1:>2}/{len(candidates_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter']:<26} "
            f"{row['target']:<16} "
            f"{row['threshold_pair']:<10} "
            f"trades={ok_rows}"
        )

    trades_df = pd.concat(trade_frames, ignore_index=True, sort=False)

    summary_df = build_replay_summary(trades_df)
    equity_df = build_combined_equity_curve(trades_df)

    trades_csv = report_dir / "microstructure_signal_replay_trades_latest.csv"
    equity_csv = report_dir / "microstructure_signal_replay_equity_curve_latest.csv"
    summary_csv = report_dir / "microstructure_signal_replay_summary_latest.csv"
    json_path = report_dir / "microstructure_signal_replay_latest.json"
    txt_path = report_dir / "microstructure_signal_replay_latest.txt"

    trades_df.to_csv(trades_csv, index=False)
    equity_df.to_csv(equity_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    status_counts = trades_df["status"].value_counts(dropna=False).to_dict()

    if not summary_df.empty:
        label_counts = summary_df["replay_label"].value_counts(dropna=False).to_dict()
    else:
        label_counts = {}

    combined_summary = {}

    if not equity_df.empty:
        combined_summary = {
            "combined_trades": int(len(equity_df)),
            "combined_win_rate": float(equity_df["win_flag"].mean()),
            "combined_total_return": float(equity_df["signed_return"].sum()),
            "combined_avg_return": float(equity_df["signed_return"].mean()),
            "combined_max_drawdown": float(equity_df["combined_drawdown"].min()),
        }

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "threshold_rows": len(threshold_df),
        "replay_candidates": len(candidates_df),
        "trade_rows": len(trades_df),
        "summary_rows": len(summary_df),
        "equity_rows": len(equity_df),
        "status_counts": status_counts,
        "label_counts": label_counts,
        "combined_summary": combined_summary,
        "top_summary": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    display_cols = [
        "replay_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "threshold_signal_label",
        "trade_count",
        "long_trades",
        "short_trades",
        "win_rate",
        "avg_signed_return",
        "total_signed_return",
        "profit_factor",
        "sharpe_like",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
        "replay_score",
        "replay_label",
    ]

    available_display_cols = [c for c in display_cols if c in summary_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL REPLAY ENGINE")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Threshold rows:    {len(threshold_df):,}")
    lines.append(f"Replay candidates: {len(candidates_df):,}")
    lines.append(f"Trade rows:        {len(trades_df):,}")
    lines.append(f"Summary rows:      {len(summary_df):,}")
    lines.append(f"Equity rows:       {len(equity_df):,}")
    lines.append("")
    lines.append(f"Trade status counts: {status_counts}")
    lines.append(f"Replay labels:       {label_counts}")
    lines.append(f"Combined summary:    {combined_summary}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP SIGNAL REPLAY RESULTS")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("No replay summary available.")
    else:
        lines.append(summary_df[available_display_cols].head(50).to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal replay complete.")
    print(f"Threshold rows:    {len(threshold_df):,}")
    print(f"Replay candidates: {len(candidates_df):,}")
    print(f"Trade rows:        {len(trades_df):,}")
    print(f"Summary rows:      {len(summary_df):,}")
    print(f"Equity rows:       {len(equity_df):,}")
    print(f"Status counts:     {status_counts}")
    print(f"Replay labels:     {label_counts}")
    print(f"Combined summary:  {combined_summary}")
    print(f"Trades CSV:        {trades_csv}")
    print(f"Equity CSV:        {equity_csv}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"JSON output:       {json_path}")
    print(f"TXT output:        {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()