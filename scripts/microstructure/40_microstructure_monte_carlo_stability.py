"""
BACQE MICROSTRUCTURE 40 - MONTE CARLO STABILITY

Purpose:
    Monte Carlo stress-test the strongest cost-resilient microstructure candidates.

Inputs:
    cost_stress_test/
        microstructure_cost_stress_test_survival_latest.csv
        microstructure_cost_stress_test_trades_latest.csv

Outputs:
    monte_carlo_stability/
        microstructure_monte_carlo_paths_latest.csv
        microstructure_monte_carlo_summary_latest.csv
        microstructure_monte_carlo_latest.json
        microstructure_monte_carlo_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

N_SIMULATIONS = 1000
RANDOM_SEED = 42

SELECTED_MAX_CANDIDATES = 10
MIN_TRADES = 50

SELECTED_COST_LEVELS = [
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


def calculate_max_drawdown(equity: np.ndarray) -> float:
    running_max = np.maximum.accumulate(equity)
    drawdown = equity - running_max
    return float(drawdown.min())


def run_monte_carlo_for_returns(
    returns: np.ndarray,
    n_simulations: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    n_trades = len(returns)

    records = []

    for sim_id in range(1, n_simulations + 1):
        sampled_returns = rng.choice(returns, size=n_trades, replace=True)
        equity = np.cumsum(sampled_returns)

        total_return = float(equity[-1])
        avg_return = float(np.mean(sampled_returns))
        median_return = float(np.median(sampled_returns))
        win_rate = float(np.mean(sampled_returns > 0))
        max_drawdown = calculate_max_drawdown(equity)

        std_return = float(np.std(sampled_returns, ddof=1)) if n_trades > 1 else np.nan
        sharpe_like = float(avg_return / std_return) if std_return and std_return > 0 else None

        gross_profit = float(sampled_returns[sampled_returns > 0].sum())
        gross_loss = float(sampled_returns[sampled_returns < 0].sum())

        profit_factor = None
        if gross_loss < 0:
            profit_factor = float(gross_profit / abs(gross_loss))

        records.append(
            {
                "simulation_id": sim_id,
                "n_trades": n_trades,
                "total_return": total_return,
                "avg_return": avg_return,
                "median_return": median_return,
                "win_rate": win_rate,
                "max_drawdown": max_drawdown,
                "std_return": std_return,
                "sharpe_like": sharpe_like,
                "profit_factor": profit_factor,
                "loss_path": total_return < 0,
                "severe_drawdown_0_005": max_drawdown <= -0.005,
                "severe_drawdown_0_010": max_drawdown <= -0.010,
                "severe_drawdown_0_020": max_drawdown <= -0.020,
            }
        )

    return pd.DataFrame(records)


def summarise_monte_carlo(paths_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
    ]

    summary = (
        paths_df
        .groupby(group_cols, dropna=False)
        .agg(
            simulations=("simulation_id", "count"),
            n_trades=("n_trades", "first"),
            mean_total_return=("total_return", "mean"),
            median_total_return=("total_return", "median"),
            p05_total_return=("total_return", lambda s: float(np.percentile(s, 5))),
            p25_total_return=("total_return", lambda s: float(np.percentile(s, 25))),
            p75_total_return=("total_return", lambda s: float(np.percentile(s, 75))),
            p95_total_return=("total_return", lambda s: float(np.percentile(s, 95))),
            mean_win_rate=("win_rate", "mean"),
            p05_win_rate=("win_rate", lambda s: float(np.percentile(s, 5))),
            p95_win_rate=("win_rate", lambda s: float(np.percentile(s, 95))),
            mean_max_drawdown=("max_drawdown", "mean"),
            median_max_drawdown=("max_drawdown", "median"),
            p05_max_drawdown=("max_drawdown", lambda s: float(np.percentile(s, 5))),
            p95_max_drawdown=("max_drawdown", lambda s: float(np.percentile(s, 95))),
            mean_profit_factor=("profit_factor", "mean"),
            median_profit_factor=("profit_factor", "median"),
            mean_sharpe_like=("sharpe_like", "mean"),
            probability_loss=("loss_path", "mean"),
            probability_drawdown_0_005=("severe_drawdown_0_005", "mean"),
            probability_drawdown_0_010=("severe_drawdown_0_010", "mean"),
            probability_drawdown_0_020=("severe_drawdown_0_020", "mean"),
        )
        .reset_index()
    )

    summary["mc_stability_score"] = (
        summary["mean_total_return"].fillna(0) * 10000
        + summary["p05_total_return"].fillna(0) * 7000
        + summary["mean_win_rate"].fillna(0.5) * 40
        + summary["mean_profit_factor"].fillna(0).clip(0, 5) * 10
        + summary["mean_sharpe_like"].fillna(0) * 30
        - summary["probability_loss"].fillna(1) * 30
        - summary["probability_drawdown_0_010"].fillna(1) * 20
    ).clip(0, 100).round(2)

    def label(row: pd.Series) -> str:
        if row["n_trades"] < MIN_TRADES:
            return "low_sample"

        if (
            row["probability_loss"] <= 0.10
            and row["p05_total_return"] > 0
            and row["mean_profit_factor"] >= 1.30
            and row["probability_drawdown_0_010"] <= 0.25
        ):
            return "mc_stable_strong"

        if (
            row["probability_loss"] <= 0.25
            and row["median_total_return"] > 0
            and row["mean_profit_factor"] >= 1.10
        ):
            return "mc_stable_research"

        if row["probability_loss"] <= 0.40 and row["median_total_return"] > 0:
            return "mc_stable_weak"

        return "mc_unstable"

    summary["mc_stability_label"] = summary.apply(label, axis=1)

    label_rank = {
        "mc_stable_strong": 1,
        "mc_stable_research": 2,
        "mc_stable_weak": 3,
        "mc_unstable": 4,
        "low_sample": 5,
    }

    summary["label_rank"] = summary["mc_stability_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "mc_stability_score",
            "p05_total_return",
            "probability_loss",
            "mean_profit_factor",
        ],
        ascending=[True, False, False, True, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["mc_rank"] = summary.index + 1

    return summary


def candidate_key_cols() -> list[str]:
    return [
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
    ]


def build_candidate_id(row: pd.Series) -> str:
    return (
        f"{row['symbol']}|{row['bar_type']}|{row['parameter']}|"
        f"{row['spread_feature']}|{row['target']}|{row['threshold_pair']}"
    )


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 40 - MONTE CARLO STABILITY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    cost_dir = analysis_dir / "cost_stress_test"

    survival_path = cost_dir / "microstructure_cost_stress_test_survival_latest.csv"
    cost_trades_path = cost_dir / "microstructure_cost_stress_test_trades_latest.csv"

    report_dir = analysis_dir / "monte_carlo_stability"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Survival input: {survival_path}")
    print(f"Cost trades:    {cost_trades_path}")
    print(f"Report dir:     {report_dir}")
    print(f"Simulations:    {N_SIMULATIONS}")
    print("-" * 90)

    if not survival_path.exists():
        raise FileNotFoundError(
            f"Missing survival file: {survival_path}. Run script 39 first."
        )

    if not cost_trades_path.exists():
        raise FileNotFoundError(
            f"Missing cost trades file: {cost_trades_path}. Run script 39 first."
        )

    survival_df = pd.read_csv(survival_path)
    cost_trades_df = pd.read_csv(cost_trades_path)

    survival_df = survival_df.sort_values(
        [
            "max_survived_cost",
            "strong_cost_levels",
            "research_cost_levels",
            "survival_score",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    selected_candidates = survival_df.head(SELECTED_MAX_CANDIDATES).copy()
    selected_candidates["candidate_id"] = selected_candidates.apply(build_candidate_id, axis=1)

    cost_trades_df["candidate_id"] = cost_trades_df.apply(build_candidate_id, axis=1)

    selected_ids = set(selected_candidates["candidate_id"])

    selected_trades = cost_trades_df[
        cost_trades_df["candidate_id"].isin(selected_ids)
        & cost_trades_df["cost_per_trade"].isin(SELECTED_COST_LEVELS)
        & (cost_trades_df["status"] == "ok")
    ].copy()

    print(f"Survival rows:        {len(survival_df):,}")
    print(f"Selected candidates:  {len(selected_candidates):,}")
    print(f"Selected trade rows:  {len(selected_trades):,}")
    print("-" * 90)

    if selected_trades.empty:
        raise RuntimeError("No selected cost-adjusted trades available for Monte Carlo.")

    rng = np.random.default_rng(RANDOM_SEED)

    path_frames = []

    group_cols = candidate_key_cols() + ["cost_per_trade"]

    for idx, (keys, group) in enumerate(selected_trades.groupby(group_cols, dropna=False), start=1):
        group = group.sort_values("trade_number").reset_index(drop=True)

        returns = pd.to_numeric(group["net_signed_return"], errors="coerce").dropna().to_numpy()

        meta = dict(zip(group_cols, keys))

        if len(returns) < 2:
            continue

        sim_df = run_monte_carlo_for_returns(
            returns=returns,
            n_simulations=N_SIMULATIONS,
            rng=rng,
        )

        for key, value in meta.items():
            sim_df[key] = value

        sim_df["created_at_utc"] = datetime.now(timezone.utc).isoformat()

        path_frames.append(sim_df)

        print(
            f"[MC] {idx:>2} "
            f"{meta['symbol']:<8} "
            f"{meta['parameter']:<26} "
            f"{meta['spread_feature']:<22} "
            f"{meta['target']:<16} "
            f"{meta['threshold_pair']:<10} "
            f"cost={meta['cost_per_trade']:<8} "
            f"trades={len(returns)}"
        )

    paths_df = pd.concat(path_frames, ignore_index=True, sort=False)
    summary_df = summarise_monte_carlo(paths_df)

    paths_csv = report_dir / "microstructure_monte_carlo_paths_latest.csv"
    summary_csv = report_dir / "microstructure_monte_carlo_summary_latest.csv"
    json_path = report_dir / "microstructure_monte_carlo_latest.json"
    txt_path = report_dir / "microstructure_monte_carlo_latest.txt"

    paths_df.to_csv(paths_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    label_counts = summary_df["mc_stability_label"].value_counts(dropna=False).to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "simulations": N_SIMULATIONS,
        "selected_candidates": len(selected_candidates),
        "selected_trade_rows": len(selected_trades),
        "path_rows": len(paths_df),
        "summary_rows": len(summary_df),
        "label_counts": label_counts,
        "top_summary": summary_df.head(50).to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    display_cols = [
        "mc_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "cost_per_trade",
        "simulations",
        "n_trades",
        "mean_total_return",
        "median_total_return",
        "p05_total_return",
        "p95_total_return",
        "mean_win_rate",
        "mean_max_drawdown",
        "p05_max_drawdown",
        "mean_profit_factor",
        "probability_loss",
        "probability_drawdown_0_010",
        "mc_stability_score",
        "mc_stability_label",
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE MONTE CARLO STABILITY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Simulations:           {N_SIMULATIONS:,}")
    lines.append(f"Survival rows:         {len(survival_df):,}")
    lines.append(f"Selected candidates:   {len(selected_candidates):,}")
    lines.append(f"Selected trade rows:   {len(selected_trades):,}")
    lines.append(f"Monte Carlo path rows: {len(paths_df):,}")
    lines.append(f"Summary rows:          {len(summary_df):,}")
    lines.append("")
    lines.append(f"MC labels: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP MONTE CARLO STABILITY RESULTS")
    lines.append("-" * 90)
    lines.append(summary_df[display_cols].head(60).to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Monte Carlo stability complete.")
    print(f"Simulations:         {N_SIMULATIONS:,}")
    print(f"Selected candidates: {len(selected_candidates):,}")
    print(f"Path rows:           {len(paths_df):,}")
    print(f"Summary rows:        {len(summary_df):,}")
    print(f"MC labels:           {label_counts}")
    print(f"Paths CSV:           {paths_csv}")
    print(f"Summary CSV:         {summary_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()