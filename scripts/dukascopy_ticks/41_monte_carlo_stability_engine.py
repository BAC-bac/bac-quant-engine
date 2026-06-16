"""
BACQE DUKASCOPY 41 - MONTE CARLO STABILITY ENGINE
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

def build_context_ledger(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_context_replay"
        / f"symbol={symbol}"
        / "trade_ledgers"
        / "horizon_context_replay_ledger_latest.parquet"
    )

def build_daily_gap_path(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_weekend_gap_research"
        / f"symbol={symbol}"
        / "gap_tables"
        / "daily_open_gap_table_latest.csv"
    )

def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_monte_carlo_stability"
        / f"symbol={symbol}"
    )

TARGET_CONDITION = "monday_asia_medium_gap_gap_down"

N_SIMULATIONS = 1000
RANDOM_SEED = 42

REMOVAL_LEVELS = [0.0, 0.10, 0.20, 0.30]
BOOTSTRAP_FRACTION = 1.0


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "simulation_results",
        output_root / "stress_results",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "mean_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown_return": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown_return": drawdown.min(),
    }


def attach_gap_info(ledger: pd.DataFrame, gaps: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.copy()
    gaps = gaps.copy()

    ledger["timestamp_utc"] = pd.to_datetime(ledger["timestamp_utc"], errors="coerce")
    ledger["trade_date"] = pd.to_datetime(ledger["timestamp_utc"].dt.date)

    gaps["date"] = pd.to_datetime(gaps["date"], errors="coerce")

    gap_cols = [
        "date",
        "open_gap_return",
        "abs_open_gap_return",
        "gap_direction",
        "monday_gap_size",
    ]

    return ledger.merge(
        gaps[gap_cols],
        left_on="trade_date",
        right_on="date",
        how="left",
    )


def load_target_trades(
    context_ledger: Path,
    daily_gap_path: Path,
) -> pd.DataFrame:
    ledger = pd.read_parquet(context_ledger)
    gaps = pd.read_csv(daily_gap_path)

    required = {
        "timestamp_utc",
        "feature",
        "target",
        "side",
        "session",
        "day_of_week",
        "net_signal_return",
    }

    missing = required - set(ledger.columns)

    if missing:
        raise ValueError(f"Missing ledger columns: {sorted(missing)}")

    df = ledger[
        (ledger["session"].astype(str) == "asia")
        & (ledger["day_of_week"].astype(str) == "Monday")
        & (ledger["feature"].astype(str) == "mid_return_1")
        & (ledger["target"].astype(str) == "future_return_1000")
        & (ledger["side"].astype(str) == "long")
    ].copy()

    df = attach_gap_info(df, gaps)

    df = df[
        (df["monday_gap_size"] == "medium_gap")
        & (df["gap_direction"] == "gap_down")
    ].copy()

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["net_signal_return"])
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    return df


def monte_carlo_bootstrap(returns: np.ndarray, rng: np.random.Generator) -> dict:
    sample_size = int(len(returns) * BOOTSTRAP_FRACTION)
    sample = rng.choice(returns, size=sample_size, replace=True)

    return evaluate_returns(pd.Series(sample))


def monte_carlo_trade_removal(
    returns: np.ndarray,
    removal_fraction: float,
    rng: np.random.Generator,
) -> dict:
    keep_fraction = 1.0 - removal_fraction
    sample_size = int(len(returns) * keep_fraction)

    if sample_size <= 0:
        return evaluate_returns(pd.Series(dtype=float))

    selected = rng.choice(returns, size=sample_size, replace=False)

    return evaluate_returns(pd.Series(selected))


def summarise_simulations(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []

    metrics = [
        "win_rate",
        "mean_return",
        "total_return",
        "profit_factor",
        "max_drawdown_return",
    ]

    for keys, group in df.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)

        row = {col: value for col, value in zip(group_cols, keys)}

        for metric in metrics:
            values = pd.to_numeric(group[metric], errors="coerce").dropna()

            row[f"{metric}_mean"] = values.mean()
            row[f"{metric}_median"] = values.median()
            row[f"{metric}_p05"] = values.quantile(0.05)
            row[f"{metric}_p25"] = values.quantile(0.25)
            row[f"{metric}_p75"] = values.quantile(0.75)
            row[f"{metric}_p95"] = values.quantile(0.95)

        row["simulations"] = len(group)
        row["pf_above_1_rate"] = (group["profit_factor"] > 1.0).mean()
        row["pf_above_1_1_rate"] = (group["profit_factor"] > 1.1).mean()
        row["pf_above_1_2_rate"] = (group["profit_factor"] > 1.2).mean()
        row["positive_return_rate"] = (group["total_return"] > 0).mean()

        rows.append(row)

    return pd.DataFrame(rows)

def run_monte_carlo_stability(symbol: str = DEFAULT_SYMBOL, ) -> None:
    symbol = symbol.upper().strip()

    context_ledger = build_context_ledger(symbol)
    daily_gap_path = build_daily_gap_path(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 41 - MONTE CARLO STABILITY ENGINE")

    ensure_dirs(output_root)

    print(f"Symbol:          {symbol}")
    print(f"Context ledger:  {context_ledger}")
    print(f"Daily gaps:      {daily_gap_path}")
    print(f"Output root:     {output_root}")
    print(f"Target condition:{TARGET_CONDITION}")
    print("-" * 90)

    if not context_ledger.exists():
        print("[STOP] Missing Script 35 ledger.")
        return

    if not daily_gap_path.exists():
        print("[STOP] Missing Script 39 daily gap table.")
        return

    trades = load_target_trades(context_ledger=context_ledger, daily_gap_path=daily_gap_path, )

    print(f"Target trades loaded: {len(trades):,}")

    if trades.empty:
        print("[STOP] No target trades found.")
        return

    base_stats = evaluate_returns(trades["net_signal_return"])

    returns = trades["net_signal_return"].to_numpy()
    rng = np.random.default_rng(RANDOM_SEED)

    simulation_rows = []
    stress_rows = []

    print("[RUN] Bootstrap simulations")

    for i in range(1, N_SIMULATIONS + 1):
        stats = monte_carlo_bootstrap(returns, rng)

        simulation_rows.append({
            "simulation_id": i,
            "simulation_type": "bootstrap",
            "removal_fraction": 0.0,
            **stats,
        })

        if i % 100 == 0:
            print(f"    bootstrap {i}/{N_SIMULATIONS}")

    print("[RUN] Trade removal stress tests")

    for removal_fraction in REMOVAL_LEVELS:
        for i in range(1, N_SIMULATIONS + 1):
            stats = monte_carlo_trade_removal(returns, removal_fraction, rng)

            stress_rows.append({
                "simulation_id": i,
                "simulation_type": "trade_removal",
                "removal_fraction": removal_fraction,
                **stats,
            })

        print(f"    removal={removal_fraction:.0%} complete")

    simulation_df = pd.DataFrame(simulation_rows)
    stress_df = pd.DataFrame(stress_rows)

    bootstrap_summary = summarise_simulations(
        simulation_df,
        ["simulation_type"],
    )

    stress_summary = summarise_simulations(
        stress_df,
        ["simulation_type", "removal_fraction"],
    )

    sim_path = output_root / "simulation_results" / "monte_carlo_bootstrap_latest.csv"
    stress_path = output_root / "stress_results" / "monte_carlo_trade_removal_latest.csv"
    bootstrap_summary_path = output_root / "simulation_results" / "monte_carlo_bootstrap_summary_latest.csv"
    stress_summary_path = output_root / "stress_results" / "monte_carlo_trade_removal_summary_latest.csv"
    report_path = output_root / "reports" / "monte_carlo_stability_report_latest.txt"

    simulation_df.to_csv(sim_path, index=False)
    stress_df.to_csv(stress_path, index=False)
    bootstrap_summary.to_csv(bootstrap_summary_path, index=False)
    stress_summary.to_csv(stress_summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY MONTE CARLO STABILITY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Target condition: {TARGET_CONDITION}\n")
        f.write(f"Trades tested: {len(trades):,}\n")
        f.write(f"Simulations: {N_SIMULATIONS}\n\n")

        f.write("Base Candidate Performance\n")
        f.write("-" * 80 + "\n")
        for key, value in base_stats.items():
            f.write(f"{key}: {value}\n")

        f.write("\n\nBootstrap Stability Summary\n")
        f.write("-" * 80 + "\n")
        f.write(bootstrap_summary.to_string(index=False))

        f.write("\n\nTrade Removal Stress Summary\n")
        f.write("-" * 80 + "\n")
        f.write(stress_summary.to_string(index=False))

        f.write("\n\nInterpretation Guide\n")
        f.write("-" * 80 + "\n")
        f.write("pf_above_1_rate: percentage of simulations where PF remains above 1.0\n")
        f.write("pf_above_1_1_rate: percentage of simulations where PF remains above 1.1\n")
        f.write("positive_return_rate: percentage of simulations where total return remains positive\n")

        f.write("\n\nOutputs:\n")
        f.write(f"Bootstrap simulations: {sim_path}\n")
        f.write(f"Trade removal simulations: {stress_path}\n")
        f.write(f"Bootstrap summary: {bootstrap_summary_path}\n")
        f.write(f"Trade removal summary: {stress_summary_path}\n")

    print("=" * 90)
    print("[DONE] Monte Carlo stability complete.")
    print(f"Bootstrap simulations: {sim_path}")
    print(f"Trade removal:          {stress_path}")
    print(f"Report:                 {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy Monte Carlo stability engine."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_monte_carlo_stability(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()