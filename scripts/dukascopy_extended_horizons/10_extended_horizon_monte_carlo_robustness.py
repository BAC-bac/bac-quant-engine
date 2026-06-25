"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 10 - MONTE CARLO ROBUSTNESS ENGINE

Purpose:
    Stress-test Script 09 replay regimes using bootstrap Monte Carlo
    resampling of file/day-level returns.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 50
DEFAULT_SIMULATIONS = 1000
RANDOM_SEED = 42

BASE_DIR = Path("E:/Quant_Lab")

REPLAY_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "regime_replay"
)

REPORT_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "monte_carlo_robustness"
)


def print_header(symbol: str, top_n: int, simulations: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 10 - MONTE CARLO ROBUSTNESS ENGINE")
    print("=" * 90)
    print(f"Symbol:      {symbol}")
    print(f"Top N:       {top_n}")
    print(f"Simulations: {simulations}")
    print(f"Replay root: {REPLAY_ROOT}")
    print(f"Report root: {REPORT_ROOT}")
    print("-" * 90)


def load_replay_raw(symbol: str) -> pd.DataFrame:
    path = REPLAY_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_raw_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 09 replay raw file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 09 replay raw file is empty.")

    return df


def load_replay_ranked(symbol: str, top_n: int) -> pd.DataFrame:
    path = REPLAY_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_ranked_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 09 replay ranked file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 09 replay ranked file is empty.")

    df = df.sort_values(
        by=["replay_score", "net_total_return", "positive_file_rate"],
        ascending=[False, False, False],
    ).head(top_n).copy()

    return df


def max_drawdown_from_returns(returns: np.ndarray) -> float:
    equity = np.cumsum(returns)
    running_max = np.maximum.accumulate(equity)
    drawdown = equity - running_max
    return float(drawdown.min()) if len(drawdown) else np.nan


def monte_carlo_for_regime(
    regime_id: str,
    returns: np.ndarray,
    simulations: int,
    rng: np.random.Generator,
) -> dict:
    returns = returns[~np.isnan(returns)]

    if len(returns) == 0:
        return {
            "regime_id": regime_id,
            "samples": 0,
            "mc_status": "no_data",
        }

    sim_totals = np.empty(simulations)
    sim_drawdowns = np.empty(simulations)
    sim_positive_rates = np.empty(simulations)

    n = len(returns)

    for i in range(simulations):
        sample = rng.choice(returns, size=n, replace=True)

        sim_totals[i] = sample.sum()
        sim_drawdowns[i] = max_drawdown_from_returns(sample)
        sim_positive_rates[i] = np.mean(sample > 0)

    probability_profitable = float(np.mean(sim_totals > 0))
    probability_drawdown_less_than_half_return = float(
        np.mean(np.abs(sim_drawdowns) < np.maximum(sim_totals * 0.5, 1e-12))
    )

    total_return_p05 = float(np.percentile(sim_totals, 5))
    total_return_p25 = float(np.percentile(sim_totals, 25))
    total_return_p50 = float(np.percentile(sim_totals, 50))
    total_return_p75 = float(np.percentile(sim_totals, 75))
    total_return_p95 = float(np.percentile(sim_totals, 95))

    dd_p05 = float(np.percentile(sim_drawdowns, 5))
    dd_p50 = float(np.percentile(sim_drawdowns, 50))
    dd_p95 = float(np.percentile(sim_drawdowns, 95))

    robustness_score = (
        probability_profitable * 100
        + max(total_return_p05, 0) * 0.10
        + total_return_p50 * 0.01
        + probability_drawdown_less_than_half_return * 25
        + np.median(sim_positive_rates) * 10
    )

    mc_status = np.select(
        [
            (probability_profitable >= 0.95) & (total_return_p05 > 0),
            (probability_profitable >= 0.85) & (total_return_p25 > 0),
            probability_profitable >= 0.70,
        ],
        [
            "mc_pass_primary",
            "mc_pass_secondary",
            "mc_watchlist",
        ],
        default="mc_fail",
    ).item()

    return {
        "regime_id": regime_id,
        "samples": int(n),
        "observed_total_return": float(returns.sum()),
        "observed_mean_return": float(returns.mean()),
        "observed_median_return": float(np.median(returns)),
        "observed_positive_rate": float(np.mean(returns > 0)),
        "probability_profitable": probability_profitable,
        "probability_drawdown_less_than_half_return": probability_drawdown_less_than_half_return,
        "mc_total_return_p05": total_return_p05,
        "mc_total_return_p25": total_return_p25,
        "mc_total_return_p50": total_return_p50,
        "mc_total_return_p75": total_return_p75,
        "mc_total_return_p95": total_return_p95,
        "mc_drawdown_p05": dd_p05,
        "mc_drawdown_p50": dd_p50,
        "mc_drawdown_p95": dd_p95,
        "mc_positive_rate_p50": float(np.percentile(sim_positive_rates, 50)),
        "robustness_score": float(robustness_score),
        "mc_status": str(mc_status),
    }


def run_monte_carlo(raw: pd.DataFrame, ranked: pd.DataFrame, simulations: int) -> pd.DataFrame:
    rng = np.random.default_rng(RANDOM_SEED)

    regime_ids = ranked["regime_id"].dropna().unique().tolist()

    rows = []

    for idx, regime_id in enumerate(regime_ids, start=1):
        regime_returns = (
            raw.loc[raw["regime_id"] == regime_id, "net_total_return"]
            .astype(float)
            .to_numpy()
        )

        result = monte_carlo_for_regime(
            regime_id=regime_id,
            returns=regime_returns,
            simulations=simulations,
            rng=rng,
        )

        rows.append(result)

        print(
            f"[MC] {idx:>3}/{len(regime_ids)} "
            f"samples={result.get('samples', 0):>5} "
            f"p_profit={result.get('probability_profitable', np.nan):.3f} "
            f"status={result.get('mc_status')}"
        )

    mc = pd.DataFrame(rows)

    merge_cols = [
        "regime_id",
        "replay_status",
        "context_type",
        "context_value",
        "target",
        "feature",
        "threshold_quantile",
        "threshold_side",
        "replay_score",
        "files_tested",
        "total_trades",
        "net_total_return",
        "positive_file_rate",
        "median_net_win_rate",
        "median_net_profit_factor",
        "first_date",
        "last_date",
    ]

    available_merge_cols = [col for col in merge_cols if col in ranked.columns]

    mc = mc.merge(
        ranked[available_merge_cols],
        on="regime_id",
        how="left",
        suffixes=("", "_replay"),
    )

    mc = mc.sort_values(
        by=[
            "mc_status",
            "robustness_score",
            "probability_profitable",
            "mc_total_return_p05",
        ],
        ascending=[True, False, False, False],
    )

    return mc


def write_outputs(symbol: str, mc: pd.DataFrame, simulations: int) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_monte_carlo_ranked_latest.csv"
    passed_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_monte_carlo_passed_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_monte_carlo_report_latest.txt"

    mc.to_csv(ranked_path, index=False)

    passed = mc[mc["mc_status"].isin(["mc_pass_primary", "mc_pass_secondary"])].copy()
    passed.to_csv(passed_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 10 - MONTE CARLO ROBUSTNESS REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Simulations per regime: {simulations}\n")
        f.write(f"Regimes tested: {len(mc)}\n")
        f.write(f"Monte Carlo passes: {len(passed)}\n\n")

        if not mc.empty:
            f.write("MONTE CARLO STATUS COUNTS\n")
            f.write("-" * 90 + "\n")
            f.write(mc["mc_status"].value_counts().to_string())
            f.write("\n\n")

            display_cols = [
                "mc_status",
                "regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                "probability_profitable",
                "mc_total_return_p05",
                "mc_total_return_p50",
                "mc_total_return_p95",
                "mc_drawdown_p50",
                "robustness_score",
                "replay_status",
                "files_tested",
                "total_trades",
                "net_total_return",
            ]

            display_cols = [col for col in display_cols if col in mc.columns]

            f.write("TOP 50 MONTE CARLO REGIMES\n")
            f.write("-" * 90 + "\n")
            f.write(mc[display_cols].head(50).to_string(index=False))
            f.write("\n\n")

            f.write("PRIMARY MONTE CARLO PASSES ONLY\n")
            f.write("-" * 90 + "\n")
            primary = mc[mc["mc_status"] == "mc_pass_primary"].copy()

            if primary.empty:
                f.write("No primary Monte Carlo passes found.\n")
            else:
                primary = primary.sort_values(
                    by=[
                        "probability_profitable",
                        "mc_total_return_p05",
                        "robustness_score",
                    ],
                    ascending=[False, False, False],
                )
                f.write(primary[display_cols].head(50).to_string(index=False))

    print(f"Monte Carlo ranked: {ranked_path}")
    print(f"Monte Carlo passed: {passed_path}")
    print(f"Text report:        {txt_path}")


def main(symbol: str, top_n: int, simulations: int) -> None:
    print_header(symbol, top_n, simulations)

    raw = load_replay_raw(symbol)
    ranked = load_replay_ranked(symbol, top_n)

    print(f"Replay raw rows loaded: {len(raw):,}")
    print(f"Ranked regimes loaded:  {len(ranked):,}")
    print("-" * 90)

    mc = run_monte_carlo(raw, ranked, simulations)

    print("-" * 90)
    print(f"Monte Carlo regimes tested: {len(mc):,}")
    print("Monte Carlo status counts:")
    print(mc["mc_status"].value_counts())
    print("-" * 90)

    write_outputs(symbol, mc, simulations)

    print("-" * 90)
    print("[DONE] Extended horizon Monte Carlo robustness complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--simulations", type=int, default=DEFAULT_SIMULATIONS)

    args = parser.parse_args()

    main(
        symbol=args.symbol.upper(),
        top_n=args.top_n,
        simulations=args.simulations,
    )