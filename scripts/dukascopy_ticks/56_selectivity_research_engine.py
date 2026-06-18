from pathlib import Path
import numpy as np
import pandas as pd


QUANT_LAB = Path(r"E:\Quant_Lab")

SYMBOL = "EURUSD"
FEATURE = "mid_return_1"
TARGET = "future_return_1000"
SIDE = "long"

SYMBOL_LEDGER_PATH = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_horizon_candidate_replay"
    / f"symbol={SYMBOL}"
    / "trade_ledgers"
    / "candidate_replay_ledger_latest.parquet"
)

LEGACY_LEDGER_PATH = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_horizon_candidate_replay"
    / "trade_ledgers"
    / "candidate_replay_ledger_latest.parquet"
)

LEDGER_PATH = (
    SYMBOL_LEDGER_PATH
    if SYMBOL_LEDGER_PATH.exists()
    else LEGACY_LEDGER_PATH
)

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_selectivity_research"
    / "symbol=EURUSD"
)

COST_SCENARIOS = {
    "half_spread_plus_low_commission": {
        "spread_fraction": 0.5,
        "commission_return": 0.000005,
    },
    "spread_only": {
        "spread_fraction": 1.0,
        "commission_return": 0.0,
    },
    "spread_plus_low_commission": {
        "spread_fraction": 1.0,
        "commission_return": 0.000005,
    },
    "spread_plus_medium_commission": {
        "spread_fraction": 1.0,
        "commission_return": 0.000010,
    },
}

QUANTILE_LEVELS = [
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    0.95,
    0.975,
    0.99,
]

POINT_SIZE = 0.00001


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
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
            "positive_month_rate": np.nan,
            "positive_year_rate": np.nan,
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
        "positive_month_rate": np.nan,
        "positive_year_rate": np.nan,
        "max_drawdown_return": drawdown.min(),
    }


def add_period_rates(df: pd.DataFrame, return_col: str) -> dict:
    result = {}

    if "month" in df.columns:
        monthly = df.groupby("month")[return_col].sum()
        result["positive_month_rate"] = (monthly > 0).mean() if len(monthly) else np.nan
    else:
        result["positive_month_rate"] = np.nan

    if "year" in df.columns:
        yearly = df.groupby("year")[return_col].sum()
        result["positive_year_rate"] = (yearly > 0).mean() if len(yearly) else np.nan
    else:
        result["positive_year_rate"] = np.nan

    return result


def prepare_candidate_ledger(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[
        (df["feature"].astype(str) == FEATURE)
        & (df["target"].astype(str) == TARGET)
        & (df["side"].astype(str) == SIDE)
    ].copy()

    if df.empty:
        return df

    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
        df["year"] = df["timestamp_utc"].dt.year
        df["month"] = df["timestamp_utc"].dt.to_period("M").astype(str)

    for col in ["signal_return", "spread", "avg_spread", "spread_points"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "signal_return" not in df.columns:
        if "future_return" in df.columns:
            df["signal_return"] = df["future_return"]
        elif TARGET in df.columns:
            df["signal_return"] = df[TARGET]
        else:
            raise ValueError("Could not find signal_return, future_return, or target return column.")

    if "feature_value" not in df.columns:
        raise ValueError("feature_value column missing from replay ledger.")

    df["signal_strength"] = pd.to_numeric(df["feature_value"], errors="coerce")

    if "spread" not in df.columns:
        if "avg_spread" in df.columns:
            df["spread"] = df["avg_spread"]
        elif "spread_points" in df.columns:
            df["spread"] = df["spread_points"] * POINT_SIZE
        else:
            raise ValueError("Could not find spread, avg_spread, or spread_points column.")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["signal_strength", "signal_return", "spread"])

    return df


def run_selectivity_tests(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    feature_abs = df["signal_strength"].abs()
    df = df.copy()
    df["signal_strength_abs"] = feature_abs

    for q in QUANTILE_LEVELS:
        threshold = df["signal_strength_abs"].quantile(q)

        selected = df[df["signal_strength_abs"] >= threshold].copy()

        if selected.empty:
            continue

        gross_stats = evaluate_returns(selected["signal_return"])
        gross_period = add_period_rates(selected, "signal_return")
        gross_stats.update(gross_period)

        for cost_name, params in COST_SCENARIOS.items():
            selected = selected.copy()

            total_cost = (
                selected["spread"] * params["spread_fraction"]
                + params["commission_return"]
            )

            selected["net_signal_return"] = selected["signal_return"] - total_cost

            net_stats = evaluate_returns(selected["net_signal_return"])
            net_period = add_period_rates(selected, "net_signal_return")
            net_stats.update(net_period)

            rows.append({
                "symbol": SYMBOL,
                "feature": FEATURE,
                "target": TARGET,
                "side": SIDE,
                "selectivity_quantile": q,
                "threshold_abs_signal": threshold,
                "cost_scenario": cost_name,
                "gross_trade_count": gross_stats["trade_count"],
                "gross_win_rate": gross_stats["win_rate"],
                "gross_mean_return": gross_stats["mean_return"],
                "gross_total_return": gross_stats["total_return"],
                "gross_profit_factor": gross_stats["profit_factor"],
                "gross_positive_month_rate": gross_stats["positive_month_rate"],
                "gross_positive_year_rate": gross_stats["positive_year_rate"],
                "net_trade_count": net_stats["trade_count"],
                "net_win_rate": net_stats["win_rate"],
                "net_mean_return": net_stats["mean_return"],
                "net_total_return": net_stats["total_return"],
                "net_profit_factor": net_stats["profit_factor"],
                "net_positive_month_rate": net_stats["positive_month_rate"],
                "net_positive_year_rate": net_stats["positive_year_rate"],
                "net_max_drawdown_return": net_stats["max_drawdown_return"],
                "avg_spread": selected["spread"].mean(),
                "avg_signal_strength_abs": selected["signal_strength_abs"].mean(),
            })

    return pd.DataFrame(rows)


def score_results(results: pd.DataFrame) -> pd.DataFrame:
    df = results.copy()

    df["survives_costs"] = df["net_profit_factor"] > 1.0
    df["strong_survivor"] = (
        (df["net_profit_factor"] >= 1.05)
        & (df["net_total_return"] > 0)
        & (df["net_positive_month_rate"] >= 0.50)
    )

    df["selectivity_score"] = (
        df["net_profit_factor"].clip(0, 1.5).fillna(0) / 1.5 * 0.40
        + df["net_win_rate"].fillna(0) * 0.20
        + df["net_positive_month_rate"].fillna(0) * 0.20
        + df["net_positive_year_rate"].fillna(0) * 0.10
        + (df["net_total_return"] > 0).astype(int) * 0.10
    )

    df = df.sort_values(
        ["survives_costs", "net_profit_factor", "selectivity_score"],
        ascending=[False, False, False],
    )

    df.insert(0, "selectivity_rank", range(1, len(df) + 1))

    return df


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 56 - SELECTIVITY RESEARCH ENGINE")
    print("=" * 90)
    print(f"Symbol: {SYMBOL}")
    print(f"Feature: {FEATURE}")
    print(f"Target: {TARGET}")
    print(f"Ledger: {LEDGER_PATH}")
    print(f"Output: {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs()

    if not LEDGER_PATH.exists():
        print(f"[STOP] Missing ledger: {LEDGER_PATH}")
        return

    ledger = pd.read_parquet(LEDGER_PATH)
    print(f"Loaded ledger rows: {len(ledger):,}")

    candidate = prepare_candidate_ledger(ledger)
    print(f"Candidate rows: {len(candidate):,}")

    if candidate.empty:
        print("[STOP] No matching candidate rows.")
        return

    results = run_selectivity_tests(candidate)
    ranked = score_results(results)

    output_results = OUTPUT_ROOT / "tables" / "selectivity_results_latest.csv"
    output_ranked = OUTPUT_ROOT / "tables" / "selectivity_ranked_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "selectivity_research_report_latest.txt"

    results.to_csv(output_results, index=False)
    ranked.to_csv(output_ranked, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY SELECTIVITY RESEARCH REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Feature: {FEATURE}\n")
        f.write(f"Target: {TARGET}\n")
        f.write(f"Side: {SIDE}\n")
        f.write(f"Candidate rows: {len(candidate):,}\n")
        f.write(f"Result rows: {len(results):,}\n\n")

        f.write("Survivor Counts\n")
        f.write("-" * 80 + "\n")
        f.write(ranked["survives_costs"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Selectivity Results\n")
        f.write("-" * 80 + "\n")
        f.write(
            ranked.head(40)[
                [
                    "selectivity_rank",
                    "selectivity_quantile",
                    "cost_scenario",
                    "threshold_abs_signal",
                    "net_trade_count",
                    "gross_profit_factor",
                    "net_profit_factor",
                    "net_win_rate",
                    "net_total_return",
                    "net_positive_month_rate",
                    "net_positive_year_rate",
                    "avg_spread",
                    "avg_signal_strength_abs",
                    "survives_costs",
                    "strong_survivor",
                    "selectivity_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Results: {output_results}\n")
        f.write(f"Ranked:  {output_ranked}\n")

    print("=" * 90)
    print("[DONE] Selectivity research complete.")
    print(f"Results: {output_results}")
    print(f"Ranked:  {output_ranked}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()