from pathlib import Path
import itertools

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
    / "dukascopy_context_conditioning_research"
    / f"symbol={SYMBOL}"
)

MIN_TRADES = 10_000

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

CONTEXT_GROUPS = [
    ["session"],
    ["spread_regime"],
    ["volatility_regime"],
    ["day_of_week"],
    ["hour"],
    ["session", "day_of_week"],
    ["session", "spread_regime"],
    ["session", "volatility_regime"],
    ["day_of_week", "spread_regime"],
    ["day_of_week", "volatility_regime"],
    ["session", "day_of_week", "spread_regime"],
    ["session", "day_of_week", "volatility_regime"],
]


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


def add_period_rates(df: pd.DataFrame, return_col: str) -> dict:
    if "month" in df.columns:
        monthly = df.groupby("month")[return_col].sum()
        positive_month_rate = (monthly > 0).mean() if len(monthly) else np.nan
    else:
        positive_month_rate = np.nan

    if "year" in df.columns:
        yearly = df.groupby("year")[return_col].sum()
        positive_year_rate = (yearly > 0).mean() if len(yearly) else np.nan
    else:
        positive_year_rate = np.nan

    return {
        "positive_month_rate": positive_month_rate,
        "positive_year_rate": positive_year_rate,
    }


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

    if "feature_value" not in df.columns:
        raise ValueError("feature_value column missing from replay ledger.")

    df["signal_strength"] = pd.to_numeric(df["feature_value"], errors="coerce")

    if "signal_return" not in df.columns:
        if "target_return" in df.columns:
            df["signal_return"] = df["target_return"]
        else:
            raise ValueError("Could not find signal_return or target_return column.")

    if "spread" not in df.columns:
        raise ValueError("spread column missing from replay ledger.")

    df["signal_return"] = pd.to_numeric(df["signal_return"], errors="coerce")
    df["spread"] = pd.to_numeric(df["spread"], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["signal_strength", "signal_return", "spread"])

    for col in [
        "session",
        "spread_regime",
        "volatility_regime",
        "day_of_week",
        "hour",
    ]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df


def context_label(group_cols: list[str], keys) -> str:
    if not isinstance(keys, tuple):
        keys = (keys,)

    parts = [
        f"{col}={value}"
        for col, value in zip(group_cols, keys)
    ]

    return " | ".join(parts)


def run_context_tests(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for group_cols in CONTEXT_GROUPS:
        missing_cols = [col for col in group_cols if col not in df.columns]

        if missing_cols:
            continue

        grouped = df.groupby(group_cols, dropna=False)

        for keys, group in grouped:
            if len(group) < MIN_TRADES:
                continue

            label = context_label(group_cols, keys)

            gross_stats = evaluate_returns(group["signal_return"])
            gross_period = add_period_rates(group, "signal_return")
            gross_stats.update(gross_period)

            for cost_name, params in COST_SCENARIOS.items():
                temp = group.copy()

                total_cost = (
                    temp["spread"] * params["spread_fraction"]
                    + params["commission_return"]
                )

                temp["net_signal_return"] = temp["signal_return"] - total_cost

                net_stats = evaluate_returns(temp["net_signal_return"])
                net_period = add_period_rates(temp, "net_signal_return")
                net_stats.update(net_period)

                rows.append({
                    "symbol": SYMBOL,
                    "feature": FEATURE,
                    "target": TARGET,
                    "side": SIDE,
                    "context_group": " + ".join(group_cols),
                    "context_label": label,
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
                    "avg_spread": temp["spread"].mean(),
                    "avg_signal_strength_abs": temp["signal_strength"].abs().mean(),
                })

    return pd.DataFrame(rows)


def score_results(results: pd.DataFrame) -> pd.DataFrame:
    df = results.copy()

    df["survives_costs"] = df["net_profit_factor"] > 1.0

    df["strong_survivor"] = (
        (df["net_profit_factor"] >= 1.05)
        & (df["net_total_return"] > 0)
        & (df["net_positive_month_rate"] >= 0.50)
        & (df["net_trade_count"] >= MIN_TRADES)
    )

    df["context_score"] = (
        df["net_profit_factor"].clip(0, 1.75).fillna(0) / 1.75 * 0.35
        + df["net_win_rate"].fillna(0) * 0.15
        + df["net_positive_month_rate"].fillna(0) * 0.20
        + df["net_positive_year_rate"].fillna(0) * 0.10
        + (df["net_total_return"] > 0).astype(int) * 0.10
        + (df["net_trade_count"].clip(0, 250_000) / 250_000) * 0.10
    )

    df = df.sort_values(
        [
            "strong_survivor",
            "survives_costs",
            "net_profit_factor",
            "context_score",
        ],
        ascending=[False, False, False, False],
    )

    df.insert(0, "context_rank", range(1, len(df) + 1))

    return df


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 57 - CONTEXT CONDITIONING RESEARCH")
    print("=" * 90)
    print(f"Symbol:  {SYMBOL}")
    print(f"Feature: {FEATURE}")
    print(f"Target:  {TARGET}")
    print(f"Side:    {SIDE}")
    print(f"Ledger:  {LEDGER_PATH}")
    print(f"Output:  {OUTPUT_ROOT}")
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

    results = run_context_tests(candidate)

    if results.empty:
        print("[STOP] No context results generated.")
        return

    ranked = score_results(results)

    output_results = OUTPUT_ROOT / "tables" / "context_conditioning_results_latest.csv"
    output_ranked = OUTPUT_ROOT / "tables" / "context_conditioning_ranked_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "context_conditioning_report_latest.txt"

    results.to_csv(output_results, index=False)
    ranked.to_csv(output_ranked, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CONTEXT CONDITIONING RESEARCH REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Feature: {FEATURE}\n")
        f.write(f"Target: {TARGET}\n")
        f.write(f"Side: {SIDE}\n")
        f.write(f"Candidate rows: {len(candidate):,}\n")
        f.write(f"Result rows: {len(results):,}\n")
        f.write(f"Minimum trades per bucket: {MIN_TRADES:,}\n\n")

        f.write("Survivor Counts\n")
        f.write("-" * 80 + "\n")
        f.write(ranked["survives_costs"].value_counts().to_string())
        f.write("\n\n")

        f.write("Strong Survivor Counts\n")
        f.write("-" * 80 + "\n")
        f.write(ranked["strong_survivor"].value_counts().to_string())
        f.write("\n\n")

        f.write("Top Context Conditioning Results\n")
        f.write("-" * 80 + "\n")
        f.write(
            ranked.head(50)[
                [
                    "context_rank",
                    "context_group",
                    "context_label",
                    "cost_scenario",
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
                    "context_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Results: {output_results}\n")
        f.write(f"Ranked:  {output_ranked}\n")

    print("=" * 90)
    print("[DONE] Context conditioning research complete.")
    print(f"Results: {output_results}")
    print(f"Ranked:  {output_ranked}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()