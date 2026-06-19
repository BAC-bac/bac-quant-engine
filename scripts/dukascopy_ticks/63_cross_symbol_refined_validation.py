"""
BACQE DUKASCOPY 63 - CROSS SYMBOL REFINED VALIDATION

Purpose:
    Validate context-conditioned survivors across multiple symbols.

Goal:
    Determine whether the context-conditioning method that found
    EURUSD's robust Asia Monday low-spread edge also reveals
    robust pockets in GBPUSD and USDJPY.

Validation:
    2023 train
    2024 validation
    2025 OOS
"""

from pathlib import Path
import numpy as np
import pandas as pd


QUANT_LAB = Path(r"E:\Quant_Lab")

SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]

FEATURE = "mid_return_1"
TARGET = "future_return_1000"
SIDE = "long"

TOP_N_PER_SYMBOL = 50
MIN_TRADES_PER_YEAR = 5_000

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_cross_symbol_refined_validation"
)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def build_context_file(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_context_conditioning_research"
        / f"symbol={symbol}"
        / "tables"
        / "context_conditioning_ranked_latest.csv"
    )


def build_ledger_file(symbol: str) -> Path:
    symbol_path = (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_candidate_replay"
        / f"symbol={symbol}"
        / "trade_ledgers"
        / "candidate_replay_ledger_latest.parquet"
    )

    legacy_path = (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_candidate_replay"
        / "trade_ledgers"
        / "candidate_replay_ledger_latest.parquet"
    )

    if symbol_path.exists():
        return symbol_path

    if symbol == "EURUSD" and legacy_path.exists():
        return legacy_path

    return symbol_path


def parse_context_label(label: str) -> dict:
    filters = {}

    for part in str(label).split("|"):
        part = part.strip()

        if "=" not in part:
            continue

        key, value = part.split("=", 1)
        filters[key.strip()] = value.strip()

    return filters


def apply_context_filter(df: pd.DataFrame, context_label: str) -> pd.DataFrame:
    filtered = df

    for column, value in parse_context_label(context_label).items():
        if column not in filtered.columns:
            return pd.DataFrame()

        filtered = filtered[filtered[column].astype(str) == str(value)]

    return filtered.copy()


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown": drawdown.min(),
    }


def build_net_returns(df: pd.DataFrame, cost_scenario: str) -> pd.Series:
    scenarios = {
        "half_spread_plus_low_commission": {
            "spread_fraction": 0.5,
            "commission": 0.000005,
        },
        "spread_only": {
            "spread_fraction": 1.0,
            "commission": 0.0,
        },
        "spread_plus_low_commission": {
            "spread_fraction": 1.0,
            "commission": 0.000005,
        },
        "spread_plus_medium_commission": {
            "spread_fraction": 1.0,
            "commission": 0.000010,
        },
    }

    params = scenarios[cost_scenario]

    total_cost = (
        df["spread"] * params["spread_fraction"]
        + params["commission"]
    )

    return df["signal_return"] - total_cost


def classify_result(row: pd.Series) -> str:
    if (
        row["train_profit_factor"] >= 1.05
        and row["validation_profit_factor"] >= 1.05
        and row["oos_profit_factor"] >= 1.05
        and row["profitable_years"] == 3
    ):
        return "cross_symbol_oos_pass"

    if (
        row["validation_profit_factor"] >= 1.00
        and row["oos_profit_factor"] >= 1.00
        and row["profitable_years"] >= 2
    ):
        return "cross_symbol_watchlist"

    return "cross_symbol_reject"


def prepare_ledger(ledger: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.copy()

    ledger = ledger[
        (ledger["feature"].astype(str) == FEATURE)
        & (ledger["target"].astype(str) == TARGET)
        & (ledger["side"].astype(str) == SIDE)
    ].copy()

    ledger["year"] = pd.to_numeric(ledger["year"], errors="coerce")
    ledger["signal_return"] = pd.to_numeric(ledger["signal_return"], errors="coerce")
    ledger["spread"] = pd.to_numeric(ledger["spread"], errors="coerce")

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["year", "signal_return", "spread"])

    ledger["year"] = ledger["year"].astype(int)

    return ledger


def load_candidate_contexts(symbol: str) -> pd.DataFrame:
    path = build_context_file(symbol)

    if not path.exists():
        print(f"[MISSING CONTEXT FILE] {symbol}: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)

    if "strong_survivor" in df.columns:
        df = df[df["strong_survivor"] == True].copy()
    elif "survives_costs" in df.columns:
        df = df[df["survives_costs"] == True].copy()

    if df.empty:
        return df

    df = df.sort_values(
        ["net_profit_factor", "net_total_return"],
        ascending=[False, False],
    ).head(TOP_N_PER_SYMBOL)

    df["symbol"] = symbol

    return df.reset_index(drop=True)


def validate_context(symbol: str, context_row: pd.Series, ledger: pd.DataFrame) -> dict | None:
    filtered = apply_context_filter(ledger, context_row["context_label"])

    if filtered.empty:
        return None

    cost_scenario = context_row["cost_scenario"]

    result = {
        "symbol": symbol,
        "context_group": context_row["context_group"],
        "context_label": context_row["context_label"],
        "cost_scenario": cost_scenario,
        "script57_net_pf": context_row.get("net_profit_factor", np.nan),
        "script57_trade_count": context_row.get("net_trade_count", np.nan),
    }

    profitable_years = 0

    for year, prefix in [
        (2023, "train"),
        (2024, "validation"),
        (2025, "oos"),
    ]:
        year_df = filtered[filtered["year"] == year].copy()
        net_returns = build_net_returns(year_df, cost_scenario)
        stats = evaluate_returns(net_returns)

        result[f"{prefix}_trade_count"] = stats["trade_count"]
        result[f"{prefix}_win_rate"] = stats["win_rate"]
        result[f"{prefix}_total_return"] = stats["total_return"]
        result[f"{prefix}_profit_factor"] = stats["profit_factor"]
        result[f"{prefix}_max_drawdown"] = stats["max_drawdown"]

        if (
            stats["trade_count"] >= MIN_TRADES_PER_YEAR
            and stats["total_return"] > 0
        ):
            profitable_years += 1

    result["profitable_years"] = profitable_years
    result["validation_label"] = classify_result(pd.Series(result))

    return result


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 63 - CROSS SYMBOL REFINED VALIDATION")
    print("=" * 90)
    print(f"Symbols: {SYMBOLS}")
    print(f"Output:  {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs()

    all_results = []

    for symbol in SYMBOLS:
        print(f"[SYMBOL] {symbol}")

        contexts = load_candidate_contexts(symbol)
        print(f"Candidate contexts: {len(contexts):,}")

        if contexts.empty:
            continue

        ledger_path = build_ledger_file(symbol)

        if not ledger_path.exists():
            print(f"[MISSING LEDGER] {symbol}: {ledger_path}")
            continue

        ledger = pd.read_parquet(ledger_path)
        ledger = prepare_ledger(ledger)

        print(f"Ledger rows after candidate filter: {len(ledger):,}")

        for idx, row in contexts.iterrows():
            print(f"  [{idx + 1}/{len(contexts)}] {row['context_label']}")

            result = validate_context(symbol, row, ledger)

            if result is not None:
                all_results.append(result)

    if not all_results:
        print("[STOP] No validation results generated.")
        return

    results = pd.DataFrame(all_results)

    results = results.sort_values(
        [
            "validation_label",
            "oos_profit_factor",
            "validation_profit_factor",
            "train_profit_factor",
        ],
        ascending=[True, False, False, False],
    )

    results.insert(0, "rank", range(1, len(results) + 1))

    results_path = OUTPUT_ROOT / "tables" / "cross_symbol_refined_validation_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "cross_symbol_refined_validation_report_latest.txt"

    results.to_csv(results_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CROSS SYMBOL REFINED VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write("Validation Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(results["validation_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Validation Counts by Symbol\n")
        f.write("-" * 80 + "\n")
        f.write(
            pd.crosstab(
                results["symbol"],
                results["validation_label"],
            ).to_string()
        )
        f.write("\n\n")

        f.write("Top Validated Contexts\n")
        f.write("-" * 80 + "\n")

        f.write(
            results.head(50)[
                [
                    "rank",
                    "symbol",
                    "context_group",
                    "context_label",
                    "cost_scenario",
                    "train_trade_count",
                    "train_profit_factor",
                    "validation_trade_count",
                    "validation_profit_factor",
                    "oos_trade_count",
                    "oos_profit_factor",
                    "profitable_years",
                    "validation_label",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Results: {results_path}\n")

    print("=" * 90)
    print("[DONE] Cross-symbol refined validation complete.")
    print(f"Results: {results_path}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()