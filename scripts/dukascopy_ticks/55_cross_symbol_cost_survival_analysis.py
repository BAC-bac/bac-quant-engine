from pathlib import Path
import pandas as pd
import numpy as np


QUANT_LAB = Path(r"E:\Quant_Lab")

SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]

INPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_cost_survival"

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_cross_symbol_cost_survival"
)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def build_cost_result_path(symbol: str) -> Path:
    symbol_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / "cost_results"
        / "cost_survival_results_latest.csv"
    )

    legacy_path = (
        INPUT_ROOT
        / "cost_results"
        / "cost_survival_results_latest.csv"
    )

    if symbol_path.exists():
        return symbol_path

    if symbol == "EURUSD" and legacy_path.exists():
        return legacy_path

    return symbol_path


def load_symbol_results(symbol: str) -> pd.DataFrame:
    path = build_cost_result_path(symbol)

    if not path.exists():
        print(f"[MISSING] {symbol}: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df["symbol"] = symbol
    df["source_path"] = str(path)

    return df


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 55 - CROSS SYMBOL COST SURVIVAL ANALYSIS")
    print("=" * 90)

    ensure_dirs()

    frames = []

    for symbol in SYMBOLS:
        df = load_symbol_results(symbol)
        print(f"{symbol}: rows={len(df):,}")

        if not df.empty:
            frames.append(df)

    if not frames:
        print("[STOP] No cost survival files loaded.")
        return

    all_results = pd.concat(frames, ignore_index=True)

    numeric_cols = [
        "avg_spread",
        "avg_total_cost_return",
        "gross_profit_factor",
        "net_win_rate",
        "net_mean_return",
        "net_total_return",
        "net_profit_factor",
        "net_positive_month_rate",
        "net_positive_year_rate",
        "cost_survival_score",
    ]

    for col in numeric_cols:
        if col in all_results.columns:
            all_results[col] = pd.to_numeric(all_results[col], errors="coerce")

    all_results["cost_drag_pf"] = (
        all_results["gross_profit_factor"] - all_results["net_profit_factor"]
    )

    all_results["cost_drag_pct_of_gross_pf"] = (
        all_results["cost_drag_pf"] / all_results["gross_profit_factor"]
    )

    all_results["survived_costs"] = (
        all_results["survival_label"].astype(str) != "fails_costs"
    )

    all_results["near_survivor"] = (
        (all_results["net_profit_factor"] >= 0.95)
        & (all_results["net_profit_factor"] < 1.0)
    )

    symbol_summary = (
        all_results
        .groupby("symbol", as_index=False)
        .agg(
            rows=("symbol", "size"),
            survivors=("survived_costs", "sum"),
            near_survivors=("near_survivor", "sum"),
            best_gross_pf=("gross_profit_factor", "max"),
            best_net_pf=("net_profit_factor", "max"),
            avg_gross_pf=("gross_profit_factor", "mean"),
            avg_net_pf=("net_profit_factor", "mean"),
            avg_spread=("avg_spread", "mean"),
            avg_cost_drag_pf=("cost_drag_pf", "mean"),
            best_cost_survival_score=("cost_survival_score", "max"),
        )
        .sort_values("best_net_pf", ascending=False)
    )

    feature_summary = (
        all_results
        .groupby(["feature", "target", "side", "cost_scenario"], as_index=False)
        .agg(
            symbols_tested=("symbol", "nunique"),
            avg_gross_pf=("gross_profit_factor", "mean"),
            avg_net_pf=("net_profit_factor", "mean"),
            best_net_pf=("net_profit_factor", "max"),
            avg_spread=("avg_spread", "mean"),
            avg_cost_drag_pf=("cost_drag_pf", "mean"),
            survivors=("survived_costs", "sum"),
            near_survivors=("near_survivor", "sum"),
        )
        .sort_values(["best_net_pf", "avg_net_pf"], ascending=False)
    )

    scenario_summary = (
        all_results
        .groupby(["symbol", "cost_scenario"], as_index=False)
        .agg(
            rows=("symbol", "size"),
            best_net_pf=("net_profit_factor", "max"),
            avg_net_pf=("net_profit_factor", "mean"),
            avg_spread=("avg_spread", "mean"),
            avg_cost_drag_pf=("cost_drag_pf", "mean"),
            survivors=("survived_costs", "sum"),
            near_survivors=("near_survivor", "sum"),
        )
        .sort_values(["symbol", "best_net_pf"], ascending=[True, False])
    )

    output_all = OUTPUT_ROOT / "tables" / "cross_symbol_cost_survival_latest.csv"
    output_symbol = OUTPUT_ROOT / "tables" / "cross_symbol_symbol_summary_latest.csv"
    output_feature = OUTPUT_ROOT / "tables" / "cross_symbol_feature_summary_latest.csv"
    output_scenario = OUTPUT_ROOT / "tables" / "cross_symbol_scenario_summary_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "cross_symbol_cost_survival_report_latest.txt"

    all_results.to_csv(output_all, index=False)
    symbol_summary.to_csv(output_symbol, index=False)
    feature_summary.to_csv(output_feature, index=False)
    scenario_summary.to_csv(output_scenario, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CROSS SYMBOL COST SURVIVAL REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write("Symbol Summary\n")
        f.write("-" * 80 + "\n")
        f.write(symbol_summary.to_string(index=False))
        f.write("\n\n")

        f.write("Best Candidates Across Symbols\n")
        f.write("-" * 80 + "\n")
        f.write(
            all_results.sort_values("net_profit_factor", ascending=False)
            .head(30)[
                [
                    "symbol",
                    "feature",
                    "target",
                    "side",
                    "cost_scenario",
                    "trade_count",
                    "avg_spread",
                    "gross_profit_factor",
                    "net_profit_factor",
                    "cost_drag_pf",
                    "net_total_return",
                    "net_positive_month_rate",
                    "net_positive_year_rate",
                    "survival_label",
                    "cost_survival_score",
                ]
            ].to_string(index=False)
        )
        f.write("\n\n")

        f.write("Feature / Horizon Summary\n")
        f.write("-" * 80 + "\n")
        f.write(feature_summary.head(30).to_string(index=False))
        f.write("\n\n")

        f.write("Outputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"All results:      {output_all}\n")
        f.write(f"Symbol summary:   {output_symbol}\n")
        f.write(f"Feature summary:  {output_feature}\n")
        f.write(f"Scenario summary: {output_scenario}\n")

    print("=" * 90)
    print("[DONE] Cross-symbol cost survival analysis complete.")
    print(f"All:      {output_all}")
    print(f"Symbols:  {output_symbol}")
    print(f"Features: {output_feature}")
    print(f"Scenario: {output_scenario}")
    print(f"Report:   {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()