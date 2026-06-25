"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 12 - CROSS YEAR STABILITY ENGINE

Purpose:
    Analyse Script 11 cross-symbol transfer results by year, quarter and month.

Goal:
    Identify whether transferred EURJPY regimes are structurally persistent
    across 2023, 2024 and 2025, or dependent on one dominant period.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_BASE_SYMBOL = "EURJPY"
DEFAULT_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]

BASE_DIR = Path("E:/Quant_Lab")

TRANSFER_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cross_symbol_transfer"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cross_year_stability"
)


def print_header(base_symbol: str, symbols: list[str]) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 12 - CROSS YEAR STABILITY ENGINE")
    print("=" * 90)
    print(f"Base symbol:  {base_symbol}")
    print(f"Symbols:      {symbols}")
    print(f"Input root:   {TRANSFER_ROOT}")
    print(f"Report root:  {REPORT_ROOT}")
    print("-" * 90)


def symbol_suffix(symbols: list[str]) -> str:
    return "_".join([symbol.lower() for symbol in symbols])


def load_transfer_raw(base_symbol: str, symbols: list[str]) -> pd.DataFrame:
    suffix = symbol_suffix(symbols)

    path = (
        TRANSFER_ROOT
        / f"{base_symbol.lower()}_to_{suffix}_cross_symbol_transfer_raw_latest.csv"
    )

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 11 raw transfer file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 11 raw transfer file is empty.")

    return df


def load_transfer_ranked(base_symbol: str, symbols: list[str]) -> pd.DataFrame:
    suffix = symbol_suffix(symbols)

    path = (
        TRANSFER_ROOT
        / f"{base_symbol.lower()}_to_{suffix}_cross_symbol_transfer_ranked_latest.csv"
    )

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 11 ranked transfer file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 11 ranked transfer file is empty.")

    return df


def clean_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "year" not in df.columns:
        df["year"] = df["date"].dt.year

    if "quarter" not in df.columns:
        df["quarter"] = df["date"].dt.to_period("Q").astype(str)

    if "month" not in df.columns:
        df["month"] = df["date"].dt.strftime("%Y-%m")

    numeric_cols = [
        "trades",
        "gross_total_return",
        "total_dynamic_cost",
        "net_total_return",
        "net_mean_return",
        "net_median_return",
        "net_win_rate",
        "net_profit_factor",
        "source_robustness_score",
        "source_probability_profitable",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)

    return df


def profit_factor_from_returns(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def aggregate_period(raw: pd.DataFrame, period_col: str) -> pd.DataFrame:
    grouped = (
        raw.groupby(
            [
                "test_symbol",
                "source_regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                period_col,
            ],
            dropna=False,
        )
        .agg(
            files=("file", "nunique"),
            trades=("trades", "sum"),
            net_total_return=("net_total_return", "sum"),
            mean_file_return=("net_total_return", "mean"),
            median_file_return=("net_total_return", "median"),
            positive_file_rate=("net_total_return", lambda x: float((x > 0).mean())),
            median_net_win_rate=("net_win_rate", "median"),
            median_net_profit_factor=("net_profit_factor", "median"),
            period_profit_factor_proxy=("net_total_return", profit_factor_from_returns),
            source_robustness_score=("source_robustness_score", "mean"),
            source_probability_profitable=("source_probability_profitable", "mean"),
        )
        .reset_index()
    )

    grouped = grouped.rename(columns={period_col: "period_value"})
    grouped["period_type"] = period_col

    grouped["period_status"] = np.select(
        [
            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.55)
            & (grouped["median_net_win_rate"] > 0.52),

            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.50)
            & (grouped["median_net_win_rate"] > 0.505),

            grouped["net_total_return"] > 0,
        ],
        [
            "period_pass_primary",
            "period_pass_secondary",
            "period_positive_weak",
        ],
        default="period_fail",
    )

    grouped["period_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["median_file_return"].fillna(0) * 100
        + (grouped["positive_file_rate"].fillna(0.5) - 0.5) * 100
        + (grouped["median_net_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["median_net_profit_factor"].fillna(0) * 10
    )

    return grouped


def build_regime_year_stability(yearly: pd.DataFrame) -> pd.DataFrame:
    if yearly.empty:
        return pd.DataFrame()

    grouped = (
        yearly.groupby(
            [
                "test_symbol",
                "source_regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
            ],
            dropna=False,
        )
        .agg(
            years_tested=("period_value", "nunique"),
            positive_years=("net_total_return", lambda x: int((x > 0).sum())),
            primary_years=("period_status", lambda x: int((x == "period_pass_primary").sum())),
            secondary_years=("period_status", lambda x: int((x == "period_pass_secondary").sum())),
            weak_positive_years=("period_status", lambda x: int((x == "period_positive_weak").sum())),
            failed_years=("period_status", lambda x: int((x == "period_fail").sum())),
            total_trades=("trades", "sum"),
            total_net_return=("net_total_return", "sum"),
            min_year_return=("net_total_return", "min"),
            max_year_return=("net_total_return", "max"),
            median_year_return=("net_total_return", "median"),
            mean_positive_file_rate=("positive_file_rate", "mean"),
            median_positive_file_rate=("positive_file_rate", "median"),
            median_net_win_rate=("median_net_win_rate", "median"),
            median_net_profit_factor=("median_net_profit_factor", "median"),
            mean_period_score=("period_score", "mean"),
            min_period_score=("period_score", "min"),
        )
        .reset_index()
    )

    grouped["positive_year_rate"] = grouped["positive_years"] / grouped["years_tested"].replace(0, np.nan)

    grouped["year_stability_score"] = (
        grouped["total_net_return"].fillna(0)
        + grouped["min_year_return"].fillna(0) * 2
        + grouped["positive_year_rate"].fillna(0) * 100
        + grouped["median_net_win_rate"].fillna(0.5) * 25
        + grouped["median_net_profit_factor"].fillna(1.0) * 15
        + np.log1p(grouped["total_trades"].fillna(0))
    )

    grouped["year_stability_status"] = np.select(
        [
            (grouped["years_tested"] >= 3)
            & (grouped["positive_years"] == grouped["years_tested"])
            & (grouped["min_year_return"] > 0)
            & (grouped["median_net_win_rate"] > 0.52)
            & (grouped["median_net_profit_factor"] > 1.10),

            (grouped["years_tested"] >= 3)
            & (grouped["positive_years"] >= grouped["years_tested"] - 1)
            & (grouped["total_net_return"] > 0)
            & (grouped["median_net_win_rate"] > 0.505),

            grouped["total_net_return"] > 0,
        ],
        [
            "year_stable_primary",
            "year_stable_secondary",
            "year_positive_but_unstable",
        ],
        default="year_unstable_or_fail",
    )

    grouped = grouped.sort_values(
        by=[
            "test_symbol",
            "year_stability_status",
            "year_stability_score",
            "positive_year_rate",
            "total_net_return",
        ],
        ascending=[True, True, False, False, False],
    )

    return grouped


def build_symbol_year_summary(yearly: pd.DataFrame) -> pd.DataFrame:
    if yearly.empty:
        return pd.DataFrame()

    summary = (
        yearly.groupby(["test_symbol", "period_value", "period_status"], dropna=False)
        .agg(
            regimes=("source_regime_id", "count"),
            total_trades=("trades", "sum"),
            total_net_return=("net_total_return", "sum"),
            median_positive_file_rate=("positive_file_rate", "median"),
            median_net_win_rate=("median_net_win_rate", "median"),
            median_net_profit_factor=("median_net_profit_factor", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["test_symbol", "period_value", "period_status"],
        ascending=[True, True, True],
    )

    return summary


def build_symbol_stability_summary(stability: pd.DataFrame) -> pd.DataFrame:
    if stability.empty:
        return pd.DataFrame()

    summary = (
        stability.groupby(["test_symbol", "year_stability_status"], dropna=False)
        .agg(
            regimes=("source_regime_id", "count"),
            total_trades=("total_trades", "sum"),
            total_net_return=("total_net_return", "sum"),
            median_positive_year_rate=("positive_year_rate", "median"),
            median_min_year_return=("min_year_return", "median"),
            median_win_rate=("median_net_win_rate", "median"),
            median_profit_factor=("median_net_profit_factor", "median"),
            median_stability_score=("year_stability_score", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["test_symbol", "year_stability_status", "total_net_return"],
        ascending=[True, True, False],
    )

    return summary


def write_outputs(
    base_symbol: str,
    symbols: list[str],
    yearly: pd.DataFrame,
    quarterly: pd.DataFrame,
    monthly: pd.DataFrame,
    stability: pd.DataFrame,
    symbol_year_summary: pd.DataFrame,
    symbol_stability_summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    suffix = symbol_suffix(symbols)

    yearly_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_yearly_latest.csv"
    quarterly_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_quarterly_latest.csv"
    monthly_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_monthly_latest.csv"
    stability_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_stability_ranked_latest.csv"
    stable_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_stable_regimes_latest.csv"
    symbol_year_summary_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_symbol_year_summary_latest.csv"
    symbol_stability_summary_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_symbol_stability_summary_latest.csv"
    txt_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_cross_year_stability_report_latest.txt"

    yearly.to_csv(yearly_path, index=False)
    quarterly.to_csv(quarterly_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    stability.to_csv(stability_path, index=False)

    stable = stability[
        stability["year_stability_status"].isin(
            ["year_stable_primary", "year_stable_secondary"]
        )
    ].copy()

    stable.to_csv(stable_path, index=False)

    symbol_year_summary.to_csv(symbol_year_summary_path, index=False)
    symbol_stability_summary.to_csv(symbol_stability_summary_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 12 - CROSS YEAR STABILITY REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Base symbol: {base_symbol}\n")
        f.write(f"Transfer symbols: {symbols}\n")
        f.write(f"Yearly rows: {len(yearly)}\n")
        f.write(f"Stable regimes: {len(stable)}\n\n")

        if not stability.empty:
            f.write("YEAR STABILITY STATUS COUNTS\n")
            f.write("-" * 90 + "\n")
            f.write(stability.groupby(["test_symbol", "year_stability_status"]).size().to_string())
            f.write("\n\n")

            display_cols = [
                "test_symbol",
                "year_stability_status",
                "source_regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                "years_tested",
                "positive_years",
                "failed_years",
                "year_stability_score",
                "total_trades",
                "total_net_return",
                "min_year_return",
                "median_year_return",
                "positive_year_rate",
                "median_net_win_rate",
                "median_net_profit_factor",
            ]

            f.write("TOP 100 YEAR-STABLE REGIMES\n")
            f.write("-" * 90 + "\n")
            f.write(stability[display_cols].head(100).to_string(index=False))
            f.write("\n\n")

            f.write("PRIMARY YEAR-STABLE REGIMES ONLY\n")
            f.write("-" * 90 + "\n")
            primary = stability[stability["year_stability_status"] == "year_stable_primary"].copy()

            if primary.empty:
                f.write("No primary year-stable regimes found.\n")
            else:
                primary = primary.sort_values(
                    by=[
                        "positive_year_rate",
                        "min_year_return",
                        "total_net_return",
                        "median_net_profit_factor",
                    ],
                    ascending=[False, False, False, False],
                )
                f.write(primary[display_cols].head(100).to_string(index=False))
                f.write("\n\n")

        if not symbol_stability_summary.empty:
            f.write("SYMBOL STABILITY SUMMARY\n")
            f.write("-" * 90 + "\n")
            f.write(symbol_stability_summary.to_string(index=False))
            f.write("\n\n")

        if not symbol_year_summary.empty:
            f.write("SYMBOL YEAR SUMMARY\n")
            f.write("-" * 90 + "\n")
            f.write(symbol_year_summary.to_string(index=False))

    print(f"Yearly:                   {yearly_path}")
    print(f"Quarterly:                {quarterly_path}")
    print(f"Monthly:                  {monthly_path}")
    print(f"Stability ranked:         {stability_path}")
    print(f"Stable regimes:           {stable_path}")
    print(f"Symbol-year summary:      {symbol_year_summary_path}")
    print(f"Symbol-stability summary: {symbol_stability_summary_path}")
    print(f"Text report:              {txt_path}")


def main(base_symbol: str, symbols: list[str]) -> None:
    base_symbol = base_symbol.upper()
    symbols = [symbol.upper() for symbol in symbols]

    print_header(base_symbol, symbols)

    raw = load_transfer_raw(base_symbol, symbols)
    _ = load_transfer_ranked(base_symbol, symbols)

    raw = clean_raw(raw)

    print(f"Transfer raw rows loaded: {len(raw):,}")
    print("-" * 90)

    yearly = aggregate_period(raw, "year")
    quarterly = aggregate_period(raw, "quarter")
    monthly = aggregate_period(raw, "month")

    stability = build_regime_year_stability(yearly)
    symbol_year_summary = build_symbol_year_summary(yearly)
    symbol_stability_summary = build_symbol_stability_summary(stability)

    print(f"Yearly rows:     {len(yearly):,}")
    print(f"Quarterly rows:  {len(quarterly):,}")
    print(f"Monthly rows:    {len(monthly):,}")
    print(f"Stability rows:  {len(stability):,}")

    if not stability.empty:
        print("Year stability status counts:")
        print(stability.groupby(["test_symbol", "year_stability_status"]).size())

    print("-" * 90)

    write_outputs(
        base_symbol=base_symbol,
        symbols=symbols,
        yearly=yearly,
        quarterly=quarterly,
        monthly=monthly,
        stability=stability,
        symbol_year_summary=symbol_year_summary,
        symbol_stability_summary=symbol_stability_summary,
    )

    print("-" * 90)
    print("[DONE] Cross year stability engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--base-symbol", default=DEFAULT_BASE_SYMBOL)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)

    args = parser.parse_args()

    main(
        base_symbol=args.base_symbol,
        symbols=args.symbols,
    )