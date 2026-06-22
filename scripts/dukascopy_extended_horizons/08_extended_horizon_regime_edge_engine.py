"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 08 - EXTENDED HORIZON REGIME EDGE ENGINE

Purpose:
    Convert Script 07 context-conditioned primary/secondary passes into a
    formal regime edge registry.

Input:
    Script 07 context ranked / passed reports.

Output:
    Regime edge registry, top regimes, and summary report.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"

BASE_DIR = Path("E:/Quant_Lab")

CONTEXT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "context_conditioning"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "regime_edge_engine"
)


MIN_TRADES_PRIMARY = 100_000
MIN_FILES_PRIMARY = 100
MIN_PROFIT_FACTOR_PRIMARY = 1.15
MIN_WIN_RATE_PRIMARY = 0.53

MIN_TRADES_SECONDARY = 25_000
MIN_FILES_SECONDARY = 25
MIN_PROFIT_FACTOR_SECONDARY = 1.05
MIN_WIN_RATE_SECONDARY = 0.505


def print_header(symbol: str) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 08 - REGIME EDGE ENGINE")
    print("=" * 90)
    print(f"Symbol:       {symbol}")
    print(f"Context root: {CONTEXT_ROOT}")
    print(f"Report root:  {REPORT_ROOT}")
    print("-" * 90)


def load_context_ranked(symbol: str) -> pd.DataFrame:
    ranked_path = CONTEXT_ROOT / f"{symbol.lower()}_extended_horizon_context_ranked_latest.csv"

    if not ranked_path.exists():
        raise FileNotFoundError(f"Missing Script 07 ranked context file: {ranked_path}")

    ranked = pd.read_csv(ranked_path)

    if ranked.empty:
        raise ValueError("Script 07 ranked context file is empty.")

    return ranked


def clean_context_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    numeric_cols = [
        "threshold_quantile",
        "context_score",
        "trades",
        "win_rate",
        "median_net_return",
        "profit_factor",
        "net_total_return",
        "files_tested",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)

    required = [
        "context_type",
        "context_value",
        "target",
        "feature",
        "threshold_quantile",
        "threshold_side",
        "context_status",
        "trades",
        "win_rate",
        "median_net_return",
        "profit_factor",
        "net_total_return",
        "files_tested",
    ]

    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Context ranked file missing required columns: {missing}")

    return df


def classify_regime_quality(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    primary_mask = (
        (df["trades"] >= MIN_TRADES_PRIMARY)
        & (df["files_tested"] >= MIN_FILES_PRIMARY)
        & (df["profit_factor"] >= MIN_PROFIT_FACTOR_PRIMARY)
        & (df["win_rate"] >= MIN_WIN_RATE_PRIMARY)
        & (df["median_net_return"] > 0)
        & (df["net_total_return"] > 0)
    )

    secondary_mask = (
        (df["trades"] >= MIN_TRADES_SECONDARY)
        & (df["files_tested"] >= MIN_FILES_SECONDARY)
        & (df["profit_factor"] >= MIN_PROFIT_FACTOR_SECONDARY)
        & (df["win_rate"] >= MIN_WIN_RATE_SECONDARY)
        & (df["median_net_return"] > 0)
        & (df["net_total_return"] > 0)
    )

    df["regime_quality"] = np.select(
        [primary_mask, secondary_mask],
        ["regime_primary", "regime_secondary"],
        default="regime_watchlist_or_fail",
    )

    return df


def build_regime_id(row: pd.Series) -> str:
    parts = [
        str(row["target"]),
        str(row["feature"]),
        str(row["threshold_side"]),
        f"q{float(row['threshold_quantile']):.2f}",
        str(row["context_type"]),
        str(row["context_value"]),
    ]

    return "__".join(parts).replace(" ", "_")


def build_regime_registry(df: pd.DataFrame) -> pd.DataFrame:
    df = classify_regime_quality(df)

    df["regime_id"] = df.apply(build_regime_id, axis=1)

    df["edge_per_trade"] = df["net_total_return"] / df["trades"].replace(0, np.nan)

    df["sample_strength_score"] = (
        np.log1p(df["trades"].fillna(0))
        + np.log1p(df["files_tested"].fillna(0)) * 2
    )

    df["quality_score"] = (
        (df["profit_factor"].fillna(1.0) - 1.0) * 100
        + (df["win_rate"].fillna(0.5) - 0.5) * 100
        + df["median_net_return"].fillna(0) * 100000
        + df["edge_per_trade"].fillna(0) * 100000
        + df["sample_strength_score"].fillna(0)
    )

    df["deployment_bias"] = np.select(
        [
            df["context_type"].isin(["context_hour", "context_session"]),
            df["context_type"].isin(["context_session_spread", "context_session_vol"]),
            df["context_type"].isin(["context_spread_vol"]),
            df["context_type"].isin(["context_spread_regime", "context_vol_regime"]),
        ],
        [
            "time_context",
            "session_plus_single_regime",
            "spread_vol_combo",
            "single_market_regime",
        ],
        default="other_context",
    )

    df["research_note"] = np.select(
        [
            df["regime_quality"] == "regime_primary",
            df["regime_quality"] == "regime_secondary",
        ],
        [
            "High-priority regime candidate for replay / walk-forward testing.",
            "Secondary regime candidate; investigate stability and overlap.",
        ],
        default="Watchlist only; insufficient robustness for next-stage testing.",
    )

    ordered_cols = [
        "regime_id",
        "regime_quality",
        "deployment_bias",
        "context_type",
        "context_value",
        "target",
        "feature",
        "threshold_quantile",
        "threshold_side",
        "context_status",
        "quality_score",
        "context_score",
        "trades",
        "files_tested",
        "win_rate",
        "profit_factor",
        "median_net_return",
        "edge_per_trade",
        "net_total_return",
        "research_note",
    ]

    registry = df[ordered_cols].copy()

    registry = registry.sort_values(
        by=[
            "regime_quality",
            "quality_score",
            "profit_factor",
            "win_rate",
            "net_total_return",
        ],
        ascending=[True, False, False, False, False],
    )

    return registry


def build_context_summary(registry: pd.DataFrame) -> pd.DataFrame:
    summary = (
        registry.groupby(["context_type", "regime_quality"], dropna=False)
        .agg(
            regimes=("regime_id", "count"),
            total_trades=("trades", "sum"),
            median_win_rate=("win_rate", "median"),
            median_profit_factor=("profit_factor", "median"),
            median_edge_per_trade=("edge_per_trade", "median"),
            total_net_return=("net_total_return", "sum"),
            median_files_tested=("files_tested", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["regime_quality", "total_net_return", "median_profit_factor"],
        ascending=[True, False, False],
    )

    return summary


def build_feature_summary(registry: pd.DataFrame) -> pd.DataFrame:
    summary = (
        registry.groupby(["target", "feature", "regime_quality"], dropna=False)
        .agg(
            regimes=("regime_id", "count"),
            total_trades=("trades", "sum"),
            median_win_rate=("win_rate", "median"),
            median_profit_factor=("profit_factor", "median"),
            median_edge_per_trade=("edge_per_trade", "median"),
            total_net_return=("net_total_return", "sum"),
            median_files_tested=("files_tested", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["regime_quality", "total_net_return", "median_profit_factor"],
        ascending=[True, False, False],
    )

    return summary


def build_hour_summary(registry: pd.DataFrame) -> pd.DataFrame:
    hour_rows = registry[registry["context_type"] == "context_hour"].copy()

    if hour_rows.empty:
        return pd.DataFrame()

    hour_rows["hour"] = pd.to_numeric(hour_rows["context_value"], errors="coerce")

    summary = (
        hour_rows.groupby(["hour", "target", "feature", "regime_quality"], dropna=False)
        .agg(
            regimes=("regime_id", "count"),
            total_trades=("trades", "sum"),
            median_win_rate=("win_rate", "median"),
            median_profit_factor=("profit_factor", "median"),
            median_edge_per_trade=("edge_per_trade", "median"),
            total_net_return=("net_total_return", "sum"),
            median_files_tested=("files_tested", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["regime_quality", "hour", "median_profit_factor"],
        ascending=[True, True, False],
    )

    return summary


def write_outputs(
    symbol: str,
    registry: pd.DataFrame,
    context_summary: pd.DataFrame,
    feature_summary: pd.DataFrame,
    hour_summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    registry_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_registry_latest.csv"
    primary_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_primary_regimes_latest.csv"
    context_summary_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_context_summary_latest.csv"
    feature_summary_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_feature_summary_latest.csv"
    hour_summary_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_hour_summary_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_edge_report_latest.txt"

    registry.to_csv(registry_path, index=False)

    primary = registry[registry["regime_quality"] == "regime_primary"].copy()
    primary.to_csv(primary_path, index=False)

    context_summary.to_csv(context_summary_path, index=False)
    feature_summary.to_csv(feature_summary_path, index=False)
    hour_summary.to_csv(hour_summary_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 08 - REGIME EDGE ENGINE REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Total regimes: {len(registry)}\n")
        f.write(f"Primary regimes: {len(primary)}\n\n")

        f.write("REGIME QUALITY COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(registry["regime_quality"].value_counts().to_string())
        f.write("\n\n")

        f.write("DEPLOYMENT BIAS COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(registry["deployment_bias"].value_counts().to_string())
        f.write("\n\n")

        display_cols = [
            "regime_quality",
            "deployment_bias",
            "context_type",
            "context_value",
            "target",
            "feature",
            "threshold_quantile",
            "threshold_side",
            "quality_score",
            "trades",
            "files_tested",
            "win_rate",
            "profit_factor",
            "median_net_return",
            "edge_per_trade",
            "net_total_return",
        ]

        f.write("TOP 100 REGIME CANDIDATES\n")
        f.write("-" * 90 + "\n")
        f.write(registry[display_cols].head(100).to_string(index=False))
        f.write("\n\n")

        f.write("TOP 50 PRIMARY REGIMES ONLY\n")
        f.write("-" * 90 + "\n")
        if primary.empty:
            f.write("No primary regimes found.\n")
        else:
            primary_sorted = primary.sort_values(
                by=["profit_factor", "win_rate", "net_total_return", "trades"],
                ascending=[False, False, False, False],
            )
            f.write(primary_sorted[display_cols].head(50).to_string(index=False))
        f.write("\n\n")

        f.write("CONTEXT SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(context_summary.head(100).to_string(index=False))
        f.write("\n\n")

        f.write("FEATURE SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(feature_summary.head(100).to_string(index=False))
        f.write("\n\n")

        if not hour_summary.empty:
            f.write("HOUR SUMMARY\n")
            f.write("-" * 90 + "\n")
            f.write(hour_summary.head(100).to_string(index=False))

    print(f"Regime registry: {registry_path}")
    print(f"Primary regimes: {primary_path}")
    print(f"Context summary: {context_summary_path}")
    print(f"Feature summary: {feature_summary_path}")
    print(f"Hour summary:    {hour_summary_path}")
    print(f"Text report:     {txt_path}")


def main(symbol: str) -> None:
    print_header(symbol)

    context_ranked = load_context_ranked(symbol)
    context_ranked = clean_context_data(context_ranked)

    print(f"Context rows loaded: {len(context_ranked):,}")
    print("-" * 90)

    registry = build_regime_registry(context_ranked)
    context_summary = build_context_summary(registry)
    feature_summary = build_feature_summary(registry)
    hour_summary = build_hour_summary(registry)

    print(f"Regimes built: {len(registry):,}")
    print("Regime quality counts:")
    print(registry["regime_quality"].value_counts())
    print("-" * 90)

    write_outputs(
        symbol=symbol,
        registry=registry,
        context_summary=context_summary,
        feature_summary=feature_summary,
        hour_summary=hour_summary,
    )

    print("-" * 90)
    print("[DONE] Extended horizon regime edge engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)

    args = parser.parse_args()

    main(symbol=args.symbol.upper())