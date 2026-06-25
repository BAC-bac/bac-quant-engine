"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 13 - CANDIDATE REGISTRY ENGINE

Purpose:
    Build a clean candidate registry from:
        Script 10 - Monte Carlo Robustness
        Script 11 - Cross Symbol Transfer
        Script 12 - Cross Year Stability

Goal:
    Create a ranked shortlist of extended-horizon regimes suitable for
    deeper replay, paper trading, live monitoring and later pre-COVID testing.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_BASE_SYMBOL = "EURJPY"
DEFAULT_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]

BASE_DIR = Path("E:/Quant_Lab")

MC_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "monte_carlo_robustness"
)

TRANSFER_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cross_symbol_transfer"
)

STABILITY_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cross_year_stability"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "candidate_registry"
)


def print_header(base_symbol: str, symbols: list[str]) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 13 - CANDIDATE REGISTRY ENGINE")
    print("=" * 90)
    print(f"Base symbol: {base_symbol}")
    print(f"Symbols:     {symbols}")
    print(f"MC root:     {MC_ROOT}")
    print(f"Transfer:    {TRANSFER_ROOT}")
    print(f"Stability:   {STABILITY_ROOT}")
    print(f"Report root: {REPORT_ROOT}")
    print("-" * 90)


def symbol_suffix(symbols: list[str]) -> str:
    return "_".join([symbol.lower() for symbol in symbols])


def load_monte_carlo(base_symbol: str) -> pd.DataFrame:
    path = MC_ROOT / f"{base_symbol.lower()}_extended_horizon_monte_carlo_ranked_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 10 Monte Carlo ranked file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Monte Carlo ranked file is empty.")

    df = df.copy()
    df["candidate_symbol"] = base_symbol
    df["candidate_source"] = "base_monte_carlo"

    return df


def load_transfer(base_symbol: str, symbols: list[str]) -> pd.DataFrame:
    suffix = symbol_suffix(symbols)

    path = (
        TRANSFER_ROOT
        / f"{base_symbol.lower()}_to_{suffix}_cross_symbol_transfer_ranked_latest.csv"
    )

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 11 transfer ranked file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Transfer ranked file is empty.")

    return df


def load_year_stability(base_symbol: str, symbols: list[str]) -> pd.DataFrame:
    suffix = symbol_suffix(symbols)

    path = (
        STABILITY_ROOT
        / f"{base_symbol.lower()}_to_{suffix}_cross_year_stability_ranked_latest.csv"
    )

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 12 year stability ranked file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Year stability ranked file is empty.")

    return df


def normalise_base_candidates(mc: pd.DataFrame, base_symbol: str) -> pd.DataFrame:
    df = mc.copy()

    rename_map = {
        "regime_id": "candidate_id",
        "mc_status": "candidate_status_source",
        "robustness_score": "source_score",
        "probability_profitable": "probability_profitable",
        "net_total_return": "net_total_return",
        "files_tested": "files_tested",
        "total_trades": "total_trades",
        "median_net_win_rate": "median_net_win_rate",
        "median_net_profit_factor": "median_net_profit_factor",
    }

    available = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=available)

    required_defaults = {
        "candidate_id": np.nan,
        "candidate_status_source": "unknown",
        "source_score": np.nan,
        "probability_profitable": np.nan,
        "net_total_return": np.nan,
        "files_tested": np.nan,
        "total_trades": np.nan,
        "median_net_win_rate": np.nan,
        "median_net_profit_factor": np.nan,
        "context_type": np.nan,
        "context_value": np.nan,
        "target": np.nan,
        "feature": np.nan,
        "threshold_quantile": np.nan,
        "threshold_side": np.nan,
    }

    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default

    df["test_symbol"] = base_symbol
    df["base_symbol"] = base_symbol
    df["candidate_layer"] = "base_symbol_mc"
    df["transfer_status"] = "base_symbol"
    df["year_stability_status"] = "base_symbol_not_cross_year_tested"
    df["positive_year_rate"] = np.nan
    df["min_year_return"] = np.nan
    df["median_year_return"] = np.nan

    return df[
        [
            "base_symbol",
            "test_symbol",
            "candidate_layer",
            "candidate_id",
            "candidate_status_source",
            "transfer_status",
            "year_stability_status",
            "context_type",
            "context_value",
            "target",
            "feature",
            "threshold_quantile",
            "threshold_side",
            "source_score",
            "probability_profitable",
            "files_tested",
            "total_trades",
            "net_total_return",
            "positive_year_rate",
            "min_year_return",
            "median_year_return",
            "median_net_win_rate",
            "median_net_profit_factor",
        ]
    ]


def normalise_transfer_candidates(transfer: pd.DataFrame, stability: pd.DataFrame, base_symbol: str) -> pd.DataFrame:
    tr = transfer.copy()

    tr = tr.rename(
        columns={
            "source_regime_id": "candidate_id",
            "transfer_score": "source_score",
        }
    )

    st = stability.copy()

    stability_cols = [
        "test_symbol",
        "source_regime_id",
        "year_stability_status",
        "years_tested",
        "positive_years",
        "failed_years",
        "positive_year_rate",
        "year_stability_score",
        "min_year_return",
        "median_year_return",
    ]

    stability_cols = [col for col in stability_cols if col in st.columns]

    st = st[stability_cols].rename(columns={"source_regime_id": "candidate_id"})

    merged = tr.merge(
        st,
        on=["test_symbol", "candidate_id"],
        how="left",
    )

    required_defaults = {
        "transfer_status": "unknown",
        "year_stability_status": "not_tested",
        "source_score": np.nan,
        "probability_profitable": np.nan,
        "net_total_return": np.nan,
        "files_tested": np.nan,
        "total_trades": np.nan,
        "median_net_win_rate": np.nan,
        "median_net_profit_factor": np.nan,
        "positive_year_rate": np.nan,
        "min_year_return": np.nan,
        "median_year_return": np.nan,
        "context_type": np.nan,
        "context_value": np.nan,
        "target": np.nan,
        "feature": np.nan,
        "threshold_quantile": np.nan,
        "threshold_side": np.nan,
    }

    for col, default in required_defaults.items():
        if col not in merged.columns:
            merged[col] = default

    merged["base_symbol"] = base_symbol
    merged["candidate_layer"] = "cross_symbol_transfer"
    merged["candidate_status_source"] = merged["transfer_status"]

    return merged[
        [
            "base_symbol",
            "test_symbol",
            "candidate_layer",
            "candidate_id",
            "candidate_status_source",
            "transfer_status",
            "year_stability_status",
            "context_type",
            "context_value",
            "target",
            "feature",
            "threshold_quantile",
            "threshold_side",
            "source_score",
            "probability_profitable",
            "files_tested",
            "total_trades",
            "net_total_return",
            "positive_year_rate",
            "min_year_return",
            "median_year_return",
            "median_net_win_rate",
            "median_net_profit_factor",
        ]
    ]


def build_validation_passport(row: pd.Series) -> str:
    checks = []

    checks.append("✓ Candidate Registry")

    status_source = str(row.get("candidate_status_source", ""))
    transfer_status = str(row.get("transfer_status", ""))
    year_status = str(row.get("year_stability_status", ""))

    if "mc_pass_primary" in status_source or "mc_pass_secondary" in status_source:
        checks.append("✓ Monte Carlo")
    else:
        checks.append("□ Monte Carlo")

    if transfer_status in ["transfer_pass_primary", "transfer_pass_secondary", "base_symbol"]:
        checks.append("✓ Cross Symbol")
    else:
        checks.append("□ Cross Symbol")

    if year_status in [
        "year_stable_primary",
        "year_stable_secondary",
        "year_positive_but_unstable",
        "base_symbol_not_cross_year_tested",
    ]:
        checks.append("✓ Cross Year")
    else:
        checks.append("□ Cross Year")

    checks.append("□ Pre-COVID")
    checks.append("□ Walk Forward")
    checks.append("□ Paper Trading")
    checks.append("□ Live")

    return " | ".join(checks)


def score_candidates(registry: pd.DataFrame) -> pd.DataFrame:
    df = registry.copy()

    numeric_cols = [
        "source_score",
        "probability_profitable",
        "files_tested",
        "total_trades",
        "net_total_return",
        "positive_year_rate",
        "min_year_return",
        "median_year_return",
        "median_net_win_rate",
        "median_net_profit_factor",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)

    df["mc_bonus"] = np.where(
        df["candidate_status_source"].astype(str).str.contains("primary", case=False, na=False),
        25,
        np.where(
            df["candidate_status_source"].astype(str).str.contains("secondary", case=False, na=False),
            10,
            0,
        ),
    )

    df["transfer_bonus"] = np.select(
        [
            df["transfer_status"] == "transfer_pass_primary",
            df["transfer_status"] == "transfer_pass_secondary",
            df["transfer_status"] == "base_symbol",
        ],
        [25, 10, 20],
        default=-20,
    )

    df["year_bonus"] = np.select(
        [
            df["year_stability_status"] == "year_stable_primary",
            df["year_stability_status"] == "year_stable_secondary",
            df["year_stability_status"] == "year_positive_but_unstable",
            df["year_stability_status"] == "base_symbol_not_cross_year_tested",
        ],
        [35, 20, 5, 15],
        default=-25,
    )

    df["candidate_score"] = (
        df["mc_bonus"]
        + df["transfer_bonus"]
        + df["year_bonus"]
        + df["probability_profitable"].fillna(0.5) * 25
        + np.log1p(df["total_trades"].fillna(0))
        + df["median_net_win_rate"].fillna(0.5) * 20
        + df["median_net_profit_factor"].fillna(1.0) * 10
        + df["positive_year_rate"].fillna(0.5) * 25
        + df["min_year_return"].fillna(0) * 0.05
        + df["net_total_return"].fillna(0) * 0.005
    )

    df["research_confidence_score"] = (
        np.where(
            df["candidate_status_source"].astype(str).str.contains("mc_pass_primary", case=False, na=False),
            25,
            np.where(
                df["candidate_status_source"].astype(str).str.contains("mc_pass_secondary", case=False, na=False),
                15,
                0,
            ),
        )
        + np.where(df["transfer_status"] == "transfer_pass_primary", 25, 0)
        + np.where(df["transfer_status"] == "transfer_pass_secondary", 15, 0)
        + np.where(df["transfer_status"] == "base_symbol", 20, 0)
        + np.where(df["year_stability_status"] == "year_stable_primary", 25, 0)
        + np.where(df["year_stability_status"] == "year_stable_secondary", 15, 0)
        + np.where(df["year_stability_status"] == "year_positive_but_unstable", 7, 0)
        + np.where(df["year_stability_status"] == "base_symbol_not_cross_year_tested", 10, 0)
        + np.where(df["positive_year_rate"].fillna(0) >= 1.0, 20, 0)
        + np.where(df["probability_profitable"].fillna(0) >= 0.90, 15, 0)
        + np.where(df["total_trades"].fillna(0) >= 5_000_000, 15, 0)
        + np.where(df["total_trades"].fillna(0) >= 1_000_000, 8, 0)
        + np.where(df["files_tested"].fillna(0) >= 500, 10, 0)
        + np.where(df["median_net_profit_factor"].fillna(0) >= 2.0, 10, 0)
        + np.where(df["median_net_win_rate"].fillna(0) >= 0.60, 10, 0)
    )

    df["research_confidence_class"] = np.select(
        [
            df["research_confidence_score"] >= 140,
            df["research_confidence_score"] >= 110,
            df["research_confidence_score"] >= 80,
            df["research_confidence_score"] >= 50,
        ],
        [
            "institutional_grade",
            "high_confidence",
            "promising",
            "early_evidence",
        ],
        default="experimental",
    )

    df["validation_passport"] = df.apply(build_validation_passport, axis=1)

    df["candidate_tier"] = np.select(
        [
            (df["candidate_score"] >= 130)
            & (
                df["year_stability_status"].isin(["year_stable_primary", "year_stable_secondary"])
                | (df["candidate_layer"] == "base_symbol_mc")
            ),

            (df["candidate_score"] >= 105)
            & (
                df["year_stability_status"].isin(
                    [
                        "year_stable_primary",
                        "year_stable_secondary",
                        "year_positive_but_unstable",
                        "base_symbol_not_cross_year_tested",
                    ]
                )
            ),

            df["candidate_score"] >= 80,
        ],
        [
            "tier_1_priority_candidate",
            "tier_2_research_candidate",
            "tier_3_watchlist_candidate",
        ],
        default="reject_or_hold",
    )

    df["recommended_next_step"] = np.select(
        [
            df["candidate_tier"] == "tier_1_priority_candidate",
            df["candidate_tier"] == "tier_2_research_candidate",
            df["candidate_tier"] == "tier_3_watchlist_candidate",
        ],
        [
            "Promote to detailed replay, drawdown, pre-COVID and paper-trading research.",
            "Keep for additional context, year, and symbol-specific validation.",
            "Monitor but do not prioritise for deployment research yet.",
        ],
        default="Hold or reject unless future evidence improves.",
    )

    df = df.sort_values(
        by=[
            "candidate_tier",
            "candidate_score",
            "test_symbol",
            "net_total_return",
        ],
        ascending=[True, False, True, False],
    )

    return df


def build_symbol_summary(registry: pd.DataFrame) -> pd.DataFrame:
    summary = (
        registry.groupby(["test_symbol", "candidate_tier"], dropna=False)
        .agg(
            candidates=("candidate_id", "count"),
            total_trades=("total_trades", "sum"),
            total_net_return=("net_total_return", "sum"),
            median_candidate_score=("candidate_score", "median"),
            median_win_rate=("median_net_win_rate", "median"),
            median_profit_factor=("median_net_profit_factor", "median"),
            median_positive_year_rate=("positive_year_rate", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["test_symbol", "candidate_tier", "total_net_return"],
        ascending=[True, True, False],
    )

    return summary


def write_outputs(base_symbol: str, symbols: list[str], registry: pd.DataFrame, summary: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    suffix = symbol_suffix(symbols)

    registry_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_candidate_registry_latest.csv"
    priority_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_priority_candidates_latest.csv"
    summary_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_candidate_symbol_summary_latest.csv"
    txt_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{suffix}_candidate_registry_report_latest.txt"

    registry.to_csv(registry_path, index=False)

    priority = registry[
        registry["candidate_tier"].isin(
            ["tier_1_priority_candidate", "tier_2_research_candidate"]
        )
    ].copy()

    priority.to_csv(priority_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 13 - CANDIDATE REGISTRY REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Base symbol: {base_symbol}\n")
        f.write(f"Symbols: {symbols}\n")
        f.write(f"Registry rows: {len(registry)}\n")
        f.write(f"Priority candidates: {len(priority)}\n\n")

        f.write("CANDIDATE TIER COUNTS\n")
        f.write("-" * 90 + "\n")
        f.write(registry.groupby(["test_symbol", "candidate_tier"]).size().to_string())
        f.write("\n\n")

        display_cols = [
            "test_symbol",
            "candidate_tier",
            "candidate_score",
            "research_confidence_score",
            "research_confidence_class",
            "validation_passport",
            "candidate_id",
            "candidate_layer",
            "transfer_status",
            "year_stability_status",
            "context_type",
            "context_value",
            "target",
            "feature",
            "threshold_quantile",
            "threshold_side",
            "files_tested",
            "total_trades",
            "net_total_return",
            "positive_year_rate",
            "min_year_return",
            "median_net_win_rate",
            "median_net_profit_factor",
            "recommended_next_step",
        ]

        f.write("TOP 100 CANDIDATES\n")
        f.write("-" * 90 + "\n")
        f.write(registry[display_cols].head(100).to_string(index=False))
        f.write("\n\n")

        f.write("SYMBOL SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(summary.to_string(index=False))

    print(f"Candidate registry: {registry_path}")
    print(f"Priority candidates: {priority_path}")
    print(f"Symbol summary:      {summary_path}")
    print(f"Text report:         {txt_path}")


def main(base_symbol: str, symbols: list[str]) -> None:
    base_symbol = base_symbol.upper()
    symbols = [symbol.upper() for symbol in symbols]

    print_header(base_symbol, symbols)

    mc = load_monte_carlo(base_symbol)
    transfer = load_transfer(base_symbol, symbols)
    stability = load_year_stability(base_symbol, symbols)

    base_candidates = normalise_base_candidates(mc, base_symbol)
    transfer_candidates = normalise_transfer_candidates(transfer, stability, base_symbol)

    registry = pd.concat([base_candidates, transfer_candidates], ignore_index=True)
    registry = score_candidates(registry)
    summary = build_symbol_summary(registry)

    print(f"Base candidates:     {len(base_candidates):,}")
    print(f"Transfer candidates: {len(transfer_candidates):,}")
    print(f"Registry rows:       {len(registry):,}")
    print("-" * 90)
    print("Candidate tier counts:")
    print(registry.groupby(["test_symbol", "candidate_tier"]).size())
    print("-" * 90)

    write_outputs(base_symbol, symbols, registry, summary)

    print("-" * 90)
    print("[DONE] Candidate registry complete")
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