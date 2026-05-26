"""
BACQE REGIME ENGINE - 56 Build Strategy Performance Registry

Creates a unified strategy performance registry from the strongest audited
strategy-performance files discovered in script 55.

Primary sources:
    strategy_performance_by_symbol_regime_small_latest
    strategy_performance_global_by_regime_small_latest
    strategy_performance_best_by_regime_small_latest
    router_strategy_validation_small_latest
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
BROKER = "FTMO"

PERFORMANCE_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_performance" / BROKER
VALIDATION_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "regime_router_validation" / BROKER

SOURCE_FILES = {
    "symbol_regime_performance": PERFORMANCE_ROOT / "strategy_performance_by_symbol_regime_small_latest.parquet",
    "global_regime_performance": PERFORMANCE_ROOT / "strategy_performance_global_by_regime_small_latest.parquet",
    "best_by_regime": PERFORMANCE_ROOT / "strategy_performance_best_by_regime_small_latest.parquet",
    "router_validation": VALIDATION_ROOT / "router_strategy_validation_small_latest.parquet",
}

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "strategy_performance_registry"


def read_table(path: Path, source_name: str) -> pd.DataFrame:
    if not path.exists():
        print(f"[WARN] Missing source: {source_name} -> {path}")
        return pd.DataFrame()

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False)
    else:
        print(f"[WARN] Unsupported source: {source_name} -> {path}")
        return pd.DataFrame()

    df["registry_source"] = source_name
    df["source_file"] = str(path)
    return df


def standardise_symbol_regime_performance(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rename_map = {
        "strategy_name": "strategy_name",
        "symbol": "symbol",
        "timeframe": "timeframe",
        "composite_regime": "composite_regime",
        "avg_return": "avg_return",
        "median_return": "median_return",
        "total_return_proxy": "total_return_proxy",
        "win_rate_pct": "win_rate_pct",
        "profit_factor": "profit_factor",
        "sharpe_proxy": "sharpe_proxy",
        "trade_count": "trade_count",
        "observation_count": "observation_count",
        "timeframe_rank": "timeframe_rank",
    }

    out = pd.DataFrame()

    for source_col, target_col in rename_map.items():
        out[target_col] = df[source_col] if source_col in df.columns else pd.NA

    out["performance_scope"] = "symbol_regime"
    out["registry_source"] = df.get("registry_source", "symbol_regime_performance")
    out["source_file"] = df.get("source_file", pd.NA)

    return out


def standardise_global_regime_performance(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()

    out["strategy_name"] = df["strategy_name"] if "strategy_name" in df.columns else pd.NA
    out["symbol"] = "GLOBAL"
    out["timeframe"] = df["timeframe"] if "timeframe" in df.columns else pd.NA
    out["composite_regime"] = df["composite_regime"] if "composite_regime" in df.columns else pd.NA
    out["avg_return"] = df["avg_return_mean"] if "avg_return_mean" in df.columns else pd.NA
    out["median_return"] = df["median_return_mean"] if "median_return_mean" in df.columns else pd.NA
    out["total_return_proxy"] = df["total_return_proxy_sum"] if "total_return_proxy_sum" in df.columns else pd.NA
    out["win_rate_pct"] = df["win_rate_mean_pct"] if "win_rate_mean_pct" in df.columns else pd.NA
    out["profit_factor"] = df["profit_factor_median"] if "profit_factor_median" in df.columns else pd.NA
    out["sharpe_proxy"] = df["sharpe_proxy_median"] if "sharpe_proxy_median" in df.columns else pd.NA
    out["trade_count"] = df["symbol_count"] if "symbol_count" in df.columns else pd.NA
    out["observation_count"] = df["symbol_count"] if "symbol_count" in df.columns else pd.NA
    out["timeframe_rank"] = df["timeframe_rank"] if "timeframe_rank" in df.columns else pd.NA

    out["performance_scope"] = "global_regime"
    out["registry_source"] = df.get("registry_source", "global_regime_performance")
    out["source_file"] = df.get("source_file", pd.NA)

    return out


def standardise_best_by_regime(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = standardise_global_regime_performance(df)
    out["performance_scope"] = "best_by_regime"
    return out


def standardise_router_validation(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()

    out["strategy_name"] = df["historical_best_strategy"] if "historical_best_strategy" in df.columns else df.get("selected_proxy_strategy", pd.NA)
    out["symbol"] = df["symbol"] if "symbol" in df.columns else pd.NA
    out["timeframe"] = df["timeframe"] if "timeframe" in df.columns else pd.NA
    out["composite_regime"] = df["current_regime"] if "current_regime" in df.columns else pd.NA
    out["avg_return"] = pd.NA
    out["median_return"] = pd.NA
    out["total_return_proxy"] = pd.NA
    out["win_rate_pct"] = df["best_strategy_win_rate_pct"] if "best_strategy_win_rate_pct" in df.columns else df.get("evidence_win_rate_pct", pd.NA)
    out["profit_factor"] = df["best_strategy_profit_factor"] if "best_strategy_profit_factor" in df.columns else df.get("evidence_profit_factor", pd.NA)
    out["sharpe_proxy"] = df["evidence_sharpe_proxy"] if "evidence_sharpe_proxy" in df.columns else pd.NA
    out["trade_count"] = df["best_strategy_observations"] if "best_strategy_observations" in df.columns else pd.NA
    out["observation_count"] = df["best_strategy_observations"] if "best_strategy_observations" in df.columns else pd.NA
    out["timeframe_rank"] = df["timeframe_rank"] if "timeframe_rank" in df.columns else pd.NA

    out["recommended_strategy_family"] = df["recommended_strategy_family"] if "recommended_strategy_family" in df.columns else pd.NA
    out["selected_proxy_strategy"] = df["selected_proxy_strategy"] if "selected_proxy_strategy" in df.columns else pd.NA
    out["forecast_signal"] = df["forecast_signal"] if "forecast_signal" in df.columns else pd.NA

    out["performance_scope"] = "router_validation"
    out["registry_source"] = df.get("registry_source", "router_validation")
    out["source_file"] = df.get("source_file", pd.NA)

    return out


def add_quality_scores(registry: pd.DataFrame) -> pd.DataFrame:
    df = registry.copy()

    numeric_cols = [
        "avg_return",
        "median_return",
        "total_return_proxy",
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "trade_count",
        "observation_count",
        "timeframe_rank",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["evidence_quality"] = "low"

    df.loc[
        (df["observation_count"].fillna(0) >= 30)
        | (df["trade_count"].fillna(0) >= 30),
        "evidence_quality",
    ] = "medium"

    df.loc[
        (df["observation_count"].fillna(0) >= 100)
        | (df["trade_count"].fillna(0) >= 100),
        "evidence_quality",
    ] = "higher"

    df["performance_score"] = 0.0

    df["performance_score"] += df["win_rate_pct"].fillna(50) - 50
    df["performance_score"] += (df["profit_factor"].fillna(1) - 1) * 25
    df["performance_score"] += df["sharpe_proxy"].fillna(0) * 10
    df["performance_score"] += df["total_return_proxy"].fillna(0) * 100

    df["performance_score"] = df["performance_score"].round(6)

    df["performance_label"] = "neutral_or_unknown"
    df.loc[df["performance_score"] >= 25, "performance_label"] = "strong_positive"
    df.loc[(df["performance_score"] >= 10) & (df["performance_score"] < 25), "performance_label"] = "positive"
    df.loc[(df["performance_score"] <= -10) & (df["performance_score"] > -25), "performance_label"] = "negative"
    df.loc[df["performance_score"] <= -25, "performance_label"] = "strong_negative"

    df["registry_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return df


def build_registry() -> pd.DataFrame:
    symbol_perf = read_table(SOURCE_FILES["symbol_regime_performance"], "symbol_regime_performance")
    global_perf = read_table(SOURCE_FILES["global_regime_performance"], "global_regime_performance")
    best_perf = read_table(SOURCE_FILES["best_by_regime"], "best_by_regime")
    validation = read_table(SOURCE_FILES["router_validation"], "router_validation")

    parts = [
        standardise_symbol_regime_performance(symbol_perf),
        standardise_global_regime_performance(global_perf),
        standardise_best_by_regime(best_perf),
        standardise_router_validation(validation),
    ]

    parts = [p for p in parts if not p.empty]

    if not parts:
        return pd.DataFrame()

    registry = pd.concat(parts, ignore_index=True)

    for col in [
        "recommended_strategy_family",
        "selected_proxy_strategy",
        "forecast_signal",
    ]:
        if col not in registry.columns:
            registry[col] = pd.NA

    registry = add_quality_scores(registry)

    key_cols = [
        "performance_scope",
        "symbol",
        "timeframe",
        "composite_regime",
        "strategy_name",
    ]

    registry = registry.sort_values(
        ["performance_score", "evidence_quality"],
        ascending=[False, True],
    ).drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)

    return registry


def build_summary(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty:
        return pd.DataFrame()

    summary = (
        registry.groupby(["performance_scope", "performance_label", "evidence_quality"], dropna=False)
        .agg(
            rows=("strategy_name", "count"),
            avg_performance_score=("performance_score", "mean"),
            avg_win_rate_pct=("win_rate_pct", "mean"),
            avg_profit_factor=("profit_factor", "mean"),
            avg_sharpe_proxy=("sharpe_proxy", "mean"),
        )
        .reset_index()
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(6)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary


def build_report(registry: pd.DataFrame, summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []
    lines.append("=" * 130)
    lines.append("BACQE STRATEGY PERFORMANCE REGISTRY")
    lines.append("=" * 130)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append("-" * 130)

    lines.append("")
    lines.append("SOURCE FILES")
    lines.append("-" * 130)
    for name, path in SOURCE_FILES.items():
        lines.append(f"{name:<35} {path}")

    lines.append("")
    lines.append("REGISTRY SUMMARY")
    lines.append("-" * 130)
    lines.append(f"Registry rows: {len(registry):,}")

    if not summary.empty:
        lines.append("")
        lines.append(summary.to_string(index=False))

    lines.append("")
    lines.append("TOP STRATEGY PERFORMANCE RECORDS")
    lines.append("-" * 130)

    if registry.empty:
        lines.append("No registry records created.")
    else:
        display_cols = [
            "performance_scope",
            "symbol",
            "timeframe",
            "composite_regime",
            "strategy_name",
            "win_rate_pct",
            "profit_factor",
            "sharpe_proxy",
            "total_return_proxy",
            "performance_score",
            "performance_label",
            "evidence_quality",
        ]
        lines.append(registry[display_cols].head(50).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 130)
    lines.append("This registry unifies strategy performance evidence across regime-performance and router-validation outputs.")
    lines.append("performance_score is a simple research score, not a final trading metric.")
    lines.append("Evidence quality is based on available observation/trade counts.")
    lines.append("Script 57 can now join this registry to current regime/strategy environments.")
    lines.append("=" * 130)

    return "\n".join(lines)


def main() -> None:
    print("=" * 130)
    print("BACQE REGIME ENGINE - 56 BUILD STRATEGY PERFORMANCE REGISTRY")
    print("=" * 130)

    registry = build_registry()
    summary = build_summary(registry)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    registry_csv = OUTPUT_ANALYSIS_DIR / "strategy_performance_registry_latest.csv"
    registry_parquet = OUTPUT_ANALYSIS_DIR / "strategy_performance_registry_latest.parquet"
    registry_json = OUTPUT_ANALYSIS_DIR / "strategy_performance_registry_latest.json"

    summary_csv = OUTPUT_ANALYSIS_DIR / "strategy_performance_registry_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "strategy_performance_registry_summary_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "strategy_performance_registry_latest.txt"

    registry.to_csv(registry_csv, index=False)
    registry.to_parquet(registry_parquet, index=False)

    with open(registry_json, "w", encoding="utf-8") as f:
        json.dump(registry.to_dict(orient="records"), f, indent=4, default=str)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(registry, summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Strategy performance registry created.")
    print(f"Registry CSV:     {registry_csv}")
    print(f"Registry Parquet: {registry_parquet}")
    print(f"Registry JSON:    {registry_json}")
    print(f"Summary CSV:      {summary_csv}")
    print(f"Summary Parquet:  {summary_parquet}")
    print(f"Report:           {report_path}")
    print("-" * 130)
    print(report)
    print("=" * 130)


if __name__ == "__main__":
    main()