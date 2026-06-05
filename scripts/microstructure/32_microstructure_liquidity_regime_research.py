"""
BACQE MICROSTRUCTURE 32 - LIQUIDITY REGIME RESEARCH

Purpose:
    Investigate whether spread/liquidity state changes model behaviour.

Inputs:
    research_datasets/**/microstructure_research_dataset.parquet
    playbooks/microstructure_playbook_latest.csv

Outputs:
    liquidity_regime_research/
        microstructure_liquidity_regime_research_latest.csv
        microstructure_liquidity_regime_summary_latest.csv
        microstructure_liquidity_regime_research_latest.json
        microstructure_liquidity_regime_research_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SPREAD_FEATURES = [
    # Native / previously expected spread features
    "avg_spread",
    "avg_spread_mean_3",
    "avg_spread_mean_5",
    "avg_spread_mean_10",
    "avg_spread_mean_20",
    "avg_spread_zscore_10",
    "avg_spread_zscore_20",

    # Derived spread features added by this script when bid/ask columns exist
    "open_spread",
    "high_spread",
    "low_spread",
    "close_spread",
    "spread_mean",
    "spread_max",
    "spread_min",
    "spread_range",
    "spread_pct_of_mid",
    "spread_mean_3",
    "spread_mean_5",
    "spread_mean_10",
    "spread_zscore_10",
    "spread_zscore_20",
]

TARGET_COLUMNS = [
    "forward_return_1",
    "forward_return_3",
    "forward_return_5",
]

REGIME_LABELS = [
    "tight_liquidity",
    "normal_liquidity",
    "wide_liquidity",
    "extreme_wide_liquidity",
]


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def parse_dataset_metadata(file_path: Path) -> dict:
    metadata = {
        "symbol": None,
        "bar_type": None,
        "parameter": None,
    }

    for part in file_path.parts:
        if part.startswith("symbol="):
            metadata["symbol"] = part.replace("symbol=", "")
        elif part.startswith("bar_type="):
            metadata["bar_type"] = part.replace("bar_type=", "")
        elif part.startswith("parameter="):
            metadata["parameter"] = part.replace("parameter=", "")

    return metadata


def add_derived_spread_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add spread features when research datasets contain bid/ask OHLC columns
    but do not yet contain explicit spread columns.

    The current research datasets include columns such as:
        open_ask/open_bid, high_ask/high_bid, low_ask/low_bid, close_ask/close_bid

    Script 32 originally expected pre-built spread features only, which caused every
    dataset to be marked as 'no_spread_features'. This function bridges that gap.
    """
    df = df.copy()

    spread_pairs = {
        "open_spread": ("open_ask", "open_bid"),
        "high_spread": ("high_ask", "high_bid"),
        "low_spread": ("low_ask", "low_bid"),
        "close_spread": ("close_ask", "close_bid"),
    }

    for spread_col, (ask_col, bid_col) in spread_pairs.items():
        if spread_col not in df.columns and {ask_col, bid_col}.issubset(df.columns):
            ask = pd.to_numeric(df[ask_col], errors="coerce")
            bid = pd.to_numeric(df[bid_col], errors="coerce")
            df[spread_col] = ask - bid

    base_spread_cols = [
        col for col in ["open_spread", "high_spread", "low_spread", "close_spread"]
        if col in df.columns
    ]

    if base_spread_cols:
        df["spread_mean"] = df[base_spread_cols].mean(axis=1)
        df["spread_max"] = df[base_spread_cols].max(axis=1)
        df["spread_min"] = df[base_spread_cols].min(axis=1)
        df["spread_range"] = df["spread_max"] - df["spread_min"]

        if "close_mid" in df.columns:
            close_mid = pd.to_numeric(df["close_mid"], errors="coerce").replace(0, np.nan)
            df["spread_pct_of_mid"] = df["spread_mean"] / close_mid

        # Rolling features make the liquidity-state logic more stable than using
        # one bar's spread in isolation.
        for window in [3, 5, 10]:
            df[f"spread_mean_{window}"] = df["spread_mean"].rolling(window=window, min_periods=1).mean()

        for window in [10, 20]:
            rolling_mean = df["spread_mean"].rolling(window=window, min_periods=3).mean()
            rolling_std = df["spread_mean"].rolling(window=window, min_periods=3).std()
            df[f"spread_zscore_{window}"] = (
                (df["spread_mean"] - rolling_mean) / rolling_std.replace(0, np.nan)
            )

    return df


def empty_summary_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
        "liquidity_regime",
        "datasets",
        "total_rows",
        "avg_rows",
        "avg_target_mean",
        "avg_positive_return_rate",
        "avg_spread_value",
        "avg_abs_correlation",
        "max_abs_correlation",
        "avg_correlation_sample_size",
        "regime_score",
        "summary_rank",
        "created_at_utc",
    ])


def empty_transition_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "tight_liquidity",
        "normal_liquidity",
        "wide_liquidity",
        "extreme_wide_liquidity",
        "tight_to_extreme_delta",
        "normal_to_wide_delta",
        "absolute_tight_to_extreme_delta",
        "absolute_normal_to_wide_delta",
        "transition_score",
        "transition_rank",
        "created_at_utc",
    ])


def assign_liquidity_regimes(df: pd.DataFrame, spread_col: str) -> pd.Series:
    spread = pd.to_numeric(df[spread_col], errors="coerce")

    q25 = spread.quantile(0.25)
    q75 = spread.quantile(0.75)
    q90 = spread.quantile(0.90)

    regimes = pd.Series(index=df.index, dtype="object")

    regimes[spread <= q25] = "tight_liquidity"
    regimes[(spread > q25) & (spread <= q75)] = "normal_liquidity"
    regimes[(spread > q75) & (spread <= q90)] = "wide_liquidity"
    regimes[spread > q90] = "extreme_wide_liquidity"

    regimes[spread.isna()] = "unknown_liquidity"

    return regimes


def safe_corr(x: pd.Series, y: pd.Series) -> tuple[float | None, int]:
    pair = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    sample_size = len(pair)

    if sample_size < 30:
        return None, sample_size

    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return None, sample_size

    corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])

    if pd.isna(corr):
        return None, sample_size

    return float(corr), sample_size


def summarise_regime(
    df: pd.DataFrame,
    metadata: dict,
    spread_col: str,
    target: str,
    regime: str,
) -> dict:
    regime_df = df[df["liquidity_regime"] == regime].copy()

    record = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": metadata["symbol"],
        "bar_type": metadata["bar_type"],
        "parameter": metadata["parameter"],
        "spread_feature": spread_col,
        "target": target,
        "liquidity_regime": regime,
        "row_count": len(regime_df),
        "target_mean": None,
        "target_median": None,
        "target_std": None,
        "positive_return_rate": None,
        "avg_spread_value": None,
        "median_spread_value": None,
        "spread_target_correlation": None,
        "correlation_sample_size": 0,
        "absolute_correlation": None,
        "status": "unknown",
    }

    if regime_df.empty:
        record["status"] = "empty_regime"
        return record

    if target not in regime_df.columns:
        record["status"] = "missing_target"
        return record

    target_series = pd.to_numeric(regime_df[target], errors="coerce")
    spread_series = pd.to_numeric(regime_df[spread_col], errors="coerce")

    valid_target = target_series.dropna()

    if valid_target.empty:
        record["status"] = "no_valid_target"
        return record

    record["target_mean"] = float(valid_target.mean())
    record["target_median"] = float(valid_target.median())
    record["target_std"] = float(valid_target.std())
    record["positive_return_rate"] = float((valid_target > 0).mean())
    record["avg_spread_value"] = float(spread_series.mean())
    record["median_spread_value"] = float(spread_series.median())

    corr, sample_size = safe_corr(spread_series, target_series)

    record["spread_target_correlation"] = corr
    record["correlation_sample_size"] = sample_size
    record["absolute_correlation"] = abs(corr) if corr is not None else None
    record["status"] = "ok"

    return record


def analyse_dataset(file_path: Path) -> list[dict]:
    metadata = parse_dataset_metadata(file_path)

    try:
        df = pd.read_parquet(file_path)
        df = add_derived_spread_features(df)
    except Exception as exc:
        return [{
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": metadata["symbol"],
            "bar_type": metadata["bar_type"],
            "parameter": metadata["parameter"],
            "spread_feature": None,
            "target": None,
            "liquidity_regime": None,
            "row_count": 0,
            "status": "failed_read",
            "error": str(exc),
        }]

    if df.empty:
        return [{
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": metadata["symbol"],
            "bar_type": metadata["bar_type"],
            "parameter": metadata["parameter"],
            "spread_feature": None,
            "target": None,
            "liquidity_regime": None,
            "row_count": 0,
            "status": "empty_dataset",
            "error": "Dataset empty.",
        }]

    records = []

    available_spread_features = [col for col in SPREAD_FEATURES if col in df.columns]
    available_targets = [col for col in TARGET_COLUMNS if col in df.columns]

    if not available_spread_features:
        return [{
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": metadata["symbol"],
            "bar_type": metadata["bar_type"],
            "parameter": metadata["parameter"],
            "spread_feature": None,
            "target": None,
            "liquidity_regime": None,
            "row_count": len(df),
            "status": "no_spread_features",
            "error": "No native or derived spread features available. Expected explicit spread fields or bid/ask OHLC columns.",
        }]

    for spread_col in available_spread_features:
        working_df = df.copy()
        working_df["liquidity_regime"] = assign_liquidity_regimes(working_df, spread_col)

        for target in available_targets:
            for regime in REGIME_LABELS:
                records.append(
                    summarise_regime(
                        df=working_df,
                        metadata=metadata,
                        spread_col=spread_col,
                        target=target,
                        regime=regime,
                    )
                )

    return records


def build_regime_summary(results_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = results_df[results_df["status"] == "ok"].copy()

    if ok_df.empty:
        return empty_summary_frame()

    grouped = (
        ok_df
        .groupby(["symbol", "bar_type", "spread_feature", "target", "liquidity_regime"], dropna=False)
        .agg(
            datasets=("parameter", "nunique"),
            total_rows=("row_count", "sum"),
            avg_rows=("row_count", "mean"),
            avg_target_mean=("target_mean", "mean"),
            avg_positive_return_rate=("positive_return_rate", "mean"),
            avg_spread_value=("avg_spread_value", "mean"),
            avg_abs_correlation=("absolute_correlation", "mean"),
            max_abs_correlation=("absolute_correlation", "max"),
            avg_correlation_sample_size=("correlation_sample_size", "mean"),
        )
        .reset_index()
    )

    grouped["regime_score"] = (
        grouped["avg_abs_correlation"].fillna(0) * 300
        + (grouped["avg_positive_return_rate"].fillna(0.5) - 0.5).abs() * 100
        + grouped["datasets"].clip(upper=10) * 2
        + np.log1p(grouped["total_rows"]) * 1.5
    ).clip(0, 100).round(2)

    grouped = grouped.sort_values(
        [
            "regime_score",
            "avg_abs_correlation",
            "total_rows",
        ],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    grouped["summary_rank"] = grouped.index + 1
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    return grouped


def build_liquidity_transition_summary(results_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = results_df[results_df["status"] == "ok"].copy()

    if ok_df.empty:
        return empty_transition_frame()

    pivot = (
        ok_df
        .pivot_table(
            index=["symbol", "bar_type", "parameter", "spread_feature", "target"],
            columns="liquidity_regime",
            values="positive_return_rate",
            aggfunc="mean",
        )
        .reset_index()
    )

    for regime in REGIME_LABELS:
        if regime not in pivot.columns:
            pivot[regime] = np.nan

    pivot["tight_to_extreme_delta"] = (
        pivot["extreme_wide_liquidity"] - pivot["tight_liquidity"]
    )

    pivot["normal_to_wide_delta"] = (
        pivot["wide_liquidity"] - pivot["normal_liquidity"]
    )

    pivot["absolute_tight_to_extreme_delta"] = pivot["tight_to_extreme_delta"].abs()
    pivot["absolute_normal_to_wide_delta"] = pivot["normal_to_wide_delta"].abs()

    pivot["transition_score"] = (
        pivot["absolute_tight_to_extreme_delta"].fillna(0) * 100
        + pivot["absolute_normal_to_wide_delta"].fillna(0) * 50
    ).clip(0, 100).round(2)

    pivot = pivot.sort_values(
        ["transition_score", "absolute_tight_to_extreme_delta"],
        ascending=[False, False],
    ).reset_index(drop=True)

    pivot["transition_rank"] = pivot.index + 1
    pivot["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    return pivot


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 32 - LIQUIDITY REGIME RESEARCH")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    research_dataset_root = analysis_dir / "research_datasets"
    report_dir = analysis_dir / "liquidity_regime_research"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Research dataset root: {research_dataset_root}")
    print(f"Report dir:            {report_dir}")
    print("-" * 90)

    dataset_files = sorted(
        research_dataset_root.glob("**/microstructure_research_dataset.parquet")
    )

    print(f"Research datasets discovered: {len(dataset_files)}")
    print("-" * 90)

    if not dataset_files:
        raise FileNotFoundError(
            f"No research datasets found in {research_dataset_root}. Run script 22 first."
        )

    records = []

    for idx, dataset_file in enumerate(dataset_files, start=1):
        dataset_records = analyse_dataset(dataset_file)
        records.extend(dataset_records)

        metadata = parse_dataset_metadata(dataset_file)

        print(
            f"[LIQUIDITY] {idx:>2}/{len(dataset_files)} "
            f"{metadata['symbol']:<8} "
            f"{metadata['bar_type']:<22} "
            f"{metadata['parameter']:<28} "
            f"records={len(dataset_records)}"
        )

    results_df = pd.DataFrame(records)
    summary_df = build_regime_summary(results_df)
    transition_df = build_liquidity_transition_summary(results_df)

    results_csv = report_dir / "microstructure_liquidity_regime_research_latest.csv"
    summary_csv = report_dir / "microstructure_liquidity_regime_summary_latest.csv"
    transition_csv = report_dir / "microstructure_liquidity_transition_summary_latest.csv"
    json_path = report_dir / "microstructure_liquidity_regime_research_latest.json"
    txt_path = report_dir / "microstructure_liquidity_regime_research_latest.txt"

    results_df.to_csv(results_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    transition_df.to_csv(transition_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_count": len(dataset_files),
        "result_rows": len(results_df),
        "summary_rows": len(summary_df),
        "transition_rows": len(transition_df),
        "top_regime_summary": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
        "top_transition_summary": transition_df.head(50).to_dict(orient="records") if not transition_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    status_counts = results_df["status"].value_counts(dropna=False).to_dict()

    display_cols = [
        "summary_rank",
        "symbol",
        "bar_type",
        "spread_feature",
        "target",
        "liquidity_regime",
        "datasets",
        "total_rows",
        "avg_positive_return_rate",
        "avg_abs_correlation",
        "max_abs_correlation",
        "regime_score",
    ]

    transition_cols = [
        "transition_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "tight_liquidity",
        "normal_liquidity",
        "wide_liquidity",
        "extreme_wide_liquidity",
        "tight_to_extreme_delta",
        "normal_to_wide_delta",
        "transition_score",
    ]

    available_display_cols = [c for c in display_cols if c in summary_df.columns]
    available_transition_cols = [c for c in transition_cols if c in transition_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE LIQUIDITY REGIME RESEARCH")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Research datasets: {len(dataset_files):,}")
    lines.append(f"Result rows:       {len(results_df):,}")
    lines.append(f"Summary rows:      {len(summary_df):,}")
    lines.append(f"Transition rows:   {len(transition_df):,}")
    lines.append("")
    lines.append(f"Status counts: {status_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP LIQUIDITY REGIME SUMMARY")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("No liquidity regime summary available.")
    else:
        lines.append(summary_df[available_display_cols].head(40).to_string(index=False))

    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP LIQUIDITY TRANSITION SUMMARY")
    lines.append("-" * 90)

    if transition_df.empty:
        lines.append("No liquidity transition summary available.")
    else:
        lines.append(transition_df[available_transition_cols].head(40).to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Liquidity regime research complete.")
    print(f"Research datasets: {len(dataset_files):,}")
    print(f"Result rows:       {len(results_df):,}")
    print(f"Summary rows:      {len(summary_df):,}")
    print(f"Transition rows:   {len(transition_df):,}")
    print(f"Status counts:     {status_counts}")
    print(f"Results CSV:       {results_csv}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"Transition CSV:    {transition_csv}")
    print(f"JSON output:       {json_path}")
    print(f"TXT output:        {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()