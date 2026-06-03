"""
BACQE MICROSTRUCTURE 34 - SIGNAL FACTORY

Purpose:
    Build simple directional research signals from the most stable
    liquidity-regime findings.

Important:
    This is NOT a trading strategy yet.
    This is a signal factory / proof-of-concept audit.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SELECTED_STABILITY_LABELS = {
    "research_stability",
    "watch_stability",
}

TARGET_COLUMNS = [
    "forward_return_1",
    "forward_return_3",
    "forward_return_5",
]

REGIME_ORDER = [
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


def find_matching_datasets(
    research_dataset_root: Path,
    symbol: str,
    bar_type: str,
) -> list[Path]:
    symbol_dir = research_dataset_root / f"symbol={symbol}" / f"bar_type={bar_type}"

    if not symbol_dir.exists():
        return []

    return sorted(symbol_dir.glob("parameter=*/microstructure_research_dataset.parquet"))


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


def infer_signal_direction(row: pd.Series) -> int:
    """
    Very simple first-pass rule:
        positive return rate > 0.52 -> long signal
        positive return rate < 0.48 -> short signal
        otherwise neutral
    """
    positive_rate = row.get("avg_positive_return_rate", np.nan)

    if pd.isna(positive_rate):
        return 0

    if positive_rate > 0.52:
        return 1

    if positive_rate < 0.48:
        return -1

    return 0


def evaluate_signal(df: pd.DataFrame, target: str, signal_col: str) -> dict:
    work = df[[target, signal_col]].copy()
    work = work.replace([np.inf, -np.inf], np.nan).dropna()

    work = work[work[signal_col] != 0].copy()

    result = {
        "signal_rows": len(work),
        "long_rows": 0,
        "short_rows": 0,
        "mean_forward_return": None,
        "median_forward_return": None,
        "hit_rate": None,
        "long_hit_rate": None,
        "short_hit_rate": None,
        "long_mean_return": None,
        "short_mean_return": None,
        "signal_return_sum": None,
        "signal_return_std": None,
        "signal_sharpe_like": None,
    }

    if work.empty:
        return result

    work["signed_forward_return"] = work[target] * work[signal_col]

    long_df = work[work[signal_col] == 1]
    short_df = work[work[signal_col] == -1]

    result["long_rows"] = len(long_df)
    result["short_rows"] = len(short_df)
    result["mean_forward_return"] = float(work["signed_forward_return"].mean())
    result["median_forward_return"] = float(work["signed_forward_return"].median())
    result["hit_rate"] = float((work["signed_forward_return"] > 0).mean())
    result["signal_return_sum"] = float(work["signed_forward_return"].sum())
    result["signal_return_std"] = float(work["signed_forward_return"].std())

    if result["signal_return_std"] and result["signal_return_std"] > 0:
        result["signal_sharpe_like"] = float(
            result["mean_forward_return"] / result["signal_return_std"]
        )

    if not long_df.empty:
        result["long_hit_rate"] = float((long_df[target] > 0).mean())
        result["long_mean_return"] = float(long_df[target].mean())

    if not short_df.empty:
        result["short_hit_rate"] = float((short_df[target] < 0).mean())
        result["short_mean_return"] = float((-short_df[target]).mean())

    return result


def classify_signal_result(row: dict) -> str:
    signal_rows = row.get("signal_rows", 0)
    hit_rate = row.get("hit_rate")
    mean_return = row.get("mean_forward_return")
    sharpe_like = row.get("signal_sharpe_like")

    if signal_rows < 50:
        return "low_sample"

    if hit_rate is None or mean_return is None:
        return "insufficient"

    if hit_rate >= 0.58 and mean_return > 0 and sharpe_like is not None and sharpe_like > 0.08:
        return "strong_signal_candidate"

    if hit_rate >= 0.54 and mean_return > 0:
        return "research_signal_candidate"

    if hit_rate >= 0.51 and mean_return > 0:
        return "weak_signal_candidate"

    return "no_signal_edge"


def build_signal_for_candidate(row: pd.Series, research_dataset_root: Path) -> list[dict]:
    symbol = row["symbol"]
    bar_type = row["bar_type"]
    spread_feature = row["spread_feature"]
    target = row["target"]

    matching_datasets = find_matching_datasets(
        research_dataset_root=research_dataset_root,
        symbol=symbol,
        bar_type=bar_type,
    )

    base = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "stability_rank": row.get("stability_rank"),
        "symbol": symbol,
        "bar_type": bar_type,
        "parameter": None,
        "spread_feature": spread_feature,
        "target": target,
        "stability_score": row.get("stability_score"),
        "stability_label": row.get("stability_label"),
        "avg_positive_return_rate": row.get("avg_positive_return_rate"),
        "avg_abs_correlation": row.get("avg_abs_correlation"),
        "status": "unknown",
        "dataset_file": None,
        "signal_direction": infer_signal_direction(row),
        "signal_rule": None,
        "signal_rows": 0,
        "long_rows": 0,
        "short_rows": 0,
        "mean_forward_return": None,
        "median_forward_return": None,
        "hit_rate": None,
        "long_hit_rate": None,
        "short_hit_rate": None,
        "long_mean_return": None,
        "short_mean_return": None,
        "signal_return_sum": None,
        "signal_return_std": None,
        "signal_sharpe_like": None,
        "signal_label": "unknown",
        "error": None,
    }

    if not matching_datasets:
        return [{
            **base,
            "status": "missing_dataset",
            "error": "Could not locate matching research datasets.",
        }]

    results = []

    for dataset_file in matching_datasets:
        record = base.copy()
        metadata = parse_dataset_metadata(dataset_file)

        record["parameter"] = metadata.get("parameter")
        record["dataset_file"] = str(dataset_file)

        try:
            df = pd.read_parquet(dataset_file)
        except Exception as exc:
            record["status"] = "failed_read"
            record["error"] = str(exc)
            results.append(record)
            continue

        if df.empty:
            record["status"] = "empty_dataset"
            record["error"] = "Dataset empty."
            results.append(record)
            continue

        if spread_feature not in df.columns:
            record["status"] = "missing_spread_feature"
            record["error"] = f"Missing spread feature: {spread_feature}"
            results.append(record)
            continue

        if target not in df.columns:
            record["status"] = "missing_target"
            record["error"] = f"Missing target: {target}"
            results.append(record)
            continue

        if "end_time" in df.columns:
            df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
            df = df.sort_values("end_time").reset_index(drop=True)

        df["liquidity_regime"] = assign_liquidity_regimes(df, spread_feature)

        signal_direction = record["signal_direction"]

        if signal_direction == 0:
            record["status"] = "neutral_research_row"
            record["signal_label"] = "neutral_no_signal"
            record["signal_rule"] = (
                "No signal because avg_positive_return_rate was between 0.48 and 0.52."
            )
            results.append(record)
            continue

        signal_col = "research_signal"
        df[signal_col] = signal_direction

        record["signal_rule"] = (
            f"Fallback global signal direction={signal_direction} "
            f"from avg_positive_return_rate={record['avg_positive_return_rate']}"
        )

        metrics = evaluate_signal(df, target=target, signal_col=signal_col)

        record.update(metrics)
        record["status"] = "ok"
        record["signal_label"] = classify_signal_result(record)

        results.append(record)

    return results


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 34 - SIGNAL FACTORY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    research_dataset_root = analysis_dir / "research_datasets"

    stability_path = (
        analysis_dir
        / "regime_edge_stability"
        / "microstructure_regime_edge_stability_latest.csv"
    )

    report_dir = analysis_dir / "signal_factory"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Stability input:       {stability_path}")
    print(f"Research dataset root: {research_dataset_root}")
    print(f"Report dir:            {report_dir}")
    print("-" * 90)

    if not stability_path.exists():
        raise FileNotFoundError(
            f"Missing stability file: {stability_path}. Run script 33 first."
        )

    stability_df = pd.read_csv(stability_path)

    candidates_df = stability_df[
        stability_df["stability_label"].isin(SELECTED_STABILITY_LABELS)
    ].copy()

    candidates_df = candidates_df.sort_values(
        ["stability_score", "avg_abs_correlation", "total_rows"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    print(f"Stability rows:   {len(stability_df):,}")
    print(f"Signal candidates:{len(candidates_df):,}")
    print("-" * 90)

    if candidates_df.empty:
        raise RuntimeError("No research/watch stability rows available for signal factory.")

    records = []

    for idx, row in candidates_df.iterrows():
        candidate_results = build_signal_for_candidate(row, research_dataset_root)
        records.extend(candidate_results)

        result = candidate_results[0]

        print(
            f"[SIGNAL] {idx + 1:>2}/{len(candidates_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row.get('spread_feature'):<24} "
            f"{row['target']:<16} "
            f"status={result['status']:<22} "
            f"label={result['signal_label']}"
        )

        if result.get("error"):
            print(f"         error={result['error']}")

    results_df = pd.DataFrame(records)

    results_df = results_df.sort_values(
        [
            "signal_label",
            "hit_rate",
            "mean_forward_return",
            "signal_rows",
        ],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    label_rank = {
        "strong_signal_candidate": 1,
        "research_signal_candidate": 2,
        "weak_signal_candidate": 3,
        "no_signal_edge": 4,
        "neutral_no_signal": 5,
        "low_sample": 6,
        "insufficient": 7,
        "unknown": 8,
    }

    results_df["signal_label_rank"] = results_df["signal_label"].map(label_rank).fillna(99)

    results_df = results_df.sort_values(
        [
            "signal_label_rank",
            "hit_rate",
            "mean_forward_return",
            "signal_rows",
        ],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    results_df["signal_rank"] = results_df.index + 1

    csv_path = report_dir / "microstructure_signal_factory_latest.csv"
    json_path = report_dir / "microstructure_signal_factory_latest.json"
    txt_path = report_dir / "microstructure_signal_factory_latest.txt"

    results_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results_df.to_dict(orient="records"), f, indent=2, default=str)

    status_counts = results_df["status"].value_counts(dropna=False).to_dict()
    label_counts = results_df["signal_label"].value_counts(dropna=False).to_dict()

    display_cols = [
        "signal_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "stability_score",
        "stability_label",
        "signal_direction",
        "signal_rows",
        "long_rows",
        "short_rows",
        "hit_rate",
        "mean_forward_return",
        "median_forward_return",
        "signal_sharpe_like",
        "signal_label",
        "signal_rule",
    ]

    available_display_cols = [c for c in display_cols if c in results_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL FACTORY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Stability rows:    {len(stability_df):,}")
    lines.append(f"Signal candidates: {len(candidates_df):,}")
    lines.append(f"Signal results:    {len(results_df):,}")
    lines.append("")
    lines.append(f"Status counts: {status_counts}")
    lines.append(f"Signal labels: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP SIGNAL FACTORY RESULTS")
    lines.append("-" * 90)
    lines.append(results_df[available_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal factory complete.")
    print(f"Stability rows:    {len(stability_df):,}")
    print(f"Signal candidates: {len(candidates_df):,}")
    print(f"Signal results:    {len(results_df):,}")
    print(f"Status counts:     {status_counts}")
    print(f"Signal labels:     {label_counts}")
    print(f"CSV output:        {csv_path}")
    print(f"JSON output:       {json_path}")
    print(f"TXT output:        {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()