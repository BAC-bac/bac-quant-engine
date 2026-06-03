"""
BACQE MICROSTRUCTURE 37 - SIGNAL THRESHOLD OPTIMIZATION

Purpose:
    Test different long/short regime decision thresholds for the
    regime-conditioned signal logic from Script 36.

Input:
    signal_factory/
        microstructure_signal_factory_latest.csv

Output:
    signal_threshold_optimization/
        microstructure_signal_threshold_optimization_folds_latest.csv
        microstructure_signal_threshold_optimization_summary_latest.csv
        microstructure_signal_threshold_optimization_latest.json
        microstructure_signal_threshold_optimization_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

SELECTED_SIGNAL_LABELS = {
    "strong_signal_candidate",
    "research_signal_candidate",
    "weak_signal_candidate",
}

THRESHOLD_PAIRS = [
    (0.51, 0.49),
    (0.52, 0.48),
    (0.53, 0.47),
    (0.55, 0.45),
    (0.60, 0.40),
]

REGIME_LABELS = [
    "tight_liquidity",
    "normal_liquidity",
    "wide_liquidity",
    "extreme_wide_liquidity",
]

MIN_TOTAL_ROWS = 100
MIN_TRAIN_ROWS = 50
MIN_TEST_ROWS = 25
MIN_REGIME_TRAIN_ROWS = 20
N_SPLITS = 5


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


def make_walk_forward_splits(n_rows: int) -> list[tuple[int, int, int]]:
    splits = []

    if n_rows < MIN_TOTAL_ROWS:
        return splits

    initial_train_end = int(n_rows * 0.50)
    remaining = n_rows - initial_train_end
    test_window = max(int(remaining / N_SPLITS), MIN_TEST_ROWS)

    train_start = 0
    train_end = initial_train_end

    while train_end + test_window <= n_rows:
        test_end = train_end + test_window

        if train_end - train_start >= MIN_TRAIN_ROWS and test_end - train_end >= MIN_TEST_ROWS:
            splits.append((train_start, train_end, test_end))

        train_end = test_end

    return splits


def infer_regime_signal_map(
    train_df: pd.DataFrame,
    target: str,
    long_threshold: float,
    short_threshold: float,
) -> dict:
    regime_map = {}

    for regime in REGIME_LABELS:
        regime_df = train_df[train_df["liquidity_regime"] == regime].copy()

        if len(regime_df) < MIN_REGIME_TRAIN_ROWS:
            regime_map[regime] = 0
            continue

        positive_rate = float((regime_df[target] > 0).mean())

        if positive_rate > long_threshold:
            regime_map[regime] = 1
        elif positive_rate < short_threshold:
            regime_map[regime] = -1
        else:
            regime_map[regime] = 0

    return regime_map


def apply_regime_signal_map(test_df: pd.DataFrame, regime_map: dict) -> pd.Series:
    return test_df["liquidity_regime"].map(regime_map).fillna(0).astype(int)


def evaluate_signal(test_df: pd.DataFrame, target: str, signal_col: str) -> dict:
    work = test_df[[target, signal_col, "liquidity_regime"]].copy()
    work[target] = pd.to_numeric(work[target], errors="coerce")
    work = work.replace([np.inf, -np.inf], np.nan).dropna()
    work = work[work[signal_col] != 0].copy()

    result = {
        "signal_rows": len(work),
        "long_rows": 0,
        "short_rows": 0,
        "active_regime_count": 0,
        "active_regimes": "",
        "hit_rate": None,
        "mean_forward_return": None,
        "median_forward_return": None,
        "signal_return_sum": None,
        "signal_return_std": None,
        "signal_sharpe_like": None,
    }

    if work.empty:
        return result

    work["signed_forward_return"] = work[target] * work[signal_col]

    result["long_rows"] = int((work[signal_col] == 1).sum())
    result["short_rows"] = int((work[signal_col] == -1).sum())
    result["active_regime_count"] = int(work["liquidity_regime"].nunique())
    result["active_regimes"] = ",".join(sorted(work["liquidity_regime"].dropna().unique()))
    result["hit_rate"] = float((work["signed_forward_return"] > 0).mean())
    result["mean_forward_return"] = float(work["signed_forward_return"].mean())
    result["median_forward_return"] = float(work["signed_forward_return"].median())
    result["signal_return_sum"] = float(work["signed_forward_return"].sum())
    result["signal_return_std"] = float(work["signed_forward_return"].std())

    if result["signal_return_std"] and result["signal_return_std"] > 0:
        result["signal_sharpe_like"] = float(
            result["mean_forward_return"] / result["signal_return_std"]
        )

    return result


def run_threshold_test_for_candidate(row: pd.Series) -> list[dict]:
    dataset_file = Path(row["dataset_file"])
    target = row["target"]
    spread_feature = row["spread_feature"]

    base = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "signal_rank": row.get("signal_rank"),
        "symbol": row.get("symbol"),
        "bar_type": row.get("bar_type"),
        "parameter": row.get("parameter"),
        "spread_feature": spread_feature,
        "target": target,
        "source_signal_label": row.get("signal_label"),
        "source_hit_rate": row.get("hit_rate"),
        "source_mean_forward_return": row.get("mean_forward_return"),
        "dataset_file": str(dataset_file),
    }

    if not dataset_file.exists():
        return [{**base, "fold": None, "status": "missing_dataset", "error": "Dataset file missing."}]

    try:
        df = pd.read_parquet(dataset_file)
    except Exception as exc:
        return [{**base, "fold": None, "status": "failed_read", "error": str(exc)}]

    if df.empty:
        return [{**base, "fold": None, "status": "empty_dataset", "error": "Dataset empty."}]

    if spread_feature not in df.columns:
        return [{**base, "fold": None, "status": "missing_spread_feature", "error": f"Missing {spread_feature}"}]

    if target not in df.columns:
        return [{**base, "fold": None, "status": "missing_target", "error": f"Missing {target}"}]

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)

    df[target] = pd.to_numeric(df[target], errors="coerce")
    df = df.dropna(subset=[target]).reset_index(drop=True)

    if len(df) < MIN_TOTAL_ROWS:
        return [{
            **base,
            "fold": None,
            "long_threshold": None,
            "short_threshold": None,
            "threshold_pair": None,
            "status": "low_rows",
            "row_count": len(df),
            "error": f"Rows below minimum: {len(df)} < {MIN_TOTAL_ROWS}",
        }]

    df["liquidity_regime"] = assign_liquidity_regimes(df, spread_feature)

    splits = make_walk_forward_splits(len(df))

    if not splits:
        return [{
            **base,
            "fold": None,
            "long_threshold": None,
            "short_threshold": None,
            "threshold_pair": None,
            "status": "no_valid_splits",
            "row_count": len(df),
            "error": "No valid walk-forward splits created.",
        }]

    records = []

    for long_threshold, short_threshold in THRESHOLD_PAIRS:
        threshold_pair = f"{long_threshold:.2f}_{short_threshold:.2f}"

        for fold_idx, (train_start, train_end, test_end) in enumerate(splits, start=1):
            train_df = df.iloc[train_start:train_end].copy()
            test_df = df.iloc[train_end:test_end].copy()

            regime_map = infer_regime_signal_map(
                train_df=train_df,
                target=target,
                long_threshold=long_threshold,
                short_threshold=short_threshold,
            )

            signal_col = "threshold_signal"
            test_df[signal_col] = apply_regime_signal_map(test_df, regime_map)

            metrics = evaluate_signal(test_df, target, signal_col)

            active_signal_directions = sorted(
                set(direction for direction in regime_map.values() if direction != 0)
            )

            fold_record = {
                **base,
                "fold": fold_idx,
                "status": "ok",
                "row_count": len(df),
                "long_threshold": long_threshold,
                "short_threshold": short_threshold,
                "threshold_pair": threshold_pair,
                "train_start_idx": train_start,
                "train_end_idx": train_end,
                "test_start_idx": train_end,
                "test_end_idx": test_end,
                "train_rows": len(train_df),
                "test_rows": len(test_df),
                "train_positive_rate": float((train_df[target] > 0).mean()),
                "test_positive_rate": float((test_df[target] > 0).mean()),
                "regime_signal_map": json.dumps(regime_map),
                "active_signal_directions": ",".join(map(str, active_signal_directions)),
                "signal_rows": metrics["signal_rows"],
                "long_rows": metrics["long_rows"],
                "short_rows": metrics["short_rows"],
                "active_regime_count": metrics["active_regime_count"],
                "active_regimes": metrics["active_regimes"],
                "hit_rate": metrics["hit_rate"],
                "mean_forward_return": metrics["mean_forward_return"],
                "median_forward_return": metrics["median_forward_return"],
                "signal_return_sum": metrics["signal_return_sum"],
                "signal_return_std": metrics["signal_return_std"],
                "signal_sharpe_like": metrics["signal_sharpe_like"],
                "error": None,
            }

            records.append(fold_record)

    return records


def classify_threshold_result(row: pd.Series) -> str:
    ok_folds = row.get("ok_folds", 0)
    active_folds = row.get("active_folds", 0)
    total_signal_rows = row.get("total_signal_rows", 0)
    avg_hit_rate = row.get("avg_hit_rate")
    avg_mean_return = row.get("avg_mean_forward_return")
    positive_fold_rate = row.get("positive_return_fold_rate", 0)
    avg_sharpe_like = row.get("avg_signal_sharpe_like", 0)

    if ok_folds < 2:
        return "insufficient"

    if active_folds < 2 or total_signal_rows < 50:
        return "low_sample"

    if (
        pd.notna(avg_hit_rate)
        and pd.notna(avg_mean_return)
        and avg_hit_rate >= 0.58
        and avg_mean_return > 0
        and positive_fold_rate >= 0.60
        and avg_sharpe_like > 0
    ):
        return "threshold_signal_strong"

    if (
        pd.notna(avg_hit_rate)
        and pd.notna(avg_mean_return)
        and avg_hit_rate >= 0.54
        and avg_mean_return > 0
        and positive_fold_rate >= 0.50
    ):
        return "threshold_signal_research"

    if (
        pd.notna(avg_hit_rate)
        and avg_hit_rate >= 0.51
        and positive_fold_rate >= 0.40
    ):
        return "threshold_signal_weak"

    return "no_threshold_signal_edge"


def summarise_threshold_tests(fold_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = fold_df[fold_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    active_df = ok_df[ok_df["signal_rows"] > 0].copy()

    group_cols = [
        "signal_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "source_signal_label",
        "source_hit_rate",
        "source_mean_forward_return",
        "threshold_pair",
        "long_threshold",
        "short_threshold",
        "dataset_file",
    ]

    grouped = (
        ok_df.groupby(group_cols, dropna=False)
        .agg(
            ok_folds=("fold", "count"),
            active_folds=("signal_rows", lambda s: int((s > 0).sum())),
            total_signal_rows=("signal_rows", "sum"),
            avg_train_positive_rate=("train_positive_rate", "mean"),
            avg_test_positive_rate=("test_positive_rate", "mean"),
            long_rows=("long_rows", "sum"),
            short_rows=("short_rows", "sum"),
            avg_active_regime_count=("active_regime_count", "mean"),
        )
        .reset_index()
    )

    active_grouped = (
        active_df.groupby(group_cols, dropna=False)
        .agg(
            avg_hit_rate=("hit_rate", "mean"),
            min_hit_rate=("hit_rate", "min"),
            max_hit_rate=("hit_rate", "max"),
            avg_mean_forward_return=("mean_forward_return", "mean"),
            min_mean_forward_return=("mean_forward_return", "min"),
            max_mean_forward_return=("mean_forward_return", "max"),
            avg_signal_sharpe_like=("signal_sharpe_like", "mean"),
            positive_return_folds=("mean_forward_return", lambda s: int((s > 0).sum())),
        )
        .reset_index()
    )

    summary = grouped.merge(active_grouped, on=group_cols, how="left")

    summary["positive_return_fold_rate"] = (
        summary["positive_return_folds"] / summary["active_folds"]
    )

    summary["threshold_score"] = (
        (summary["avg_hit_rate"].fillna(0.5) - 0.5) * 320
        + summary["positive_return_fold_rate"].fillna(0) * 30
        + np.log1p(summary["total_signal_rows"].fillna(0)) * 3
        + summary["avg_signal_sharpe_like"].fillna(0) * 25
        + summary["avg_active_regime_count"].fillna(0) * 2
    ).clip(0, 100).round(2)

    summary["threshold_signal_label"] = summary.apply(classify_threshold_result, axis=1)
    summary["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    label_rank = {
        "threshold_signal_strong": 1,
        "threshold_signal_research": 2,
        "threshold_signal_weak": 3,
        "no_threshold_signal_edge": 4,
        "low_sample": 5,
        "insufficient": 6,
    }

    summary["label_rank"] = summary["threshold_signal_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "threshold_score",
            "avg_hit_rate",
            "positive_return_fold_rate",
            "total_signal_rows",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["threshold_rank"] = summary.index + 1

    return summary


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 37 - SIGNAL THRESHOLD OPTIMIZATION")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    signal_factory_path = (
        analysis_dir
        / "signal_factory"
        / "microstructure_signal_factory_latest.csv"
    )

    report_dir = analysis_dir / "signal_threshold_optimization"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Signal factory: {signal_factory_path}")
    print(f"Report dir:     {report_dir}")
    print("-" * 90)

    if not signal_factory_path.exists():
        raise FileNotFoundError(
            f"Missing signal factory file: {signal_factory_path}. Run script 34 first."
        )

    signal_df = pd.read_csv(signal_factory_path)

    candidates_df = signal_df[
        signal_df["signal_label"].isin(SELECTED_SIGNAL_LABELS)
    ].copy()

    candidates_df = candidates_df.sort_values(
        ["signal_label_rank", "hit_rate", "mean_forward_return", "signal_rows"],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    print(f"Signal factory rows: {len(signal_df):,}")
    print(f"Threshold candidates: {len(candidates_df):,}")
    print(f"Threshold pairs:      {THRESHOLD_PAIRS}")
    print("-" * 90)

    if candidates_df.empty:
        raise RuntimeError("No signal candidates found for threshold optimization.")

    fold_records = []

    for idx, row in candidates_df.iterrows():
        records = run_threshold_test_for_candidate(row)
        fold_records.extend(records)

        print(
            f"[THRESHOLD] {idx + 1:>2}/{len(candidates_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter']:<26} "
            f"{row['target']:<16} "
            f"{row['signal_label']:<26} "
            f"records={len(records)}"
        )

    fold_df = pd.DataFrame(fold_records)
    summary_df = summarise_threshold_tests(fold_df)

    fold_csv = report_dir / "microstructure_signal_threshold_optimization_folds_latest.csv"
    summary_csv = report_dir / "microstructure_signal_threshold_optimization_summary_latest.csv"
    json_path = report_dir / "microstructure_signal_threshold_optimization_latest.json"
    txt_path = report_dir / "microstructure_signal_threshold_optimization_latest.txt"

    fold_df.to_csv(fold_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "signal_factory_rows": len(signal_df),
        "candidate_rows": len(candidates_df),
        "threshold_pairs": THRESHOLD_PAIRS,
        "fold_rows": len(fold_df),
        "summary_rows": len(summary_df),
        "top_summary": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    fold_status_counts = fold_df["status"].value_counts(dropna=False).to_dict()

    if not summary_df.empty:
        label_counts = summary_df["threshold_signal_label"].value_counts(dropna=False).to_dict()
    else:
        label_counts = {}

    display_cols = [
        "threshold_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
        "threshold_pair",
        "source_signal_label",
        "ok_folds",
        "active_folds",
        "total_signal_rows",
        "avg_hit_rate",
        "min_hit_rate",
        "max_hit_rate",
        "avg_mean_forward_return",
        "positive_return_fold_rate",
        "avg_signal_sharpe_like",
        "avg_active_regime_count",
        "threshold_score",
        "threshold_signal_label",
    ]

    available_display_cols = [c for c in display_cols if c in summary_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL THRESHOLD OPTIMIZATION")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Signal factory rows:   {len(signal_df):,}")
    lines.append(f"Candidate rows:        {len(candidates_df):,}")
    lines.append(f"Threshold pairs:       {THRESHOLD_PAIRS}")
    lines.append(f"Fold rows:             {len(fold_df):,}")
    lines.append(f"Summary rows:          {len(summary_df):,}")
    lines.append("")
    lines.append(f"Fold status counts:    {fold_status_counts}")
    lines.append(f"Threshold labels:      {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP SIGNAL THRESHOLD OPTIMIZATION RESULTS")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("No threshold optimization summary available.")
    else:
        lines.append(summary_df[available_display_cols].head(50).to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal threshold optimization complete.")
    print(f"Signal factory rows: {len(signal_df):,}")
    print(f"Candidate rows:      {len(candidates_df):,}")
    print(f"Threshold pairs:     {THRESHOLD_PAIRS}")
    print(f"Fold rows:           {len(fold_df):,}")
    print(f"Summary rows:        {len(summary_df):,}")
    print(f"Fold status counts:  {fold_status_counts}")
    print(f"Threshold labels:    {label_counts}")
    print(f"Fold CSV:            {fold_csv}")
    print(f"Summary CSV:         {summary_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()