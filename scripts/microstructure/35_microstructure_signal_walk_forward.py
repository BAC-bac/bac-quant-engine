"""
BACQE MICROSTRUCTURE 35 - SIGNAL WALK FORWARD VALIDATION

Purpose:
    Walk-forward validate signal candidates created by Script 34.

Inputs:
    signal_factory/
        microstructure_signal_factory_latest.csv

Outputs:
    signal_walk_forward/
        microstructure_signal_walk_forward_folds_latest.csv
        microstructure_signal_walk_forward_summary_latest.csv
        microstructure_signal_walk_forward_latest.json
        microstructure_signal_walk_forward_latest.txt
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

MIN_TOTAL_ROWS = 100
MIN_TRAIN_ROWS = 50
MIN_TEST_ROWS = 25
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


def infer_direction_from_train(train_df: pd.DataFrame, target: str) -> int:
    positive_rate = (train_df[target] > 0).mean()

    if positive_rate > 0.52:
        return 1

    if positive_rate < 0.48:
        return -1

    return 0


def evaluate_signal_fold(
    test_df: pd.DataFrame,
    target: str,
    signal_direction: int,
) -> dict:
    result = {
        "signal_rows": len(test_df),
        "long_rows": 0,
        "short_rows": 0,
        "hit_rate": None,
        "mean_forward_return": None,
        "median_forward_return": None,
        "signal_return_sum": None,
        "signal_return_std": None,
        "signal_sharpe_like": None,
    }

    if signal_direction == 0 or test_df.empty:
        result["signal_rows"] = 0
        return result

    work = test_df[[target]].copy()
    work[target] = pd.to_numeric(work[target], errors="coerce")
    work = work.replace([np.inf, -np.inf], np.nan).dropna()

    if work.empty:
        result["signal_rows"] = 0
        return result

    work["signal_direction"] = signal_direction
    work["signed_forward_return"] = work[target] * signal_direction

    result["signal_rows"] = len(work)
    result["long_rows"] = int((work["signal_direction"] == 1).sum())
    result["short_rows"] = int((work["signal_direction"] == -1).sum())
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


def run_walk_forward_for_signal(row: pd.Series) -> list[dict]:
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
            "status": "no_valid_splits",
            "row_count": len(df),
            "error": "No valid walk-forward splits created.",
        }]

    records = []

    for fold_idx, (train_start, train_end, test_end) in enumerate(splits, start=1):
        train_df = df.iloc[train_start:train_end].copy()
        test_df = df.iloc[train_end:test_end].copy()

        signal_direction = infer_direction_from_train(train_df, target)
        metrics = evaluate_signal_fold(test_df, target, signal_direction)

        fold_record = {
            **base,
            "fold": fold_idx,
            "status": "ok",
            "row_count": len(df),
            "train_start_idx": train_start,
            "train_end_idx": train_end,
            "test_start_idx": train_end,
            "test_end_idx": test_end,
            "train_rows": len(train_df),
            "test_rows": len(test_df),
            "train_positive_rate": float((train_df[target] > 0).mean()),
            "test_positive_rate": float((test_df[target] > 0).mean()),
            "signal_direction": signal_direction,
            "signal_rows": metrics["signal_rows"],
            "long_rows": metrics["long_rows"],
            "short_rows": metrics["short_rows"],
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


def classify_signal_walk_forward(row: pd.Series) -> str:
    ok_folds = row.get("ok_folds", 0)
    active_folds = row.get("active_folds", 0)
    avg_hit_rate = row.get("avg_hit_rate")
    avg_mean_return = row.get("avg_mean_forward_return")
    positive_fold_rate = row.get("positive_return_fold_rate", 0)
    total_signal_rows = row.get("total_signal_rows", 0)

    if ok_folds < 2:
        return "insufficient"

    if active_folds < 2 or total_signal_rows < 50:
        return "low_sample"

    if (
        pd.notna(avg_hit_rate)
        and pd.notna(avg_mean_return)
        and avg_hit_rate >= 0.56
        and avg_mean_return > 0
        and positive_fold_rate >= 0.60
    ):
        return "walk_forward_signal_strong"

    if (
        pd.notna(avg_hit_rate)
        and pd.notna(avg_mean_return)
        and avg_hit_rate >= 0.53
        and avg_mean_return > 0
        and positive_fold_rate >= 0.50
    ):
        return "walk_forward_signal_research"

    if (
        pd.notna(avg_hit_rate)
        and avg_hit_rate >= 0.51
        and positive_fold_rate >= 0.40
    ):
        return "walk_forward_signal_weak"

    return "no_walk_forward_signal_edge"


def summarise_signal_walk_forward(fold_df: pd.DataFrame) -> pd.DataFrame:
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

    summary = grouped.merge(
        active_grouped,
        on=group_cols,
        how="left",
    )

    summary["positive_return_fold_rate"] = (
        summary["positive_return_folds"] / summary["active_folds"]
    )

    summary["signal_walk_forward_score"] = (
        (summary["avg_hit_rate"].fillna(0.5) - 0.5) * 300
        + summary["positive_return_fold_rate"].fillna(0) * 30
        + np.log1p(summary["total_signal_rows"].fillna(0)) * 3
        + summary["avg_signal_sharpe_like"].fillna(0) * 20
    ).clip(0, 100).round(2)

    summary["walk_forward_signal_label"] = summary.apply(classify_signal_walk_forward, axis=1)
    summary["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    label_rank = {
        "walk_forward_signal_strong": 1,
        "walk_forward_signal_research": 2,
        "walk_forward_signal_weak": 3,
        "no_walk_forward_signal_edge": 4,
        "low_sample": 5,
        "insufficient": 6,
    }

    summary["label_rank"] = summary["walk_forward_signal_label"].map(label_rank).fillna(99)

    summary = summary.sort_values(
        [
            "label_rank",
            "signal_walk_forward_score",
            "avg_hit_rate",
            "positive_return_fold_rate",
            "total_signal_rows",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    summary["signal_walk_forward_rank"] = summary.index + 1

    return summary


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 35 - SIGNAL WALK FORWARD VALIDATION")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    signal_factory_path = (
        analysis_dir
        / "signal_factory"
        / "microstructure_signal_factory_latest.csv"
    )

    report_dir = analysis_dir / "signal_walk_forward"
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
    print(f"WF signal candidates: {len(candidates_df):,}")
    print("-" * 90)

    if candidates_df.empty:
        raise RuntimeError("No signal candidates found for walk-forward validation.")

    fold_records = []

    for idx, row in candidates_df.iterrows():
        records = run_walk_forward_for_signal(row)
        fold_records.extend(records)

        print(
            f"[SIGNAL_WF] {idx + 1:>2}/{len(candidates_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter']:<26} "
            f"{row['target']:<16} "
            f"{row['signal_label']:<26} "
            f"records={len(records)}"
        )

    fold_df = pd.DataFrame(fold_records)
    summary_df = summarise_signal_walk_forward(fold_df)

    fold_csv = report_dir / "microstructure_signal_walk_forward_folds_latest.csv"
    summary_csv = report_dir / "microstructure_signal_walk_forward_summary_latest.csv"
    json_path = report_dir / "microstructure_signal_walk_forward_latest.json"
    txt_path = report_dir / "microstructure_signal_walk_forward_latest.txt"

    fold_df.to_csv(fold_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "signal_factory_rows": len(signal_df),
        "candidate_rows": len(candidates_df),
        "fold_rows": len(fold_df),
        "summary_rows": len(summary_df),
        "top_summary": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    fold_status_counts = fold_df["status"].value_counts(dropna=False).to_dict()

    if not summary_df.empty:
        label_counts = summary_df["walk_forward_signal_label"].value_counts(dropna=False).to_dict()
    else:
        label_counts = {}

    display_cols = [
        "signal_walk_forward_rank",
        "symbol",
        "bar_type",
        "parameter",
        "spread_feature",
        "target",
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
        "signal_walk_forward_score",
        "walk_forward_signal_label",
    ]

    available_display_cols = [c for c in display_cols if c in summary_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE SIGNAL WALK FORWARD VALIDATION")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Signal factory rows:  {len(signal_df):,}")
    lines.append(f"Candidate rows:       {len(candidates_df):,}")
    lines.append(f"Fold rows:            {len(fold_df):,}")
    lines.append(f"Summary rows:         {len(summary_df):,}")
    lines.append("")
    lines.append(f"Fold status counts:   {fold_status_counts}")
    lines.append(f"WF signal labels:     {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP SIGNAL WALK FORWARD RESULTS")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("No signal walk-forward summary available.")
    else:
        lines.append(summary_df[available_display_cols].head(40).to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Signal walk-forward validation complete.")
    print(f"Signal factory rows: {len(signal_df):,}")
    print(f"Candidate rows:      {len(candidates_df):,}")
    print(f"Fold rows:           {len(fold_df):,}")
    print(f"Summary rows:        {len(summary_df):,}")
    print(f"Fold status counts:  {fold_status_counts}")
    print(f"WF signal labels:    {label_counts}")
    print(f"Fold CSV:            {fold_csv}")
    print(f"Summary CSV:         {summary_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()