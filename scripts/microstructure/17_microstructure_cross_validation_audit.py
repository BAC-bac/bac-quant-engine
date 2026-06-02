"""
BACQE MICROSTRUCTURE 17 - MICROSTRUCTURE CROSS VALIDATION AUDIT

Purpose:
    Validate whether predictive relationships from Script 13 survive
    a simple time-based train/test split.

Method:
    For each candidate from the predictive audit:
        - Load feature dataset
        - Sort by end_time
        - Split 70% train / 30% test
        - Calculate train correlation
        - Calculate test correlation
        - Check direction consistency
        - Check correlation decay

Important:
    This is not model training.
    This is a robustness audit.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

WATCHLIST_LEVELS = {
    "weak_watchlist",
    "research_watchlist",
    "strong_watchlist",
}


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


def direction(value: float | None) -> str:
    if value is None:
        return "none"
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "zero"


def validation_label(row: dict) -> str:
    train_corr = row.get("train_correlation")
    test_corr = row.get("test_correlation")
    train_n = row.get("train_sample_size", 0)
    test_n = row.get("test_sample_size", 0)
    same_direction = row.get("same_direction", False)
    test_abs = row.get("test_abs_correlation")
    decay_ratio = row.get("correlation_decay_ratio")

    if train_corr is None or test_corr is None:
        return "insufficient"

    if train_n < 100 or test_n < 50:
        return "low_sample"

    if not same_direction:
        return "failed_direction"

    if test_abs is not None and test_abs >= 0.05 and decay_ratio is not None and decay_ratio >= 0.50:
        return "validated"

    if test_abs is not None and test_abs >= 0.025 and decay_ratio is not None and decay_ratio >= 0.25:
        return "partially_validated"

    return "weak_validation"


def audit_single_relationship(row: pd.Series, train_fraction: float = 0.70) -> dict:
    file_path = Path(row["file_path"])
    feature_name = row["feature_name"]
    target = row["target"]

    result = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": row.get("symbol"),
        "bar_type": row.get("bar_type"),
        "parameter": row.get("parameter"),
        "feature_name": feature_name,
        "feature_family": row.get("feature_family"),
        "target": target,
        "signal_strength": row.get("signal_strength"),
        "original_correlation": row.get("correlation"),
        "original_abs_correlation": row.get("abs_correlation"),
        "original_sample_size": row.get("sample_size"),
        "file_path": str(file_path),
        "status": "unknown",
        "row_count": 0,
        "train_rows": 0,
        "test_rows": 0,
        "train_correlation": None,
        "test_correlation": None,
        "train_abs_correlation": None,
        "test_abs_correlation": None,
        "train_sample_size": 0,
        "test_sample_size": 0,
        "train_direction": "none",
        "test_direction": "none",
        "same_direction": False,
        "correlation_decay_ratio": None,
        "validation_label": "unknown",
        "error": None,
    }

    if not file_path.exists():
        result["status"] = "missing_file"
        result["error"] = "Feature file does not exist."
        result["validation_label"] = "insufficient"
        return result

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        result["status"] = "failed_read"
        result["error"] = str(exc)
        result["validation_label"] = "insufficient"
        return result

    if df.empty:
        result["status"] = "empty"
        result["error"] = "Feature file is empty."
        result["validation_label"] = "insufficient"
        return result

    if feature_name not in df.columns:
        result["status"] = "missing_feature"
        result["error"] = f"Missing feature column: {feature_name}"
        result["validation_label"] = "insufficient"
        return result

    if target not in df.columns:
        result["status"] = "missing_target"
        result["error"] = f"Missing target column: {target}"
        result["validation_label"] = "insufficient"
        return result

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    result["row_count"] = len(df)

    split_idx = int(len(df) * train_fraction)

    if split_idx <= 0 or split_idx >= len(df):
        result["status"] = "bad_split"
        result["error"] = "Unable to create train/test split."
        result["validation_label"] = "insufficient"
        return result

    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    result["train_rows"] = len(train_df)
    result["test_rows"] = len(test_df)

    train_corr, train_n = safe_corr(
        pd.to_numeric(train_df[feature_name], errors="coerce"),
        pd.to_numeric(train_df[target], errors="coerce"),
    )

    test_corr, test_n = safe_corr(
        pd.to_numeric(test_df[feature_name], errors="coerce"),
        pd.to_numeric(test_df[target], errors="coerce"),
    )

    result["train_correlation"] = train_corr
    result["test_correlation"] = test_corr
    result["train_abs_correlation"] = abs(train_corr) if train_corr is not None else None
    result["test_abs_correlation"] = abs(test_corr) if test_corr is not None else None
    result["train_sample_size"] = train_n
    result["test_sample_size"] = test_n

    result["train_direction"] = direction(train_corr)
    result["test_direction"] = direction(test_corr)

    result["same_direction"] = (
        result["train_direction"] == result["test_direction"]
        and result["train_direction"] not in {"none", "zero"}
    )

    if result["train_abs_correlation"] not in {None, 0} and result["test_abs_correlation"] is not None:
        result["correlation_decay_ratio"] = result["test_abs_correlation"] / result["train_abs_correlation"]

    result["status"] = "ok"
    result["validation_label"] = validation_label(result)

    return result


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 17 - MICROSTRUCTURE CROSS VALIDATION AUDIT")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    predictive_audit_path = (
        analysis_dir
        / "predictive_audit"
        / "microstructure_predictive_audit_latest.csv"
    )

    report_dir = analysis_dir / "cross_validation_audit"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Predictive audit: {predictive_audit_path}")
    print(f"Report dir:       {report_dir}")
    print("-" * 90)

    if not predictive_audit_path.exists():
        raise FileNotFoundError(
            f"Missing predictive audit file: {predictive_audit_path}. "
            "Run script 13 first."
        )

    audit_df = pd.read_csv(predictive_audit_path)

    candidates_df = audit_df[
        audit_df["signal_strength"].isin(WATCHLIST_LEVELS)
    ].copy()

    candidates_df = candidates_df.sort_values(
        ["abs_correlation", "sample_size"],
        ascending=[False, False],
    ).reset_index(drop=True)

    print(f"Predictive audit rows: {len(audit_df):,}")
    print(f"CV candidates:         {len(candidates_df):,}")
    print("-" * 90)

    results = []

    for idx, row in candidates_df.iterrows():
        result = audit_single_relationship(row)
        results.append(result)

        if (idx + 1) % 250 == 0 or idx == 0:
            print(
                f"[CV] {idx + 1:,}/{len(candidates_df):,} "
                f"latest={result['symbol']} {result['feature_name']} -> "
                f"{result['target']} label={result['validation_label']}"
            )

    results_df = pd.DataFrame(results)

    results_df = results_df.sort_values(
        [
            "validation_label",
            "test_abs_correlation",
            "correlation_decay_ratio",
            "test_sample_size",
        ],
        ascending=[True, False, False, False],
        na_position="last",
    )

    label_order = {
        "validated": 1,
        "partially_validated": 2,
        "weak_validation": 3,
        "failed_direction": 4,
        "low_sample": 5,
        "insufficient": 6,
    }

    results_df["validation_rank_group"] = (
        results_df["validation_label"].map(label_order).fillna(99)
    )

    results_df = results_df.sort_values(
        [
            "validation_rank_group",
            "test_abs_correlation",
            "correlation_decay_ratio",
            "test_sample_size",
        ],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    csv_path = report_dir / "microstructure_cross_validation_audit_latest.csv"
    json_path = report_dir / "microstructure_cross_validation_audit_latest.json"
    txt_path = report_dir / "microstructure_cross_validation_audit_latest.txt"

    results_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results_df.to_dict(orient="records"), f, indent=2, default=str)

    label_counts = results_df["validation_label"].value_counts(dropna=False).to_dict()
    status_counts = results_df["status"].value_counts(dropna=False).to_dict()
    family_counts = results_df["feature_family"].value_counts(dropna=False).to_dict()

    validated_df = results_df[
        results_df["validation_label"].isin(["validated", "partially_validated"])
    ].copy()

    top_validated = validated_df.head(50)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "feature_name",
            "feature_family",
            "target",
            "original_correlation",
            "train_correlation",
            "test_correlation",
            "test_abs_correlation",
            "correlation_decay_ratio",
            "train_sample_size",
            "test_sample_size",
            "validation_label",
        ]
    ] if not validated_df.empty else pd.DataFrame()

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE CROSS VALIDATION AUDIT")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Predictive audit rows: {len(audit_df):,}")
    lines.append(f"CV candidates:         {len(candidates_df):,}")
    lines.append(f"CV results:            {len(results_df):,}")
    lines.append("")
    lines.append(f"Validation label counts: {label_counts}")
    lines.append(f"Status counts:           {status_counts}")
    lines.append(f"Feature family counts:   {family_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP VALIDATED / PARTIALLY VALIDATED RELATIONSHIPS")
    lines.append("-" * 90)

    if top_validated.empty:
        lines.append("No validated relationships found under current thresholds.")
    else:
        lines.append(top_validated.to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Microstructure cross validation audit complete.")
    print(f"CV candidates:       {len(candidates_df):,}")
    print(f"CV results:          {len(results_df):,}")
    print(f"Validation counts:   {label_counts}")
    print(f"Status counts:       {status_counts}")
    print(f"Family counts:       {family_counts}")
    print(f"Validated rows:      {len(validated_df):,}")
    print(f"CSV output:          {csv_path}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()