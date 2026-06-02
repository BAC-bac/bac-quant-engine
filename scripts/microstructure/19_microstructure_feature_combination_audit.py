"""
BACQE MICROSTRUCTURE 19 - FEATURE COMBINATION AUDIT

Purpose:
    Test simple two-feature combinations using validated microstructure features.

Method:
    For each feature dataset:
        - Load validated relationships from Script 17
        - Select top validated features per symbol/bar_type/parameter/target
        - Build simple combinations:
            feature_a + feature_b
            feature_a - feature_b
            feature_a * feature_b
        - Correlate each combination with the target
        - Compare combination correlation against the best individual feature

Important:
    This is NOT strategy modelling.
    This is an interaction discovery audit.
"""

from pathlib import Path
from datetime import datetime, timezone
from itertools import combinations
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

VALIDATION_LEVELS = {"validated", "partially_validated"}
MAX_FEATURES_PER_GROUP = 8
MIN_SAMPLE_SIZE = 50


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


def safe_corr(x: pd.Series, y: pd.Series) -> tuple[float | None, int]:
    pair = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    sample_size = len(pair)

    if sample_size < MIN_SAMPLE_SIZE:
        return None, sample_size

    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return None, sample_size

    corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])

    if pd.isna(corr):
        return None, sample_size

    return float(corr), sample_size


def classify_combination_result(
    combo_abs_corr: float | None,
    best_individual_abs_corr: float | None,
    sample_size: int,
) -> str:
    if combo_abs_corr is None or best_individual_abs_corr is None:
        return "insufficient"

    improvement = combo_abs_corr - best_individual_abs_corr

    if sample_size < 100:
        return "low_sample"

    if improvement >= 0.05 and combo_abs_corr >= 0.10:
        return "strong_improvement"

    if improvement >= 0.025 and combo_abs_corr >= 0.075:
        return "research_improvement"

    if improvement > 0:
        return "minor_improvement"

    return "no_improvement"


def make_combination(a: pd.Series, b: pd.Series, method: str) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")

    if method == "sum":
        return a + b

    if method == "difference":
        return a - b

    if method == "product":
        return a * b

    raise ValueError(f"Unknown combination method: {method}")


def audit_group(group_df: pd.DataFrame) -> list[dict]:
    group_df = group_df.sort_values(
        ["test_abs_correlation", "test_sample_size"],
        ascending=[False, False],
    ).head(MAX_FEATURES_PER_GROUP)

    if len(group_df) < 2:
        return []

    file_path = Path(group_df["file_path"].iloc[0])
    target = group_df["target"].iloc[0]

    if not file_path.exists():
        return []

    try:
        feature_df = pd.read_parquet(file_path)
    except Exception:
        return []

    if feature_df.empty or target not in feature_df.columns:
        return []

    records = []

    for row_a, row_b in combinations(group_df.to_dict(orient="records"), 2):
        feature_a = row_a["feature_name"]
        feature_b = row_b["feature_name"]

        if feature_a not in feature_df.columns or feature_b not in feature_df.columns:
            continue

        target_series = pd.to_numeric(feature_df[target], errors="coerce")

        corr_a, sample_a = safe_corr(feature_df[feature_a], target_series)
        corr_b, sample_b = safe_corr(feature_df[feature_b], target_series)

        abs_corr_a = abs(corr_a) if corr_a is not None else None
        abs_corr_b = abs(corr_b) if corr_b is not None else None

        if abs_corr_a is None or abs_corr_b is None:
            continue

        best_individual_abs_corr = max(abs_corr_a, abs_corr_b)
        best_individual_feature = feature_a if abs_corr_a >= abs_corr_b else feature_b

        for method in ["sum", "difference", "product"]:
            combo_series = make_combination(
                feature_df[feature_a],
                feature_df[feature_b],
                method,
            )

            combo_corr, combo_sample = safe_corr(combo_series, target_series)
            combo_abs_corr = abs(combo_corr) if combo_corr is not None else None

            improvement = (
                combo_abs_corr - best_individual_abs_corr
                if combo_abs_corr is not None
                else None
            )

            records.append(
                {
                    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                    "symbol": row_a["symbol"],
                    "bar_type": row_a["bar_type"],
                    "parameter": row_a["parameter"],
                    "target": target,
                    "file_path": str(file_path),
                    "feature_a": feature_a,
                    "feature_a_family": row_a["feature_family"],
                    "feature_a_corr": corr_a,
                    "feature_a_abs_corr": abs_corr_a,
                    "feature_b": feature_b,
                    "feature_b_family": row_b["feature_family"],
                    "feature_b_corr": corr_b,
                    "feature_b_abs_corr": abs_corr_b,
                    "combination_method": method,
                    "combination_name": f"{feature_a}__{method}__{feature_b}",
                    "combination_corr": combo_corr,
                    "combination_abs_corr": combo_abs_corr,
                    "combination_sample_size": combo_sample,
                    "best_individual_feature": best_individual_feature,
                    "best_individual_abs_corr": best_individual_abs_corr,
                    "correlation_improvement": improvement,
                    "combination_label": classify_combination_result(
                        combo_abs_corr,
                        best_individual_abs_corr,
                        combo_sample,
                    ),
                }
            )

    return records


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 19 - FEATURE COMBINATION AUDIT")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    cv_path = (
        analysis_dir
        / "cross_validation_audit"
        / "microstructure_cross_validation_audit_latest.csv"
    )

    report_dir = analysis_dir / "feature_combination_audit"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cross-validation audit: {cv_path}")
    print(f"Report dir:             {report_dir}")
    print(f"Max features/group:     {MAX_FEATURES_PER_GROUP}")
    print("-" * 90)

    if not cv_path.exists():
        raise FileNotFoundError(
            f"Missing cross-validation audit file: {cv_path}. Run script 17 first."
        )

    cv_df = pd.read_csv(cv_path)

    validated_df = cv_df[
        cv_df["validation_label"].isin(VALIDATION_LEVELS)
    ].copy()

    group_cols = ["symbol", "bar_type", "parameter", "target", "file_path"]

    print(f"CV rows:                {len(cv_df):,}")
    print(f"Validated/partial rows: {len(validated_df):,}")
    print(f"Groups to audit:        {validated_df.groupby(group_cols).ngroups:,}")
    print("-" * 90)

    all_records = []

    for idx, (_, group_df) in enumerate(validated_df.groupby(group_cols), start=1):
        records = audit_group(group_df)
        all_records.extend(records)

        if idx % 25 == 0 or idx == 1:
            first = group_df.iloc[0]
            print(
                f"[GROUP] {idx:,} "
                f"{first['symbol']} {first['bar_type']} {first['parameter']} "
                f"{first['target']} combinations={len(records):,}"
            )

    results_df = pd.DataFrame(all_records)

    if results_df.empty:
        raise RuntimeError("No feature combination audit records created.")

    results_df = results_df.sort_values(
        ["correlation_improvement", "combination_abs_corr", "combination_sample_size"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    csv_path = report_dir / "microstructure_feature_combination_audit_latest.csv"
    json_path = report_dir / "microstructure_feature_combination_audit_latest.json"
    txt_path = report_dir / "microstructure_feature_combination_audit_latest.txt"

    results_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results_df.to_dict(orient="records"), f, indent=2, default=str)

    label_counts = results_df["combination_label"].value_counts(dropna=False).to_dict()
    method_counts = results_df["combination_method"].value_counts(dropna=False).to_dict()

    improvement_df = results_df[
        results_df["combination_label"].isin(
            ["strong_improvement", "research_improvement", "minor_improvement"]
        )
    ].copy()

    top_combinations = results_df.head(50)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "target",
            "feature_a",
            "feature_b",
            "combination_method",
            "combination_corr",
            "combination_abs_corr",
            "best_individual_abs_corr",
            "correlation_improvement",
            "combination_sample_size",
            "combination_label",
        ]
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE COMBINATION AUDIT")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"CV rows:                {len(cv_df):,}")
    lines.append(f"Validated/partial rows: {len(validated_df):,}")
    lines.append(f"Combination records:    {len(results_df):,}")
    lines.append(f"Improvement rows:       {len(improvement_df):,}")
    lines.append("")
    lines.append(f"Combination labels: {label_counts}")
    lines.append(f"Method counts:      {method_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 50 FEATURE COMBINATIONS")
    lines.append("-" * 90)
    lines.append(top_combinations.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Feature combination audit complete.")
    print(f"Combination records: {len(results_df):,}")
    print(f"Improvement rows:    {len(improvement_df):,}")
    print(f"Label counts:        {label_counts}")
    print(f"Method counts:       {method_counts}")
    print(f"CSV output:          {csv_path}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()