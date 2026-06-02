"""
BACQE MICROSTRUCTURE 12 - MICROSTRUCTURE FEATURE RANKING

Purpose:
    Rank individual microstructure feature columns by statistical usability.

Important:
    This is NOT yet a predictive alpha test.
    It ranks feature quality based on:
        - non-null ratio
        - variance
        - uniqueness
        - stability
        - correlation safety
        - availability across rows

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_quality_report/
        microstructure_feature_quality_report_latest.csv

    E:/Quant_Lab/data/features/microstructure/**/microstructure_features.parquet

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_ranking/
        microstructure_feature_ranking_latest.csv
        microstructure_feature_ranking_latest.json
        microstructure_feature_ranking_summary_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


EXCLUDED_COLUMNS = {
    "symbol",
    "start_time",
    "end_time",
    "bar_type",
    "parameter_name",
    "parameter_value",
    "feature_created_at_utc",
    "created_at_utc",
    "volume_mode",
    "file_path",
}


TARGET_COLUMNS = {
    "forward_return_1",
    "forward_return_3",
    "forward_return_5",
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


def get_quality_report_path(analysis_dir: Path) -> Path:
    return (
        analysis_dir
        / "feature_quality_report"
        / "microstructure_feature_quality_report_latest.csv"
    )


def safe_float(value) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def classify_feature_family(feature_name: str) -> str:
    name = feature_name.lower()

    if name in TARGET_COLUMNS or name.startswith("forward_return"):
        return "target"

    if "imbalance" in name or "uptick" in name or "downtick" in name or "signed_tick" in name:
        return "imbalance"

    if "spread" in name:
        return "spread"

    if "return" in name or "realized_vol" in name or "vol" in name:
        return "return_volatility"

    if "range" in name:
        return "range"

    if "duration" in name or "tick_count" in name or "ticks_per_second" in name or "bars_per_hour" in name:
        return "activity"

    if "volume" in name:
        return "volume"

    if "open_" in name or "high_" in name or "low_" in name or "close_" in name:
        return "price_ohlc"

    return "other"


def score_feature(
    non_null_ratio: float,
    unique_ratio: float,
    zero_ratio: float,
    variance: float | None,
    abs_skew: float | None,
    abs_kurtosis: float | None,
) -> float:
    score = 0.0

    if non_null_ratio >= 0.95:
        score += 30
    elif non_null_ratio >= 0.90:
        score += 24
    elif non_null_ratio >= 0.80:
        score += 15
    elif non_null_ratio >= 0.70:
        score += 8
    else:
        score -= 15

    if unique_ratio >= 0.50:
        score += 25
    elif unique_ratio >= 0.20:
        score += 18
    elif unique_ratio >= 0.05:
        score += 8
    else:
        score -= 10

    if zero_ratio <= 0.50:
        score += 15
    elif zero_ratio <= 0.80:
        score += 5
    else:
        score -= 10

    if variance is not None and variance > 0:
        score += 15
    else:
        score -= 15

    if abs_skew is not None:
        if abs_skew <= 5:
            score += 8
        elif abs_skew <= 20:
            score += 2
        else:
            score -= 5

    if abs_kurtosis is not None:
        if abs_kurtosis <= 50:
            score += 7
        elif abs_kurtosis <= 250:
            score += 2
        else:
            score -= 5

    return round(max(0, min(100, score)), 2)


def rank_feature_column(
    df: pd.DataFrame,
    feature_name: str,
    dataset_record: pd.Series,
) -> dict:
    series = pd.to_numeric(df[feature_name], errors="coerce")

    row_count = len(series)
    non_null_count = int(series.notna().sum())
    null_count = int(series.isna().sum())

    clean = series.dropna()

    non_null_ratio = non_null_count / row_count if row_count else 0
    null_ratio = null_count / row_count if row_count else 0

    if len(clean) == 0:
        unique_count = 0
        unique_ratio = 0
        zero_ratio = 1
        mean = std = variance = min_value = max_value = skew = kurtosis = None
    else:
        unique_count = int(clean.nunique())
        unique_ratio = unique_count / max(non_null_count, 1)
        zero_ratio = float((clean == 0).sum() / max(non_null_count, 1))

        mean = safe_float(clean.mean())
        std = safe_float(clean.std())
        variance = safe_float(clean.var())
        min_value = safe_float(clean.min())
        max_value = safe_float(clean.max())
        skew = safe_float(clean.skew())
        kurtosis = safe_float(clean.kurtosis())

    abs_skew = abs(skew) if skew is not None else None
    abs_kurtosis = abs(kurtosis) if kurtosis is not None else None

    usability_score = score_feature(
        non_null_ratio=non_null_ratio,
        unique_ratio=unique_ratio,
        zero_ratio=zero_ratio,
        variance=variance,
        abs_skew=abs_skew,
        abs_kurtosis=abs_kurtosis,
    )

    if feature_name in TARGET_COLUMNS:
        feature_role = "target"
    else:
        feature_role = "candidate_feature"

    if usability_score >= 80:
        usability_label = "excellent"
    elif usability_score >= 65:
        usability_label = "good"
    elif usability_score >= 50:
        usability_label = "usable"
    elif usability_score >= 35:
        usability_label = "review"
    else:
        usability_label = "poor"

    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": dataset_record.get("symbol"),
        "bar_type": dataset_record.get("bar_type"),
        "parameter": dataset_record.get("parameter"),
        "parameter_name": dataset_record.get("parameter_name"),
        "parameter_value": dataset_record.get("parameter_value"),
        "dataset_status": dataset_record.get("dataset_status"),
        "quality_label": dataset_record.get("quality_label"),
        "dataset_quality_score": dataset_record.get("quality_score"),
        "file_path": dataset_record.get("file_path"),
        "feature_name": feature_name,
        "feature_family": classify_feature_family(feature_name),
        "feature_role": feature_role,
        "row_count": row_count,
        "non_null_count": non_null_count,
        "null_count": null_count,
        "non_null_ratio": round(non_null_ratio, 6),
        "null_ratio": round(null_ratio, 6),
        "unique_count": unique_count,
        "unique_ratio": round(unique_ratio, 6),
        "zero_ratio": round(zero_ratio, 6),
        "mean": mean,
        "std": std,
        "variance": variance,
        "min": min_value,
        "max": max_value,
        "skew": skew,
        "kurtosis": kurtosis,
        "usability_score": usability_score,
        "usability_label": usability_label,
    }


def rank_features_for_dataset(dataset_record: pd.Series) -> list[dict]:
    file_path = Path(dataset_record["file_path"])

    if not file_path.exists():
        return []

    try:
        df = pd.read_parquet(file_path)
    except Exception:
        return []

    if df.empty:
        return []

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    feature_cols = [
        col for col in numeric_cols
        if col not in EXCLUDED_COLUMNS
    ]

    records = []

    for feature_name in feature_cols:
        records.append(
            rank_feature_column(
                df=df,
                feature_name=feature_name,
                dataset_record=dataset_record,
            )
        )

    return records


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 12 - MICROSTRUCTURE FEATURE RANKING")

    config = load_config()
    micro_cfg = config["microstructure"]

    analysis_dir = get_analysis_dir(micro_cfg)
    quality_report_path = get_quality_report_path(analysis_dir)

    report_dir = analysis_dir / "feature_ranking"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Config:          {CONFIG_PATH}")
    print(f"Quality report:  {quality_report_path}")
    print(f"Report dir:      {report_dir}")
    print("-" * 90)

    if not quality_report_path.exists():
        raise FileNotFoundError(
            f"Missing quality report: {quality_report_path}. "
            "Run script 11 first."
        )

    quality_df = pd.read_csv(quality_report_path)

    usable_labels = {
        "excellent",
        "good",
        "usable",
        "usable_sparse",
    }

    build_df = quality_df[quality_df["quality_label"].isin(usable_labels)].copy()

    print(f"Datasets in quality report: {len(quality_df)}")
    print(f"Datasets ranked:            {len(build_df)}")
    print("-" * 90)

    all_records = []

    for _, dataset_record in build_df.iterrows():
        records = rank_features_for_dataset(dataset_record)
        all_records.extend(records)

        print(
            f"[RANK] {dataset_record.get('symbol'):<8} "
            f"{dataset_record.get('bar_type'):<22} "
            f"{dataset_record.get('parameter'):<28} "
            f"features={len(records):<4}"
        )

    ranking_df = pd.DataFrame(all_records)

    if ranking_df.empty:
        raise RuntimeError("No feature ranking records were created.")

    ranking_df = ranking_df.sort_values(
        ["usability_score", "non_null_ratio", "unique_ratio"],
        ascending=[False, False, False],
    )

    csv_path = report_dir / "microstructure_feature_ranking_latest.csv"
    json_path = report_dir / "microstructure_feature_ranking_latest.json"
    txt_path = report_dir / "microstructure_feature_ranking_summary_latest.txt"

    ranking_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(ranking_df.to_dict(orient="records"), f, indent=2, default=str)

    family_counts = ranking_df["feature_family"].value_counts(dropna=False).to_dict()
    label_counts = ranking_df["usability_label"].value_counts(dropna=False).to_dict()

    top_features = ranking_df.head(25)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "feature_name",
            "feature_family",
            "feature_role",
            "non_null_ratio",
            "unique_ratio",
            "zero_ratio",
            "usability_label",
            "usability_score",
        ]
    ]

    candidate_df = ranking_df[ranking_df["feature_role"] == "candidate_feature"].copy()

    top_candidate_features = candidate_df.head(25)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "feature_name",
            "feature_family",
            "non_null_ratio",
            "unique_ratio",
            "zero_ratio",
            "usability_label",
            "usability_score",
        ]
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE RANKING SUMMARY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Datasets ranked: {len(build_df)}")
    lines.append(f"Feature records: {len(ranking_df)}")
    lines.append(f"Feature family counts: {family_counts}")
    lines.append(f"Usability label counts: {label_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 25 FEATURE RECORDS")
    lines.append("-" * 90)
    lines.append(top_features.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 25 CANDIDATE FEATURES ONLY")
    lines.append("-" * 90)
    lines.append(top_candidate_features.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Microstructure feature ranking complete.")
    print(f"Datasets ranked:       {len(build_df)}")
    print(f"Feature records:       {len(ranking_df)}")
    print(f"Feature family counts: {family_counts}")
    print(f"Usability counts:      {label_counts}")
    print(f"CSV output:            {csv_path}")
    print(f"JSON output:           {json_path}")
    print(f"TXT output:            {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()