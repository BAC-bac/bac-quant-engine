"""
BACQE MICROSTRUCTURE 09 - VALIDATE MICROSTRUCTURE FEATURES
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(micro_cfg["output"].get("analysis_dir", "E:/Quant_Lab/data/analysis/microstructure"))


def get_features_dir(micro_cfg: dict) -> Path:
    return Path(micro_cfg["output"].get("features_dir", "E:/Quant_Lab/data/features/microstructure"))


def validate_feature_file(file_path: Path, min_rows: int) -> dict:
    result = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_path": str(file_path),
        "file_exists": file_path.exists(),
        "status": "unknown",
        "row_count": 0,
        "column_count": 0,
        "feature_column_count": 0,
        "start_time_min": None,
        "end_time_max": None,
        "duplicate_end_time_rows": None,
        "total_nan_count": None,
        "total_inf_count": None,
        "nan_ratio": None,
        "missing_forward_return_1": None,
        "missing_forward_return_3": None,
        "missing_forward_return_5": None,
        "constant_numeric_columns": 0,
        "issues": [],
    }

    if not file_path.exists():
        result["status"] = "missing"
        result["issues"].append("file_missing")
        return result

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        result["status"] = "failed_read"
        result["issues"].append(f"failed_read: {exc}")
        return result

    result["row_count"] = len(df)
    result["column_count"] = len(df.columns)

    if df.empty:
        result["status"] = "empty"
        result["issues"].append("empty_file")
        return result

    if result["row_count"] < min_rows:
        result["issues"].append("below_min_rows")

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        result["end_time_max"] = str(df["end_time"].max())
        result["duplicate_end_time_rows"] = int(df.duplicated(subset=["end_time"]).sum())

    if "start_time" in df.columns:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        result["start_time_min"] = str(df["start_time"].min())

    numeric_df = df.select_dtypes(include=[np.number])

    result["feature_column_count"] = int(
        len([c for c in df.columns if c not in {"symbol", "start_time", "end_time"}])
    )

    total_cells = max(df.shape[0] * df.shape[1], 1)
    total_nan = int(df.isna().sum().sum())
    result["total_nan_count"] = total_nan
    result["nan_ratio"] = float(total_nan / total_cells)

    if not numeric_df.empty:
        inf_count = int(np.isinf(numeric_df.to_numpy()).sum())
        result["total_inf_count"] = inf_count

        constant_cols = []
        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            if len(series) > 1 and series.nunique() <= 1:
                constant_cols.append(col)

        result["constant_numeric_columns"] = len(constant_cols)

    for col in ["forward_return_1", "forward_return_3", "forward_return_5"]:
        if col in df.columns:
            result[f"missing_{col}"] = int(df[col].isna().sum())
        else:
            result["issues"].append(f"missing_column_{col}")

    if result["total_inf_count"] and result["total_inf_count"] > 0:
        result["issues"].append("inf_values_found")

    if result["duplicate_end_time_rows"] and result["duplicate_end_time_rows"] > 0:
        result["issues"].append("duplicate_end_times")

    if result["nan_ratio"] is not None and result["nan_ratio"] > 0.25:
        result["issues"].append("high_nan_ratio")

    if result["feature_column_count"] < 50:
        result["issues"].append("low_feature_column_count")

    result["status"] = "warning" if result["issues"] else "ok"
    return result


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 09 - VALIDATE MICROSTRUCTURE FEATURES")

    config = load_config()
    micro_cfg = config["microstructure"]

    features_dir = get_features_dir(micro_cfg)
    analysis_dir = get_analysis_dir(micro_cfg)
    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    report_dir = analysis_dir / "feature_validation"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Features dir: {features_dir}")
    print(f"Report dir:   {report_dir}")
    print("-" * 90)

    feature_files = sorted(features_dir.glob("**/microstructure_features.parquet"))
    print(f"Feature files discovered: {len(feature_files)}")
    print("-" * 90)

    results = []

    for file_path in feature_files:
        result = validate_feature_file(file_path, min_rows=min_rows)
        results.append(result)

        print(
            f"[CHECK] status={result['status']:<8} "
            f"rows={result['row_count']:<7,} "
            f"cols={result['column_count']:<4} "
            f"nan_ratio={result['nan_ratio'] if result['nan_ratio'] is not None else 'NA'} "
            f"-> {file_path}"
        )

        if result["issues"]:
            print(f"        issues={result['issues']}")

    results_df = pd.DataFrame(results)

    csv_path = report_dir / "microstructure_feature_validation_latest.csv"
    json_path = report_dir / "microstructure_feature_validation_latest.json"

    results_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    status_counts = results_df["status"].value_counts(dropna=False).to_dict() if not results_df.empty else {}

    print("-" * 90)
    print("[DONE] Microstructure feature validation complete.")
    print(f"Files checked: {len(results_df)}")
    print(f"Status counts: {status_counts}")
    print(f"CSV output:    {csv_path}")
    print(f"JSON output:   {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()