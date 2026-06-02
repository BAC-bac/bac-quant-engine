"""
BACQE MICROSTRUCTURE 10 - MICROSTRUCTURE FEATURE INVENTORY

Purpose:
    Create a registry/inventory of all microstructure feature datasets.

Inputs:
    E:/Quant_Lab/data/features/microstructure/**/microstructure_features.parquet

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_inventory/
        microstructure_feature_inventory_latest.csv
        microstructure_feature_inventory_latest.json
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


def get_features_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "features_dir",
            "E:/Quant_Lab/data/features/microstructure",
        )
    )


def parse_path_metadata(file_path: Path) -> dict:
    parts = file_path.parts

    metadata = {
        "symbol": None,
        "bar_type": None,
        "parameter": None,
        "parameter_name": None,
        "parameter_value": None,
    }

    for part in parts:
        if part.startswith("symbol="):
            metadata["symbol"] = part.replace("symbol=", "")

        elif part.startswith("bar_type="):
            metadata["bar_type"] = part.replace("bar_type=", "")

        elif part.startswith("parameter="):
            parameter = part.replace("parameter=", "")
            metadata["parameter"] = parameter

            split_parameter = parameter.rsplit("_", 1)

            if len(split_parameter) == 2:
                metadata["parameter_name"] = split_parameter[0]

                try:
                    metadata["parameter_value"] = int(split_parameter[1])
                except ValueError:
                    metadata["parameter_value"] = split_parameter[1]

    return metadata


def classify_feature_columns(columns: list[str]) -> dict:
    feature_groups = {
        "return_features": [
            c for c in columns
            if "return" in c or "realized_vol" in c
        ],
        "range_features": [
            c for c in columns
            if "range" in c
        ],
        "spread_features": [
            c for c in columns
            if "spread" in c
        ],
        "activity_features": [
            c for c in columns
            if "tick_count" in c or "duration" in c or "bars_per_hour" in c or "ticks_per_second" in c
        ],
        "imbalance_features": [
            c for c in columns
            if "imbalance" in c or "uptick" in c or "downtick" in c or "signed_tick" in c
        ],
        "target_features": [
            c for c in columns
            if c.startswith("forward_return")
        ],
    }

    return {f"{group}_count": len(cols) for group, cols in feature_groups.items()}


def summarise_feature_file(file_path: Path, min_rows: int) -> dict:
    checked_at = datetime.now(timezone.utc).isoformat()
    path_meta = parse_path_metadata(file_path)

    record = {
        "checked_at_utc": checked_at,
        "file_path": str(file_path),
        "file_exists": file_path.exists(),
        "read_status": "unknown",
        "dataset_status": "unknown",
        "row_count": 0,
        "column_count": 0,
        "numeric_column_count": 0,
        "object_column_count": 0,
        "datetime_column_count": 0,
        "start_time_min": None,
        "end_time_max": None,
        "nan_count_total": None,
        "nan_ratio": None,
        "inf_count_total": None,
        "constant_numeric_column_count": None,
        "feature_column_count": None,
        "memory_mb": None,
        "issues": [],
        **path_meta,
    }

    if not file_path.exists():
        record["read_status"] = "missing"
        record["dataset_status"] = "missing"
        record["issues"].append("file_missing")
        return record

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        record["read_status"] = "failed_read"
        record["dataset_status"] = "failed_read"
        record["issues"].append(f"failed_read: {exc}")
        return record

    record["read_status"] = "ok"
    record["row_count"] = len(df)
    record["column_count"] = len(df.columns)
    record["memory_mb"] = round(float(df.memory_usage(deep=True).sum() / 1024 / 1024), 4)

    if df.empty:
        record["dataset_status"] = "empty"
        record["issues"].append("empty_file")
        return record

    if "start_time" in df.columns:
        start_time = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        record["start_time_min"] = str(start_time.min())

    if "end_time" in df.columns:
        end_time = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        record["end_time_max"] = str(end_time.max())

    numeric_df = df.select_dtypes(include=[np.number])
    object_df = df.select_dtypes(include=["object"])
    datetime_df = df.select_dtypes(include=["datetime", "datetimetz"])

    record["numeric_column_count"] = len(numeric_df.columns)
    record["object_column_count"] = len(object_df.columns)
    record["datetime_column_count"] = len(datetime_df.columns)

    total_cells = max(df.shape[0] * df.shape[1], 1)
    nan_count = int(df.isna().sum().sum())

    record["nan_count_total"] = nan_count
    record["nan_ratio"] = float(nan_count / total_cells)

    if not numeric_df.empty:
        record["inf_count_total"] = int(np.isinf(numeric_df.to_numpy()).sum())

        constant_cols = []
        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            if len(series) > 1 and series.nunique() <= 1:
                constant_cols.append(col)

        record["constant_numeric_column_count"] = len(constant_cols)
    else:
        record["inf_count_total"] = 0
        record["constant_numeric_column_count"] = 0

    excluded_cols = {
        "symbol",
        "start_time",
        "end_time",
        "bar_type",
        "parameter_name",
        "parameter_value",
        "feature_created_at_utc",
    }

    record["feature_column_count"] = len([c for c in df.columns if c not in excluded_cols])

    group_counts = classify_feature_columns(list(df.columns))
    record.update(group_counts)

    if record["row_count"] < min_rows:
        record["issues"].append("below_min_rows")

    if record["inf_count_total"] > 0:
        record["issues"].append("inf_values_found")

    if record["nan_ratio"] is not None and record["nan_ratio"] > 0.25:
        record["issues"].append("high_nan_ratio")

    if record["feature_column_count"] is not None and record["feature_column_count"] < 50:
        record["issues"].append("low_feature_count")

    if record["issues"]:
        if record["issues"] == ["below_min_rows"]:
            record["dataset_status"] = "usable_sparse"
        else:
            record["dataset_status"] = "warning"
    else:
        record["dataset_status"] = "ready"

    return record


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 10 - MICROSTRUCTURE FEATURE INVENTORY")

    config = load_config()
    micro_cfg = config["microstructure"]

    features_dir = get_features_dir(micro_cfg)
    analysis_dir = get_analysis_dir(micro_cfg)
    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    report_dir = analysis_dir / "feature_inventory"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Config:       {CONFIG_PATH}")
    print(f"Features dir: {features_dir}")
    print(f"Report dir:   {report_dir}")
    print(f"Min rows:     {min_rows}")
    print("-" * 90)

    feature_files = sorted(features_dir.glob("**/microstructure_features.parquet"))

    records = []

    for file_path in feature_files:
        record = summarise_feature_file(file_path, min_rows=min_rows)
        records.append(record)

        print(
            f"[ITEM] {record['symbol']:<8} "
            f"{record['bar_type']:<22} "
            f"{str(record['parameter']):<28} "
            f"status={record['dataset_status']:<14} "
            f"rows={record['row_count']:<7,} "
            f"cols={record['column_count']:<4} "
            f"nan_ratio={record['nan_ratio']}"
        )

        if record["issues"]:
            print(f"       issues={record['issues']}")

    inventory_df = pd.DataFrame(records)

    csv_path = report_dir / "microstructure_feature_inventory_latest.csv"
    json_path = report_dir / "microstructure_feature_inventory_latest.json"

    inventory_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

    status_counts = (
        inventory_df["dataset_status"].value_counts(dropna=False).to_dict()
        if not inventory_df.empty
        else {}
    )

    bar_type_counts = (
        inventory_df["bar_type"].value_counts(dropna=False).to_dict()
        if not inventory_df.empty
        else {}
    )

    print("-" * 90)
    print("[DONE] Microstructure feature inventory generated.")
    print(f"Feature files discovered: {len(feature_files)}")
    print(f"Inventory rows:           {len(inventory_df)}")
    print(f"Status counts:            {status_counts}")
    print(f"Bar type counts:          {bar_type_counts}")
    print(f"CSV output:               {csv_path}")
    print(f"JSON output:              {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()