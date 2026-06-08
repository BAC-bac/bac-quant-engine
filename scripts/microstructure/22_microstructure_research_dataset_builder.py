"""
BACQE MICROSTRUCTURE 22 - RESEARCH DATASET BUILDER

Purpose:
    Build clean modelling-ready research datasets from the microstructure
    feature store using the Script 21 master shortlist.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/research_master_shortlist/
        microstructure_research_master_shortlist_features_latest.csv

    E:/Quant_Lab/data/features/microstructure/**/microstructure_features.parquet

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/research_datasets/
        symbol=GBPUSD/bar_type=volume_bars/parameter=volume_threshold_100/
            microstructure_research_dataset.parquet

        microstructure_research_dataset_inventory_latest.csv
        microstructure_research_dataset_inventory_latest.json
        microstructure_research_dataset_inventory_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

PROTECTED_SPREAD_FEATURES = [
    "open_spread",
    "high_spread",
    "low_spread",
    "close_spread",
    "spread_mean",
    "spread_min",
    "spread_max",
    "spread_range",
    "spread_pct_of_mid",
    "spread_mean_3",
    "spread_mean_5",
    "spread_mean_10",
]

TARGET_COLUMNS = [
    "forward_return_1",
    "forward_return_3",
    "forward_return_5",
]

CORE_METADATA_COLUMNS = [
    "symbol",
    "start_time",
    "end_time",
    "bar_type",
    "parameter_name",
    "parameter_value",
]

TOP_FEATURE_LIMIT = 30

SELECTED_TIERS = {
    "tier_1_core_candidate",
    "tier_2_microstructure_candidate",
    "tier_2_baseline_candidate",
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


def get_features_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "features_dir",
            "E:/Quant_Lab/data/features/microstructure",
        )
    )


def parse_feature_file_metadata(file_path: Path) -> dict:
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


def load_selected_features(shortlist_path: Path) -> list[str]:
    if not shortlist_path.exists():
        raise FileNotFoundError(
            f"Missing master shortlist file: {shortlist_path}. "
            "Run script 21 first."
        )

    shortlist_df = pd.read_csv(shortlist_path)

    selected_df = shortlist_df[
        shortlist_df["master_tier"].isin(SELECTED_TIERS)
    ].copy()

    selected_df = selected_df.sort_values(
        ["master_score", "validated_count", "avg_test_abs_correlation"],
        ascending=[False, False, False],
    )

    selected_features = (
        selected_df["feature_name"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .head(TOP_FEATURE_LIMIT)
        .tolist()
    )

    all_features = selected_features.copy()

    for feature in PROTECTED_SPREAD_FEATURES:
        if feature not in all_features:
            all_features.append(feature)

    return all_features


def clean_research_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.replace([np.inf, -np.inf], np.nan)

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")

    if "start_time" in df.columns:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")

    df = df.sort_values("end_time").reset_index(drop=True)

    return df


def build_dataset_for_file(
    feature_file: Path,
    selected_features: list[str],
    output_root: Path,
) -> dict:
    metadata = parse_feature_file_metadata(feature_file)

    record = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": metadata["symbol"],
        "bar_type": metadata["bar_type"],
        "parameter": metadata["parameter"],
        "input_file": str(feature_file),
        "output_file": None,
        "status": "unknown",
        "row_count": 0,
        "column_count": 0,
        "selected_feature_count": 0,
        "available_selected_feature_count": 0,
        "target_count": 0,
        "nan_ratio": None,
        "dropped_missing_targets_rows": 0,
        "error": None,
    }

    try:
        df = pd.read_parquet(feature_file)
    except Exception as exc:
        record["status"] = "failed_read"
        record["error"] = str(exc)
        return record

    if df.empty:
        record["status"] = "empty_input"
        record["error"] = "Feature file is empty."
        return record

    available_metadata = [c for c in CORE_METADATA_COLUMNS if c in df.columns]
    available_targets = [c for c in TARGET_COLUMNS if c in df.columns]
    available_features = [c for c in selected_features if c in df.columns]

    record["selected_feature_count"] = len(selected_features)
    record["available_selected_feature_count"] = len(available_features)
    record["target_count"] = len(available_targets)

    if not available_features:
        record["status"] = "no_selected_features"
        record["error"] = "No selected shortlist features found in this feature file."
        return record

    if not available_targets:
        record["status"] = "no_targets"
        record["error"] = "No target columns found."
        return record

    keep_cols = available_metadata + available_features + available_targets
    research_df = df[keep_cols].copy()
    research_df = clean_research_dataset(research_df)

    before_drop = len(research_df)
    research_df = research_df.dropna(subset=available_targets, how="all").reset_index(drop=True)
    after_drop = len(research_df)

    record["dropped_missing_targets_rows"] = before_drop - after_drop

    if research_df.empty:
        record["status"] = "empty_after_target_drop"
        record["error"] = "No rows left after dropping missing target rows."
        return record

    total_cells = max(research_df.shape[0] * research_df.shape[1], 1)

    record["row_count"] = len(research_df)
    record["column_count"] = len(research_df.columns)
    record["nan_ratio"] = float(research_df.isna().sum().sum() / total_cells)

    symbol = metadata["symbol"]
    bar_type = metadata["bar_type"]
    parameter = metadata["parameter"]

    save_dir = (
        output_root
        / f"symbol={symbol}"
        / f"bar_type={bar_type}"
        / f"parameter={parameter}"
    )
    save_dir.mkdir(parents=True, exist_ok=True)

    output_file = save_dir / "microstructure_research_dataset.parquet"

    try:
        research_df.to_parquet(output_file, index=False)
    except Exception as exc:
        record["status"] = "failed_write"
        record["error"] = str(exc)
        return record

    record["output_file"] = str(output_file)
    record["status"] = "ok"

    return record


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 22 - RESEARCH DATASET BUILDER")

    config = load_config()
    micro_cfg = config["microstructure"]

    analysis_dir = get_analysis_dir(micro_cfg)
    features_dir = get_features_dir(micro_cfg)

    shortlist_path = (
        analysis_dir
        / "research_master_shortlist"
        / "microstructure_research_master_shortlist_features_latest.csv"
    )

    output_root = analysis_dir / "research_datasets"
    output_root.mkdir(parents=True, exist_ok=True)

    print(f"Shortlist:    {shortlist_path}")
    print(f"Features dir: {features_dir}")
    print(f"Output root:  {output_root}")
    print("-" * 90)

    selected_features = load_selected_features(shortlist_path)

    print(f"Selected feature limit: {TOP_FEATURE_LIMIT}")
    print(f"Selected features:      {selected_features}")
    print("-" * 90)

    feature_files = sorted(features_dir.glob("**/microstructure_features.parquet"))

    print(f"Feature files discovered: {len(feature_files)}")
    print("-" * 90)

    records = []

    for feature_file in feature_files:
        record = build_dataset_for_file(
            feature_file=feature_file,
            selected_features=selected_features,
            output_root=output_root,
        )

        records.append(record)

        print(
            f"[BUILD] {record['symbol']:<8} "
            f"{record['bar_type']:<22} "
            f"{str(record['parameter']):<28} "
            f"status={record['status']:<22} "
            f"rows={record['row_count']:<7,} "
            f"features={record['available_selected_feature_count']}"
        )

        if record["error"]:
            print(f"        error={record['error']}")

    inventory_df = pd.DataFrame(records)

    inventory_csv = output_root / "microstructure_research_dataset_inventory_latest.csv"
    inventory_json = output_root / "microstructure_research_dataset_inventory_latest.json"
    inventory_txt = output_root / "microstructure_research_dataset_inventory_latest.txt"

    inventory_df.to_csv(inventory_csv, index=False)

    with open(inventory_json, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

    status_counts = inventory_df["status"].value_counts(dropna=False).to_dict()
    ok_df = inventory_df[inventory_df["status"] == "ok"].copy()

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE RESEARCH DATASET INVENTORY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Feature files discovered: {len(feature_files)}")
    lines.append(f"Research datasets built: {len(ok_df)}")
    lines.append(f"Status counts: {status_counts}")
    lines.append("")
    lines.append(f"Selected features ({len(selected_features)}):")
    for feature in selected_features:
        lines.append(f"  - {feature}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("DATASET INVENTORY")
    lines.append("-" * 90)

    if not ok_df.empty:
        display_cols = [
            "symbol",
            "bar_type",
            "parameter",
            "status",
            "row_count",
            "column_count",
            "available_selected_feature_count",
            "target_count",
            "nan_ratio",
            "output_file",
        ]
        lines.append(ok_df[display_cols].to_string(index=False))
    else:
        lines.append("No successful research datasets built.")

    lines.append("")
    lines.append("=" * 90)

    with open(inventory_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Research dataset build complete.")
    print(f"Feature files discovered: {len(feature_files)}")
    print(f"Research datasets built:  {len(ok_df)}")
    print(f"Status counts:            {status_counts}")
    print(f"Inventory CSV:            {inventory_csv}")
    print(f"Inventory JSON:           {inventory_json}")
    print(f"Inventory TXT:            {inventory_txt}")
    print("=" * 90)


if __name__ == "__main__":
    main()