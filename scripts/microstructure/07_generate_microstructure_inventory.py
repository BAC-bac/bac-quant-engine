"""
BACQE MICROSTRUCTURE 07 - GENERATE MICROSTRUCTURE INVENTORY

Purpose:
    Build a single inventory of all current microstructure outputs.

Covers:
    - tick_bars
    - volume_bars
    - tick_imbalance_bars

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/inventory/
        microstructure_inventory_latest.csv
        microstructure_inventory_latest.json
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
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


def safe_read_parquet_summary(file_path: Path) -> dict:
    result = {
        "file_exists": file_path.exists(),
        "row_count": 0,
        "column_count": 0,
        "start_time_min": None,
        "end_time_max": None,
        "null_count_total": None,
        "read_status": "unknown",
        "read_error": None,
    }

    if not file_path.exists():
        result["read_status"] = "missing"
        return result

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        result["read_status"] = "failed_read"
        result["read_error"] = str(exc)
        return result

    result["read_status"] = "ok"
    result["row_count"] = len(df)
    result["column_count"] = len(df.columns)
    result["null_count_total"] = int(df.isna().sum().sum())

    if "start_time" in df.columns:
        result["start_time_min"] = str(
            pd.to_datetime(df["start_time"], utc=True, errors="coerce").min()
        )

    if "end_time" in df.columns:
        result["end_time_max"] = str(
            pd.to_datetime(df["end_time"], utc=True, errors="coerce").max()
        )

    return result


def classify_dataset_status(summary: dict, min_rows: int) -> str:
    if not summary["file_exists"]:
        return "missing"

    if summary["read_status"] != "ok":
        return "failed_read"

    if summary["row_count"] == 0:
        return "empty"

    if summary["row_count"] < min_rows:
        return "usable_sparse"

    return "ready"


def build_inventory_record(
    bar_type: str,
    symbol: str,
    parameter_name: str,
    parameter_value: int,
    file_path: Path,
    min_rows: int,
) -> dict:
    checked_at = datetime.now(timezone.utc).isoformat()
    summary = safe_read_parquet_summary(file_path)
    dataset_status = classify_dataset_status(summary, min_rows)

    return {
        "checked_at_utc": checked_at,
        "bar_type": bar_type,
        "symbol": symbol,
        "parameter_name": parameter_name,
        "parameter_value": parameter_value,
        "dataset_status": dataset_status,
        "file_path": str(file_path),
        **summary,
    }


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 07 - GENERATE MICROSTRUCTURE INVENTORY")

    config = load_config()
    micro_cfg = config["microstructure"]

    microstructure_dir = Path(micro_cfg["output"]["microstructure_dir"])
    analysis_dir = Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )

    report_dir = analysis_dir / "inventory"
    report_dir.mkdir(parents=True, exist_ok=True)

    symbols = micro_cfg["symbols"]
    tick_sizes = micro_cfg["tick_bars"]["sizes"]
    volume_thresholds = micro_cfg["volume_bars"]["thresholds"]
    imbalance_thresholds = micro_cfg["imbalance_bars"]["tick_imbalance_thresholds"]
    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    print(f"Config:              {CONFIG_PATH}")
    print(f"Microstructure dir:  {microstructure_dir}")
    print(f"Report dir:          {report_dir}")
    print(f"Symbols:             {symbols}")
    print("-" * 90)

    records = []

    for symbol in symbols:
        for tick_size in tick_sizes:
            file_path = (
                microstructure_dir
                / "tick_bars"
                / f"symbol={symbol}"
                / f"tick_size={tick_size}"
                / "tick_bars.parquet"
            )

            records.append(
                build_inventory_record(
                    bar_type="tick_bars",
                    symbol=symbol,
                    parameter_name="tick_size",
                    parameter_value=tick_size,
                    file_path=file_path,
                    min_rows=min_rows,
                )
            )

        for threshold in volume_thresholds:
            file_path = (
                microstructure_dir
                / "volume_bars"
                / f"symbol={symbol}"
                / f"volume_threshold={threshold}"
                / "volume_bars.parquet"
            )

            records.append(
                build_inventory_record(
                    bar_type="volume_bars",
                    symbol=symbol,
                    parameter_name="volume_threshold",
                    parameter_value=threshold,
                    file_path=file_path,
                    min_rows=min_rows,
                )
            )

        for threshold in imbalance_thresholds:
            file_path = (
                microstructure_dir
                / "tick_imbalance_bars"
                / f"symbol={symbol}"
                / f"imbalance_threshold={threshold}"
                / "tick_imbalance_bars.parquet"
            )

            records.append(
                build_inventory_record(
                    bar_type="tick_imbalance_bars",
                    symbol=symbol,
                    parameter_name="imbalance_threshold",
                    parameter_value=threshold,
                    file_path=file_path,
                    min_rows=min_rows,
                )
            )

    inventory_df = pd.DataFrame(records)

    csv_path = report_dir / "microstructure_inventory_latest.csv"
    json_path = report_dir / "microstructure_inventory_latest.json"

    inventory_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

    status_counts = inventory_df["dataset_status"].value_counts(dropna=False).to_dict()
    type_counts = inventory_df["bar_type"].value_counts(dropna=False).to_dict()

    print("[SUMMARY]")
    print(f"Datasets checked: {len(inventory_df)}")
    print(f"Status counts:    {status_counts}")
    print(f"Type counts:      {type_counts}")
    print("-" * 90)

    for _, row in inventory_df.iterrows():
        print(
            f"[ITEM] {row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter_name']}={row['parameter_value']:<5} "
            f"status={row['dataset_status']:<14} "
            f"rows={row['row_count']:,}"
        )

    print("-" * 90)
    print("[DONE] Microstructure inventory generated.")
    print(f"CSV output:  {csv_path}")
    print(f"JSON output: {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()