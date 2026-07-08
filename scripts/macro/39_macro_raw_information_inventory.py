from pathlib import Path
import platform
from datetime import datetime

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"
REPORT_DIR = PROJECT_ROOT / "reports"

OUTPUT_CSV = REPORT_DIR / "macro_raw_information_inventory_latest.csv"
OUTPUT_TXT = REPORT_DIR / "macro_raw_information_inventory_latest.txt"


def load_config() -> dict:
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def select_existing_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        if candidate is None:
            continue

        path = Path(candidate)
        if path.exists():
            return path

    return Path(next(candidate for candidate in candidates if candidate is not None))


def get_raw_information_root(config: dict) -> Path:
    system = platform.system().lower()
    paths = config["information_data"]["raw_root"]

    if system == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
            ]
        )

    return Path(paths["linux"])

def safe_read_preview(path: Path) -> dict:
    result = {
        "read_ok": False,
        "rows": None,
        "columns": None,
        "column_names": None,
        "date_min": None,
        "date_max": None,
        "error": None,
    }

    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        elif path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        else:
            return result

        result["read_ok"] = True
        result["rows"] = len(df)
        result["columns"] = len(df.columns)
        result["column_names"] = " | ".join(map(str, df.columns[:40]))

        date_candidates = [
            col for col in df.columns
            if any(token in str(col).lower() for token in ["date", "time", "timestamp", "datetime"])
        ]

        for col in date_candidates:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().sum() > 0:
                result["date_min"] = parsed.min()
                result["date_max"] = parsed.max()
                break

    except Exception as exc:
        result["error"] = str(exc)

    return result


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    config = load_config()
    raw_root = get_raw_information_root(config)

    if not raw_root.exists():
        raise FileNotFoundError(f"Raw information root not found: {raw_root}")

    rows = []

    for path in raw_root.rglob("*"):
        if not path.is_file():
            continue

        relative = path.relative_to(raw_root)
        parts = relative.parts

        dataset = parts[0] if parts else "unknown"

        row = {
            "dataset": dataset,
            "file_name": path.name,
            "suffix": path.suffix.lower(),
            "full_path": str(path),
            "relative_path": str(relative),
            "size_bytes": path.stat().st_size,
            "modified": datetime.fromtimestamp(path.stat().st_mtime),
            "is_latest": "latest" in path.name.lower(),
        }

        if path.suffix.lower() in [".csv", ".parquet"]:
            row.update(safe_read_preview(path))
        else:
            row.update(
                {
                    "read_ok": False,
                    "rows": None,
                    "columns": None,
                    "column_names": None,
                    "date_min": None,
                    "date_max": None,
                    "error": None,
                }
            )

        rows.append(row)

    inventory = pd.DataFrame(rows)

    if inventory.empty:
        raise RuntimeError(f"No files found under: {raw_root}")

    inventory = inventory.sort_values(["dataset", "is_latest", "modified"], ascending=[True, False, False])
    inventory.to_csv(OUTPUT_CSV, index=False)

    readable_files = inventory[inventory["read_ok"] == True].copy()
    latest_files = inventory[inventory["is_latest"] == True].copy()

    with OUTPUT_TXT.open("w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("BACQE MACRO RAW INFORMATION INVENTORY\n")
        f.write("=" * 90 + "\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Project root: {PROJECT_ROOT}\n")
        f.write(f"Raw information root: {raw_root}\n\n")

        f.write("-" * 90 + "\n")
        f.write("SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(f"Total files found: {len(inventory)}\n")
        f.write(f"Readable CSV/Parquet files: {len(readable_files)}\n")
        f.write(f"Latest marker files: {len(latest_files)}\n\n")

        f.write("Datasets found:\n")
        for dataset, count in inventory["dataset"].value_counts().sort_index().items():
            readable_count = len(readable_files[readable_files["dataset"] == dataset])
            latest_count = len(latest_files[latest_files["dataset"] == dataset])
            f.write(f"  {dataset}: {count} files | readable={readable_count} | latest={latest_count}\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("LATEST READABLE FILES\n")
        f.write("-" * 90 + "\n")

        latest_readable = readable_files[readable_files["is_latest"] == True].copy()

        if latest_readable.empty:
            f.write("No latest readable files found.\n")
        else:
            for _, row in latest_readable.iterrows():
                f.write(f"\nDataset: {row['dataset']}\n")
                f.write(f"File: {row['full_path']}\n")
                f.write(f"Rows: {row['rows']}\n")
                f.write(f"Columns: {row['columns']}\n")
                f.write(f"Date min: {row['date_min']}\n")
                f.write(f"Date max: {row['date_max']}\n")
                f.write(f"Columns preview: {row['column_names']}\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("REBUILD USEFULNESS\n")
        f.write("-" * 90 + "\n")

        expected = [
            "fred_macro_series",
            "boe_bank_rate",
            "us_treasury_yield_curve",
            "cftc_cot_tff",
            "ons_uk_macro",
            "cross_asset_macro_snapshots",
            "financial_headline_snapshots",
        ]

        for dataset in expected:
            exists = dataset in set(inventory["dataset"])
            readable_latest = not latest_readable[latest_readable["dataset"] == dataset].empty
            status = "ready" if readable_latest else "present_but_no_latest_readable" if exists else "missing"
            f.write(f"{dataset}: {status}\n")

        f.write("\n")
        f.write("=" * 90 + "\n")
        f.write("RECOMMENDED NEXT ACTIONS\n")
        f.write("=" * 90 + "\n")
        f.write("1. Confirm latest readable files exist for core macro sources.\n")
        f.write("2. Build a macro raw-to-processed rebuild script using the available latest files.\n")
        f.write("3. Regenerate macro_data/processed CSV outputs required by scripts 01-38.\n")

    print("=" * 90)
    print("BACQE MACRO RAW INFORMATION INVENTORY COMPLETE")
    print("=" * 90)
    print(f"Raw root: {raw_root}")
    print(f"CSV saved to: {OUTPUT_CSV}")
    print(f"TXT saved to: {OUTPUT_TXT}")
    print()
    print(f"Total files found: {len(inventory)}")
    print(f"Readable CSV/Parquet files: {len(readable_files)}")
    print(f"Latest marker files: {len(latest_files)}")

    print("\nDatasets found:")
    for dataset, count in inventory["dataset"].value_counts().sort_index().items():
        print(f"  {dataset}: {count}")


if __name__ == "__main__":
    main()