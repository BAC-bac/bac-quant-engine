from pathlib import Path
import platform
from datetime import datetime

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"
REPORT_DIR = PROJECT_ROOT / "reports"

OUTPUT_CSV = REPORT_DIR / "macro_source_gap_report_latest.csv"
OUTPUT_TXT = REPORT_DIR / "macro_source_gap_report_latest.txt"


EXPECTED_MANUAL_SOURCES = [
    {
        "source_name": "IMF World Economic Outlook macro export",
        "required_for_script": "01_clean_imf_debt_to_gdp.py",
        "expected_legacy_path": "macro_data/raw/imf_weo_2026_macro.csv",
        "recommended_shared_path": "data/raw/macro/imf_weo/imf_weo_2026_macro.csv",
        "required_columns": [
            "DATASET",
            "SERIES_CODE",
            "COUNTRY",
            "INDICATOR",
            "FREQUENCY",
        ],
        "key_series_hint": "GGXWDG_NGDP",
        "purpose": "Foundation source for debt-to-GDP long table and early sovereign debt scoring.",
        "recovery_action": "Download or restore IMF WEO CSV, then place it in the recommended shared path.",
    }
]


RAW_INFORMATION_DATASETS = [
    "cross_asset_macro_snapshots",
    "financial_headline_snapshots",
    "fred_macro_series",
    "boe_bank_rate",
    "us_treasury_yield_curve",
    "cftc_cot_tff",
    "ons_uk_macro",
]


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


def get_data_lake_root(config: dict) -> Path:
    system = platform.system().lower()
    paths = config["data_lake_root"]

    if system == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    return Path(paths["linux"])


def get_raw_information_root(config: dict) -> Path:
    system = platform.system().lower()
    paths = config["information_data"]["raw_root"]

    if system == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    return Path(paths["linux"])


def inspect_candidate_file(path: Path, required_columns: list[str], key_series_hint: str | None) -> dict:
    result = {
        "file_exists": path.exists(),
        "read_ok": False,
        "rows": None,
        "columns": None,
        "columns_preview": None,
        "missing_required_columns": None,
        "key_series_matches": None,
        "error": None,
    }

    if not path.exists():
        return result

    try:
        df = pd.read_csv(path)
        result["read_ok"] = True
        result["rows"] = len(df)
        result["columns"] = len(df.columns)
        result["columns_preview"] = " | ".join(map(str, df.columns[:40]))

        missing_required = [col for col in required_columns if col not in df.columns]
        result["missing_required_columns"] = " | ".join(missing_required)

        if key_series_hint and "SERIES_CODE" in df.columns:
            result["key_series_matches"] = int(
                df["SERIES_CODE"].astype(str).str.contains(key_series_hint, na=False).sum()
            )

    except Exception as exc:
        result["error"] = str(exc)

    return result


def inventory_raw_information(raw_root: Path) -> pd.DataFrame:
    rows = []

    if not raw_root.exists():
        return pd.DataFrame()

    for dataset in RAW_INFORMATION_DATASETS:
        dataset_root = raw_root / dataset

        if not dataset_root.exists():
            rows.append(
                {
                    "dataset": dataset,
                    "exists": False,
                    "file_count": 0,
                    "latest_count": 0,
                    "readable_file_count": 0,
                    "latest_files": "",
                }
            )
            continue

        files = [p for p in dataset_root.rglob("*") if p.is_file()]
        latest_files = [p for p in files if "latest" in p.name.lower()]
        readable_files = [p for p in files if p.suffix.lower() in [".csv", ".parquet"]]

        rows.append(
            {
                "dataset": dataset,
                "exists": True,
                "file_count": len(files),
                "latest_count": len(latest_files),
                "readable_file_count": len(readable_files),
                "latest_files": " | ".join(str(p) for p in latest_files[:10]),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    config = load_config()
    data_lake_root = get_data_lake_root(config)
    raw_information_root = get_raw_information_root(config)

    rows = []

    for source in EXPECTED_MANUAL_SOURCES:
        legacy_repo_path = PROJECT_ROOT / source["expected_legacy_path"]
        shared_path = data_lake_root / source["recommended_shared_path"]

        legacy_result = inspect_candidate_file(
            legacy_repo_path,
            source["required_columns"],
            source["key_series_hint"],
        )

        shared_result = inspect_candidate_file(
            shared_path,
            source["required_columns"],
            source["key_series_hint"],
        )

        status = "available_shared" if shared_result["file_exists"] else (
            "available_legacy_repo" if legacy_result["file_exists"] else "missing"
        )

        rows.append(
            {
                "source_name": source["source_name"],
                "required_for_script": source["required_for_script"],
                "status": status,
                "legacy_repo_path": str(legacy_repo_path),
                "legacy_exists": legacy_result["file_exists"],
                "legacy_read_ok": legacy_result["read_ok"],
                "shared_path": str(shared_path),
                "shared_exists": shared_result["file_exists"],
                "shared_read_ok": shared_result["read_ok"],
                "shared_rows": shared_result["rows"],
                "shared_columns": shared_result["columns"],
                "shared_key_series_matches": shared_result["key_series_matches"],
                "purpose": source["purpose"],
                "recovery_action": source["recovery_action"],
            }
        )

    gap_df = pd.DataFrame(rows)
    raw_info_df = inventory_raw_information(raw_information_root)

    gap_df.to_csv(OUTPUT_CSV, index=False)

    with OUTPUT_TXT.open("w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("BACQE MACRO SOURCE GAP REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Project root: {PROJECT_ROOT}\n")
        f.write(f"Selected data lake root: {data_lake_root}\n")
        f.write(f"Selected raw information root: {raw_information_root}\n\n")

        f.write("-" * 90 + "\n")
        f.write("MANUAL SOURCE DEPENDENCY STATUS\n")
        f.write("-" * 90 + "\n")

        for _, row in gap_df.iterrows():
            f.write(f"\nSource: {row['source_name']}\n")
            f.write(f"Required for: {row['required_for_script']}\n")
            f.write(f"Status: {row['status']}\n")
            f.write(f"Legacy repo path: {row['legacy_repo_path']}\n")
            f.write(f"Legacy exists: {row['legacy_exists']}\n")
            f.write(f"Shared path: {row['shared_path']}\n")
            f.write(f"Shared exists: {row['shared_exists']}\n")
            f.write(f"Shared read ok: {row['shared_read_ok']}\n")
            f.write(f"Shared rows: {row['shared_rows']}\n")
            f.write(f"Shared columns: {row['shared_columns']}\n")
            f.write(f"Shared key series matches: {row['shared_key_series_matches']}\n")
            f.write(f"Purpose: {row['purpose']}\n")
            f.write(f"Recovery action: {row['recovery_action']}\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("AVAILABLE RAW INFORMATION LAYER\n")
        f.write("-" * 90 + "\n")

        if raw_info_df.empty:
            f.write("Raw information root missing or empty.\n")
        else:
            f.write(raw_info_df.to_string(index=False))
            f.write("\n")

        f.write("\n")
        f.write("=" * 90 + "\n")
        f.write("RECOMMENDED RECOVERY PLAN\n")
        f.write("=" * 90 + "\n")
        f.write("1. Restore/download IMF WEO CSV used by Script 01.\n")
        f.write("2. Place it in the shared data lake path:\n")
        f.write("   data/raw/macro/imf_weo/imf_weo_2026_macro.csv\n")
        f.write("3. Refactor Script 01 to read the shared path via config/paths.yaml.\n")
        f.write("4. Run Script 01 and confirm debt_to_gdp_long.csv is regenerated.\n")
        f.write("5. Continue sequentially through the processed macro pipeline.\n")
        f.write("\n")
        f.write("Strategic note:\n")
        f.write("The original macro branch appears to have started as a manual IMF WEO export workflow.\n")
        f.write("That is acceptable for recovery. Automating IMF collection should come after the old pipeline runs end-to-end again.\n")

    print("=" * 90)
    print("BACQE MACRO SOURCE GAP REPORT COMPLETE")
    print("=" * 90)
    print(f"Data lake root: {data_lake_root}")
    print(f"Raw information root: {raw_information_root}")
    print(f"CSV saved to: {OUTPUT_CSV}")
    print(f"TXT saved to: {OUTPUT_TXT}")
    print()
    print(gap_df[["source_name", "required_for_script", "status", "shared_path"]].to_string(index=False))


if __name__ == "__main__":
    main()