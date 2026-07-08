from __future__ import annotations

import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

REPORT_DIR = PROJECT_ROOT / "reports"
OUTPUT_CSV = REPORT_DIR / "information_data_audit_latest.csv"
OUTPUT_TXT = REPORT_DIR / "information_data_audit_latest.txt"


EXPECTED_DATASETS = [
    {
        "dataset": "cross_asset_macro_snapshots",
        "source": "yfinance",
        "collector_script": "01_collect_cross_asset_macro_snapshot.py",
        "credentials_required": False,
        "priority": 2,
    },
    {
        "dataset": "economic_calendar_snapshots",
        "source": "calendar",
        "collector_script": "02_collect_economic_calendar_snapshot.py",
        "credentials_required": False,
        "priority": 3,
    },
    {
        "dataset": "financial_headline_snapshots",
        "source": "rss",
        "collector_script": "03_collect_financial_headlines_rss.py",
        "credentials_required": False,
        "priority": 3,
    },
    {
        "dataset": "fred_macro_series",
        "source": "fred",
        "collector_script": "04_collect_fred_macro_series.py",
        "credentials_required": True,
        "credential_env_var": "FRED_API_KEY",
        "priority": 1,
    },
    {
        "dataset": "boe_bank_rate",
        "source": "bank_of_england",
        "collector_script": "05_collect_boe_bank_rate.py",
        "credentials_required": False,
        "priority": 1,
    },
    {
        "dataset": "us_treasury_yield_curve",
        "source": "us_treasury_fiscaldata",
        "collector_script": "06_collect_us_treasury_yield_curve.py",
        "credentials_required": False,
        "priority": 1,
    },
    {
        "dataset": "cftc_cot_tff",
        "source": "cftc",
        "collector_script": "07_collect_cftc_cot_tff.py",
        "credentials_required": False,
        "priority": 2,
    },
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
    paths = config["data_lake_root"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    return Path(paths["linux"])


def read_preview(path: Path) -> dict:
    result = {
        "read_ok": False,
        "rows": None,
        "columns": None,
        "min_date": None,
        "max_date": None,
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

        for date_col in ["date", "snapshot_date", "run_time_utc"]:
            if date_col in df.columns:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                result["min_date"] = dates.min()
                result["max_date"] = dates.max()
                break

    except Exception as exc:
        result["error"] = str(exc)

    return result


def find_latest_files(dataset_root: Path) -> list[Path]:
    if not dataset_root.exists():
        return []

    files = [
        p for p in dataset_root.rglob("*")
        if p.is_file() and "latest" in p.name.lower() and p.suffix.lower() in [".csv", ".parquet"]
    ]

    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def audit_dataset(data_lake_root: Path, item: dict) -> dict:
    dataset_root = (
        data_lake_root
        / "data"
        / "raw"
        / "information_data"
        / item["dataset"]
    )

    collector_path = PROJECT_ROOT / "scripts" / "information_data" / item["collector_script"]

    latest_files = find_latest_files(dataset_root)
    latest_file = latest_files[0] if latest_files else None

    all_files = []
    if dataset_root.exists():
        all_files = [p for p in dataset_root.rglob("*") if p.is_file()]

    preview = read_preview(latest_file) if latest_file else {
        "read_ok": False,
        "rows": None,
        "columns": None,
        "min_date": None,
        "max_date": None,
        "error": None,
    }

    latest_modified_utc = None
    age_hours = None

    if latest_file:
        latest_modified_utc = datetime.fromtimestamp(
            latest_file.stat().st_mtime,
            tz=timezone.utc,
        )
        age_hours = (
            datetime.now(timezone.utc) - latest_modified_utc
        ).total_seconds() / 3600

    if not collector_path.exists():
        status = "missing_collector"
    elif not dataset_root.exists():
        status = "missing_dataset_root"
    elif not latest_file:
        status = "missing_latest_file"
    elif not preview["read_ok"]:
        status = "latest_unreadable"
    else:
        status = "ok"

    return {
        "dataset": item["dataset"],
        "source": item["source"],
        "priority": item["priority"],
        "collector_script": item["collector_script"],
        "collector_exists": collector_path.exists(),
        "credentials_required": item.get("credentials_required", False),
        "credential_env_var": item.get("credential_env_var", ""),
        "dataset_root": str(dataset_root),
        "dataset_root_exists": dataset_root.exists(),
        "file_count": len(all_files),
        "latest_file": str(latest_file) if latest_file else "",
        "latest_file_exists": latest_file is not None,
        "latest_modified_utc": latest_modified_utc,
        "latest_age_hours": round(age_hours, 2) if age_hours is not None else None,
        "read_ok": preview["read_ok"],
        "rows": preview["rows"],
        "columns": preview["columns"],
        "min_date": preview["min_date"],
        "max_date": preview["max_date"],
        "error": preview["error"],
        "status": status,
    }


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    config = load_config()
    data_lake_root = get_data_lake_root(config)

    rows = [audit_dataset(data_lake_root, item) for item in EXPECTED_DATASETS]
    audit_df = pd.DataFrame(rows)

    audit_df = audit_df.sort_values(["priority", "dataset"]).reset_index(drop=True)

    audit_df.to_csv(OUTPUT_CSV, index=False)

    status_counts = audit_df["status"].value_counts().to_dict()

    with OUTPUT_TXT.open("w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("BACQE INFORMATION DATA AUDIT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Generated UTC: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Project root:   {PROJECT_ROOT}\n")
        f.write(f"Data lake root: {data_lake_root}\n")
        f.write("\n")

        f.write("-" * 90 + "\n")
        f.write("STATUS SUMMARY\n")
        f.write("-" * 90 + "\n")
        for status, count in status_counts.items():
            f.write(f"{status}: {count}\n")

        f.write("\n")
        f.write("-" * 90 + "\n")
        f.write("DATASET AUDIT\n")
        f.write("-" * 90 + "\n")

        display_cols = [
            "dataset",
            "source",
            "priority",
            "status",
            "collector_exists",
            "dataset_root_exists",
            "file_count",
            "latest_file_exists",
            "latest_age_hours",
            "read_ok",
            "rows",
            "columns",
            "min_date",
            "max_date",
        ]

        f.write(audit_df[display_cols].to_string(index=False))
        f.write("\n\n")

        f.write("-" * 90 + "\n")
        f.write("CREDENTIAL REQUIREMENTS\n")
        f.write("-" * 90 + "\n")

        creds = audit_df[audit_df["credentials_required"] == True].copy()
        if creds.empty:
            f.write("No credentials required by registered sources.\n")
        else:
            f.write(
                creds[
                    [
                        "dataset",
                        "source",
                        "credential_env_var",
                        "status",
                    ]
                ].to_string(index=False)
            )
            f.write("\n")

        f.write("\n")
        f.write("=" * 90 + "\n")
        f.write("RECOMMENDED NEXT ACTIONS\n")
        f.write("=" * 90 + "\n")

        for _, row in audit_df.iterrows():
            if row["status"] == "ok":
                continue

            f.write(f"\nDataset: {row['dataset']}\n")
            f.write(f"Status:  {row['status']}\n")

            if row["status"] == "missing_collector":
                f.write(f"Action:  Create or restore collector script {row['collector_script']}.\n")
            elif row["status"] == "missing_dataset_root":
                f.write("Action:  Run the collector to create the raw dataset root.\n")
            elif row["status"] == "missing_latest_file":
                f.write("Action:  Run the collector and confirm it writes *_latest.csv/parquet.\n")
            elif row["status"] == "latest_unreadable":
                f.write(f"Action:  Inspect latest file read error: {row['error']}\n")

            if row["credentials_required"]:
                f.write(f"Note:    Requires credential env var: {row['credential_env_var']}\n")

    print("=" * 90)
    print("BACQE INFORMATION DATA AUDIT COMPLETE")
    print("=" * 90)
    print(f"Data lake root: {data_lake_root}")
    print(f"CSV saved to:   {OUTPUT_CSV}")
    print(f"TXT saved to:   {OUTPUT_TXT}")
    print()
    print(audit_df[["dataset", "source", "priority", "status", "file_count", "latest_file_exists", "rows"]].to_string(index=False))


if __name__ == "__main__":
    main()