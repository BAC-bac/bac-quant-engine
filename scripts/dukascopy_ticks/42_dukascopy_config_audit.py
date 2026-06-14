"""
BACQE DUKASCOPY 42 - CONFIG AUDIT

Purpose:
    Validate config/dukascopy_research.yaml before multi-symbol research runs.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml


CONFIG_PATH = Path("config/dukascopy_research.yaml")


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_root_config(config: dict) -> dict:
    if "dukascopy_research" not in config:
        raise KeyError("Missing top-level key: dukascopy_research")

    return config["dukascopy_research"]


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def audit_config(cfg: dict) -> list[dict]:
    rows = []

    enabled = cfg.get("enabled", False)
    rows.append({"check": "enabled", "status": "ok" if enabled else "warning", "detail": enabled})

    symbols = cfg.get("symbols", [])
    rows.append({
        "check": "symbols",
        "status": "ok" if symbols else "error",
        "detail": ", ".join(symbols) if symbols else "No symbols configured",
    })

    date_range = cfg.get("date_range", {})
    start = date_range.get("start")
    end = date_range.get("end")

    try:
        start_dt = parse_date(start)
        end_dt = parse_date(end)
        date_ok = start_dt <= end_dt
        days = (end_dt - start_dt).days + 1
    except Exception as e:
        date_ok = False
        days = 0
        rows.append({"check": "date_range_parse", "status": "error", "detail": str(e)})
    else:
        rows.append({
            "check": "date_range",
            "status": "ok" if date_ok else "error",
            "detail": f"{start} to {end} ({days} calendar days)",
        })

    paths = cfg.get("paths", {})

    for key in ["raw_root", "processed_root", "analysis_root"]:
        value = paths.get(key)
        if value is None:
            rows.append({"check": f"path_{key}", "status": "error", "detail": "missing"})
            continue

        path = Path(value)
        rows.append({
            "check": f"path_{key}",
            "status": "ok" if path.exists() else "warning",
            "detail": str(path),
        })

    features = cfg.get("features", {})
    horizons = features.get("horizons", [])

    rows.append({
        "check": "feature_horizons",
        "status": "ok" if horizons else "warning",
        "detail": horizons,
    })

    for key in ["engineered_root", "horizon_root"]:
        value = features.get(key)
        if value is None:
            rows.append({"check": f"features_{key}", "status": "warning", "detail": "missing"})
            continue

        path = Path(value)
        rows.append({
            "check": f"features_{key}",
            "status": "ok" if path.exists() else "warning",
            "detail": str(path),
        })

    download = cfg.get("download", {})
    rows.append({
        "check": "download_enabled",
        "status": "ok",
        "detail": download.get("enabled", False),
    })
    rows.append({
        "check": "download_overwrite_existing",
        "status": "ok",
        "detail": download.get("overwrite_existing", False),
    })

    estimated_daily_files = len(symbols) * days if symbols and days else 0

    rows.append({
        "check": "estimated_symbol_days",
        "status": "ok" if estimated_daily_files else "warning",
        "detail": estimated_daily_files,
    })

    return rows


def print_workload(cfg: dict) -> None:
    symbols = cfg.get("symbols", [])
    date_range = cfg.get("date_range", {})
    start_dt = parse_date(date_range["start"])
    end_dt = parse_date(date_range["end"])
    days = (end_dt - start_dt).days + 1

    print("WORKLOAD ESTIMATE")
    print("-" * 90)
    print(f"Symbols:             {len(symbols)}")
    print(f"Calendar days:        {days}")
    print(f"Symbol-days:          {len(symbols) * days}")
    print(f"Approx daily files:   {len(symbols) * days}")
    print()

    for symbol in symbols:
        print(f"  {symbol}: {days} calendar days")


def main() -> None:
    banner("BACQE DUKASCOPY 42 - CONFIG AUDIT")

    print(f"Config path: {CONFIG_PATH}")
    print("-" * 90)

    config = load_config()
    cfg = get_root_config(config)

    rows = audit_config(cfg)
    audit_df = pd.DataFrame(rows)

    print(audit_df.to_string(index=False))
    print("-" * 90)
    print_workload(cfg)

    errors = audit_df[audit_df["status"] == "error"]
    warnings = audit_df[audit_df["status"] == "warning"]

    print("=" * 90)

    if not errors.empty:
        print("[FAIL] Config audit found errors.")
        print(errors.to_string(index=False))
    else:
        print("[PASS] Config audit found no blocking errors.")

    if not warnings.empty:
        print()
        print("[WARNINGS]")
        print(warnings.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()