"""
BACQE DUKASCOPY 43 - SYMBOL INVENTORY

Purpose:
    Audit configured Dukascopy symbols and identify which daily datasets already exist
    across raw, processed, engineered, and horizon feature stages.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

from dukascopy_contract import get_symbol_metadata, inventory_normalised_symbol


CONFIG_PATH = Path("config/dukascopy_research.yaml")


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config["dukascopy_research"]


def date_range(start: str, end: str) -> pd.DatetimeIndex:
    return pd.date_range(
        datetime.strptime(start, "%Y-%m-%d"),
        datetime.strptime(end, "%Y-%m-%d"),
        freq="D",
    )


def ensure_output_dirs(analysis_root: Path) -> Path:
    output_root = analysis_root / "dukascopy_symbol_inventory"

    for folder in [
        output_root,
        output_root / "inventory",
        output_root / "missing_dates",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)

    return output_root


def extract_date_from_filename(path: Path) -> str | None:
    """
    Expected examples:
        EURUSD_2023-01-01_ticks.parquet
        EURUSD_2023-01-01_engineered_features.parquet
        EURUSD_2023-01-01_horizon_features.parquet
    """
    parts = path.stem.split("_")

    for part in parts:
        try:
            pd.to_datetime(part, format="%Y-%m-%d", errors="raise")
            return part
        except Exception:
            continue

    return None


def collect_dates_from_files(root: Path, symbol: str, suffix_hint: str) -> set[str]:
    if not root.exists():
        return set()

    symbol_root = root / f"symbol={symbol}"

    if not symbol_root.exists():
        return set()

    dates = set()

    for path in symbol_root.rglob("*"):
        if not path.is_file():
            continue

        if suffix_hint and suffix_hint not in path.name:
            continue

        date_str = extract_date_from_filename(path)

        if date_str:
            dates.add(date_str)

    return dates


def build_inventory(cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    symbols = [get_symbol_metadata(symbol).symbol for symbol in cfg["symbols"]]
    start = cfg["date_range"]["start"]
    end = cfg["date_range"]["end"]

    all_dates = [d.strftime("%Y-%m-%d") for d in date_range(start, end)]

    paths = cfg["paths"]
    features = cfg["features"]

    raw_root = Path(paths["raw_root"])
    processed_ticks_root = Path(paths["processed_root"]) / "dukascopy_ticks"
    engineered_root = Path(features["engineered_root"])
    horizon_root = Path(features["horizon_root"])

    inventory_rows = []
    missing_rows = []

    for symbol in symbols:
        print(f"[SYMBOL] {symbol}")

        processed_contract = inventory_normalised_symbol(processed_ticks_root, symbol)
        processed_dates = processed_contract["all_dates"]
        certified_processed_dates = processed_contract["certified_dates"]

        engineered_dates = collect_dates_from_files(
            engineered_root,
            symbol,
            "_engineered_features.parquet",
        )

        horizon_dates = collect_dates_from_files(
            horizon_root,
            symbol,
            "_horizon_features.parquet",
        )

        # Raw Dukascopy layouts can vary, so for now we only count matching files under raw root.
        raw_dates = collect_dates_from_files(
            raw_root,
            symbol,
            "",
        )

        for date_str in all_dates:
            has_raw = date_str in raw_dates
            has_processed = date_str in processed_dates
            has_certified_processed = date_str in certified_processed_dates
            has_engineered = date_str in engineered_dates
            has_horizon = date_str in horizon_dates

            inventory_rows.append({
                "symbol": symbol,
                "date": date_str,
                "has_raw": has_raw,
                "has_processed_ticks": has_processed,
                "has_certified_processed_ticks": has_certified_processed,
                "has_engineered_features": has_engineered,
                "has_horizon_features": has_horizon,
            })

            if not has_processed:
                missing_rows.append({
                    "symbol": symbol,
                    "date": date_str,
                    "missing_stage": "processed_ticks",
                })

            if has_processed and not has_certified_processed:
                missing_rows.append({
                    "symbol": symbol,
                    "date": date_str,
                    "missing_stage": "processed_ticks_uncertified",
                })

            if has_processed and not has_engineered:
                missing_rows.append({
                    "symbol": symbol,
                    "date": date_str,
                    "missing_stage": "engineered_features",
                })

            if has_engineered and not has_horizon:
                missing_rows.append({
                    "symbol": symbol,
                    "date": date_str,
                    "missing_stage": "horizon_features",
                })

    return pd.DataFrame(inventory_rows), pd.DataFrame(missing_rows)


def summarise_inventory(inventory: pd.DataFrame) -> pd.DataFrame:
    summary = (
        inventory.groupby("symbol", as_index=False)
        .agg(
            calendar_days=("date", "count"),
            raw_days=("has_raw", "sum"),
            processed_tick_days=("has_processed_ticks", "sum"),
            certified_processed_tick_days=("has_certified_processed_ticks", "sum"),
            engineered_feature_days=("has_engineered_features", "sum"),
            horizon_feature_days=("has_horizon_features", "sum"),
        )
    )

    summary["missing_processed_tick_days"] = (
        summary["calendar_days"] - summary["processed_tick_days"]
    )

    summary["uncertified_processed_tick_days"] = (
        summary["processed_tick_days"] - summary["certified_processed_tick_days"]
    )

    summary["missing_engineered_feature_days"] = (
        summary["processed_tick_days"] - summary["engineered_feature_days"]
    )

    summary["missing_horizon_feature_days"] = (
        summary["engineered_feature_days"] - summary["horizon_feature_days"]
    )

    return summary


def main() -> None:
    banner("BACQE DUKASCOPY 43 - SYMBOL INVENTORY")

    if not CONFIG_PATH.exists():
        print(f"[STOP] Missing config: {CONFIG_PATH}")
        return

    cfg = load_config()

    analysis_root = Path(cfg["paths"]["analysis_root"])
    output_root = ensure_output_dirs(analysis_root)

    print(f"Config:      {CONFIG_PATH}")
    print(f"Output root: {output_root}")
    print("-" * 90)

    inventory, missing = build_inventory(cfg)
    summary = summarise_inventory(inventory)

    inventory_path = output_root / "inventory" / "dukascopy_symbol_inventory_latest.csv"
    summary_path = output_root / "inventory" / "dukascopy_symbol_inventory_summary_latest.csv"
    missing_path = output_root / "missing_dates" / "dukascopy_missing_dates_latest.csv"
    report_path = output_root / "reports" / "dukascopy_symbol_inventory_report_latest.txt"

    inventory.to_csv(inventory_path, index=False)
    summary.to_csv(summary_path, index=False)
    missing.to_csv(missing_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY SYMBOL INVENTORY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Config: {CONFIG_PATH}\n")
        f.write(f"Inventory rows: {len(inventory):,}\n")
        f.write(f"Missing rows: {len(missing):,}\n\n")

        f.write("Summary by Symbol\n")
        f.write("-" * 80 + "\n")
        f.write(summary.to_string(index=False))

        f.write("\n\nMissing Stage Counts\n")
        f.write("-" * 80 + "\n")

        if missing.empty:
            f.write("No missing stages detected.\n")
        else:
            f.write(
                missing.groupby(["symbol", "missing_stage"])
                .size()
                .reset_index(name="missing_count")
                .to_string(index=False)
            )

        f.write("\n\nOutputs:\n")
        f.write(f"Inventory: {inventory_path}\n")
        f.write(f"Summary:   {summary_path}\n")
        f.write(f"Missing:   {missing_path}\n")

    print(summary.to_string(index=False))
    print("-" * 90)

    if missing.empty:
        print("[PASS] No missing stages detected.")
    else:
        print("[INFO] Missing stages detected:")
        print(
            missing.groupby(["symbol", "missing_stage"])
            .size()
            .reset_index(name="missing_count")
            .to_string(index=False)
        )

    print("=" * 90)
    print("[DONE] Symbol inventory complete.")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()
