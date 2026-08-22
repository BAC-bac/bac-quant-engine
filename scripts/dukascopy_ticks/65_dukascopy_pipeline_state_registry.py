"""
BACQE DUKASCOPY 65 - PIPELINE STATE REGISTRY

Purpose:
    Build a symbol-level state registry for the Dukascopy pipeline.

Reads:
    config/dukascopy_research.yaml

Reports:
    raw ticks
    processed ticks
    tick bars
    TIBS
    engineered features
    horizon features
"""

from pathlib import Path
import yaml
import pandas as pd

from dukascopy_contract import (
    NORMALISATION_SCHEMA_VERSION,
    SYMBOL_METADATA_SCHEMA_VERSION,
    get_symbol_metadata,
    inventory_normalised_symbol,
)


CONFIG_PATH = Path("config/dukascopy_research.yaml")

REPORT_ROOT = Path(
    "E:/Quant_Lab/data/analysis/dukascopy_pipeline_state_registry"
)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["dukascopy_research"]


def date_count(start: str, end: str) -> int:
    return len(pd.date_range(start=start, end=end, freq="D"))


def count_parquet(root: Path) -> int:
    if not root.exists():
        return 0
    return len(list(root.rglob("*.parquet")))


def count_daily_symbol_files(root: Path, symbol: str, pattern: str) -> int:
    if not root.exists():
        return 0

    files = list(root.rglob(pattern))
    dates = set()

    for path in files:
        name = path.name
        if symbol not in name:
            continue

        extracted = pd.Series([name]).str.extract(r"(\d{4}[-_]\d{2}[-_]\d{2})")[0].iloc[0]
        if pd.notna(extracted):
            dates.add(str(extracted).replace("_", "-"))

    return len(dates)


def pct(value: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((value / total) * 100, 2)


def stage_status(days: int, expected: int) -> str:
    if days == 0:
        return "missing"
    if days >= expected * 0.95:
        return "complete_or_near_complete"
    if days >= expected * 0.50:
        return "partial"
    return "low_coverage"


def certified_processed_status(
    total_days: int,
    certified_days: int,
    expected_source_days: int,
    uncertified_files: int,
) -> str:
    if total_days == 0:
        return "missing"
    if uncertified_files or certified_days != total_days:
        return "legacy_or_uncertified_contract"
    if expected_source_days and certified_days >= expected_source_days:
        return "certified_complete"
    return "certified_incomplete"


def build_registry() -> pd.DataFrame:
    cfg = load_config()

    symbols = [get_symbol_metadata(symbol).symbol for symbol in cfg["symbols"]]
    start = cfg["date_range"]["start"]
    end = cfg["date_range"]["end"]

    expected_calendar_days = date_count(start, end)

    raw_root = Path(cfg["paths"]["raw_root"])
    processed_root = Path(cfg["paths"]["processed_root"])

    engineered_root = Path(cfg["features"]["engineered_root"])
    horizon_root = Path(cfg["features"]["horizon_root"])

    tick_bar_root = processed_root / "dukascopy_tick_bars"
    tib_root = processed_root / "dukascopy_tick_imbalance_bars"

    rows = []

    for symbol in symbols:
        raw_symbol_root = raw_root / f"symbol={symbol}"
        processed_symbol_root = processed_root / "dukascopy_ticks" / f"symbol={symbol}"
        tick_bar_symbol_root = tick_bar_root / f"symbol={symbol}"
        tib_symbol_root = tib_root / f"symbol={symbol}"
        engineered_symbol_root = engineered_root / f"symbol={symbol}"
        horizon_symbol_root = horizon_root / f"symbol={symbol}"

        raw_days = count_daily_symbol_files(raw_symbol_root, symbol, "*.csv")
        if raw_days == 0:
            raw_days = count_daily_symbol_files(raw_symbol_root, symbol, "*.parquet")

        processed_inventory = inventory_normalised_symbol(
            processed_root / "dukascopy_ticks", symbol
        )
        processed_tick_days = len(processed_inventory["all_dates"])
        certified_processed_tick_days = len(processed_inventory["certified_dates"])
        raw_source_days = count_daily_symbol_files(raw_symbol_root, symbol, "*.bi5")
        processed_contract_status = certified_processed_status(
            total_days=processed_tick_days,
            certified_days=certified_processed_tick_days,
            expected_source_days=raw_source_days,
            uncertified_files=processed_inventory["uncertified_files"],
        )

        engineered_days = count_daily_symbol_files(
            engineered_symbol_root,
            symbol,
            "*.parquet",
        )

        horizon_days = count_daily_symbol_files(
            horizon_symbol_root,
            symbol,
            "*.parquet",
        )

        tick_bar_files = count_parquet(tick_bar_symbol_root)
        tib_files = count_parquet(tib_symbol_root)

        rows.append(
            {
                "symbol": symbol,
                "start_date": start,
                "end_date": end,
                "expected_calendar_days": expected_calendar_days,

                "raw_days": raw_days,
                "raw_coverage_pct": pct(raw_days, expected_calendar_days),
                "raw_status": stage_status(raw_days, expected_calendar_days),

                "processed_tick_days": processed_tick_days,
                "processed_tick_coverage_pct": pct(processed_tick_days, expected_calendar_days),
                "certified_processed_tick_days": certified_processed_tick_days,
                "certified_processed_tick_coverage_pct": pct(
                    certified_processed_tick_days, raw_source_days
                ),
                "processed_tick_uncertified_files": processed_inventory["uncertified_files"],
                "processed_tick_status": processed_contract_status,
                "normalisation_schema_version_required": NORMALISATION_SCHEMA_VERSION,
                "symbol_metadata_version_required": SYMBOL_METADATA_SCHEMA_VERSION,

                "tick_bar_files": tick_bar_files,
                "tick_bar_status": "present" if tick_bar_files > 0 else "missing",

                "tib_files": tib_files,
                "tib_status": "present" if tib_files > 0 else "missing",

                "engineered_feature_days": engineered_days,
                "engineered_feature_coverage_pct": pct(engineered_days, expected_calendar_days),
                "engineered_feature_status": stage_status(engineered_days, expected_calendar_days),

                "horizon_feature_days": horizon_days,
                "horizon_feature_coverage_pct": pct(horizon_days, expected_calendar_days),
                "horizon_feature_status": stage_status(horizon_days, expected_calendar_days),
            }
        )

    return pd.DataFrame(rows)


def write_outputs(registry: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    registry_path = REPORT_ROOT / "dukascopy_pipeline_state_registry_latest.csv"
    report_path = REPORT_ROOT / "dukascopy_pipeline_state_registry_report_latest.txt"

    registry.to_csv(registry_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 65 - PIPELINE STATE REGISTRY\n")
        f.write("=" * 90 + "\n\n")

        f.write("REGISTRY\n")
        f.write("-" * 90 + "\n")
        f.write(registry.to_string(index=False))
        f.write("\n\n")

        f.write("STAGE STATUS COUNTS\n")
        f.write("-" * 90 + "\n")

        status_cols = [
            "raw_status",
            "processed_tick_status",
            "tick_bar_status",
            "tib_status",
            "engineered_feature_status",
            "horizon_feature_status",
        ]

        for col in status_cols:
            f.write(f"\n{col}\n")
            f.write(registry[col].value_counts().to_string())
            f.write("\n")

    print("=" * 90)
    print("BACQE DUKASCOPY 65 - PIPELINE STATE REGISTRY")
    print("=" * 90)
    print(registry.to_string(index=False))
    print("-" * 90)
    print(f"Registry: {registry_path}")
    print(f"Report:   {report_path}")
    print("=" * 90)


def main() -> None:
    registry = build_registry()
    write_outputs(registry)


if __name__ == "__main__":
    main()
