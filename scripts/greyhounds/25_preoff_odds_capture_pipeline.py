from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


SNAPSHOT_MINUTES = [10, 5, 2, 1]

SYSTEM_PRIORITY = [
    "HENLOW_TRAP_2",
    "ROMFORD_TRAP_3",
    "HARLOW_TRAP_1",
    "TOWCESTER_TRAP_3",
]


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def select_best_bet(race_df: pd.DataFrame):
    for system in SYSTEM_PRIORITY:
        subset = race_df[race_df["system_name"] == system]
        if not subset.empty:
            return subset.sort_values("bsp", ascending=False).iloc[0]
    return None


def build_signal_universe(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for event_id, race_df in df.groupby("event_id"):
        selected = select_best_bet(race_df)
        if selected is not None:
            rows.append(selected)

    signals = pd.DataFrame(rows).copy()

    keep_cols = [
        "event_id",
        "event_dt",
        "race_date",
        "track_key",
        "race_time",
        "system_name",
        "trap",
        "dog_clean",
        "bsp",
        "is_winner",
    ]

    available_cols = [col for col in keep_cols if col in signals.columns]
    return signals[available_cols].sort_values("event_dt").reset_index(drop=True)


def build_capture_schedule(signals: pd.DataFrame) -> pd.DataFrame:
    schedule_rows = []

    for _, row in signals.iterrows():
        race_time = pd.to_datetime(row["event_dt"], errors="coerce")

        for minutes_before in SNAPSHOT_MINUTES:
            snapshot_time = race_time - pd.Timedelta(minutes=minutes_before)

            schedule_rows.append(
                {
                    "event_id": row["event_id"],
                    "event_dt": race_time,
                    "snapshot_time": snapshot_time,
                    "minutes_before": minutes_before,
                    "track_key": row["track_key"],
                    "race_time": row.get("race_time"),
                    "system_name": row["system_name"],
                    "trap": row["trap"],
                    "dog_clean": row["dog_clean"],
                    "target_bsp": row.get("bsp"),
                    "capture_status": "planned",
                    "captured_price": None,
                    "captured_at": None,
                    "notes": None,
                }
            )

    return pd.DataFrame(schedule_rows).sort_values(["snapshot_time", "event_dt"])


def create_empty_live_capture_table() -> pd.DataFrame:
    columns = [
        "event_id",
        "market_id",
        "selection_id",
        "event_dt",
        "snapshot_time",
        "captured_at",
        "minutes_before",
        "track_key",
        "race_time",
        "system_name",
        "trap",
        "dog_clean",
        "available_to_back_price",
        "available_to_back_size",
        "last_traded_price",
        "available_to_lay_price",
        "available_to_lay_size",
        "capture_status",
        "source",
        "notes",
    ]

    return pd.DataFrame(columns=columns)


def main() -> None:
    print("Starting pre-off odds capture pipeline...")

    paths = load_paths()
    reports_dir = Path(paths["greyhounds"]["reports"])
    output_dir = reports_dir / "preoff_odds_capture"

    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = reports_dir / "equity_curves" / "system_equity_bet_level.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)].copy()

    signals = build_signal_universe(df)
    schedule = build_capture_schedule(signals)
    empty_capture_table = create_empty_live_capture_table()

    signals_path = output_dir / "historical_signal_universe.parquet"
    schedule_path = output_dir / "preoff_capture_schedule.csv"
    live_capture_template_path = output_dir / "live_preoff_capture_template.csv"

    signals.to_parquet(signals_path, index=False)
    schedule.to_csv(schedule_path, index=False)
    empty_capture_table.to_csv(live_capture_template_path, index=False)

    readme_path = output_dir / "README_preoff_capture_plan.md"

    readme_text = f"""# Pre-Off Odds Capture Plan

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Purpose

The current greyhound strategy has been validated using BSP, but price sensitivity testing showed that profitability depends heavily on execution price.

This capture pipeline prepares the structure needed to collect real pre-off prices before any live execution is attempted.

## Snapshot Times

The planned snapshots are:

{SNAPSHOT_MINUTES} minutes before the scheduled race start.

## Files Produced

- `historical_signal_universe.parquet`
  - Historical one-bet-per-race signal universe.
- `preoff_capture_schedule.csv`
  - Historical-style capture schedule for validating the design.
- `live_preoff_capture_template.csv`
  - Empty table structure for future live Betfair API captures.

## Live Capture Fields

For each future signal, the live system should collect:

- market_id
- selection_id
- available_to_back_price
- available_to_back_size
- last_traded_price
- available_to_lay_price
- available_to_lay_size
- captured_at
- minutes_before

## Research Goal

Compare captured pre-off prices against final BSP.

Key metrics:

- preoff_price / BSP
- average execution factor
- break-even threshold
- price drift by track
- price drift by trap
- price drift by minutes_before
- price drift by system

## Current Price Sensitivity Finding

The strategy remains profitable around 96% of BSP, but becomes negative around 95% of BSP.

Therefore, the system needs real pre-off price evidence before any live execution.
"""

    readme_path.write_text(readme_text, encoding="utf-8")

    print("\nPre-off odds capture pipeline complete.")
    print(f"Signals saved to: {signals_path}")
    print(f"Capture schedule saved to: {schedule_path}")
    print(f"Live capture template saved to: {live_capture_template_path}")
    print(f"Plan saved to: {readme_path}")
    print(f"Historical signals: {len(signals):,}")
    print(f"Scheduled snapshots: {len(schedule):,}")


if __name__ == "__main__":
    main()