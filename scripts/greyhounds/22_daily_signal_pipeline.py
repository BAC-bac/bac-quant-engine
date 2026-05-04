from pathlib import Path
import pandas as pd
import yaml
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


# 🔥 Your production config
BASE_STAKE = 25.0
BETFAIR_COMMISSION = 0.02

DD_LEVEL_1 = -0.05
DD_LEVEL_2 = -0.10
DD_LEVEL_3 = -0.15

STAKE_MULT_1 = 1.0
STAKE_MULT_2 = 0.8
STAKE_MULT_3 = 0.6
STAKE_MULT_4 = 0.3


SYSTEM_PRIORITY = [
    "HENLOW_TRAP_2",
    "ROMFORD_TRAP_3",
    "HARLOW_TRAP_1",
    "TOWCESTER_TRAP_3",
]


# 🔹 Simulated current bankroll (later we make this dynamic)
CURRENT_BANKROLL = 40231.72
CURRENT_PEAK = 40231.72


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def get_multiplier(drawdown):
    if drawdown <= DD_LEVEL_3:
        return STAKE_MULT_4
    elif drawdown <= DD_LEVEL_2:
        return STAKE_MULT_3
    elif drawdown <= DD_LEVEL_1:
        return STAKE_MULT_2
    return STAKE_MULT_1


def select_best_bet(race_df):
    for system in SYSTEM_PRIORITY:
        subset = race_df[race_df["system_name"] == system]
        if not subset.empty:
            return subset.sort_values("bsp", ascending=False).iloc[0]
    return None


def build_signals(df):
    signals = []

    # Use latest available date in dataset
    today = df["race_date"].max()
    print(f"Using latest available race date: {today}")

    today_df = df[df["race_date"] == today]

    if today_df.empty:
        print("No races found for today.")
        return pd.DataFrame()

    drawdown = (CURRENT_BANKROLL - CURRENT_PEAK) / CURRENT_PEAK if CURRENT_PEAK else 0
    multiplier = get_multiplier(drawdown)

    for event_id, race_df in today_df.groupby("event_id"):

        selected = select_best_bet(race_df)

        if selected is None:
            continue

        stake = BASE_STAKE * multiplier

        signals.append({
            "date": today,
            "event_id": event_id,
            "track": selected["track_key"],
            "race_time": selected["event_dt"],
            "trap": selected["trap"],
            "dog": selected["dog_clean"],
            "system": selected["system_name"],
            "bsp": selected["bsp"],
            "stake": round(stake, 2),
            "notes": f"DD adj {multiplier:.2f}x"
        })

    return pd.DataFrame(signals)


def main():
    print("Running daily signal pipeline...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "live_signals"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"])
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)]

    signals = build_signals(df)

    if signals.empty:
        return

    file_name = f"signals_{datetime.today().strftime('%Y_%m_%d')}.csv"
    output_path = output_dir / file_name

    signals.to_csv(output_path, index=False)

    print("\nSignals generated:")
    print(signals)

    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()