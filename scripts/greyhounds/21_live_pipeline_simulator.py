from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
BASE_STAKE = 25.0
BETFAIR_COMMISSION = 0.02


# ✅ Optimised survival config (your final)
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


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def apply_commission(p):
    return p * (1 - BETFAIR_COMMISSION) if p > 0 else p


def get_multiplier(dd):
    if dd <= DD_LEVEL_3:
        return STAKE_MULT_4
    elif dd <= DD_LEVEL_2:
        return STAKE_MULT_3
    elif dd <= DD_LEVEL_1:
        return STAKE_MULT_2
    return STAKE_MULT_1


def select_best_bet(race_df):
    for system in SYSTEM_PRIORITY:
        subset = race_df[race_df["system_name"] == system]
        if not subset.empty:
            return subset.sort_values("bsp", ascending=False).iloc[0]
    return None


def simulate_live(df):
    df = df.sort_values("event_dt").copy()
    df["race_date"] = pd.to_datetime(df["race_date"])

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    results = []

    unique_days = sorted(df["race_date"].dropna().unique())

    for day in unique_days:
        day_df = df[df["race_date"] == day]

        for event_id, race_df in day_df.groupby("event_id"):

            if bankroll <= 0:
                break

            selected = select_best_bet(race_df)
            if selected is None:
                continue

            drawdown = (bankroll - peak) / peak if peak else 0
            multiplier = get_multiplier(drawdown)

            stake = min(BASE_STAKE * multiplier, bankroll)

            raw_profit = stake * selected["back_profit_1pt"]
            net_profit = apply_commission(raw_profit)

            bankroll_before = bankroll
            bankroll_after = bankroll + net_profit

            if bankroll_after < 0:
                bankroll_after = 0

            peak = max(peak, bankroll_after)

            results.append({
                "date": day,
                "event_id": event_id,
                "system": selected["system_name"],
                "stake": stake,
                "profit": net_profit,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll_after,
                "drawdown_pct": drawdown
            })

            bankroll = bankroll_after

    return pd.DataFrame(results)


def build_summary(sim_df):
    final = sim_df["bankroll_after"].iloc[-1]

    return {
        "initial": INITIAL_BANKROLL,
        "final": final,
        "return_pct": final / INITIAL_BANKROLL - 1,
        "bets": len(sim_df),
        "max_drawdown": sim_df["drawdown_pct"].min()
    }


def main():
    print("Starting LIVE pipeline simulation...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "live_simulation"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"])
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)]

    sim_df = simulate_live(df)
    summary = build_summary(sim_df)

    sim_df.to_parquet(output_dir / "live_simulation_log.parquet")
    sim_df.head(10000).to_csv(output_dir / "live_simulation_sample.csv")

    pd.DataFrame([summary]).to_csv(output_dir / "live_simulation_summary.csv", index=False)

    print("\nLIVE simulation complete.")
    print(summary)


if __name__ == "__main__":
    main()