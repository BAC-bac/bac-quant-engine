from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
BASE_STAKE_PCT = 0.01
MAX_RACE_EXPOSURE_PCT = 0.02  # max 2% of bankroll per race


# 🎯 Portfolio weights (THIS is where you control your strategy)
SYSTEM_WEIGHTS = {
    "ROMFORD_TRAP_3": 0.25,
    "HARLOW_TRAP_1": 0.20,
    "HENLOW_TRAP_2": 0.15,
    "TOWCESTER_TRAP_3": 0.20,
    "BSP_8_TO_13": 0.20,
}


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def normalise_weights(weights: dict):
    total = sum(weights.values())
    return {k: v / total for k, v in weights.items()}


def simulate_allocator(df: pd.DataFrame):
    df = df.sort_values("event_dt").copy().reset_index(drop=True)

    weights = normalise_weights(SYSTEM_WEIGHTS)

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    results = []

    grouped = df.groupby("event_id")

    for event_id, race_df in grouped:
        if bankroll <= 0:
            break

        race_df = race_df.copy()

        # filter only weighted systems
        race_df = race_df[race_df["system_name"].isin(weights.keys())]

        if race_df.empty:
            continue

        # base stake pool for this race
        max_race_stake = bankroll * MAX_RACE_EXPOSURE_PCT

        stakes = []

        for _, row in race_df.iterrows():
            weight = weights[row["system_name"]]

            raw_stake = bankroll * BASE_STAKE_PCT * weight
            stakes.append(raw_stake)

        total_raw = sum(stakes)

        # scale to race cap
        if total_raw > max_race_stake:
            scale = max_race_stake / total_raw
            stakes = [s * scale for s in stakes]

        race_profit = 0.0

        for i, (_, row) in enumerate(race_df.iterrows()):
            stake = stakes[i]

            profit = stake * row["portfolio_profit_1pt"]
            race_profit += profit

            results.append({
                "event_id": event_id,
                "event_dt": row["event_dt"],
                "system_name": row["system_name"],
                "stake": stake,
                "profit": profit,
                "bankroll_before": bankroll
            })

        bankroll += race_profit

        peak = max(peak, bankroll)
        drawdown = bankroll - peak
        drawdown_pct = drawdown / peak if peak else 0

        # update last row with portfolio-level info
        results[-1].update({
            "bankroll_after": bankroll,
            "drawdown_pct": drawdown_pct
        })

    return pd.DataFrame(results)


def build_summary(sim_df: pd.DataFrame):
    if sim_df.empty:
        return {}

    final_bankroll = sim_df["bankroll_after"].iloc[-1]
    max_dd = sim_df["drawdown_pct"].min()

    return {
        "initial_bankroll": INITIAL_BANKROLL,
        "final_bankroll": final_bankroll,
        "return_pct": final_bankroll / INITIAL_BANKROLL - 1,
        "bets": len(sim_df),
        "total_profit": final_bankroll - INITIAL_BANKROLL,
        "max_drawdown_pct": max_dd
    }


def main():
    print("Starting portfolio allocator Option C...")

    paths = load_paths()
    portfolio_dir = Path(paths["greyhounds"]["reports"]) / "portfolios"
    output_dir = Path(paths["greyhounds"]["reports"]) / "allocator_option_c"

    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = portfolio_dir / "portfolio_bet_log.parquet"

    df = pd.read_parquet(input_path)

    sim_df = simulate_allocator(df)

    summary = build_summary(sim_df)

    sim_df.to_parquet(output_dir / "allocator_bet_log.parquet")
    sim_df.head(10000).to_csv(output_dir / "allocator_sample.csv")

    pd.DataFrame([summary]).to_csv(output_dir / "allocator_summary.csv", index=False)

    print("\nAllocator complete.")
    print(summary)


if __name__ == "__main__":
    main()