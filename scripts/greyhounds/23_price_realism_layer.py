from pathlib import Path
import pandas as pd
import yaml
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
BASE_STAKE = 25.0
BETFAIR_COMMISSION = 0.02


# 🔥 Slippage assumptions
ODDS_DEGRADATION = 0.95   # You get 95% of BSP (worse price)
RANDOM_NOISE = 0.02       # ±2% randomness


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


def select_best_bet(race_df):
    for system in SYSTEM_PRIORITY:
        subset = race_df[race_df["system_name"] == system]
        if not subset.empty:
            return subset.sort_values("bsp", ascending=False).iloc[0]
    return None


def simulate_price_realism(df):
    df = df.sort_values("event_dt").copy()

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    results = []

    for _, race_df in df.groupby("event_id"):

        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)
        if selected is None:
            continue

        bsp = selected["bsp"]

        # 🔥 Add randomness
        noise = np.random.uniform(-RANDOM_NOISE, RANDOM_NOISE)

        # 🔥 Simulate worse fill
        simulated_odds = bsp * (ODDS_DEGRADATION + noise)

        # Prevent unrealistic odds
        simulated_odds = max(1.01, simulated_odds)

        stake = min(BASE_STAKE, bankroll)

        if selected["is_winner"]:
            profit = stake * (simulated_odds - 1)
        else:
            profit = -stake

        profit = apply_commission(profit)

        bankroll_before = bankroll
        bankroll_after = bankroll + profit

        if bankroll_after < 0:
            bankroll_after = 0

        peak = max(peak, bankroll_after)
        drawdown = (bankroll_after - peak) / peak if peak else 0

        results.append({
            "event_id": selected["event_id"],
            "system": selected["system_name"],
            "bsp": bsp,
            "simulated_odds": simulated_odds,
            "stake": stake,
            "profit": profit,
            "bankroll_after": bankroll_after,
            "drawdown_pct": drawdown
        })

        bankroll = bankroll_after

    return pd.DataFrame(results)


def build_summary(df):
    final = df["bankroll_after"].iloc[-1]

    return {
        "initial": INITIAL_BANKROLL,
        "final": final,
        "return_pct": final / INITIAL_BANKROLL - 1,
        "bets": len(df),
        "max_drawdown": df["drawdown_pct"].min()
    }


def main():
    print("Starting price realism simulation...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "price_realism"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"])

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)]

    sim_df = simulate_price_realism(df)
    summary = build_summary(sim_df)

    sim_df.to_parquet(output_dir / "price_realism_log.parquet")
    sim_df.head(10000).to_csv(output_dir / "price_realism_sample.csv")

    pd.DataFrame([summary]).to_csv(output_dir / "price_realism_summary.csv", index=False)

    print("\nPrice realism simulation complete.")
    print(summary)


if __name__ == "__main__":
    main()