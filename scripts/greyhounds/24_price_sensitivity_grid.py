from pathlib import Path
import pandas as pd
import yaml
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
BASE_STAKE = 25.0
BETFAIR_COMMISSION = 0.02


SYSTEM_PRIORITY = [
    "HENLOW_TRAP_2",
    "ROMFORD_TRAP_3",
    "HARLOW_TRAP_1",
    "TOWCESTER_TRAP_3",
]


# 🔥 Grid of execution quality
ODDS_FACTORS = np.arange(1.00, 0.94, -0.01)


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


def simulate(df, odds_factor):
    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    for _, race_df in df.groupby("event_id"):

        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)
        if selected is None:
            continue

        bsp = selected["bsp"]

        simulated_odds = max(1.01, bsp * odds_factor)

        stake = min(BASE_STAKE, bankroll)

        if selected["is_winner"]:
            profit = stake * (simulated_odds - 1)
        else:
            profit = -stake

        profit = apply_commission(profit)

        bankroll_after = bankroll + profit

        if bankroll_after < 0:
            bankroll_after = 0

        peak = max(peak, bankroll_after)
        drawdown = (bankroll_after - peak) / peak if peak else 0

        bankroll = bankroll_after

    return bankroll, drawdown


def main():
    print("Starting price sensitivity grid...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "price_sensitivity"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)
    df["event_dt"] = pd.to_datetime(df["event_dt"])

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)]

    results = []

    for factor in ODDS_FACTORS:
        final, dd = simulate(df, factor)

        results.append({
            "odds_factor": round(factor, 2),
            "final_bankroll": final,
            "return_pct": final / INITIAL_BANKROLL - 1,
            "max_drawdown": dd
        })

        print(f"{factor:.2f} → £{final:,.2f} | return {(final/INITIAL_BANKROLL - 1)*100:.2f}% | DD {dd:.2%}")

    results_df = pd.DataFrame(results)

    results_df.to_csv(output_dir / "price_sensitivity_grid.csv", index=False)

    print("\nSaved to:", output_dir)


if __name__ == "__main__":
    main()