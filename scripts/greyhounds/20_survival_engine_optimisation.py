from pathlib import Path
import pandas as pd
import yaml
import itertools


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


DD_LEVEL_1 = -0.05
DD_LEVEL_2 = -0.10
DD_LEVEL_3 = -0.15

STAKE_MULT_1 = 1.0
STAKE_MULT_2 = 0.8
STAKE_MULT_3 = 0.6
STAKE_MULT_4 = 0.3


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def apply_commission(p):
    return p * (1 - BETFAIR_COMMISSION) if p > 0 else p


def select_best_bet(race_df):
    for system in SYSTEM_PRIORITY:
        candidates = race_df[race_df["system_name"] == system]

        if not candidates.empty:
            return candidates.sort_values("bsp", ascending=False).iloc[0]

    return None


def get_multiplier(dd, levels, mults):
    l1, l2, l3 = levels
    m1, m2, m3, m4 = mults

    if dd <= l3:
        return m4
    elif dd <= l2:
        return m3
    elif dd <= l1:
        return m2
    else:
        return m1


def simulate(df, levels, mults):
    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    for _, race_df in df.groupby("event_id"):
        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)
        if selected is None:
            continue

        dd = (bankroll - peak) / peak if peak else 0

        mult = get_multiplier(dd, levels, mults)
        stake = min(BASE_STAKE * mult, bankroll)

        profit = stake * selected["back_profit_1pt"]
        profit = apply_commission(profit)

        bankroll += profit
        peak = max(peak, bankroll)

    return bankroll


def simulate_full(df, levels, mults):
    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL
    dd_min = 0

    for _, race_df in df.groupby("event_id"):
        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)
        if selected is None:
            continue

        dd = (bankroll - peak) / peak if peak else 0
        mult = get_multiplier(dd, levels, mults)

        stake = min(BASE_STAKE * mult, bankroll)
        profit = apply_commission(stake * selected["back_profit_1pt"])

        bankroll += profit
        peak = max(peak, bankroll)

        dd_current = (bankroll - peak) / peak
        dd_min = min(dd_min, dd_current)

    return bankroll, dd_min


def main():
    print("Starting survival optimisation...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "survival_optimisation"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)
    df["event_dt"] = pd.to_datetime(df["event_dt"])

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)].copy()
    df = df.sort_values("event_dt")

    results = []

    for levels, mults in itertools.product(DD_LEVELS, STAKE_MULTS):
        final, dd = simulate_full(df, levels, mults)

        results.append({
            "levels": levels,
            "multipliers": mults,
            "final_bankroll": final,
            "return_pct": final / INITIAL_BANKROLL - 1,
            "max_drawdown": dd,
            "score": (final / INITIAL_BANKROLL) + dd  # simple risk-adjusted score
        })

        print(f"Tested {levels} | {mults} → £{final:,.0f} | DD {dd:.2%}")

    results_df = pd.DataFrame(results).sort_values("score", ascending=False)

    results_df.to_csv(output_dir / "survival_optimisation_results.csv", index=False)

    print("\nTop 10 configurations:")
    print(results_df.head(10).round(4))


if __name__ == "__main__":
    main()