from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
BASE_STAKE = 25.0
BETFAIR_COMMISSION = 0.02


# 🔥 Drawdown control levels
DD_LEVEL_1 = -0.05   # -5%
DD_LEVEL_2 = -0.10   # -10%
DD_LEVEL_3 = -0.15   # -15%

STAKE_MULT_1 = 1.0   # normal
STAKE_MULT_2 = 0.75
STAKE_MULT_3 = 0.50
STAKE_MULT_4 = 0.25


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
        candidates = race_df[race_df["system_name"] == system]

        if not candidates.empty:
            return candidates.sort_values("bsp", ascending=False).iloc[0]

    return None


def get_stake_multiplier(drawdown_pct):
    if drawdown_pct <= DD_LEVEL_3:
        return STAKE_MULT_4
    elif drawdown_pct <= DD_LEVEL_2:
        return STAKE_MULT_3
    elif drawdown_pct <= DD_LEVEL_1:
        return STAKE_MULT_2
    else:
        return STAKE_MULT_1


def simulate(df):
    df = df.sort_values("event_dt").copy()

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL

    results = []

    for event_id, race_df in df.groupby("event_id"):

        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)

        if selected is None:
            continue

        drawdown = bankroll - peak
        drawdown_pct = drawdown / peak if peak else 0

        stake_mult = get_stake_multiplier(drawdown_pct)
        stake = BASE_STAKE * stake_mult
        stake = min(stake, bankroll)

        raw_profit = stake * selected["back_profit_1pt"]
        net_profit = apply_commission(raw_profit)

        bankroll_before = bankroll
        bankroll_after = bankroll + net_profit

        if bankroll_after < 0:
            bankroll_after = 0

        peak = max(peak, bankroll_after)

        results.append({
            "event_id": event_id,
            "event_dt": selected["event_dt"],
            "system_name": selected["system_name"],
            "stake": stake,
            "stake_multiplier": stake_mult,
            "profit": net_profit,
            "bankroll_before": bankroll_before,
            "bankroll_after": bankroll_after,
            "drawdown_pct": drawdown_pct
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
    print("Starting survival engine...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "survival_engine"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"])

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)]

    sim = simulate(df)
    summary = build_summary(sim)

    sim.to_parquet(output_dir / "survival_log.parquet")
    sim.head(10000).to_csv(output_dir / "survival_sample.csv")

    pd.DataFrame([summary]).to_csv(output_dir / "survival_summary.csv", index=False)

    print("\nSurvival engine complete.")
    print(summary)


if __name__ == "__main__":
    main()