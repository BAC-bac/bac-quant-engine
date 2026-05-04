from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.0
FLAT_STAKE = 25.0

SYSTEM_PRIORITY = [
    "HENLOW_TRAP_2",
    "ROMFORD_TRAP_3",
    "HARLOW_TRAP_1",
    "TOWCESTER_TRAP_3",
]


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def select_best_bet(race_df):
    for system in SYSTEM_PRIORITY:
        candidates = race_df[race_df["system_name"] == system]

        if not candidates.empty:
            # 🔥 choose best candidate within system
            candidates = candidates.sort_values("bsp", ascending=False)

            return candidates.iloc[0]

    return None


def simulate(df: pd.DataFrame):
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

        stake = min(FLAT_STAKE, bankroll)

        profit = stake * selected["back_profit_1pt"]

        bankroll_before = bankroll
        bankroll_after = bankroll + profit

        if bankroll_after < 0:
            bankroll_after = 0

        peak = max(peak, bankroll_after)

        drawdown = bankroll_after - peak
        drawdown_pct = drawdown / peak if peak else 0

        results.append({
            "event_id": event_id,
            "event_dt": selected["event_dt"],
            "system_name": selected["system_name"],
            "stake": stake,
            "profit": profit,
            "bankroll_before": bankroll_before,
            "bankroll_after": bankroll_after,
            "drawdown_pct": drawdown_pct
        })

        bankroll = bankroll_after

    return pd.DataFrame(results)


def build_summary(sim_df: pd.DataFrame):
    if sim_df.empty:
        return {}

    final = sim_df["bankroll_after"].iloc[-1]

    return {
        "initial_bankroll": INITIAL_BANKROLL,
        "final_bankroll": final,
        "return_pct": final / INITIAL_BANKROLL - 1,
        "bets": len(sim_df),
        "total_profit": final - INITIAL_BANKROLL,
        "max_drawdown_pct": sim_df["drawdown_pct"].min()
    }


def main():
    print("Starting SINGLE BET allocator...")

    paths = load_paths()
    portfolio_dir = Path(paths["greyhounds"]["reports"]) / "portfolios"
    output_dir = Path(paths["greyhounds"]["reports"]) / "allocator_single_bet"

    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"

    df = pd.read_parquet(input_path)

    sim_df = simulate(df)

    summary = build_summary(sim_df)

    sim_df.to_parquet(output_dir / "single_bet_log.parquet")
    sim_df.head(10000).to_csv(output_dir / "single_bet_sample.csv")

    pd.DataFrame([summary]).to_csv(output_dir / "single_bet_summary.csv", index=False)

    print("\nAllocator complete.")
    print(summary)


if __name__ == "__main__":
    main()