from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.00
FLAT_STAKE = 25.00
BETFAIR_COMMISSION = 0.02  # Ben's current assumption: 2%

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
        candidates = race_df[race_df["system_name"] == system]

        if not candidates.empty:
            return candidates.sort_values("bsp", ascending=False).iloc[0]

    return None


def apply_commission(profit_before_commission: float) -> float:
    if profit_before_commission > 0:
        return profit_before_commission * (1 - BETFAIR_COMMISSION)

    return profit_before_commission


def simulate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("event_dt").copy()

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL
    rows = []

    for event_id, race_df in df.groupby("event_id"):
        if bankroll <= 0:
            break

        selected = select_best_bet(race_df)

        if selected is None:
            continue

        stake = min(FLAT_STAKE, bankroll)

        raw_profit = stake * selected["back_profit_1pt"]
        net_profit = apply_commission(raw_profit)

        bankroll_before = bankroll
        bankroll_after = bankroll + net_profit

        if bankroll_after < 0:
            bankroll_after = 0

        peak = max(peak, bankroll_after)

        drawdown_cash = bankroll_after - peak
        drawdown_pct = drawdown_cash / peak if peak else 0

        rows.append(
            {
                "event_id": event_id,
                "event_dt": selected["event_dt"],
                "race_date": selected.get("race_date"),
                "year_month": selected.get("year_month"),
                "system_name": selected["system_name"],
                "track_key": selected.get("track_key"),
                "trap": selected.get("trap"),
                "dog_clean": selected.get("dog_clean"),
                "bsp": selected["bsp"],
                "is_winner": selected["is_winner"],
                "stake": stake,
                "raw_profit": raw_profit,
                "commission_paid": raw_profit - net_profit if raw_profit > 0 else 0,
                "net_profit": net_profit,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll_after,
                "peak_bankroll": peak,
                "drawdown_cash": drawdown_cash,
                "drawdown_pct": drawdown_pct,
            }
        )

        bankroll = bankroll_after

    return pd.DataFrame(rows)


def build_summary(sim_df: pd.DataFrame) -> pd.DataFrame:
    initial = INITIAL_BANKROLL
    final = sim_df["bankroll_after"].iloc[-1]
    profit = final - initial
    total_staked = sim_df["stake"].sum()

    summary = {
        "initial_bankroll": initial,
        "final_bankroll": final,
        "profit_cash": profit,
        "return_pct": profit / initial,
        "bets": len(sim_df),
        "winners": int(sim_df["is_winner"].sum()),
        "strike_rate": sim_df["is_winner"].mean(),
        "total_staked": total_staked,
        "profit_on_turnover": profit / total_staked,
        "gross_profit_before_commission": sim_df["raw_profit"].sum(),
        "total_commission_paid": sim_df["commission_paid"].sum(),
        "net_profit_after_commission": sim_df["net_profit"].sum(),
        "max_drawdown_cash": sim_df["drawdown_cash"].min(),
        "max_drawdown_pct": sim_df["drawdown_pct"].min(),
        "largest_stake": sim_df["stake"].max(),
        "smallest_stake": sim_df["stake"].min(),
        "date_min": sim_df["event_dt"].min(),
        "date_max": sim_df["event_dt"].max(),
    }

    return pd.DataFrame([summary])


def build_yearly(sim_df: pd.DataFrame) -> pd.DataFrame:
    sim_df = sim_df.copy()
    sim_df["year"] = pd.to_datetime(sim_df["event_dt"]).dt.year

    yearly = (
        sim_df.groupby("year", dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            total_staked=("stake", "sum"),
            raw_profit=("raw_profit", "sum"),
            commission_paid=("commission_paid", "sum"),
            net_profit=("net_profit", "sum"),
            bankroll_after=("bankroll_after", "last"),
            max_drawdown_pct=("drawdown_pct", "min"),
        )
        .reset_index()
    )

    yearly["strike_rate"] = yearly["winners"] / yearly["bets"]
    yearly["profit_on_turnover"] = yearly["net_profit"] / yearly["total_staked"]

    return yearly


def build_monthly(sim_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        sim_df.groupby("year_month", dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            total_staked=("stake", "sum"),
            raw_profit=("raw_profit", "sum"),
            commission_paid=("commission_paid", "sum"),
            net_profit=("net_profit", "sum"),
            bankroll_after=("bankroll_after", "last"),
            max_drawdown_pct=("drawdown_pct", "min"),
        )
        .reset_index()
    )

    monthly["strike_rate"] = monthly["winners"] / monthly["bets"]
    monthly["profit_on_turnover"] = monthly["net_profit"] / monthly["total_staked"]

    return monthly


def main() -> None:
    print("Starting allocator realism layer...")
    print(f"Initial bankroll: £{INITIAL_BANKROLL:,.2f}")
    print(f"Flat stake: £{FLAT_STAKE:,.2f}")
    print(f"Commission: {BETFAIR_COMMISSION:.2%} on winning bets")

    paths = load_paths()
    reports_dir = Path(paths["greyhounds"]["reports"])

    input_path = reports_dir / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = reports_dir / "allocator_realism_layer"

    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    sim_df = simulate(df)

    summary = build_summary(sim_df)
    yearly = build_yearly(sim_df)
    monthly = build_monthly(sim_df)

    sim_df.to_parquet(output_dir / "realism_bet_log.parquet", index=False)
    sim_df.head(10000).to_csv(output_dir / "realism_bet_log_sample.csv", index=False)
    summary.to_csv(output_dir / "realism_summary.csv", index=False)
    yearly.to_csv(output_dir / "realism_yearly.csv", index=False)
    monthly.to_csv(output_dir / "realism_monthly.csv", index=False)

    print("\nRealism layer complete.")
    print(f"Saved outputs to: {output_dir}")

    print("\nSummary:")
    print(summary.round(4))

    print("\nYearly:")
    print(yearly.round(4))


if __name__ == "__main__":
    main()