from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.00
FLAT_STAKE = 25.00
BETFAIR_COMMISSION = 0.02

LOOKBACK_MONTHS = 3
MIN_LOOKBACK_BETS = 300
MIN_ROLLING_POT = 0.02  # Profit on turnover threshold

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


def apply_commission(profit: float) -> float:
    if profit > 0:
        return profit * (1 - BETFAIR_COMMISSION)
    return profit


def build_single_bet_candidates(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for event_id, race_df in df.groupby("event_id"):
        selected = select_best_bet(race_df)

        if selected is None:
            continue

        rows.append(selected)

    candidates = pd.DataFrame(rows).copy()
    candidates = candidates.sort_values("event_dt").reset_index(drop=True)

    candidates["raw_profit_flat"] = FLAT_STAKE * candidates["back_profit_1pt"]
    candidates["net_profit_flat"] = candidates["raw_profit_flat"].apply(apply_commission)
    candidates["turnover_flat"] = FLAT_STAKE

    return candidates


def build_monthly_regime_table(candidates: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        candidates.groupby("year_month", dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            turnover=("turnover_flat", "sum"),
            net_profit=("net_profit_flat", "sum"),
        )
        .reset_index()
        .sort_values("year_month")
    )

    monthly["strike_rate"] = monthly["winners"] / monthly["bets"]
    monthly["profit_on_turnover"] = monthly["net_profit"] / monthly["turnover"]

    monthly["rolling_bets"] = (
        monthly["bets"]
        .shift(1)
        .rolling(LOOKBACK_MONTHS, min_periods=1)
        .sum()
    )

    monthly["rolling_profit"] = (
        monthly["net_profit"]
        .shift(1)
        .rolling(LOOKBACK_MONTHS, min_periods=1)
        .sum()
    )

    monthly["rolling_turnover"] = (
        monthly["turnover"]
        .shift(1)
        .rolling(LOOKBACK_MONTHS, min_periods=1)
        .sum()
    )

    monthly["rolling_pot"] = monthly["rolling_profit"] / monthly["rolling_turnover"]

    monthly["regime_trade_allowed"] = (
        (monthly["rolling_bets"] >= MIN_LOOKBACK_BETS)
        & (monthly["rolling_pot"] >= MIN_ROLLING_POT)
    )

    monthly.loc[monthly["rolling_bets"].isna(), "regime_trade_allowed"] = False

    return monthly


def simulate_with_regime(candidates: pd.DataFrame, regime: pd.DataFrame) -> pd.DataFrame:
    regime_map = dict(zip(regime["year_month"], regime["regime_trade_allowed"]))

    bankroll = INITIAL_BANKROLL
    peak = INITIAL_BANKROLL
    rows = []

    for _, row in candidates.iterrows():
        month = row["year_month"]
        allowed = bool(regime_map.get(month, False))

        if not allowed:
            continue

        stake = min(FLAT_STAKE, bankroll)

        if stake <= 0:
            break

        raw_profit = stake * row["back_profit_1pt"]
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
                "event_id": row["event_id"],
                "event_dt": row["event_dt"],
                "race_date": row["race_date"],
                "year_month": row["year_month"],
                "system_name": row["system_name"],
                "track_key": row["track_key"],
                "trap": row["trap"],
                "dog_clean": row["dog_clean"],
                "bsp": row["bsp"],
                "is_winner": row["is_winner"],
                "stake": stake,
                "raw_profit": raw_profit,
                "net_profit": net_profit,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll_after,
                "peak_bankroll": peak,
                "drawdown_cash": drawdown_cash,
                "drawdown_pct": drawdown_pct,
                "regime_trade_allowed": allowed,
            }
        )

        bankroll = bankroll_after

    return pd.DataFrame(rows)


def build_summary(sim_df: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    if sim_df.empty:
        return pd.DataFrame(
            [
                {
                    "initial_bankroll": INITIAL_BANKROLL,
                    "final_bankroll": INITIAL_BANKROLL,
                    "profit_cash": 0,
                    "return_pct": 0,
                    "bets_taken": 0,
                    "candidate_bets": len(candidates),
                    "bet_participation_rate": 0,
                    "max_drawdown_pct": 0,
                }
            ]
        )

    final = sim_df["bankroll_after"].iloc[-1]
    profit = final - INITIAL_BANKROLL
    total_staked = sim_df["stake"].sum()

    return pd.DataFrame(
        [
            {
                "initial_bankroll": INITIAL_BANKROLL,
                "final_bankroll": final,
                "profit_cash": profit,
                "return_pct": profit / INITIAL_BANKROLL,
                "bets_taken": len(sim_df),
                "candidate_bets": len(candidates),
                "bet_participation_rate": len(sim_df) / len(candidates),
                "winners": int(sim_df["is_winner"].sum()),
                "strike_rate": sim_df["is_winner"].mean(),
                "total_staked": total_staked,
                "profit_on_turnover": profit / total_staked if total_staked else 0,
                "max_drawdown_cash": sim_df["drawdown_cash"].min(),
                "max_drawdown_pct": sim_df["drawdown_pct"].min(),
                "date_min": sim_df["event_dt"].min(),
                "date_max": sim_df["event_dt"].max(),
                "lookback_months": LOOKBACK_MONTHS,
                "min_lookback_bets": MIN_LOOKBACK_BETS,
                "min_rolling_pot": MIN_ROLLING_POT,
            }
        ]
    )


def main() -> None:
    print("Starting greyhound regime filter...")
    print(f"Lookback months: {LOOKBACK_MONTHS}")
    print(f"Min lookback bets: {MIN_LOOKBACK_BETS}")
    print(f"Min rolling POT: {MIN_ROLLING_POT:.2%}")

    paths = load_paths()
    reports_dir = Path(paths["greyhounds"]["reports"])

    input_path = reports_dir / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = reports_dir / "regime_filter"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    df = df[df["system_name"].isin(SYSTEM_PRIORITY)].copy()

    candidates = build_single_bet_candidates(df)
    regime = build_monthly_regime_table(candidates)
    sim_df = simulate_with_regime(candidates, regime)
    summary = build_summary(sim_df, candidates)

    candidates.to_parquet(output_dir / "regime_candidates.parquet", index=False)
    candidates.head(10000).to_csv(output_dir / "regime_candidates_sample.csv", index=False)
    regime.to_csv(output_dir / "monthly_regime_table.csv", index=False)
    sim_df.to_parquet(output_dir / "regime_filtered_bet_log.parquet", index=False)
    sim_df.head(10000).to_csv(output_dir / "regime_filtered_bet_log_sample.csv", index=False)
    summary.to_csv(output_dir / "regime_filter_summary.csv", index=False)

    print("\nRegime filter complete.")
    print(f"Saved outputs to: {output_dir}")

    print("\nSummary:")
    print(summary.round(4))

    print("\nMonthly regime table preview:")
    print(regime.tail(15).round(4))


if __name__ == "__main__":
    main()