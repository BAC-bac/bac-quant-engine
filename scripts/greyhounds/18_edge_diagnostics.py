from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


FLAT_STAKE = 25.0
BETFAIR_COMMISSION = 0.02


def load_paths():
    with open(PATHS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def apply_commission(p):
    return p * (1 - BETFAIR_COMMISSION) if p > 0 else p


def build_base(df):
    df = df.copy()

    df["event_dt"] = pd.to_datetime(df["event_dt"])
    df = df.sort_values("event_dt")

    df["profit"] = df["back_profit_1pt"] * FLAT_STAKE
    df["profit"] = df["profit"].apply(apply_commission)

    df["cum_profit"] = df["profit"].cumsum()

    return df


# ----------------------------
# Rolling Metrics
# ----------------------------
def rolling_analysis(df):
    df = df.copy()

    window = 500  # rolling bets

    df["rolling_profit"] = df["profit"].rolling(window).sum()
    df["rolling_turnover"] = FLAT_STAKE * window
    df["rolling_pot"] = df["rolling_profit"] / df["rolling_turnover"]

    return df


# ----------------------------
# Streak Analysis
# ----------------------------
def streak_analysis(df):
    df = df.copy()

    df["is_win"] = df["profit"] > 0

    streaks = []
    current_streak = 0

    for win in df["is_win"]:
        if not win:
            current_streak += 1
        else:
            if current_streak > 0:
                streaks.append(current_streak)
            current_streak = 0

    if current_streak > 0:
        streaks.append(current_streak)

    if not streaks:
        return pd.DataFrame()

    return pd.DataFrame({
        "max_losing_streak": [max(streaks)],
        "avg_losing_streak": [sum(streaks) / len(streaks)],
        "total_streaks": [len(streaks)]
    })


# ----------------------------
# Drawdown Analysis
# ----------------------------
def drawdown_analysis(df):
    df = df.copy()

    df["equity"] = 10000 + df["cum_profit"]
    df["peak"] = df["equity"].cummax()
    df["drawdown"] = df["equity"] - df["peak"]
    df["drawdown_pct"] = df["drawdown"] / df["peak"]

    return df


# ----------------------------
# Distribution Analysis
# ----------------------------
def distribution_analysis(df):
    desc = df["profit"].describe()

    return pd.DataFrame({
        "mean_profit": [desc["mean"]],
        "median_profit": [desc["50%"]],
        "std_profit": [desc["std"]],
        "min_profit": [desc["min"]],
        "max_profit": [desc["max"]],
    })


def main():
    print("Starting edge diagnostics...")

    paths = load_paths()
    input_path = Path(paths["greyhounds"]["reports"]) / "equity_curves" / "system_equity_bet_level.parquet"
    output_dir = Path(paths["greyhounds"]["reports"]) / "edge_diagnostics"

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)

    # 🔥 ONLY track/trap systems (your real edge)
    systems = ["HENLOW_TRAP_2", "ROMFORD_TRAP_3", "HARLOW_TRAP_1", "TOWCESTER_TRAP_3"]
    df = df[df["system_name"].isin(systems)].copy()

    df = build_base(df)

    rolling_df = rolling_analysis(df)
    dd_df = drawdown_analysis(df)

    streak_df = streak_analysis(df)
    dist_df = distribution_analysis(df)

    # Save outputs
    rolling_df.to_parquet(output_dir / "rolling_analysis.parquet")
    dd_df.to_parquet(output_dir / "drawdown_analysis.parquet")
    streak_df.to_csv(output_dir / "streak_analysis.csv", index=False)
    dist_df.to_csv(output_dir / "distribution_analysis.csv", index=False)

    print("\nDiagnostics complete.")

    print("\nStreaks:")
    print(streak_df)

    print("\nDistribution:")
    print(dist_df)

    print("\nWorst drawdown:")
    print(dd_df["drawdown_pct"].min())


if __name__ == "__main__":
    main()