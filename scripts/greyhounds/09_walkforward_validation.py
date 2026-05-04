from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


TRAIN_END_YEAR = 2021
TEST_START_YEAR = 2022


CANDIDATE_SYSTEMS = {
    "BSP_8_TO_13": {
        "description": "Back all UK greyhounds with BSP between 8.00 and 13.00",
        "track_key": None,
        "trap": None,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "ROMFORD_TRAP_3": {
        "description": "Back Romford trap 3",
        "track_key": "romford",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HARLOW_TRAP_1": {
        "description": "Back Harlow trap 1",
        "track_key": "harlow",
        "trap": 1,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HENLOW_TRAP_2": {
        "description": "Back Henlow trap 2",
        "track_key": "henlow",
        "trap": 2,
        "bsp_min": None,
        "bsp_max": None,
    },
    "TOWCESTER_TRAP_3": {
        "description": "Back Towcester trap 3",
        "track_key": "towcester",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
    "ROMFORD_TRAP_3_BSP_8_TO_13": {
        "description": "Back Romford trap 3 only when BSP is between 8.00 and 13.00",
        "track_key": "romford",
        "trap": 3,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "HARLOW_TRAP_1_BSP_8_TO_13": {
        "description": "Back Harlow trap 1 only when BSP is between 8.00 and 13.00",
        "track_key": "harlow",
        "trap": 1,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "HENLOW_TRAP_2_BSP_8_TO_13": {
        "description": "Back Henlow trap 2 only when BSP is between 8.00 and 13.00",
        "track_key": "henlow",
        "trap": 2,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "TOWCESTER_TRAP_3_BSP_8_TO_13": {
        "description": "Back Towcester trap 3 only when BSP is between 8.00 and 13.00",
        "track_key": "towcester",
        "trap": 3,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
}


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def apply_candidate_filter(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    system_df = df.copy()

    if rules["track_key"] is not None:
        system_df = system_df[system_df["track_key"] == rules["track_key"]]

    if rules["trap"] is not None:
        system_df = system_df[system_df["trap"] == rules["trap"]]

    if rules["bsp_min"] is not None:
        system_df = system_df[system_df["bsp"] >= rules["bsp_min"]]

    if rules["bsp_max"] is not None:
        system_df = system_df[system_df["bsp"] < rules["bsp_max"]]

    return system_df.sort_values("event_dt").copy()


def max_drawdown_points(profit_series: pd.Series) -> float:
    if profit_series.empty:
        return 0.0

    equity = profit_series.cumsum()
    peak = equity.cummax()
    drawdown = equity - peak

    return float(drawdown.min())


def profit_factor(profit_series: pd.Series):
    gross_profit = profit_series[profit_series > 0].sum()
    gross_loss = abs(profit_series[profit_series < 0].sum())

    if gross_loss == 0:
        return None

    return float(gross_profit / gross_loss)


def summarise(system_name: str, description: str, split_name: str, df: pd.DataFrame) -> dict:
    bets = len(df)

    if bets == 0:
        return {
            "system_name": system_name,
            "description": description,
            "split": split_name,
            "bets": 0,
            "races": 0,
            "winners": 0,
            "strike_rate": 0.0,
            "avg_bsp": None,
            "median_bsp": None,
            "total_profit": 0.0,
            "roi": 0.0,
            "max_drawdown_points": 0.0,
            "profit_factor": None,
            "date_min": None,
            "date_max": None,
        }

    total_profit = float(df["back_profit_1pt"].sum())

    return {
        "system_name": system_name,
        "description": description,
        "split": split_name,
        "bets": bets,
        "races": df["event_id"].nunique(),
        "winners": int(df["is_winner"].sum()),
        "strike_rate": float(df["is_winner"].mean()),
        "avg_bsp": float(df["bsp"].mean()),
        "median_bsp": float(df["bsp"].median()),
        "total_profit": total_profit,
        "roi": total_profit / bets,
        "max_drawdown_points": max_drawdown_points(df["back_profit_1pt"]),
        "profit_factor": profit_factor(df["back_profit_1pt"]),
        "date_min": df["event_dt"].min(),
        "date_max": df["event_dt"].max(),
    }


def build_yearly_report(system_name: str, split_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    report = (
        df.groupby("year", dropna=False)
        .agg(
            bets=("event_id", "size"),
            races=("event_id", "nunique"),
            winners=("is_winner", "sum"),
            avg_bsp=("bsp", "mean"),
            median_bsp=("bsp", "median"),
            total_profit=("back_profit_1pt", "sum"),
        )
        .reset_index()
    )

    report["system_name"] = system_name
    report["split"] = split_name
    report["strike_rate"] = report["winners"] / report["bets"]
    report["roi"] = report["total_profit"] / report["bets"]

    return report[
        [
            "system_name",
            "split",
            "year",
            "bets",
            "races",
            "winners",
            "strike_rate",
            "avg_bsp",
            "median_bsp",
            "total_profit",
            "roi",
        ]
    ]


def build_monthly_report(system_name: str, split_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    report = (
        df.groupby("year_month", dropna=False)
        .agg(
            bets=("event_id", "size"),
            races=("event_id", "nunique"),
            winners=("is_winner", "sum"),
            avg_bsp=("bsp", "mean"),
            median_bsp=("bsp", "median"),
            total_profit=("back_profit_1pt", "sum"),
        )
        .reset_index()
    )

    report["system_name"] = system_name
    report["split"] = split_name
    report["strike_rate"] = report["winners"] / report["bets"]
    report["roi"] = report["total_profit"] / report["bets"]

    return report[
        [
            "system_name",
            "split",
            "year_month",
            "bets",
            "races",
            "winners",
            "strike_rate",
            "avg_bsp",
            "median_bsp",
            "total_profit",
            "roi",
        ]
    ]


def build_walkforward_comparison(summary: pd.DataFrame) -> pd.DataFrame:
    wide = summary.pivot_table(
        index=["system_name", "description"],
        columns="split",
        values=[
            "bets",
            "total_profit",
            "roi",
            "strike_rate",
            "max_drawdown_points",
            "profit_factor",
        ],
        aggfunc="first",
    )

    wide.columns = [f"{metric}_{split}".lower() for metric, split in wide.columns]
    wide = wide.reset_index()

    if {"roi_train", "roi_test"}.issubset(wide.columns):
        wide["roi_decay"] = wide["roi_test"] - wide["roi_train"]
        wide["roi_retention"] = wide["roi_test"] / wide["roi_train"]

    if {"total_profit_train", "total_profit_test"}.issubset(wide.columns):
        wide["profit_change"] = wide["total_profit_test"] - wide["total_profit_train"]

    if "roi_test" in wide.columns:
        wide = wide.sort_values(["roi_test", "bets_test"], ascending=[False, False])

    return wide


def main() -> None:
    print("Starting walk-forward validation...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"]) / "walkforward"

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df = df.sort_values("event_dt").reset_index(drop=True)
    df["year"] = df["event_dt"].dt.year
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    train_df = df[df["year"] <= TRAIN_END_YEAR].copy()
    test_df = df[df["year"] >= TEST_START_YEAR].copy()

    print(f"Rows loaded: {len(df):,}")
    print(f"Train rows <= {TRAIN_END_YEAR}: {len(train_df):,}")
    print(f"Test rows >= {TEST_START_YEAR}: {len(test_df):,}")
    print(f"Full date range: {df['event_dt'].min()} to {df['event_dt'].max()}")

    summary_rows = []
    yearly_reports = []
    monthly_reports = []

    for system_name, rules in CANDIDATE_SYSTEMS.items():
        description = rules["description"]

        full_system_df = apply_candidate_filter(df, rules)
        train_system_df = apply_candidate_filter(train_df, rules)
        test_system_df = apply_candidate_filter(test_df, rules)

        summary_rows.append(summarise(system_name, description, "full", full_system_df))
        summary_rows.append(summarise(system_name, description, "train", train_system_df))
        summary_rows.append(summarise(system_name, description, "test", test_system_df))

        yearly_reports.append(build_yearly_report(system_name, "full", full_system_df))
        yearly_reports.append(build_yearly_report(system_name, "train", train_system_df))
        yearly_reports.append(build_yearly_report(system_name, "test", test_system_df))

        monthly_reports.append(build_monthly_report(system_name, "full", full_system_df))
        monthly_reports.append(build_monthly_report(system_name, "train", train_system_df))
        monthly_reports.append(build_monthly_report(system_name, "test", test_system_df))

        train_roi = train_system_df["back_profit_1pt"].sum() / len(train_system_df) if len(train_system_df) else 0
        test_roi = test_system_df["back_profit_1pt"].sum() / len(test_system_df) if len(test_system_df) else 0

        print(
            f"{system_name}: "
            f"train bets {len(train_system_df):,}, train ROI {train_roi:.4f} | "
            f"test bets {len(test_system_df):,}, test ROI {test_roi:.4f}"
        )

    summary = pd.DataFrame(summary_rows)
    yearly = pd.concat(yearly_reports, ignore_index=True) if yearly_reports else pd.DataFrame()
    monthly = pd.concat(monthly_reports, ignore_index=True) if monthly_reports else pd.DataFrame()
    comparison = build_walkforward_comparison(summary)

    summary_path = reports_dir / "walkforward_summary_long.csv"
    comparison_path = reports_dir / "walkforward_comparison.csv"
    yearly_path = reports_dir / "walkforward_yearly.csv"
    monthly_path = reports_dir / "walkforward_monthly.csv"

    summary.to_csv(summary_path, index=False)
    comparison.to_csv(comparison_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    monthly.to_csv(monthly_path, index=False)

    print("\nWalk-forward validation complete.")
    print(f"Saved long summary to: {summary_path}")
    print(f"Saved comparison to: {comparison_path}")
    print(f"Saved yearly report to: {yearly_path}")
    print(f"Saved monthly report to: {monthly_path}")

    print("\nWalk-forward comparison:")
    print(comparison.round(4))


if __name__ == "__main__":
    main()