"""
BACQE DUKASCOPY 38 - MARKET STRUCTURE INVESTIGATION
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_ROOT = (
    QUANT_LAB / "data" / "processed" / "dukascopy_horizon_features" / f"symbol={SYMBOL}"
)

OUTPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_market_structure_investigation"

TARGET = "future_return_1000"
FEATURE = "mid_return_1"

MAX_ROWS_PER_FILE = 10_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "structure_tables",
        OUTPUT_ROOT / "continuation_tables",
        OUTPUT_ROOT / "gap_tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_files() -> list[Path]:
    return sorted(INPUT_ROOT.rglob("*.parquet")) if INPUT_ROOT.exists() else []


def assign_session(hour: int) -> str:
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london_morning"
    if 12 <= hour < 16:
        return "london_newyork_overlap"
    if 16 <= hour < 21:
        return "newyork_afternoon"
    return "late_us"


def add_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
    df = df.dropna(subset=["timestamp_utc"])

    df["year"] = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.tz_localize(None).dt.to_period("M").astype(str)
    df["day_of_week"] = df["timestamp_utc"].dt.day_name()
    df["hour"] = df["timestamp_utc"].dt.hour
    df["session"] = df["hour"].apply(assign_session)

    if "rolling_return_std_50" in df.columns:
        df["volatility_proxy"] = df["rolling_return_std_50"]
    else:
        df["volatility_proxy"] = df["mid"].pct_change().rolling(50).std()

    return df


def evaluate_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []

    for keys, group in df.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)

        base = {col: value for col, value in zip(group_cols, keys)}

        valid = group[[FEATURE, TARGET, "spread", "volatility_proxy"]].replace(
            [np.inf, -np.inf], np.nan
        ).dropna()

        if valid.empty:
            continue

        continuation_long = valid.loc[valid[FEATURE] > 0, TARGET]
        continuation_short = -valid.loc[valid[FEATURE] < 0, TARGET]

        continuation_all = pd.concat([continuation_long, continuation_short])

        base.update({
            "rows": len(valid),
            "avg_spread": valid["spread"].mean(),
            "median_spread": valid["spread"].median(),
            "avg_volatility_proxy": valid["volatility_proxy"].mean(),
            "median_volatility_proxy": valid["volatility_proxy"].median(),
            "feature_positive_rate": (valid[FEATURE] > 0).mean(),
            "future_positive_rate": (valid[TARGET] > 0).mean(),
            "continuation_count": len(continuation_all),
            "continuation_win_rate": (continuation_all > 0).mean() if len(continuation_all) else np.nan,
            "continuation_mean_return": continuation_all.mean() if len(continuation_all) else np.nan,
            "continuation_total_return": continuation_all.sum() if len(continuation_all) else np.nan,
        })

        rows.append(base)

    return pd.DataFrame(rows)


def calculate_daily_open_gap_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Proxy for weekend/open gap behaviour:
    first mid of day versus previous day's last mid.
    """
    daily = (
        df.sort_values("timestamp_utc")
        .groupby(df["timestamp_utc"].dt.date)
        .agg(
            date=("timestamp_utc", "first"),
            first_mid=("mid", "first"),
            last_mid=("mid", "last"),
            first_spread=("spread", "first"),
            avg_spread=("spread", "mean"),
            rows=("mid", "count"),
        )
        .reset_index(drop=True)
    )

    daily["prev_last_mid"] = daily["last_mid"].shift(1)
    daily["open_gap_return"] = daily["first_mid"] / daily["prev_last_mid"] - 1
    daily["date"] = pd.to_datetime(daily["date"])
    daily["day_of_week"] = daily["date"].dt.day_name()
    daily["year"] = daily["date"].dt.year
    daily["month"] = daily["date"].dt.to_period("M").astype(str)

    return daily


def main() -> None:
    banner("BACQE DUKASCOPY 38 - MARKET STRUCTURE INVESTIGATION")

    ensure_dirs()

    print(f"Symbol:      {SYMBOL}")
    print(f"Input root:  {INPUT_ROOT}")
    print(f"Output root: {OUTPUT_ROOT}")
    print(f"Feature:     {FEATURE}")
    print(f"Target:      {TARGET}")
    print("-" * 90)

    files = discover_files()
    print(f"Files discovered: {len(files)}")
    print("-" * 90)

    if not files:
        print("[STOP] No files found.")
        return

    frames = []

    for i, path in enumerate(files, start=1):
        try:
            df = pd.read_parquet(path)

            required = {"timestamp_utc", "mid", "spread", FEATURE, TARGET}
            missing = required - set(df.columns)

            if missing:
                continue

            df = add_context(df)

            if len(df) > MAX_ROWS_PER_FILE:
                df = df.sample(MAX_ROWS_PER_FILE, random_state=42).sort_values("timestamp_utc")

            frames.append(df)

        except Exception as e:
            print(f"[ERROR] {path.name}: {e}")

        if i % 100 == 0 or i == len(files):
            print(f"Processed {i}/{len(files)} files")

    if not frames:
        print("[STOP] No usable data loaded.")
        return

    data = pd.concat(frames, ignore_index=True)
    print(f"Combined sampled rows: {len(data):,}")

    numeric_cols = data.select_dtypes(include=[np.number]).columns
    data[numeric_cols] = data[numeric_cols].replace([np.inf, -np.inf], np.nan)
    data = data.dropna(subset=["spread", FEATURE, TARGET, "volatility_proxy"])

    # Structure tables
    by_hour = evaluate_group(data, ["hour"])
    by_day = evaluate_group(data, ["day_of_week"])
    by_session = evaluate_group(data, ["session"])
    by_session_day = evaluate_group(data, ["session", "day_of_week"])
    by_day_hour = evaluate_group(data, ["day_of_week", "hour"])
    by_year_day = evaluate_group(data, ["year", "day_of_week"])

    # Daily gap proxy
    daily_gap = calculate_daily_open_gap_proxy(data)

    gap_by_day = (
        daily_gap.dropna(subset=["open_gap_return"])
        .groupby("day_of_week", as_index=False)
        .agg(
            days=("open_gap_return", "count"),
            avg_open_gap_return=("open_gap_return", "mean"),
            median_open_gap_return=("open_gap_return", "median"),
            avg_abs_open_gap_return=("open_gap_return", lambda x: x.abs().mean()),
            max_abs_open_gap_return=("open_gap_return", lambda x: x.abs().max()),
            avg_first_spread=("first_spread", "mean"),
            avg_day_spread=("avg_spread", "mean"),
        )
    )

    # Monday Asia focus
    monday_asia = data[
        (data["day_of_week"] == "Monday")
        & (data["session"] == "asia")
    ].copy()

    monday_asia_by_hour = evaluate_group(monday_asia, ["hour"]) if not monday_asia.empty else pd.DataFrame()

    # Save outputs
    by_hour_path = OUTPUT_ROOT / "structure_tables" / "structure_by_hour_latest.csv"
    by_day_path = OUTPUT_ROOT / "structure_tables" / "structure_by_day_latest.csv"
    by_session_path = OUTPUT_ROOT / "structure_tables" / "structure_by_session_latest.csv"
    by_session_day_path = OUTPUT_ROOT / "structure_tables" / "structure_by_session_day_latest.csv"
    by_day_hour_path = OUTPUT_ROOT / "continuation_tables" / "continuation_by_day_hour_latest.csv"
    by_year_day_path = OUTPUT_ROOT / "continuation_tables" / "continuation_by_year_day_latest.csv"
    gap_by_day_path = OUTPUT_ROOT / "gap_tables" / "open_gap_by_day_latest.csv"
    monday_asia_path = OUTPUT_ROOT / "continuation_tables" / "monday_asia_by_hour_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "market_structure_investigation_report_latest.txt"

    by_hour.to_csv(by_hour_path, index=False)
    by_day.to_csv(by_day_path, index=False)
    by_session.to_csv(by_session_path, index=False)
    by_session_day.to_csv(by_session_day_path, index=False)
    by_day_hour.to_csv(by_day_hour_path, index=False)
    by_year_day.to_csv(by_year_day_path, index=False)
    gap_by_day.to_csv(gap_by_day_path, index=False)
    monday_asia_by_hour.to_csv(monday_asia_path, index=False)

    top_continuation = by_day_hour.sort_values(
        "continuation_mean_return",
        ascending=False
    ).head(30)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY MARKET STRUCTURE INVESTIGATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Files processed: {len(files)}\n")
        f.write(f"Combined sampled rows: {len(data):,}\n")
        f.write(f"Feature: {FEATURE}\n")
        f.write(f"Target: {TARGET}\n\n")

        f.write("Average Structure by Session\n")
        f.write("-" * 80 + "\n")
        f.write(
            by_session[
                [
                    "session",
                    "rows",
                    "avg_spread",
                    "avg_volatility_proxy",
                    "continuation_win_rate",
                    "continuation_mean_return",
                    "continuation_total_return",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nAverage Structure by Day of Week\n")
        f.write("-" * 80 + "\n")
        f.write(
            by_day[
                [
                    "day_of_week",
                    "rows",
                    "avg_spread",
                    "avg_volatility_proxy",
                    "continuation_win_rate",
                    "continuation_mean_return",
                    "continuation_total_return",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOpen Gap Proxy by Day of Week\n")
        f.write("-" * 80 + "\n")
        f.write(gap_by_day.to_string(index=False))

        f.write("\n\nMonday Asia by Hour\n")
        f.write("-" * 80 + "\n")
        if monday_asia_by_hour.empty:
            f.write("No Monday Asia rows available.\n")
        else:
            f.write(
                monday_asia_by_hour[
                    [
                        "hour",
                        "rows",
                        "avg_spread",
                        "avg_volatility_proxy",
                        "continuation_win_rate",
                        "continuation_mean_return",
                        "continuation_total_return",
                    ]
                ].to_string(index=False)
            )

        f.write("\n\nTop Day/Hour Continuation Contexts\n")
        f.write("-" * 80 + "\n")
        f.write(
            top_continuation[
                [
                    "day_of_week",
                    "hour",
                    "rows",
                    "avg_spread",
                    "avg_volatility_proxy",
                    "continuation_win_rate",
                    "continuation_mean_return",
                    "continuation_total_return",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"By hour: {by_hour_path}\n")
        f.write(f"By day: {by_day_path}\n")
        f.write(f"By session: {by_session_path}\n")
        f.write(f"By session/day: {by_session_day_path}\n")
        f.write(f"By day/hour: {by_day_hour_path}\n")
        f.write(f"By year/day: {by_year_day_path}\n")
        f.write(f"Gap by day: {gap_by_day_path}\n")
        f.write(f"Monday Asia: {monday_asia_path}\n")

    print("=" * 90)
    print("[DONE] Market structure investigation complete.")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()