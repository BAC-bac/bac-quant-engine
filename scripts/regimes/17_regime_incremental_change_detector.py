"""
BACQE Script 17
Regime Incremental Change Detector

Purpose:
- Read Script 16 latest incremental ledger
- Compare full base layers against recent incremental layers
- Detect broker/timeframe/symbol combinations where recent data is newer
- Produce an actionable change/update plan

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

LEDGER_LATEST_CSV = LEDGER_DIR / "regime_incremental_ledger_latest.csv"

OUTPUT_DIR = LEDGER_DIR

COMPARE_PAIRS = [
    {
        "full_stage": "features",
        "recent_stage": "recent_features",
        "plan_type": "feature_update",
    },
    {
        "full_stage": "classified",
        "recent_stage": "recent_classified",
        "plan_type": "classification_update",
    },
]


# ============================================================
# HELPERS
# ============================================================

def safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def load_ledger() -> pd.DataFrame:
    if not LEDGER_LATEST_CSV.exists():
        raise FileNotFoundError(f"Ledger file not found: {LEDGER_LATEST_CSV}")

    df = pd.read_csv(LEDGER_LATEST_CSV)

    required_cols = {
        "stage",
        "broker",
        "timeframe",
        "symbol",
        "file_path",
        "rows",
        "latest_timestamp",
        "modified_time",
        "status",
    }

    missing = required_cols.difference(df.columns)

    if missing:
        raise ValueError(f"Ledger is missing required columns: {sorted(missing)}")

    df = df[df["status"].eq("ok")].copy()

    df["latest_timestamp_dt"] = safe_to_datetime(df["latest_timestamp"])
    df["modified_time_dt"] = safe_to_datetime(df["modified_time"])

    return df


def prepare_stage(df: pd.DataFrame, stage: str) -> pd.DataFrame:
    stage_df = df[df["stage"].eq(stage)].copy()

    grouped = (
        stage_df.groupby(["broker", "timeframe", "symbol"], dropna=False)
        .agg(
            file_count=("file_path", "count"),
            total_rows=("rows", "sum"),
            latest_timestamp=("latest_timestamp_dt", "max"),
            latest_modified=("modified_time_dt", "max"),
            total_size_mb=("size_mb", "sum"),
            example_file_path=("file_path", "first"),
        )
        .reset_index()
    )

    grouped["total_size_mb"] = grouped["total_size_mb"].round(4)

    return grouped


def build_change_plan_for_pair(
    ledger_df: pd.DataFrame,
    full_stage: str,
    recent_stage: str,
    plan_type: str,
) -> pd.DataFrame:

    full_df = prepare_stage(ledger_df, full_stage)
    recent_df = prepare_stage(ledger_df, recent_stage)

    merged = full_df.merge(
        recent_df,
        on=["broker", "timeframe", "symbol"],
        how="outer",
        suffixes=("_full", "_recent"),
        indicator=True,
    )

    merged["plan_type"] = plan_type
    merged["full_stage"] = full_stage
    merged["recent_stage"] = recent_stage

    merged["time_gap"] = merged["latest_timestamp_recent"] - merged["latest_timestamp_full"]
    merged["gap_minutes"] = merged["time_gap"].dt.total_seconds() / 60
    merged["gap_hours"] = merged["gap_minutes"] / 60
    merged["gap_days"] = merged["gap_hours"] / 24

    merged["recent_is_newer"] = (
        merged["latest_timestamp_recent"].notna()
        & merged["latest_timestamp_full"].notna()
        & (merged["latest_timestamp_recent"] > merged["latest_timestamp_full"])
    )

    merged["missing_full"] = merged["_merge"].eq("right_only")
    merged["missing_recent"] = merged["_merge"].eq("left_only")

    merged["update_needed"] = (
        merged["recent_is_newer"]
        | merged["missing_full"]
    )

    def recommend(row):
        if row["missing_full"]:
            return "Create full base file from recent layer or investigate missing historical base."
        if row["missing_recent"]:
            return "No recent layer found. No incremental update available."
        if row["recent_is_newer"]:
            return "Recent layer is newer than full layer. Candidate for append/merge into full base."
        return "Full layer is up to date relative to recent layer."

    merged["recommended_action"] = merged.apply(recommend, axis=1)

    output_cols = [
        "plan_type",
        "broker",
        "timeframe",
        "symbol",
        "update_needed",
        "recommended_action",
        "latest_timestamp_full",
        "latest_timestamp_recent",
        "gap_minutes",
        "gap_hours",
        "gap_days",
        "file_count_full",
        "file_count_recent",
        "total_rows_full",
        "total_rows_recent",
        "total_size_mb_full",
        "total_size_mb_recent",
        "example_file_path_full",
        "example_file_path_recent",
        "full_stage",
        "recent_stage",
    ]

    return merged[output_cols].sort_values(
        by=["update_needed", "plan_type", "broker", "timeframe", "symbol"],
        ascending=[False, True, True, True, True],
    )


def build_summary(change_plan: pd.DataFrame) -> pd.DataFrame:
    summary = (
        change_plan.groupby(["plan_type", "broker", "timeframe"], dropna=False)
        .agg(
            symbols_checked=("symbol", "count"),
            updates_needed=("update_needed", "sum"),
            max_gap_hours=("gap_hours", "max"),
            avg_gap_hours=("gap_hours", "mean"),
            total_full_rows=("total_rows_full", "sum"),
            total_recent_rows=("total_rows_recent", "sum"),
        )
        .reset_index()
    )

    summary["max_gap_hours"] = summary["max_gap_hours"].round(3)
    summary["avg_gap_hours"] = summary["avg_gap_hours"].round(3)

    return summary.sort_values(
        by=["updates_needed", "max_gap_hours"],
        ascending=[False, False],
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME INCREMENTAL CHANGE DETECTOR")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Ledger file:  {LEDGER_LATEST_CSV}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    ledger_df = load_ledger()

    all_plans = []

    for pair in COMPARE_PAIRS:
        print(
            f"[COMPARE] {pair['full_stage']} "
            f"vs {pair['recent_stage']} "
            f"({pair['plan_type']})"
        )

        plan_df = build_change_plan_for_pair(
            ledger_df=ledger_df,
            full_stage=pair["full_stage"],
            recent_stage=pair["recent_stage"],
            plan_type=pair["plan_type"],
        )

        all_plans.append(plan_df)

    change_plan = pd.concat(all_plans, ignore_index=True)
    summary = build_summary(change_plan)

    change_plan_timestamped_csv = OUTPUT_DIR / f"regime_incremental_change_plan_{run_timestamp}.csv"
    change_plan_latest_csv = OUTPUT_DIR / "regime_incremental_change_plan_latest.csv"

    summary_timestamped_csv = OUTPUT_DIR / f"regime_incremental_change_summary_{run_timestamp}.csv"
    summary_latest_csv = OUTPUT_DIR / "regime_incremental_change_summary_latest.csv"

    json_path = OUTPUT_DIR / f"regime_incremental_change_plan_{run_timestamp}.json"

    change_plan.to_csv(change_plan_timestamped_csv, index=False)
    change_plan.to_csv(change_plan_latest_csv, index=False)

    summary.to_csv(summary_timestamped_csv, index=False)
    summary.to_csv(summary_latest_csv, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "ledger_file": str(LEDGER_LATEST_CSV),
        "change_plan_timestamped_csv": str(change_plan_timestamped_csv),
        "change_plan_latest_csv": str(change_plan_latest_csv),
        "summary_timestamped_csv": str(summary_timestamped_csv),
        "summary_latest_csv": str(summary_latest_csv),
        "total_rows": int(len(change_plan)),
        "updates_needed": int(change_plan["update_needed"].sum()),
        "next_recommended_step": (
            "Inspect change plan. If sensible, build Script 18 as a safe dry-run incremental "
            "merge planner before modifying any full historical files."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Incremental change plan created.")
    print(f"Change plan latest CSV: {change_plan_latest_csv}")
    print(f"Summary latest CSV:     {summary_latest_csv}")
    print(f"JSON report:            {json_path}")

    print("-" * 90)
    print(f"Total broker/timeframe/symbol checks: {len(change_plan)}")
    print(f"Updates needed: {int(change_plan['update_needed'].sum())}")

    print("\nSummary preview:")
    print(summary.head(40).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()