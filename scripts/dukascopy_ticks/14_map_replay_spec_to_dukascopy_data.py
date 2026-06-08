"""
BACQE DUKASCOPY 14 - MAP REPLAY SPEC TO DUKASCOPY DATA

Purpose:
    Validate whether the EURUSD primary replay spec can be mapped onto
    the available Dukascopy Tick Imbalance Bar data.

This script does not perform replay.
It checks:
    - replay spec exists
    - TIB files exist
    - required TIB columns exist
    - weekdays can be mapped
    - sessions can be mapped approximately from UTC hour
    - threshold-pair inputs are available
"""

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

SPEC_PATH = (
    DATA_ROOT
    / "analysis"
    / "dukascopy_ticks"
    / "candidate_replay_prep"
    / "eurusd_primary_replay_spec.csv"
)

TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "candidate_replay_prep"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

IMBALANCE_THRESHOLDS = [25, 50, 100]

REQUIRED_TIB_COLUMNS = [
    "timestamp_start",
    "timestamp_end",
    "symbol",
    "source",
    "imbalance_threshold",
    "tick_count",
    "duration_seconds",
    "open",
    "high",
    "low",
    "close",
    "return_close_to_close",
    "range",
    "range_points",
    "spread_mean",
    "signed_tick_sum",
    "buy_ticks",
    "sell_ticks",
    "imbalance_direction",
]


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def classify_session(timestamp) -> str:
    """
    Approximate session labels from UTC hour.

    These labels are deliberately aligned to the names found in the
    validation review file:
        asia_late_overnight
        london_mid_morning
        pre_new_york

    This is a first-pass mapping and can be refined later.
    """

    hour = pd.Timestamp(timestamp).hour

    if 0 <= hour <= 5:
        return "asia_late_overnight"

    if 8 <= hour <= 11:
        return "london_mid_morning"

    if 12 <= hour <= 13:
        return "pre_new_york"

    if 14 <= hour <= 16:
        return "new_york_open"

    if 17 <= hour <= 20:
        return "new_york_afternoon"

    if 21 <= hour <= 23:
        return "rollover_late"

    return "other"


def load_all_tib_metadata() -> tuple[pd.DataFrame, list[dict]]:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dfs = []
    file_rows = []

    for dt in date_range(start, end):
        for threshold in IMBALANCE_THRESHOLDS:
            path = tib_path(SYMBOL, dt, threshold)

            if not path.exists():
                file_rows.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "threshold": threshold,
                    "status": "missing",
                    "rows": 0,
                    "path": str(path),
                    "missing_columns": "",
                    "error": "",
                })
                continue

            try:
                df = pd.read_parquet(path)
            except Exception as exc:
                file_rows.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "threshold": threshold,
                    "status": "read_error",
                    "rows": 0,
                    "path": str(path),
                    "missing_columns": "",
                    "error": repr(exc),
                })
                continue

            missing_cols = [
                col for col in REQUIRED_TIB_COLUMNS
                if col not in df.columns
            ]

            status = "ok" if not missing_cols else "missing_columns"

            file_rows.append({
                "date": dt.strftime("%Y-%m-%d"),
                "threshold": threshold,
                "status": status,
                "rows": len(df),
                "path": str(path),
                "missing_columns": ",".join(missing_cols),
                "error": "",
            })

            if status == "ok" and not df.empty:
                df = df.copy()
                df["source_file"] = str(path)
                df["date"] = dt.strftime("%Y-%m-%d")
                df["weekday"] = pd.to_datetime(df["timestamp_start"]).dt.day_name()
                df["session"] = df["timestamp_start"].apply(classify_session)
                dfs.append(df)

    if dfs:
        tib_df = pd.concat(dfs, ignore_index=True)
    else:
        tib_df = pd.DataFrame()

    return tib_df, file_rows


def split_csv_field(value) -> list[str]:
    if pd.isna(value):
        return []

    return [
        item.strip()
        for item in str(value).split(",")
        if item.strip()
    ]


def build_spec_mapping_report(spec_df: pd.DataFrame, tib_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    available_weekdays = set(tib_df["weekday"].unique()) if not tib_df.empty else set()
    available_sessions = set(tib_df["session"].unique()) if not tib_df.empty else set()

    for _, row in spec_df.iterrows():
        required_weekdays = set(split_csv_field(row.get("weekdays")))
        required_sessions = set(split_csv_field(row.get("sessions")))

        weekday_overlap = sorted(required_weekdays.intersection(available_weekdays))
        session_overlap = sorted(required_sessions.intersection(available_sessions))

        if tib_df.empty:
            matched_rows = 0
        else:
            matched = tib_df.copy()

            if required_weekdays:
                matched = matched[matched["weekday"].isin(required_weekdays)]

            if required_sessions:
                matched = matched[matched["session"].isin(required_sessions)]

            matched_rows = len(matched)

        rows.append({
            "replay_id": row["replay_id"],
            "symbol": row["symbol"],
            "filter_name": row["filter_name"],
            "validation_rank": row["validation_rank"],
            "cost_per_trade": row["cost_per_trade"],
            "threshold_pair": row["threshold_pair"],
            "buy_threshold": row["buy_threshold"],
            "sell_threshold": row["sell_threshold"],
            "required_weekdays": ",".join(sorted(required_weekdays)),
            "available_weekday_overlap": ",".join(weekday_overlap),
            "required_sessions": ",".join(sorted(required_sessions)),
            "available_session_overlap": ",".join(session_overlap),
            "matched_tib_rows_after_time_filters": matched_rows,
            "mapping_status": "ready" if matched_rows > 0 else "no_matching_rows",
        })

    return pd.DataFrame(rows)


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 14 - MAP REPLAY SPEC TO DUKASCOPY DATA")
    print("=" * 90)

    if not SPEC_PATH.exists():
        print(f"[ERROR] Replay spec missing: {SPEC_PATH}")
        return

    spec_df = pd.read_csv(SPEC_PATH)

    print(f"Loaded replay specs: {len(spec_df):,}")
    print(f"Spec: {SPEC_PATH}")
    print("-" * 90)

    tib_df, file_rows = load_all_tib_metadata()
    file_report_df = pd.DataFrame(file_rows)

    print(f"Loaded TIB rows: {len(tib_df):,}")

    if not tib_df.empty:
        print("\n[AVAILABLE WEEKDAYS]")
        print(tib_df["weekday"].value_counts().to_string())

        print("\n[AVAILABLE SESSIONS]")
        print(tib_df["session"].value_counts().to_string())

        print("\n[AVAILABLE THRESHOLDS]")
        print(tib_df["imbalance_threshold"].value_counts().sort_index().to_string())

    mapping_df = build_spec_mapping_report(spec_df, tib_df)

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    file_report_path = REPORT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_tib_file_mapping_report.csv"
    mapping_report_path = REPORT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_replay_spec_mapping_report.csv"

    file_report_df.to_csv(file_report_path, index=False)
    mapping_df.to_csv(mapping_report_path, index=False)

    print("-" * 90)
    print("[MAPPING STATUS]")
    print(mapping_df["mapping_status"].value_counts(dropna=False).to_string())

    print("\n[PREVIEW]")
    print(mapping_df.head(10).to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"TIB file mapping report: {file_report_path}")
    print(f"Replay mapping report:   {mapping_report_path}")
    print("[DONE] Replay spec mapping complete.")


if __name__ == "__main__":
    main()