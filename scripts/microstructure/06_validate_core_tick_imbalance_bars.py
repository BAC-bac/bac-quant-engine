"""
BACQE MICROSTRUCTURE 06 - VALIDATE CORE TICK IMBALANCE BARS

Purpose:
    Validate core tick imbalance bar outputs created by:
        scripts/microstructure/05_build_core_tick_imbalance_bars.py

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/core_tick_imbalance_bar_validation/
        core_tick_imbalance_bar_validation_latest.csv
        core_tick_imbalance_bar_validation_latest.json
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_tick_imbalance_file(
    file_path: Path,
    symbol: str,
    imbalance_threshold: int,
    min_rows: int,
) -> dict:
    checked_at = datetime.now(timezone.utc).isoformat()

    result = {
        "checked_at_utc": checked_at,
        "symbol": symbol,
        "imbalance_threshold": imbalance_threshold,
        "file_path": str(file_path),
        "file_exists": file_path.exists(),
        "status": "unknown",
        "row_count": 0,
        "start_time_min": None,
        "end_time_max": None,
        "null_count_total": None,
        "duplicate_time_rows": None,
        "bad_ohlc_rows": None,
        "negative_spread_rows": None,
        "bad_duration_rows": None,
        "bad_tick_count_rows": None,
        "bad_signed_tick_rows": None,
        "bad_threshold_rows": None,
        "partial_bar_count": None,
        "return_null_rows": None,
        "avg_duration_seconds": None,
        "avg_spread": None,
        "max_spread": None,
        "avg_tick_count": None,
        "avg_abs_signed_tick_sum": None,
        "issues": [],
    }

    if not file_path.exists():
        result["status"] = "missing"
        result["issues"].append("file_missing")
        return result

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        result["status"] = "failed_read"
        result["issues"].append(f"failed_read: {exc}")
        return result

    result["row_count"] = len(df)

    if df.empty:
        result["status"] = "empty"
        result["issues"].append("empty_file")
        return result

    required_columns = [
        "symbol",
        "start_time",
        "end_time",
        "open_mid",
        "high_mid",
        "low_mid",
        "close_mid",
        "open_bid",
        "high_bid",
        "low_bid",
        "close_bid",
        "open_ask",
        "high_ask",
        "low_ask",
        "close_ask",
        "avg_spread",
        "max_spread",
        "tick_count",
        "uptick_count",
        "downtick_count",
        "signed_tick_sum",
        "abs_signed_tick_sum",
        "imbalance_threshold",
        "volume",
        "volume_real",
        "bar_id",
        "is_partial_bar",
        "duration_seconds",
        "return_mid",
        "range_mid",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        result["issues"].append(f"missing_columns: {missing_columns}")

    if "start_time" in df.columns:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        result["start_time_min"] = str(df["start_time"].min())

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        result["end_time_max"] = str(df["end_time"].max())

    result["null_count_total"] = int(df.isna().sum().sum())

    if "is_partial_bar" in df.columns:
        df["is_partial_bar"] = df["is_partial_bar"].fillna(False).astype(bool)
        result["partial_bar_count"] = int(df["is_partial_bar"].sum())

    if "start_time" in df.columns and "end_time" in df.columns:
        result["duplicate_time_rows"] = int(
            df.duplicated(subset=["start_time", "end_time"]).sum()
        )

    if all(col in df.columns for col in ["high_mid", "low_mid", "open_mid", "close_mid"]):
        bad_ohlc = (
            (df["high_mid"] < df["low_mid"])
            | (df["open_mid"] > df["high_mid"])
            | (df["open_mid"] < df["low_mid"])
            | (df["close_mid"] > df["high_mid"])
            | (df["close_mid"] < df["low_mid"])
        )
        result["bad_ohlc_rows"] = int(bad_ohlc.sum())

    if "avg_spread" in df.columns:
        result["negative_spread_rows"] = int((df["avg_spread"] < 0).sum())
        result["avg_spread"] = float(df["avg_spread"].mean())

    if "max_spread" in df.columns:
        result["max_spread"] = float(df["max_spread"].max())

    if "duration_seconds" in df.columns:
        result["bad_duration_rows"] = int((df["duration_seconds"] < 0).sum())
        result["avg_duration_seconds"] = float(df["duration_seconds"].mean())

    if "tick_count" in df.columns:
        result["bad_tick_count_rows"] = int((df["tick_count"] <= 0).sum())
        result["avg_tick_count"] = float(df["tick_count"].mean())

    if all(col in df.columns for col in ["uptick_count", "downtick_count", "tick_count"]):
        bad_signed_count = df["uptick_count"] + df["downtick_count"] != df["tick_count"]
        result["bad_signed_tick_rows"] = int(bad_signed_count.sum())

    if "abs_signed_tick_sum" in df.columns:
        result["avg_abs_signed_tick_sum"] = float(df["abs_signed_tick_sum"].mean())

        threshold_check = df["abs_signed_tick_sum"] < imbalance_threshold

        if "is_partial_bar" in df.columns:
            threshold_check = threshold_check & (~df["is_partial_bar"])

        result["bad_threshold_rows"] = int(threshold_check.sum())

    if "return_mid" in df.columns:
        result["return_null_rows"] = int(df["return_mid"].isna().sum())

    if result["row_count"] < min_rows:
        result["issues"].append("below_min_rows")

    numeric_issue_checks = {
        "bad_ohlc_rows": result["bad_ohlc_rows"],
        "negative_spread_rows": result["negative_spread_rows"],
        "bad_duration_rows": result["bad_duration_rows"],
        "bad_tick_count_rows": result["bad_tick_count_rows"],
        "bad_signed_tick_rows": result["bad_signed_tick_rows"],
        "bad_threshold_rows": result["bad_threshold_rows"],
    }

    for issue_name, value in numeric_issue_checks.items():
        if value is not None and value > 0:
            result["issues"].append(issue_name)

    if missing_columns:
        result["status"] = "warning"
    elif result["issues"]:
        result["status"] = "warning"
    else:
        result["status"] = "ok"

    return result


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 06 - VALIDATE CORE TICK IMBALANCE BARS")

    config = load_config()
    micro_cfg = config["microstructure"]

    output_dir = Path(micro_cfg["output"]["microstructure_dir"])
    analysis_dir = Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )

    report_dir = analysis_dir / "core_tick_imbalance_bar_validation"
    report_dir.mkdir(parents=True, exist_ok=True)

    symbols = micro_cfg["symbols"]
    thresholds = micro_cfg["imbalance_bars"]["tick_imbalance_thresholds"]
    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    print(f"Config:                 {CONFIG_PATH}")
    print(f"Input bars:             {output_dir / 'tick_imbalance_bars'}")
    print(f"Report dir:             {report_dir}")
    print(f"Symbols:                {symbols}")
    print(f"Imbalance thresholds:   {thresholds}")
    print("-" * 90)

    results = []

    for symbol in symbols:
        for threshold in thresholds:
            file_path = (
                output_dir
                / "tick_imbalance_bars"
                / f"symbol={symbol}"
                / f"imbalance_threshold={threshold}"
                / "tick_imbalance_bars.parquet"
            )

            result = validate_tick_imbalance_file(
                file_path=file_path,
                symbol=symbol,
                imbalance_threshold=threshold,
                min_rows=min_rows,
            )

            results.append(result)

            print(
                f"[CHECK] {symbol:<8} threshold={threshold:<5} "
                f"status={result['status']:<10} rows={result['row_count']:,} "
                f"partial={result['partial_bar_count']}"
            )

            if result["issues"]:
                print(f"        issues={result['issues']}")

    report_df = pd.DataFrame(results)

    csv_path = report_dir / "core_tick_imbalance_bar_validation_latest.csv"
    json_path = report_dir / "core_tick_imbalance_bar_validation_latest.json"

    report_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    status_counts = report_df["status"].value_counts(dropna=False).to_dict()

    print("-" * 90)
    print("[DONE] Core tick imbalance bar validation complete.")
    print(f"Files checked: {len(report_df)}")
    print(f"Status counts: {status_counts}")
    print(f"CSV output:    {csv_path}")
    print(f"JSON output:   {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()