"""
BACQE MICROSTRUCTURE 04 - VALIDATE CORE VOLUME BARS

Purpose:
    Validate core volume bar outputs created by:
        scripts/microstructure/03_build_core_volume_bars.py

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/core_volume_bar_validation/
        core_volume_bar_validation_latest.csv
        core_volume_bar_validation_latest.json
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


def validate_volume_bar_file(
    file_path: Path,
    symbol: str,
    volume_threshold: int,
    min_rows: int,
) -> dict:
    checked_at = datetime.now(timezone.utc).isoformat()

    result = {
        "checked_at_utc": checked_at,
        "symbol": symbol,
        "volume_threshold": volume_threshold,
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
        "bad_volume_rows": None,
        "bad_threshold_rows": None,
        "return_null_rows": None,
        "avg_duration_seconds": None,
        "avg_spread": None,
        "max_spread": None,
        "avg_bar_volume": None,
        "volume_mode": None,
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
        "avg_spread",
        "max_spread",
        "tick_count",
        "source_volume",
        "source_volume_real",
        "bar_volume",
        "volume_mode",
        "volume_threshold",
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

    if "volume_mode" in df.columns:
        modes = df["volume_mode"].dropna().unique().tolist()
        result["volume_mode"] = ",".join(map(str, modes))

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

    if "bar_volume" in df.columns:
        result["bad_volume_rows"] = int((df["bar_volume"] <= 0).sum())
        result["avg_bar_volume"] = float(df["bar_volume"].mean())

        bad_threshold = df["bar_volume"] != volume_threshold

        # The final bar may be partial, so allow it.
        if len(df) > 1:
            bad_threshold.iloc[-1] = False

        result["bad_threshold_rows"] = int(bad_threshold.sum())

    if "return_mid" in df.columns:
        result["return_null_rows"] = int(df["return_mid"].isna().sum())

    if result["row_count"] < min_rows:
        result["issues"].append("below_min_rows")

    numeric_issue_checks = {
        "bad_ohlc_rows": result["bad_ohlc_rows"],
        "negative_spread_rows": result["negative_spread_rows"],
        "bad_duration_rows": result["bad_duration_rows"],
        "bad_volume_rows": result["bad_volume_rows"],
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
    print_header("BACQE MICROSTRUCTURE 04 - VALIDATE CORE VOLUME BARS")

    config = load_config()
    micro_cfg = config["microstructure"]

    output_dir = Path(micro_cfg["output"]["microstructure_dir"])
    analysis_dir = Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )

    report_dir = analysis_dir / "core_volume_bar_validation"
    report_dir.mkdir(parents=True, exist_ok=True)

    symbols = micro_cfg["symbols"]
    thresholds = micro_cfg["volume_bars"]["thresholds"]
    min_rows = micro_cfg.get("validation", {}).get("min_rows", 100)

    print(f"Config:             {CONFIG_PATH}")
    print(f"Input bars:         {output_dir / 'volume_bars'}")
    print(f"Report dir:         {report_dir}")
    print(f"Symbols:            {symbols}")
    print(f"Volume thresholds:  {thresholds}")
    print("-" * 90)

    results = []

    for symbol in symbols:
        for threshold in thresholds:
            file_path = (
                output_dir
                / "volume_bars"
                / f"symbol={symbol}"
                / f"volume_threshold={threshold}"
                / "volume_bars.parquet"
            )

            result = validate_volume_bar_file(
                file_path=file_path,
                symbol=symbol,
                volume_threshold=threshold,
                min_rows=min_rows,
            )

            results.append(result)

            print(
                f"[CHECK] {symbol:<8} threshold={threshold:<5} "
                f"status={result['status']:<10} rows={result['row_count']:,}"
            )

            if result["issues"]:
                print(f"        issues={result['issues']}")

    report_df = pd.DataFrame(results)

    csv_path = report_dir / "core_volume_bar_validation_latest.csv"
    json_path = report_dir / "core_volume_bar_validation_latest.json"

    report_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    status_counts = report_df["status"].value_counts(dropna=False).to_dict()

    print("-" * 90)
    print("[DONE] Core volume bar validation complete.")
    print(f"Files checked: {len(report_df)}")
    print(f"Status counts: {status_counts}")
    print(f"CSV output:    {csv_path}")
    print(f"JSON output:   {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()