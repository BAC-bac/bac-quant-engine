"""
BACQE REGIME ENGINE - 47 Build Current Regime Dashboard

Builds a current cross-symbol regime dashboard from recent classified regime files.

Outputs:
    E:/Quant_Lab/reports/regimes/current_dashboard/current_regime_dashboard_latest.txt
    E:/Quant_Lab/data/analysis/regimes/current_regime_dashboard_latest.csv
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
BROKER = "FTMO"

RECENT_REGIME_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "regimes"
    / "recent"
    / "classified"
    / BROKER
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "current_dashboard"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "GBPJPY",
    "EURGBP",
    "XAUUSD",
]

TIMEFRAMES = [
    "M15",
    "M30",
    "H1",
    "H4",
    "D1",
]


def regime_file_path(symbol: str, timeframe: str) -> Path:
    return (
        RECENT_REGIME_ROOT
        / timeframe
        / f"{symbol}_{timeframe}_recent_regimes.parquet"
    )


def load_latest_regime(symbol: str, timeframe: str) -> dict:
    path = regime_file_path(symbol, timeframe)

    record = {
        "symbol": symbol,
        "timeframe": timeframe,
        "file_path": str(path),
        "file_found": path.exists(),
        "read_status": "not_found",
        "latest_time": None,
        "trend_state": None,
        "volatility_state": None,
        "momentum_state": None,
        "trend_strength_state": None,
        "composite_regime": None,
        "regime_confidence": None,
        "rows": None,
        "error_message": None,
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    if not path.exists():
        return record

    try:
        df = pd.read_parquet(path)

        record["read_status"] = "success"
        record["rows"] = len(df)

        if df.empty:
            record["read_status"] = "empty"
            return record

        if "time" not in df.columns:
            record["read_status"] = "missing_time_column"
            return record

        df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
        df = df.dropna(subset=["time"]).sort_values("time")

        if df.empty:
            record["read_status"] = "no_valid_time"
            return record

        latest = df.iloc[-1]

        record["latest_time"] = latest.get("time")
        record["trend_state"] = latest.get("trend_state")
        record["volatility_state"] = latest.get("volatility_state")
        record["momentum_state"] = latest.get("momentum_state")
        record["trend_strength_state"] = latest.get("trend_strength_state")
        record["composite_regime"] = latest.get("composite_regime")
        record["regime_confidence"] = latest.get("regime_confidence")

        return record

    except Exception as exc:
        record["read_status"] = "failed"
        record["error_message"] = str(exc)[:500]
        return record


def classify_dashboard_bias(row: pd.Series) -> str:
    composite = str(row.get("composite_regime", "")).lower()
    trend = str(row.get("trend_state", "")).lower()
    vol = str(row.get("volatility_state", "")).lower()
    confidence = row.get("regime_confidence")

    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0

    if "bull_trend" in composite or trend == "bull_trend":
        if "high_vol" in composite or "high" in vol:
            return "bullish_high_vol"
        return "bullish"

    if "bear_trend" in composite or trend == "bear_trend":
        if "high_vol" in composite or "high" in vol:
            return "bearish_high_vol"
        return "bearish"

    if "volatile" in composite or "high" in vol:
        return "volatile_unclear"

    if "quiet" in composite or "low" in vol:
        return "quiet_range"

    if confidence < 0.55:
        return "low_confidence_unclear"

    return "neutral_or_transition"


def build_dashboard_rows() -> pd.DataFrame:
    records = []

    for symbol in SYMBOLS:
        for timeframe in TIMEFRAMES:
            records.append(load_latest_regime(symbol, timeframe))

    dashboard = pd.DataFrame(records)

    if not dashboard.empty:
        dashboard["dashboard_bias"] = dashboard.apply(classify_dashboard_bias, axis=1)

        dashboard["latest_time"] = pd.to_datetime(
            dashboard["latest_time"],
            errors="coerce",
            utc=True,
        )

        numeric_cols = dashboard.select_dtypes(include=["float", "int"]).columns
        dashboard[numeric_cols] = dashboard[numeric_cols].round(8)

    return dashboard


def build_symbol_summary(dashboard: pd.DataFrame) -> pd.DataFrame:
    good = dashboard[dashboard["read_status"] == "success"].copy()

    if good.empty:
        return pd.DataFrame()

    summary = (
        good.groupby("symbol")
        .agg(
            timeframes_available=("timeframe", "count"),
            avg_regime_confidence=("regime_confidence", "mean"),
            latest_time=("latest_time", "max"),
            dominant_bias=("dashboard_bias", lambda s: s.value_counts().index[0]),
            composite_regimes=("composite_regime", lambda s: " | ".join(s.dropna().astype(str).unique())),
        )
        .reset_index()
    )

    summary["avg_regime_confidence"] = summary["avg_regime_confidence"].round(6)
    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(["dominant_bias", "avg_regime_confidence"], ascending=[True, False])


def build_text_report(dashboard: pd.DataFrame, symbol_summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 100)
    lines.append("BACQE CURRENT REGIME DASHBOARD")
    lines.append("=" * 100)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Broker:          {BROKER}")
    lines.append(f"Input root:      {RECENT_REGIME_ROOT}")
    lines.append("-" * 100)

    lines.append("")
    lines.append("SYMBOL SUMMARY")
    lines.append("-" * 100)

    if symbol_summary.empty:
        lines.append("No successful symbol summary rows.")
    else:
        lines.append(symbol_summary.to_string(index=False))

    lines.append("")
    lines.append("CURRENT REGIME GRID")
    lines.append("-" * 100)

    display_cols = [
        "symbol",
        "timeframe",
        "latest_time",
        "trend_state",
        "volatility_state",
        "momentum_state",
        "composite_regime",
        "regime_confidence",
        "dashboard_bias",
        "read_status",
    ]

    available_cols = [col for col in display_cols if col in dashboard.columns]

    lines.append(dashboard[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 100)
    lines.append("This is a regime dashboard, not a trading signal.")
    lines.append("Bullish/bearish labels describe current regime structure, not trade recommendations.")
    lines.append("High-volatility trend states may be directional but also higher risk.")
    lines.append("Quiet/range states may favour patience, compression analysis, or mean-reversion research.")
    lines.append("This dashboard is designed to become the market-regime companion to the microstructure dashboard.")
    lines.append("=" * 100)

    return "\n".join(lines)


def main() -> None:
    print("=" * 100)
    print("BACQE REGIME ENGINE - 47 BUILD CURRENT REGIME DASHBOARD")
    print("=" * 100)
    print(f"Recent regime root: {RECENT_REGIME_ROOT}")
    print("-" * 100)

    dashboard = build_dashboard_rows()
    symbol_summary = build_symbol_summary(dashboard)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    dashboard_csv = OUTPUT_ANALYSIS_DIR / "current_regime_dashboard_latest.csv"
    dashboard_parquet = OUTPUT_ANALYSIS_DIR / "current_regime_dashboard_latest.parquet"

    summary_csv = OUTPUT_ANALYSIS_DIR / "current_regime_symbol_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "current_regime_symbol_summary_latest.parquet"

    report_txt = OUTPUT_REPORT_DIR / "current_regime_dashboard_latest.txt"
    report_json = OUTPUT_REPORT_DIR / "current_regime_dashboard_latest.json"

    dashboard.to_csv(dashboard_csv, index=False)
    dashboard.to_parquet(dashboard_parquet, index=False)

    symbol_summary.to_csv(summary_csv, index=False)
    symbol_summary.to_parquet(summary_parquet, index=False)

    report = build_text_report(dashboard, symbol_summary)
    report_txt.write_text(report, encoding="utf-8")

    payload = {
        "report_time_utc": datetime.now(timezone.utc).isoformat(),
        "broker": BROKER,
        "symbols": SYMBOLS,
        "timeframes": TIMEFRAMES,
        "dashboard": dashboard.astype(str).to_dict(orient="records"),
        "symbol_summary": symbol_summary.astype(str).to_dict(orient="records"),
    }

    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] Current regime dashboard created.")
    print(f"Dashboard CSV:     {dashboard_csv}")
    print(f"Dashboard Parquet: {dashboard_parquet}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"Summary Parquet:   {summary_parquet}")
    print(f"Report TXT:        {report_txt}")
    print(f"Report JSON:       {report_json}")
    print("-" * 100)

    print(report)
    print("=" * 100)


if __name__ == "__main__":
    main()