"""
BACQE REGIME ENGINE - 49 Build Market Regime Alignment Engine

Scores cross-timeframe regime alignment for each symbol.

Input:
    E:/Quant_Lab/data/analysis/regimes/current_regime_dashboard_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/regimes/market_regime_alignment_latest.csv
    E:/Quant_Lab/reports/regimes/market_alignment/market_regime_alignment_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "current_regime_dashboard_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "market_alignment"


def directional_score(row: pd.Series) -> int:
    bias = str(row.get("dashboard_bias", "")).lower()
    trend = str(row.get("trend_state", "")).lower()
    composite = str(row.get("composite_regime", "")).lower()

    if "bullish" in bias or "bull_trend" in trend or "bull_trend" in composite:
        return 1

    if "bearish" in bias or "bear_trend" in trend or "bear_trend" in composite:
        return -1

    return 0


def volatility_score(row: pd.Series) -> int:
    vol = str(row.get("volatility_state", "")).lower()
    composite = str(row.get("composite_regime", "")).lower()
    bias = str(row.get("dashboard_bias", "")).lower()

    if "high" in vol or "high_vol" in composite or "volatile" in bias:
        return 1

    if "low" in vol or "quiet" in composite or "quiet" in bias:
        return -1

    return 0


def classify_alignment_label(direction_mean: float, directional_strength: float, volatility_mean: float) -> str:
    if directional_strength >= 0.75:
        if direction_mean > 0:
            return "strong_bullish_alignment"
        if direction_mean < 0:
            return "strong_bearish_alignment"

    if directional_strength >= 0.50:
        if direction_mean > 0:
            return "moderate_bullish_alignment"
        if direction_mean < 0:
            return "moderate_bearish_alignment"

    if abs(volatility_mean) >= 0.60:
        if volatility_mean > 0:
            return "volatile_alignment"
        return "quiet_compressed_alignment"

    return "mixed_or_transition"


def classify_risk_environment(row: pd.Series) -> str:
    alignment = row.get("alignment_label", "")
    vol_mean = row.get("volatility_alignment_score", 0)
    confidence = row.get("avg_regime_confidence", 0)

    if "strong_bullish" in alignment and vol_mean <= 0.25 and confidence >= 0.60:
        return "directional_risk_on"

    if "strong_bearish" in alignment and vol_mean <= 0.25 and confidence >= 0.60:
        return "directional_risk_off_or_defensive"

    if vol_mean > 0.40:
        return "high_volatility_caution"

    if vol_mean < -0.40:
        return "quiet_compression_watch"

    return "mixed_caution"


def build_alignment_scores(df: pd.DataFrame) -> pd.DataFrame:
    data = df[df["read_status"] == "success"].copy()

    data["regime_confidence"] = pd.to_numeric(data["regime_confidence"], errors="coerce").fillna(0)
    data["directional_score"] = data.apply(directional_score, axis=1)
    data["volatility_score"] = data.apply(volatility_score, axis=1)

    records = []

    for symbol, group in data.groupby("symbol"):
        direction_values = group["directional_score"]
        volatility_values = group["volatility_score"]

        bullish_count = int((direction_values == 1).sum())
        bearish_count = int((direction_values == -1).sum())
        neutral_count = int((direction_values == 0).sum())

        high_vol_count = int((volatility_values == 1).sum())
        low_vol_count = int((volatility_values == -1).sum())
        normal_vol_count = int((volatility_values == 0).sum())

        timeframe_count = len(group)

        direction_mean = direction_values.mean()
        directional_strength = abs(direction_values.sum()) / timeframe_count if timeframe_count else 0
        volatility_mean = volatility_values.mean()

        avg_confidence = group["regime_confidence"].mean()

        alignment_label = classify_alignment_label(
            direction_mean=direction_mean,
            directional_strength=directional_strength,
            volatility_mean=volatility_mean,
        )

        record = {
            "symbol": symbol,
            "timeframe_count": timeframe_count,
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
            "high_vol_count": high_vol_count,
            "low_vol_count": low_vol_count,
            "normal_vol_count": normal_vol_count,
            "directional_alignment_score": round(direction_mean, 6),
            "directional_strength_score": round(directional_strength, 6),
            "volatility_alignment_score": round(volatility_mean, 6),
            "avg_regime_confidence": round(avg_confidence, 6),
            "alignment_label": alignment_label,
            "latest_time": group["latest_time"].max(),
            "timeframes": " | ".join(group["timeframe"].astype(str).tolist()),
            "dashboard_biases": " | ".join(group["dashboard_bias"].astype(str).tolist()),
            "composite_regimes": " | ".join(group["composite_regime"].astype(str).tolist()),
            "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        records.append(record)

    alignment = pd.DataFrame(records)

    if alignment.empty:
        return alignment

    alignment["risk_environment_label"] = alignment.apply(classify_risk_environment, axis=1)

    alignment = alignment.sort_values(
        ["directional_strength_score", "avg_regime_confidence"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return alignment


def build_report(alignment: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "symbol",
        "alignment_label",
        "risk_environment_label",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
        "bullish_count",
        "bearish_count",
        "neutral_count",
        "high_vol_count",
        "low_vol_count",
        "composite_regimes",
    ]

    lines = []

    lines.append("=" * 110)
    lines.append("BACQE MARKET REGIME ALIGNMENT ENGINE")
    lines.append("=" * 110)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 110)

    if alignment.empty:
        lines.append("No alignment rows produced.")
    else:
        lines.append(alignment[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 110)
    lines.append("directional_alignment_score ranges from -1 bearish to +1 bullish.")
    lines.append("directional_strength_score measures agreement strength regardless of direction.")
    lines.append("volatility_alignment_score ranges from -1 quiet/compressed to +1 high volatility.")
    lines.append("alignment_label describes cross-timeframe structure, not a trade recommendation.")
    lines.append("risk_environment_label is a research context label, not a signal.")
    lines.append("=" * 110)

    return "\n".join(lines)


def main() -> None:
    print("=" * 110)
    print("BACQE REGIME ENGINE - 49 BUILD MARKET REGIME ALIGNMENT ENGINE")
    print("=" * 110)
    print(f"Input: {INPUT_PATH}")
    print("-" * 110)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input dashboard not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    alignment = build_alignment_scores(df)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "market_regime_alignment_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "market_regime_alignment_latest.parquet"
    json_path = OUTPUT_ANALYSIS_DIR / "market_regime_alignment_latest.json"
    report_path = OUTPUT_REPORT_DIR / "market_regime_alignment_latest.txt"

    alignment.to_csv(csv_path, index=False)
    alignment.to_parquet(parquet_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(alignment.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(alignment)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Market regime alignment engine created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"JSON:    {json_path}")
    print(f"Report:  {report_path}")
    print("-" * 110)
    print(report)
    print("=" * 110)


if __name__ == "__main__":
    main()