"""
BACQE Script 37
Regime Risk Score Builder

Purpose:
- Convert filtered regime instability rankings into actionable regime risk scores
- Create risk bands:
  - low
  - medium
  - high
  - extreme
- Add suggested strategy posture:
  - normal_trade_allowed
  - selective_trade_allowed
  - defensive_mode
  - convex_only_or_avoid
- Produce symbol/regime, symbol/timeframe, and regime-family risk outputs

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_instability_rankings"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_risk_intelligence"

FILTERED_ALL = INPUT_DIR / "regime_instability_filtered_all_latest.csv"
TIMEFRAME_SUMMARY = INPUT_DIR / "regime_instability_filtered_timeframe_summary_latest.csv"
REGIME_FAMILY_SUMMARY = INPUT_DIR / "regime_instability_filtered_regime_family_summary_latest.csv"


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def risk_band(score: float) -> str:
    try:
        s = float(score)
    except Exception:
        return "unknown"

    if s < 0.30:
        return "low"
    if s < 0.40:
        return "medium"
    if s < 0.50:
        return "high"
    return "extreme"


def strategy_posture(band: str, regime_family: str) -> str:
    band = str(band).lower()
    family = str(regime_family).lower()

    if band == "low":
        return "normal_trade_allowed"

    if band == "medium":
        return "selective_trade_allowed"

    if band == "high":
        if "volatile" in family or "transition" in family:
            return "defensive_mode"
        return "selective_trade_allowed"

    if band == "extreme":
        if "volatile" in family or "transition" in family:
            return "convex_only_or_avoid"
        return "defensive_mode"

    return "manual_review"


def risk_commentary(row) -> str:
    band = row["regime_risk_band"]
    family = str(row["regime_family"])
    leave_prob = float(row.get("leave_probability", 0))
    avg_bars = float(row.get("avg_segment_bars", 0))

    if band == "low":
        return "Stable regime profile. Standard strategy logic may be allowed."

    if band == "medium":
        return "Moderate instability. Prefer confirmation filters and normal risk controls."

    if band == "high":
        return (
            f"High instability in {family}. Leave probability around {leave_prob:.2f}; "
            f"average duration around {avg_bars:.2f} bars. Use reduced risk or defensive filters."
        )

    if band == "extreme":
        return (
            f"Extreme instability in {family}. Regime may be short-lived or prone to transitions. "
            "Avoid fragile mean-reversion assumptions unless specifically validated."
        )

    return "Manual review required."


def build_symbol_regime_risk(filtered: pd.DataFrame) -> pd.DataFrame:
    df = filtered.copy()

    numeric_cols = [
        "instability_score",
        "stability_score",
        "leave_probability",
        "stay_probability",
        "regime_change_rate",
        "avg_segment_bars",
        "median_segment_bars",
        "max_segment_bars",
        "segments",
        "total_from_regime",
        "total_transitions",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    robust = df[df["passes_sample_filter"].astype(str).str.lower().eq("true")].copy()

    robust["regime_risk_score"] = robust["instability_score"].round(6)
    robust["regime_risk_band"] = robust["regime_risk_score"].apply(risk_band)
    robust["strategy_posture"] = robust.apply(
        lambda row: strategy_posture(row["regime_risk_band"], row["regime_family"]),
        axis=1,
    )
    robust["risk_commentary"] = robust.apply(risk_commentary, axis=1)

    robust["risk_rank"] = robust["regime_risk_score"].rank(
        method="dense",
        ascending=False,
    ).astype(int)

    return robust.sort_values(
        ["regime_risk_score", "leave_probability", "regime_change_rate"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def build_symbol_timeframe_risk(symbol_regime_risk: pd.DataFrame) -> pd.DataFrame:
    summary = (
        symbol_regime_risk.groupby(
            ["broker", "timeframe", "timeframe_group", "symbol"],
            dropna=False,
        )
        .agg(
            regimes_scored=("regime", "count"),
            avg_regime_risk_score=("regime_risk_score", "mean"),
            max_regime_risk_score=("regime_risk_score", "max"),
            avg_leave_probability=("leave_probability", "mean"),
            max_leave_probability=("leave_probability", "max"),
            avg_regime_change_rate=("regime_change_rate", "mean"),
            avg_segment_bars=("avg_segment_bars", "mean"),
            high_or_extreme_regimes=(
                "regime_risk_band",
                lambda x: int(pd.Series(x).isin(["high", "extreme"]).sum()),
            ),
            defensive_or_convex_regimes=(
                "strategy_posture",
                lambda x: int(pd.Series(x).isin(["defensive_mode", "convex_only_or_avoid"]).sum()),
            ),
        )
        .reset_index()
    )

    for col in [
        "avg_regime_risk_score",
        "max_regime_risk_score",
        "avg_leave_probability",
        "max_leave_probability",
        "avg_regime_change_rate",
        "avg_segment_bars",
    ]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    summary["symbol_timeframe_risk_band"] = summary["avg_regime_risk_score"].apply(risk_band)

    summary["symbol_timeframe_posture"] = summary["symbol_timeframe_risk_band"].map({
        "low": "normal_trade_allowed",
        "medium": "selective_trade_allowed",
        "high": "defensive_mode",
        "extreme": "convex_only_or_avoid",
    }).fillna("manual_review")

    return summary.sort_values(
        ["avg_regime_risk_score", "max_regime_risk_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def build_regime_family_risk(regime_family_summary: pd.DataFrame) -> pd.DataFrame:
    df = regime_family_summary.copy()

    for col in [
        "avg_instability_score",
        "avg_leave_probability",
        "avg_stay_probability",
        "avg_segment_bars",
        "robust_regime_rows",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["family_risk_score"] = df["avg_instability_score"].round(6)
    df["family_risk_band"] = df["family_risk_score"].apply(risk_band)

    df["family_strategy_posture"] = df.apply(
        lambda row: strategy_posture(row["family_risk_band"], row["regime_family"]),
        axis=1,
    )

    return df.sort_values(
        ["family_risk_score", "avg_leave_probability"],
        ascending=[False, False],
    ).reset_index(drop=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME RISK SCORE BUILDER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input dir:    {INPUT_DIR}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    filtered = read_required(FILTERED_ALL)
    timeframe_summary = read_required(TIMEFRAME_SUMMARY)
    regime_family_summary = read_required(REGIME_FAMILY_SUMMARY)

    print(f"Filtered instability rows loaded: {len(filtered):,}")
    print(f"Timeframe summary rows loaded:    {len(timeframe_summary):,}")
    print(f"Regime family rows loaded:        {len(regime_family_summary):,}")

    symbol_regime_risk = build_symbol_regime_risk(filtered)
    symbol_timeframe_risk = build_symbol_timeframe_risk(symbol_regime_risk)
    regime_family_risk = build_regime_family_risk(regime_family_summary)

    high_risk = symbol_regime_risk[
        symbol_regime_risk["regime_risk_band"].isin(["high", "extreme"])
    ].copy()

    defensive_posture = symbol_regime_risk[
        symbol_regime_risk["strategy_posture"].isin(["defensive_mode", "convex_only_or_avoid"])
    ].copy()

    low_risk = symbol_regime_risk[
        symbol_regime_risk["regime_risk_band"].eq("low")
    ].copy()

    outputs = {
        "symbol_regime_risk": OUTPUT_DIR / "regime_symbol_regime_risk_latest.csv",
        "symbol_timeframe_risk": OUTPUT_DIR / "regime_symbol_timeframe_risk_latest.csv",
        "regime_family_risk": OUTPUT_DIR / "regime_family_risk_latest.csv",
        "high_risk": OUTPUT_DIR / "regime_high_risk_latest.csv",
        "defensive_posture": OUTPUT_DIR / "regime_defensive_posture_latest.csv",
        "low_risk": OUTPUT_DIR / "regime_low_risk_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    symbol_regime_risk.to_csv(outputs["symbol_regime_risk"], index=False)
    symbol_timeframe_risk.to_csv(outputs["symbol_timeframe_risk"], index=False)
    regime_family_risk.to_csv(outputs["regime_family_risk"], index=False)
    high_risk.to_csv(outputs["high_risk"], index=False)
    defensive_posture.to_csv(outputs["defensive_posture"], index=False)
    low_risk.to_csv(outputs["low_risk"], index=False)

    symbol_regime_risk.to_csv(timestamped["symbol_regime_risk"], index=False)
    symbol_timeframe_risk.to_csv(timestamped["symbol_timeframe_risk"], index=False)
    regime_family_risk.to_csv(timestamped["regime_family_risk"], index=False)
    high_risk.to_csv(timestamped["high_risk"], index=False)
    defensive_posture.to_csv(timestamped["defensive_posture"], index=False)
    low_risk.to_csv(timestamped["low_risk"], index=False)

    band_counts = symbol_regime_risk["regime_risk_band"].value_counts().to_dict()
    posture_counts = symbol_regime_risk["strategy_posture"].value_counts().to_dict()

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "symbol_regime_risk_rows": int(len(symbol_regime_risk)),
        "symbol_timeframe_risk_rows": int(len(symbol_timeframe_risk)),
        "regime_family_risk_rows": int(len(regime_family_risk)),
        "high_or_extreme_rows": int(len(high_risk)),
        "defensive_posture_rows": int(len(defensive_posture)),
        "low_risk_rows": int(len(low_risk)),
        "band_counts": band_counts,
        "posture_counts": posture_counts,
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect high-risk and defensive posture outputs. "
            "Next script can map risk bands into strategy allocation/veto rules."
        ),
    }

    json_latest = OUTPUT_DIR / "regime_risk_score_builder_latest.json"
    json_ts = OUTPUT_DIR / f"regime_risk_score_builder_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Regime risk scores created.")
    print(f"Symbol-regime risk rows:    {len(symbol_regime_risk):,}")
    print(f"Symbol-timeframe risk rows: {len(symbol_timeframe_risk):,}")
    print(f"Regime-family risk rows:    {len(regime_family_risk):,}")
    print(f"High/extreme rows:          {len(high_risk):,}")
    print(f"Defensive posture rows:     {len(defensive_posture):,}")
    print(f"Low-risk rows:              {len(low_risk):,}")
    print(f"Symbol-regime output:       {outputs['symbol_regime_risk']}")
    print(f"Symbol-timeframe output:    {outputs['symbol_timeframe_risk']}")
    print(f"Family-risk output:         {outputs['regime_family_risk']}")
    print(f"JSON summary:               {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()