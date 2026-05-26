"""
BACQE REGIME ENGINE - 48 Combined Market Intelligence Dashboard

Combines:
    - Cross-symbol regime dashboard
    - GBPUSD live microstructure dashboard

Outputs:
    E:/Quant_Lab/reports/bacqe_market_intelligence/combined_market_intelligence_latest.txt
    E:/Quant_Lab/reports/bacqe_market_intelligence/combined_market_intelligence_latest.json
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

REGIME_DASHBOARD_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "current_regime_dashboard_latest.csv"
)

REGIME_SYMBOL_SUMMARY_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "current_regime_symbol_summary_latest.csv"
)

MICRO_DASHBOARD_JSON_PATH = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "live_state_dashboard"
    / "live_state_dashboard_latest.json"
)

OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_market_intelligence"


def load_regime_dashboard() -> pd.DataFrame:
    if not REGIME_DASHBOARD_PATH.exists():
        raise FileNotFoundError(f"Regime dashboard not found: {REGIME_DASHBOARD_PATH}")

    return pd.read_csv(REGIME_DASHBOARD_PATH, low_memory=False)


def load_symbol_summary() -> pd.DataFrame:
    if not REGIME_SYMBOL_SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Symbol summary not found: {REGIME_SYMBOL_SUMMARY_PATH}")

    return pd.read_csv(REGIME_SYMBOL_SUMMARY_PATH, low_memory=False)


def load_micro_dashboard() -> dict:
    if not MICRO_DASHBOARD_JSON_PATH.exists():
        raise FileNotFoundError(f"Microstructure dashboard not found: {MICRO_DASHBOARD_JSON_PATH}")

    with open(MICRO_DASHBOARD_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def classify_symbol_alignment(symbol_rows: pd.DataFrame) -> str:
    biases = symbol_rows["dashboard_bias"].dropna().astype(str).tolist()

    bullish = sum("bullish" in b for b in biases)
    bearish = sum("bearish" in b for b in biases)
    quiet = sum("quiet" in b for b in biases)
    volatile = sum("volatile" in b for b in biases)
    neutral = sum("neutral" in b or "transition" in b for b in biases)

    if bullish >= 3 and bearish == 0:
        return "bullish_alignment"

    if bearish >= 3 and bullish == 0:
        return "bearish_alignment"

    if quiet >= 3:
        return "quiet_or_compressed_alignment"

    if volatile >= 2:
        return "volatile_mixed_alignment"

    if bullish > 0 and bearish > 0:
        return "mixed_conflict"

    if neutral >= 3:
        return "transition_or_range_alignment"

    return "mixed_or_unclear"


def build_market_alignment_summary(regime_df: pd.DataFrame) -> pd.DataFrame:
    records = []

    for symbol, group in regime_df.groupby("symbol"):
        successful = group[group["read_status"] == "success"].copy()

        if successful.empty:
            records.append(
                {
                    "symbol": symbol,
                    "timeframes": 0,
                    "alignment_label": "no_data",
                    "avg_confidence": None,
                    "bullish_count": 0,
                    "bearish_count": 0,
                    "quiet_count": 0,
                    "volatile_count": 0,
                    "neutral_or_transition_count": 0,
                }
            )
            continue

        biases = successful["dashboard_bias"].dropna().astype(str)

        record = {
            "symbol": symbol,
            "timeframes": len(successful),
            "alignment_label": classify_symbol_alignment(successful),
            "avg_confidence": pd.to_numeric(successful["regime_confidence"], errors="coerce").mean(),
            "bullish_count": int(biases.str.contains("bullish").sum()),
            "bearish_count": int(biases.str.contains("bearish").sum()),
            "quiet_count": int(biases.str.contains("quiet").sum()),
            "volatile_count": int(biases.str.contains("volatile").sum()),
            "neutral_or_transition_count": int(
                biases.str.contains("neutral|transition", regex=True).sum()
            ),
        }

        records.append(record)

    summary = pd.DataFrame(records)
    summary["avg_confidence"] = summary["avg_confidence"].round(6)

    return summary.sort_values(["alignment_label", "avg_confidence"], ascending=[True, False])


def build_combined_payload(
    regime_df: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    market_alignment: pd.DataFrame,
    micro: dict,
) -> dict:
    payload = {
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
        "regime_dashboard_path": str(REGIME_DASHBOARD_PATH),
        "regime_symbol_summary_path": str(REGIME_SYMBOL_SUMMARY_PATH),
        "micro_dashboard_path": str(MICRO_DASHBOARD_JSON_PATH),
        "market_alignment": market_alignment.to_dict(orient="records"),
        "regime_symbol_summary": symbol_summary.astype(str).to_dict(orient="records"),
        "regime_grid": regime_df.astype(str).to_dict(orient="records"),
        "microstructure_dashboard": micro,
    }

    return payload


def build_text_dashboard(payload: dict) -> str:
    micro = payload["microstructure_dashboard"]
    market_alignment = pd.DataFrame(payload["market_alignment"])
    symbol_summary = pd.DataFrame(payload["regime_symbol_summary"])
    regime_grid = pd.DataFrame(payload["regime_grid"])

    lines = []

    lines.append("=" * 110)
    lines.append("BACQE COMBINED MARKET INTELLIGENCE DASHBOARD")
    lines.append("=" * 110)
    lines.append(f"Dashboard time UTC: {payload.get('dashboard_time_utc')}")
    lines.append("-" * 110)

    lines.append("")
    lines.append("MARKET ALIGNMENT SUMMARY")
    lines.append("-" * 110)
    lines.append(market_alignment.to_string(index=False))

    lines.append("")
    lines.append("REGIME SYMBOL SUMMARY")
    lines.append("-" * 110)
    lines.append(symbol_summary.to_string(index=False))

    lines.append("")
    lines.append("GBPUSD MICROSTRUCTURE INTELLIGENCE")
    lines.append("-" * 110)
    lines.append(f"Symbol:                 {micro.get('symbol')}")
    lines.append(f"Latest bar start:       {micro.get('latest_bar_start_time')}")
    lines.append(f"Bar type:               {micro.get('bar_type')}")
    lines.append(f"Microstructure regime:  {micro.get('microstructure_regime')}")
    lines.append(f"M15 composite regime:   {micro.get('m15_composite_regime')}")
    lines.append(f"M15 trend state:        {micro.get('m15_trend_state')}")
    lines.append(f"M15 volatility state:   {micro.get('m15_volatility_state')}")
    lines.append(f"Primary state:          {micro.get('primary_current_state')}")
    lines.append(f"Expected next state:    {micro.get('primary_expected_next_state')}")
    lines.append(f"Transition probability: {micro.get('primary_transition_probability')}")
    lines.append(f"Self-transition prob:   {micro.get('primary_self_transition_probability')}")
    lines.append(f"Live bias:              {micro.get('primary_live_bias')}")
    lines.append(f"Actionability:          {micro.get('primary_actionability')}")

    lines.append("")
    lines.append("GBPUSD CROSS-LAYER CHECK")
    lines.append("-" * 110)

    gbpusd_rows = regime_grid[regime_grid["symbol"] == "GBPUSD"].copy()

    if gbpusd_rows.empty:
        lines.append("No GBPUSD regime rows found.")
    else:
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
        ]

        available_cols = [col for col in display_cols if col in gbpusd_rows.columns]
        lines.append(gbpusd_rows[available_cols].to_string(index=False))

    lines.append("")
    lines.append("FULL REGIME GRID")
    lines.append("-" * 110)

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
    ]

    available_cols = [col for col in display_cols if col in regime_grid.columns]
    lines.append(regime_grid[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 110)
    lines.append("This dashboard is a market-intelligence layer, not a trading signal.")
    lines.append("The regime layer describes cross-symbol, multi-timeframe structure.")
    lines.append("The microstructure layer describes event-time behaviour for GBPUSD.")
    lines.append("Cross-layer agreement is more interesting than any single isolated reading.")
    lines.append("This is the first combined BACQE operator-console style report.")
    lines.append("=" * 110)

    return "\n".join(lines)


def main() -> None:
    print("=" * 110)
    print("BACQE REGIME ENGINE - 48 COMBINED MARKET INTELLIGENCE DASHBOARD")
    print("=" * 110)
    print(f"Regime dashboard:      {REGIME_DASHBOARD_PATH}")
    print(f"Regime symbol summary: {REGIME_SYMBOL_SUMMARY_PATH}")
    print(f"Micro dashboard JSON:  {MICRO_DASHBOARD_JSON_PATH}")
    print("-" * 110)

    regime_df = load_regime_dashboard()
    symbol_summary = load_symbol_summary()
    micro = load_micro_dashboard()

    market_alignment = build_market_alignment_summary(regime_df)

    payload = build_combined_payload(
        regime_df=regime_df,
        symbol_summary=symbol_summary,
        market_alignment=market_alignment,
        micro=micro,
    )

    text_dashboard = build_text_dashboard(payload)

    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    txt_path = OUTPUT_REPORT_DIR / "combined_market_intelligence_latest.txt"
    json_path = OUTPUT_REPORT_DIR / "combined_market_intelligence_latest.json"

    txt_path.write_text(text_dashboard, encoding="utf-8")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] Combined market intelligence dashboard created.")
    print(f"TXT:  {txt_path}")
    print(f"JSON: {json_path}")
    print("-" * 110)
    print(text_dashboard)
    print("=" * 110)


if __name__ == "__main__":
    main()