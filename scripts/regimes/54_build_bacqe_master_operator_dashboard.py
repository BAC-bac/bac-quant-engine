"""
BACQE REGIME ENGINE - 54 Build BACQE Master Operator Dashboard

Combines:
    - market regime alignment
    - strategy router dashboard
    - strategy-regime mapping
    - GBPUSD microstructure dashboard

This is the first master BACQE operator console.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

ALIGNMENT_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "market_regime_alignment_latest.csv"
ROUTER_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "strategy_router_dashboard_latest.csv"
MAPPING_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "strategy_regime_mapping_latest.csv"
MICRO_JSON_PATH = DATA_LAKE_ROOT / "reports" / "tick_research" / "live_state_dashboard" / "live_state_dashboard_latest.json"

OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_master_operator"
OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"


def read_csv_required(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{name} not found: {path}")

    df = pd.read_csv(path, low_memory=False)

    if df.empty:
        raise ValueError(f"{name} is empty: {path}")

    return df


def read_json_required(path: Path, name: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{name} not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_symbol_master_table(
    alignment: pd.DataFrame,
    router: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    cols_alignment = [
        "symbol",
        "alignment_label",
        "risk_environment_label",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    cols_router = [
        "symbol",
        "primary_strategy_environment",
        "strategy_family",
        "directional_bias",
        "risk_mode",
        "research_priority",
    ]

    cols_mapping = [
        "symbol",
        "primary_strategy",
        "secondary_strategy",
        "avoid_strategy",
        "strategy_confidence",
        "execution_posture",
        "environment_logic",
    ]

    master = alignment[cols_alignment].merge(
        router[cols_router],
        on="symbol",
        how="left",
    )

    master = master.merge(
        mapping[cols_mapping],
        on="symbol",
        how="left",
    )

    posture_rank = {
        "research_ready_environment": 1,
        "selective_research_environment": 2,
        "wait_for_expansion_confirmation": 3,
        "observation_or_defensive_only": 4,
        "background_monitoring": 5,
    }

    priority_rank = {
        "high_priority_watchlist": 1,
        "medium_priority_watchlist": 2,
        "compression_watchlist": 3,
        "background_monitor": 4,
    }

    master["execution_posture_rank"] = master["execution_posture"].map(posture_rank).fillna(99).astype(int)
    master["research_priority_rank"] = master["research_priority"].map(priority_rank).fillna(99).astype(int)

    master["master_dashboard_time_utc"] = datetime.now(timezone.utc).isoformat()

    master = master.sort_values(
        [
            "execution_posture_rank",
            "research_priority_rank",
            "directional_strength_score",
            "avg_regime_confidence",
        ],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)

    return master


def classify_master_market_mode(master: pd.DataFrame) -> str:
    ready = (master["execution_posture"] == "research_ready_environment").sum()
    defensive = (master["execution_posture"] == "observation_or_defensive_only").sum()
    compression = (master["execution_posture"] == "wait_for_expansion_confirmation").sum()

    bullish_ready = (
        (master["execution_posture"] == "research_ready_environment")
        & (master["directional_bias"] == "bullish_bias")
    ).sum()

    bearish_ready = (
        (master["execution_posture"] == "research_ready_environment")
        & (master["directional_bias"] == "bearish_bias")
    ).sum()

    if ready >= 2 and bullish_ready > 0 and bearish_ready > 0:
        return "mixed_directional_opportunity_environment"

    if ready >= 2 and bullish_ready >= 2:
        return "broad_bullish_opportunity_environment"

    if ready >= 2 and bearish_ready >= 2:
        return "broad_bearish_opportunity_environment"

    if compression >= 2:
        return "compression_watch_environment"

    if defensive >= len(master) / 2:
        return "defensive_transition_environment"

    return "selective_opportunity_environment"


def build_json_payload(master: pd.DataFrame, micro: dict) -> dict:
    return {
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
        "master_market_mode": classify_master_market_mode(master),
        "source_files": {
            "alignment": str(ALIGNMENT_PATH),
            "router": str(ROUTER_PATH),
            "mapping": str(MAPPING_PATH),
            "microstructure": str(MICRO_JSON_PATH),
        },
        "symbol_master_table": master.to_dict(orient="records"),
        "microstructure_dashboard": micro,
    }


def build_text_dashboard(master: pd.DataFrame, micro: dict) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()
    market_mode = classify_master_market_mode(master)

    display_cols = [
        "symbol",
        "alignment_label",
        "directional_bias",
        "primary_strategy_environment",
        "primary_strategy",
        "secondary_strategy",
        "avoid_strategy",
        "strategy_confidence",
        "execution_posture",
        "research_priority",
        "risk_mode",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    lines = []

    lines.append("=" * 140)
    lines.append("BACQE MASTER OPERATOR DASHBOARD")
    lines.append("=" * 140)
    lines.append(f"Dashboard time UTC: {now_utc}")
    lines.append(f"Master market mode: {market_mode}")
    lines.append("-" * 140)

    lines.append("")
    lines.append("MASTER SYMBOL ROUTING TABLE")
    lines.append("-" * 140)
    lines.append(master[display_cols].to_string(index=False))

    lines.append("")
    lines.append("RESEARCH-READY ENVIRONMENTS")
    lines.append("-" * 140)
    ready = master[master["execution_posture"] == "research_ready_environment"]
    if ready.empty:
        lines.append("No research-ready environments currently detected.")
    else:
        lines.append(ready[display_cols].to_string(index=False))

    lines.append("")
    lines.append("SELECTIVE / WATCHLIST ENVIRONMENTS")
    lines.append("-" * 140)
    selective = master[
        master["execution_posture"].isin(
            ["selective_research_environment", "wait_for_expansion_confirmation"]
        )
    ]
    if selective.empty:
        lines.append("No selective/watchlist environments currently detected.")
    else:
        lines.append(selective[display_cols].to_string(index=False))

    lines.append("")
    lines.append("DEFENSIVE / OBSERVATION ENVIRONMENTS")
    lines.append("-" * 140)
    defensive = master[master["execution_posture"] == "observation_or_defensive_only"]
    if defensive.empty:
        lines.append("No defensive/observation environments currently detected.")
    else:
        lines.append(defensive[display_cols].to_string(index=False))

    lines.append("")
    lines.append("GBPUSD MICROSTRUCTURE SNAPSHOT")
    lines.append("-" * 140)
    lines.append(f"Symbol:                 {micro.get('symbol')}")
    lines.append(f"Latest bar start:       {micro.get('latest_bar_start_time')}")
    lines.append(f"Bar type:               {micro.get('bar_type')}")
    lines.append(f"Microstructure regime:  {micro.get('microstructure_regime')}")
    lines.append(f"M15 composite regime:   {micro.get('m15_composite_regime')}")
    lines.append(f"Primary current state:  {micro.get('primary_current_state')}")
    lines.append(f"Expected next state:    {micro.get('primary_expected_next_state')}")
    lines.append(f"Transition probability: {micro.get('primary_transition_probability')}")
    lines.append(f"Live bias:              {micro.get('primary_live_bias')}")
    lines.append(f"Actionability:          {micro.get('primary_actionability')}")

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 140)
    lines.append("This is a master research/operator dashboard, not a trade signal engine.")
    lines.append("It combines market alignment, strategy environment routing, strategy-regime mapping, and microstructure state intelligence.")
    lines.append("Research-ready means structurally interesting enough for deeper analysis, not automatic execution.")
    lines.append("Avoid-strategy identifies strategy families currently conflicting with the detected environment.")
    lines.append("Future versions can add macro bias, performance-by-regime, and live scheduler integration.")
    lines.append("=" * 140)

    return "\n".join(lines)


def main() -> None:
    print("=" * 140)
    print("BACQE REGIME ENGINE - 54 BUILD BACQE MASTER OPERATOR DASHBOARD")
    print("=" * 140)

    alignment = read_csv_required(ALIGNMENT_PATH, "Market regime alignment")
    router = read_csv_required(ROUTER_PATH, "Strategy router dashboard")
    mapping = read_csv_required(MAPPING_PATH, "Strategy-regime mapping")
    micro = read_json_required(MICRO_JSON_PATH, "Microstructure dashboard")

    master = build_symbol_master_table(alignment, router, mapping)
    payload = build_json_payload(master, micro)
    text_dashboard = build_text_dashboard(master, micro)

    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "bacqe_master_operator_dashboard_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "bacqe_master_operator_dashboard_latest.parquet"

    txt_path = OUTPUT_REPORT_DIR / "bacqe_master_operator_dashboard_latest.txt"
    json_path = OUTPUT_REPORT_DIR / "bacqe_master_operator_dashboard_latest.json"

    master.to_csv(csv_path, index=False)
    master.to_parquet(parquet_path, index=False)

    txt_path.write_text(text_dashboard, encoding="utf-8")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] BACQE master operator dashboard created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"TXT:     {txt_path}")
    print(f"JSON:    {json_path}")
    print("-" * 140)
    print(text_dashboard)
    print("=" * 140)


if __name__ == "__main__":
    main()