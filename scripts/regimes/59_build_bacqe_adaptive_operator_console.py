"""
BACQE REGIME ENGINE - 59 Build BACQE Adaptive Operator Console

Combines:
    - BACQE master operator dashboard
    - adaptive strategy opportunities
    - opportunity summary
    - GBPUSD microstructure snapshot via master dashboard

This is a research/operator console, not a trading signal engine.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

MASTER_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_master_operator_dashboard_latest.csv"
OPPORTUNITIES_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "adaptive_strategy_opportunities_latest.csv"
OPPORTUNITY_SUMMARY_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "adaptive_strategy_opportunity_summary_latest.csv"
MICRO_JSON_PATH = DATA_LAKE_ROOT / "reports" / "tick_research" / "live_state_dashboard" / "live_state_dashboard_latest.json"

OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_adaptive_operator"
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


def classify_adaptive_market_mode(master: pd.DataFrame, summary: pd.DataFrame) -> str:
    ready = (master["execution_posture"] == "research_ready_environment").sum()
    defensive = (master["execution_posture"] == "observation_or_defensive_only").sum()
    wait = (master["execution_posture"] == "wait_for_expansion_confirmation").sum()

    elite = summary["elite_count"].sum() if "elite_count" in summary.columns else 0
    strong = summary["strong_count"].sum() if "strong_count" in summary.columns else 0

    if ready >= 2 and elite >= 100:
        return "active_adaptive_research_environment"

    if ready >= 1 and strong >= 50:
        return "selective_adaptive_research_environment"

    if wait >= 1 and ready >= 1:
        return "mixed_ready_and_confirmation_environment"

    if defensive >= len(master) / 2:
        return "defensive_observation_environment"

    return "background_monitoring_environment"


def build_console_table(master: pd.DataFrame, opportunities: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    opp_best = (
        opportunities.sort_values(["symbol", "opportunity_score"], ascending=[True, False])
        .groupby("symbol", as_index=False)
        .first()
    )

    summary_cols = [
        "symbol",
        "candidates",
        "elite_count",
        "strong_count",
        "watch_count",
        "defensive_count",
        "best_opportunity_score",
        "avg_opportunity_score",
    ]

    summary_use = summary[[col for col in summary_cols if col in summary.columns]].copy()

    opp_cols = [
        "symbol",
        "candidate_strategy_name",
        "candidate_timeframe",
        "candidate_composite_regime",
        "opportunity_score",
        "opportunity_quality",
        "recommended_action",
        "evidence_quality",
    ]

    opp_use = opp_best[[col for col in opp_cols if col in opp_best.columns]].copy()

    console = master.merge(summary_use, on="symbol", how="left")
    console = console.merge(opp_use, on="symbol", how="left")

    posture_rank = {
        "research_ready_environment": 1,
        "selective_research_environment": 2,
        "wait_for_expansion_confirmation": 3,
        "observation_or_defensive_only": 4,
        "background_monitoring": 5,
    }

    console["console_rank"] = console["execution_posture"].map(posture_rank).fillna(99).astype(int)

    console = console.sort_values(
        ["console_rank", "best_opportunity_score", "directional_strength_score"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    console["console_time_utc"] = datetime.now(timezone.utc).isoformat()

    return console


def build_json_payload(console: pd.DataFrame, master: pd.DataFrame, opportunities: pd.DataFrame, summary: pd.DataFrame, micro: dict) -> dict:
    return {
        "console_time_utc": datetime.now(timezone.utc).isoformat(),
        "adaptive_market_mode": classify_adaptive_market_mode(master, summary),
        "source_files": {
            "master_operator_dashboard": str(MASTER_PATH),
            "adaptive_opportunities": str(OPPORTUNITIES_PATH),
            "adaptive_opportunity_summary": str(OPPORTUNITY_SUMMARY_PATH),
            "microstructure_dashboard": str(MICRO_JSON_PATH),
        },
        "console_table": console.to_dict(orient="records"),
        "opportunity_summary": summary.to_dict(orient="records"),
        "top_opportunities": opportunities.to_dict(orient="records"),
        "microstructure_dashboard": micro,
    }


def build_text_console(console: pd.DataFrame, master: pd.DataFrame, opportunities: pd.DataFrame, summary: pd.DataFrame, micro: dict) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()
    adaptive_mode = classify_adaptive_market_mode(master, summary)

    display_cols = [
        "symbol",
        "execution_posture",
        "research_priority",
        "primary_strategy_environment",
        "primary_strategy",
        "candidate_strategy_name",
        "candidate_timeframe",
        "candidate_composite_regime",
        "opportunity_score",
        "opportunity_quality",
        "recommended_action",
        "directional_bias",
        "risk_mode",
        "best_opportunity_score",
        "elite_count",
        "strong_count",
        "watch_count",
        "defensive_count",
    ]

    available_display_cols = [col for col in display_cols if col in console.columns]

    lines = []

    lines.append("=" * 150)
    lines.append("BACQE ADAPTIVE OPERATOR CONSOLE")
    lines.append("=" * 150)
    lines.append(f"Console time UTC:    {now_utc}")
    lines.append(f"Adaptive market mode:{adaptive_mode}")
    lines.append("-" * 150)

    lines.append("")
    lines.append("ADAPTIVE SYMBOL CONSOLE")
    lines.append("-" * 150)
    lines.append(console[available_display_cols].to_string(index=False))

    lines.append("")
    lines.append("RESEARCH-READY OPPORTUNITIES")
    lines.append("-" * 150)

    ready = console[console["execution_posture"] == "research_ready_environment"]
    if ready.empty:
        lines.append("No research-ready opportunities currently detected.")
    else:
        lines.append(ready[available_display_cols].to_string(index=False))

    lines.append("")
    lines.append("SELECTIVE / CONFIRMATION WATCH")
    lines.append("-" * 150)

    selective = console[
        console["execution_posture"].isin(
            ["selective_research_environment", "wait_for_expansion_confirmation"]
        )
    ]

    if selective.empty:
        lines.append("No selective or confirmation-watch environments currently detected.")
    else:
        lines.append(selective[available_display_cols].to_string(index=False))

    lines.append("")
    lines.append("DEFENSIVE / OBSERVATION ONLY")
    lines.append("-" * 150)

    defensive = console[console["execution_posture"] == "observation_or_defensive_only"]

    if defensive.empty:
        lines.append("No defensive observation-only environments currently detected.")
    else:
        lines.append(defensive[available_display_cols].to_string(index=False))

    lines.append("")
    lines.append("GBPUSD MICROSTRUCTURE SNAPSHOT")
    lines.append("-" * 150)
    lines.append(f"Microstructure regime:  {micro.get('microstructure_regime')}")
    lines.append(f"M15 composite regime:   {micro.get('m15_composite_regime')}")
    lines.append(f"Primary state:          {micro.get('primary_current_state')}")
    lines.append(f"Expected next state:    {micro.get('primary_expected_next_state')}")
    lines.append(f"Transition probability: {micro.get('primary_transition_probability')}")
    lines.append(f"Live bias:              {micro.get('primary_live_bias')}")
    lines.append(f"Actionability:          {micro.get('primary_actionability')}")

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 150)
    lines.append("This console combines market structure, adaptive strategy routing, historical performance evidence, and microstructure context.")
    lines.append("It ranks research opportunities, not trades.")
    lines.append("research_ready_environment means deeper research is justified, not automatic execution.")
    lines.append("wait_for_expansion_confirmation means structure is promising but participation/volatility confirmation is still required.")
    lines.append("observation_or_defensive_only means historical candidates may exist, but current market structure blocks activation.")
    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    print("=" * 150)
    print("BACQE REGIME ENGINE - 59 BUILD BACQE ADAPTIVE OPERATOR CONSOLE")
    print("=" * 150)

    master = read_csv_required(MASTER_PATH, "BACQE master operator dashboard")
    opportunities = read_csv_required(OPPORTUNITIES_PATH, "Adaptive strategy opportunities")
    summary = read_csv_required(OPPORTUNITY_SUMMARY_PATH, "Adaptive strategy opportunity summary")
    micro = read_json_required(MICRO_JSON_PATH, "Microstructure dashboard")

    console = build_console_table(master, opportunities, summary)
    payload = build_json_payload(console, master, opportunities, summary, micro)
    text_console = build_text_console(console, master, opportunities, summary, micro)

    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "bacqe_adaptive_operator_console_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "bacqe_adaptive_operator_console_latest.parquet"

    txt_path = OUTPUT_REPORT_DIR / "bacqe_adaptive_operator_console_latest.txt"
    json_path = OUTPUT_REPORT_DIR / "bacqe_adaptive_operator_console_latest.json"

    console.to_csv(csv_path, index=False)
    console.to_parquet(parquet_path, index=False)

    txt_path.write_text(text_console, encoding="utf-8")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] BACQE adaptive operator console created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"TXT:     {txt_path}")
    print(f"JSON:    {json_path}")
    print("-" * 150)
    print(text_console)
    print("=" * 150)


if __name__ == "__main__":
    main()