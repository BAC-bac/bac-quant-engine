"""
BACQE REGIME ENGINE - 52 Build Strategy Router Dashboard

Creates a clean operator dashboard from the strategy environment router.

Input:
    E:/Quant_Lab/data/analysis/regimes/strategy_environment_router_latest.csv

Outputs:
    E:/Quant_Lab/reports/regimes/strategy_router_dashboard/strategy_router_dashboard_latest.txt
    E:/Quant_Lab/reports/regimes/strategy_router_dashboard/strategy_router_dashboard_latest.json
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
    / "strategy_environment_router_latest.csv"
)

OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "strategy_router_dashboard"
OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"


PRIORITY_RANK = {
    "high_priority_watchlist": 1,
    "medium_priority_watchlist": 2,
    "compression_watchlist": 3,
    "background_monitor": 4,
}


def load_router() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Strategy environment router file not found: {INPUT_PATH}")

    router = pd.read_csv(INPUT_PATH, low_memory=False)

    if router.empty:
        raise ValueError("Strategy environment router file is empty.")

    return router


def prepare_dashboard(router: pd.DataFrame) -> pd.DataFrame:
    dashboard = router.copy()

    dashboard["priority_rank"] = (
        dashboard["research_priority"]
        .map(PRIORITY_RANK)
        .fillna(99)
        .astype(int)
    )

    numeric_cols = [
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    for col in numeric_cols:
        dashboard[col] = pd.to_numeric(dashboard[col], errors="coerce")

    dashboard["dashboard_time_utc"] = datetime.now(timezone.utc).isoformat()

    dashboard = dashboard.sort_values(
        [
            "priority_rank",
            "directional_strength_score",
            "avg_regime_confidence",
        ],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return dashboard


def section_title(priority: str) -> str:
    title_map = {
        "high_priority_watchlist": "HIGH PRIORITY WATCHLIST",
        "medium_priority_watchlist": "MEDIUM PRIORITY WATCHLIST",
        "compression_watchlist": "COMPRESSION WATCHLIST",
        "background_monitor": "BACKGROUND MONITOR",
    }

    return title_map.get(priority, priority.upper())


def build_json_payload(dashboard: pd.DataFrame) -> dict:
    return {
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(INPUT_PATH),
        "symbols_total": len(dashboard),
        "priority_counts": dashboard["research_priority"].value_counts().to_dict(),
        "dashboard": dashboard.to_dict(orient="records"),
    }


def build_text_dashboard(dashboard: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "symbol",
        "primary_strategy_environment",
        "strategy_family",
        "directional_bias",
        "risk_mode",
        "alignment_label",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    lines = []

    lines.append("=" * 120)
    lines.append("BACQE STRATEGY ROUTER DASHBOARD")
    lines.append("=" * 120)
    lines.append(f"Dashboard time UTC: {now_utc}")
    lines.append(f"Input:              {INPUT_PATH}")
    lines.append("-" * 120)

    lines.append("")
    lines.append("EXECUTIVE SUMMARY")
    lines.append("-" * 120)

    priority_counts = dashboard["research_priority"].value_counts()

    for priority, count in priority_counts.items():
        lines.append(f"{priority:<30} {count}")

    lines.append("")
    lines.append("ROUTER SECTIONS")
    lines.append("-" * 120)

    for priority in PRIORITY_RANK.keys():
        section = dashboard[dashboard["research_priority"] == priority].copy()

        lines.append("")
        lines.append(section_title(priority))
        lines.append("-" * 120)

        if section.empty:
            lines.append("No symbols in this section.")
        else:
            lines.append(section[display_cols].to_string(index=False))

    unknown = dashboard[~dashboard["research_priority"].isin(PRIORITY_RANK.keys())].copy()

    if not unknown.empty:
        lines.append("")
        lines.append("OTHER / UNKNOWN PRIORITY")
        lines.append("-" * 120)
        lines.append(unknown[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 120)
    lines.append("This dashboard ranks strategy environments, not trade signals.")
    lines.append("High priority means structurally interesting enough for focused research monitoring.")
    lines.append("Compression watchlist means quiet/compressed conditions that may favour breakout-watch or mean-reversion research.")
    lines.append("Background monitor means the current structure is mixed, transitional, or low conviction.")
    lines.append("Risk mode is contextual guidance for research classification, not position-sizing advice.")
    lines.append("=" * 120)

    return "\n".join(lines)


def main() -> None:
    print("=" * 120)
    print("BACQE REGIME ENGINE - 52 BUILD STRATEGY ROUTER DASHBOARD")
    print("=" * 120)
    print(f"Input: {INPUT_PATH}")
    print("-" * 120)

    router = load_router()
    dashboard = prepare_dashboard(router)

    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "strategy_router_dashboard_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "strategy_router_dashboard_latest.parquet"

    txt_path = OUTPUT_REPORT_DIR / "strategy_router_dashboard_latest.txt"
    json_path = OUTPUT_REPORT_DIR / "strategy_router_dashboard_latest.json"

    dashboard.to_csv(csv_path, index=False)
    dashboard.to_parquet(parquet_path, index=False)

    payload = build_json_payload(dashboard)
    text_dashboard = build_text_dashboard(dashboard)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    txt_path.write_text(text_dashboard, encoding="utf-8")

    print("[DONE] Strategy router dashboard created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"TXT:     {txt_path}")
    print(f"JSON:    {json_path}")
    print("-" * 120)
    print(text_dashboard)
    print("=" * 120)


if __name__ == "__main__":
    main()