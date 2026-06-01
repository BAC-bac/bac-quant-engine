"""
BACQE TICK RESEARCH - 32 Build Live State Dashboard

Creates a readable dashboard-style report from the latest current
microstructure state score.

Inputs:
    E:/Quant_Lab/data/analysis/tick_research/current_microstructure_state_score_latest.csv

Outputs:
    E:/Quant_Lab/reports/tick_research/live_state_dashboard/live_state_dashboard_latest.txt
    E:/Quant_Lab/reports/tick_research/live_state_dashboard/live_state_dashboard_latest.json
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
    / "tick_research"
    / "current_state"
    / "_master"
    / "master_current_microstructure_state_score_latest.csv"
)

OUTPUT_REPORT_DIR = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "live_state_dashboard"
)


def load_current_state() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Current state score file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if df.empty:
        raise ValueError("Current state score file is empty.")

    return df


def select_primary_states(df: pd.DataFrame) -> pd.DataFrame:
    priority = {
        "microstructure_regime": 1,
        "compact_multi_layer_state": 2,
        "trend_micro_state": 3,
        "vol_micro_state": 4,
        "momentum_micro_state": 5,
        "trend_strength_micro_state": 6,
        "multi_layer_state": 7,
    }

    data = df.copy()
    data["priority"] = data["state_column"].map(priority).fillna(999)

    watchlist_rank = {
        "research_watchlist_high_confidence": 1,
        "research_watchlist": 2,
        "diagnostic_only": 3,
        "ignore_unknown_state": 4,
        "too_early": 5,
    }

    data["actionability_rank"] = data["actionability"].map(watchlist_rank).fillna(999)

    return (
        data.sort_values(
            ["symbol", "actionability_rank", "priority"],
            ascending=[True, True, True],
        )
        .groupby("symbol", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )


def build_dashboard_payload(df: pd.DataFrame) -> dict:
    primary_states = select_primary_states(df)

    watchlist = df[df["actionability"].isin(["research_watchlist", "research_watchlist_high_confidence", ])].copy()

    actionability_rank = {"research_watchlist_high_confidence": 1, "research_watchlist": 2, }

    forecast_quality_rank = {"high_confidence": 1, "stronger": 2, "usable": 3, "weak": 4, "low_sample": 5, }

    watchlist["actionability_rank"] = (watchlist["actionability"].map(actionability_rank).fillna(999))

    watchlist["forecast_quality_rank"] = (watchlist["forecast_quality"].map(forecast_quality_rank).fillna(999))

    watchlist["abs_persistence_edge"] = (pd.to_numeric(watchlist["expected_persistence_edge"], errors="coerce", ).abs())

    if not watchlist.empty:
        watchlist = watchlist.sort_values(["actionability_rank", "forecast_quality_rank", "abs_persistence_edge",
            "most_likely_transition_probability", ], ascending=[True, True, False, False], ).drop(
            columns=["actionability_rank", "forecast_quality_rank", "abs_persistence_edge", ], errors="ignore", )

    dashboard = {
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(INPUT_PATH),
        "symbols": sorted(df["symbol"].dropna().astype(str).unique().tolist()),
        "symbol_count": int(df["symbol"].nunique()),
        "row_count": len(df),
        "primary_states": primary_states.to_dict(orient="records"),
        "watchlist_states": watchlist.to_dict(orient="records"),
        "all_state_scores": df.to_dict(orient="records"),
    }

    return dashboard


def format_pct(value) -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "n/a"


def format_num(value, digits: int = 6) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def build_text_dashboard(payload: dict) -> str:
    lines = []

    symbols = payload.get("symbols", [])
    symbol_count = payload.get("symbol_count", len(symbols))
    primary_states = payload.get("primary_states", [])
    watchlist_states = payload.get("watchlist_states", [])
    all_state_scores = payload.get("all_state_scores", [])

    lines.append("=" * 100)
    lines.append("BACQE LIVE MICROSTRUCTURE STATE DASHBOARD")
    lines.append("=" * 100)
    lines.append(f"Dashboard time UTC:     {payload.get('dashboard_time_utc')}")
    lines.append(f"Symbols monitored:      {symbol_count}")
    lines.append(f"Symbols:                {', '.join(symbols)}")
    lines.append(f"Total state scores:     {payload.get('row_count')}")
    lines.append("-" * 100)

    lines.append("")
    lines.append("PRIMARY STATE BY SYMBOL")
    lines.append("-" * 100)

    if not primary_states:
        lines.append("No primary states available.")
    else:
        for row in primary_states:
            lines.append(
                f"{row.get('symbol')} | "
                f"bar={row.get('bar_type')} | "
                f"lens={row.get('state_column')} | "
                f"state={row.get('current_state')} | "
                f"next={row.get('most_likely_next_state')} | "
                f"transition={format_pct(float(row.get('most_likely_transition_probability', 0)) * 100)} | "
                f"edge={format_num(row.get('expected_persistence_edge'), 4)} | "
                f"quality={row.get('forecast_quality')} | "
                f"bias={row.get('live_bias')} | "
                f"action={row.get('actionability')}"
            )

    lines.append("")
    lines.append("RESEARCH WATCHLIST STATES")
    lines.append("-" * 100)

    if not watchlist_states:
        lines.append("No current states met research watchlist criteria.")
    else:
        for row in watchlist_states:
            lines.append(
                f"{row.get('symbol')} | "
                f"bar={row.get('bar_type')} | "
                f"lens={row.get('state_column')} | "
                f"state={row.get('current_state')} | "
                f"next={row.get('most_likely_next_state')} | "
                f"transition={format_pct(float(row.get('most_likely_transition_probability', 0)) * 100)} | "
                f"self={format_pct(float(row.get('self_transition_probability', 0)) * 100)} | "
                f"edge={format_num(row.get('expected_persistence_edge'), 4)} | "
                f"quality={row.get('forecast_quality')} | "
                f"bias={row.get('live_bias')} | "
                f"action={row.get('actionability')}"
            )

    lines.append("")
    lines.append("ALL STATE LENSES")
    lines.append("-" * 100)

    for row in all_state_scores:
        lines.append(
            f"{row.get('symbol')} | "
            f"{row.get('state_column')} | "
            f"bar={row.get('bar_type')} | "
            f"state={row.get('current_state')} | "
            f"next={row.get('most_likely_next_state')} | "
            f"transition={format_pct(float(row.get('most_likely_transition_probability', 0)) * 100)} | "
            f"edge={format_num(row.get('expected_persistence_edge'), 4)} | "
            f"quality={row.get('forecast_quality')} | "
            f"bias={row.get('live_bias')} | "
            f"action={row.get('actionability')}"
        )

    lines.append("")
    lines.append("INTERPRETATION")
    lines.append("-" * 100)
    lines.append("This dashboard is a research intelligence surface, not a trading instruction.")
    lines.append("research_watchlist means the state is statistically interesting enough to monitor.")
    lines.append("ignore_unknown_state means the state was retained for diagnostics but should not be treated as actionable.")
    lines.append("The dashboard becomes more valuable as the tick dataset grows.")
    lines.append("=" * 100)

    return "\n".join(lines)


def main() -> None:
    print("=" * 100)
    print("BACQE TICK RESEARCH - 32 BUILD LIVE STATE DASHBOARD")
    print("=" * 100)
    print(f"Input: {INPUT_PATH}")
    print("-" * 100)

    df = load_current_state()
    payload = build_dashboard_payload(df)
    text_dashboard = build_text_dashboard(payload)

    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    txt_path = OUTPUT_REPORT_DIR / "live_state_dashboard_latest.txt"
    json_path = OUTPUT_REPORT_DIR / "live_state_dashboard_latest.json"

    txt_path.write_text(text_dashboard, encoding="utf-8")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] Live state dashboard created.")
    print(f"TXT:  {txt_path}")
    print(f"JSON: {json_path}")
    print("-" * 100)
    print(text_dashboard)
    print("=" * 100)


if __name__ == "__main__":
    main()