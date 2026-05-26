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
    / "current_microstructure_state_score_latest.csv"
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


def select_primary_state(df: pd.DataFrame) -> pd.Series:
    priority = {
        "compact_multi_layer_state": 1,
        "trend_micro_state": 2,
        "vol_micro_state": 3,
        "microstructure_regime": 4,
        "m15_composite_regime": 5,
    }

    data = df.copy()
    data["priority"] = data["state_column"].map(priority).fillna(999)

    data = data.sort_values(
        ["actionability", "forecast_quality", "priority"],
        ascending=[True, True, True],
    )

    return data.iloc[0]


def build_dashboard_payload(df: pd.DataFrame) -> dict:
    primary = select_primary_state(df)

    first = df.iloc[0]

    dashboard = {
        "dashboard_time_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(INPUT_PATH),
        "symbol": first.get("symbol"),
        "latest_bar_start_time": str(first.get("latest_bar_start_time")),
        "latest_bar_end_time": str(first.get("latest_bar_end_time")),
        "bar_type": first.get("bar_type"),
        "bar_family": first.get("bar_family"),
        "microstructure_regime": first.get("microstructure_regime"),
        "m15_composite_regime": first.get("m15_composite_regime"),
        "m15_trend_state": first.get("m15_trend_state"),
        "m15_volatility_state": first.get("m15_volatility_state"),
        "m15_momentum_state": first.get("m15_momentum_state"),
        "latest_return": first.get("latest_return"),
        "latest_abs_return": first.get("latest_abs_return"),
        "latest_duration_seconds": first.get("latest_duration_seconds"),
        "latest_tick_count": first.get("latest_tick_count"),
        "latest_ticks_per_second": first.get("latest_ticks_per_second"),
        "latest_m15_regime_confidence": first.get("latest_m15_regime_confidence"),
        "primary_state_column": primary.get("state_column"),
        "primary_current_state": primary.get("current_state"),
        "primary_expected_next_state": primary.get("most_likely_next_state"),
        "primary_transition_probability": primary.get("most_likely_transition_probability"),
        "primary_self_transition_probability": primary.get("self_transition_probability"),
        "primary_state_stability_label": primary.get("state_stability_label"),
        "primary_expected_persist_pct": primary.get("expected_persist_pct"),
        "primary_expected_flip_pct": primary.get("expected_flip_pct"),
        "primary_expected_persistence_edge": primary.get("expected_persistence_edge"),
        "primary_expected_behaviour_label": primary.get("expected_behaviour_label"),
        "primary_forecast_quality": primary.get("forecast_quality"),
        "primary_live_bias": primary.get("live_bias"),
        "primary_actionability": primary.get("actionability"),
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

    lines.append("=" * 100)
    lines.append("BACQE LIVE MICROSTRUCTURE STATE DASHBOARD")
    lines.append("=" * 100)
    lines.append(f"Dashboard time UTC:     {payload.get('dashboard_time_utc')}")
    lines.append(f"Symbol:                 {payload.get('symbol')}")
    lines.append(f"Latest bar start:       {payload.get('latest_bar_start_time')}")
    lines.append(f"Latest bar end:         {payload.get('latest_bar_end_time')}")
    lines.append("-" * 100)

    lines.append("")
    lines.append("CURRENT MARKET STATE")
    lines.append("-" * 100)
    lines.append(f"Bar type:               {payload.get('bar_type')}")
    lines.append(f"Bar family:             {payload.get('bar_family')}")
    lines.append(f"Microstructure regime:  {payload.get('microstructure_regime')}")
    lines.append(f"M15 composite regime:   {payload.get('m15_composite_regime')}")
    lines.append(f"M15 trend state:        {payload.get('m15_trend_state')}")
    lines.append(f"M15 volatility state:   {payload.get('m15_volatility_state')}")
    lines.append(f"M15 momentum state:     {payload.get('m15_momentum_state')}")
    lines.append(f"M15 confidence:         {format_num(payload.get('latest_m15_regime_confidence'), 4)}")

    lines.append("")
    lines.append("LATEST BAR METRICS")
    lines.append("-" * 100)
    lines.append(f"Latest return:          {format_num(payload.get('latest_return'), 8)}")
    lines.append(f"Latest absolute return: {format_num(payload.get('latest_abs_return'), 8)}")
    lines.append(f"Duration seconds:       {format_num(payload.get('latest_duration_seconds'), 2)}")
    lines.append(f"Tick count:             {format_num(payload.get('latest_tick_count'), 2)}")
    lines.append(f"Ticks per second:       {format_num(payload.get('latest_ticks_per_second'), 6)}")

    lines.append("")
    lines.append("PRIMARY STATE FORECAST")
    lines.append("-" * 100)
    lines.append(f"State lens:             {payload.get('primary_state_column')}")
    lines.append(f"Current state:          {payload.get('primary_current_state')}")
    lines.append(f"Expected next state:    {payload.get('primary_expected_next_state')}")
    lines.append(f"Transition probability: {format_pct(float(payload.get('primary_transition_probability', 0)) * 100)}")
    lines.append(f"Self-transition prob:   {format_pct(float(payload.get('primary_self_transition_probability', 0)) * 100)}")
    lines.append(f"State stability:        {payload.get('primary_state_stability_label')}")
    lines.append(f"Expected persist pct:   {format_pct(payload.get('primary_expected_persist_pct'))}")
    lines.append(f"Expected flip pct:      {format_pct(payload.get('primary_expected_flip_pct'))}")
    lines.append(f"Persistence edge:       {format_num(payload.get('primary_expected_persistence_edge'), 4)}")
    lines.append(f"Expected behaviour:     {payload.get('primary_expected_behaviour_label')}")
    lines.append(f"Forecast quality:       {payload.get('primary_forecast_quality')}")
    lines.append(f"Live bias:              {payload.get('primary_live_bias')}")
    lines.append(f"Actionability:          {payload.get('primary_actionability')}")

    lines.append("")
    lines.append("ALL STATE LENSES")
    lines.append("-" * 100)

    for row in payload.get("all_state_scores", []):
        lines.append(
            f"{row.get('state_column')} | "
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
    lines.append("diagnostic_only means useful context, but not enough evidence for signal research yet.")
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