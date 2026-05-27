"""
BACQE REGIME ENGINE - 66 Build Streamlit BACQE Terminal

Creates a Streamlit dashboard app file for the BACQE live state, alerts,
selection dashboard, and health monitor.
"""

from pathlib import Path


PROJECT_ROOT = Path(r"C:\Users\benco\PycharmProjects\BAC_Quant_Engine")

APP_DIR = PROJECT_ROOT / "apps"
APP_PATH = APP_DIR / "bacqe_streamlit_terminal.py"


APP_CODE = r'''
import json
from pathlib import Path

import pandas as pd
import streamlit as st


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

STATE_PATH = DATA_LAKE_ROOT / "data" / "state" / "bacqe_state_registry_latest.csv"
ALERTS_PATH = DATA_LAKE_ROOT / "data" / "alerts" / "bacqe_alerts_latest.csv"
HEALTH_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_live_status_health_latest.csv"
SELECTION_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "adaptive_strategy_selection_dashboard_latest.csv"
SNAPSHOT_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_live_status_snapshot_latest.json"


st.set_page_config(
    page_title="BACQE Adaptive Terminal",
    layout="wide",
)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


state = read_csv(STATE_PATH)
alerts = read_csv(ALERTS_PATH)
health = read_csv(HEALTH_PATH)
selection = read_csv(SELECTION_PATH)
snapshot = read_json(SNAPSHOT_PATH)


st.title("BACQE Adaptive Operator Terminal")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Adaptive Market Mode", snapshot.get("adaptive_market_mode", "unknown"))

with col2:
    st.metric("Priority Research", len(snapshot.get("priority_research", [])))

with col3:
    st.metric("Expansion Confirmation", len(snapshot.get("expansion_confirmation", [])))

with col4:
    if not health.empty and "freshness_status" in health.columns:
        unhealthy = health[health["freshness_status"] != "fresh"].shape[0]
        st.metric("Non-Fresh Outputs", unhealthy)
    else:
        st.metric("Non-Fresh Outputs", "unknown")


st.divider()

st.subheader("Current BACQE State Registry")
if state.empty:
    st.warning("State registry not found.")
else:
    st.dataframe(state, use_container_width=True)

st.subheader("BACQE Alerts")
if alerts.empty:
    st.info("No alerts found.")
else:
    st.dataframe(alerts, use_container_width=True)

st.subheader("Adaptive Strategy Selection")
if selection.empty:
    st.warning("Selection dashboard not found.")
else:
    st.dataframe(selection, use_container_width=True)

st.subheader("Operational Health")
if health.empty:
    st.warning("Health monitor not found.")
else:
    st.dataframe(health, use_container_width=True)

st.caption("BACQE Terminal is a research/operator dashboard, not a trading signal engine.")
'''


def main() -> None:
    print("=" * 120)
    print("BACQE REGIME ENGINE - 66 BUILD STREAMLIT BACQE TERMINAL")
    print("=" * 120)

    APP_DIR.mkdir(parents=True, exist_ok=True)
    APP_PATH.write_text(APP_CODE, encoding="utf-8")

    print("[DONE] Streamlit BACQE terminal created.")
    print(f"App path: {APP_PATH}")
    print("-" * 120)
    print("Run with:")
    print(f"streamlit run {APP_PATH}")
    print("=" * 120)


if __name__ == "__main__":
    main()