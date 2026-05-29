
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

TICK_COMPARISON_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "tick_vs_imbalance"
    / "tick_vs_imbalance_bar_comparison_latest.csv"
)

TICK_EFFICIENCY_MASTER_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_efficiency"
    / "_master"
    / "master_bar_efficiency_analysis_latest.csv"
)

TICK_WINNERS_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_efficiency"
    / "_master"
    / "symbol_winners_bar_efficiency_latest.csv"
)

VOL_CLUSTERING_MASTER_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "volatility_clustering"
    / "_master"
    / "master_volatility_clustering_analysis_latest.csv"
)

VOL_CLUSTERING_WINNERS_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "volatility_clustering"
    / "_master"
    / "symbol_winners_volatility_clustering_latest.csv"
)

BAR_ENTROPY_MASTER_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_entropy"
    / "_master"
    / "master_bar_entropy_analysis_latest.csv"
)

BAR_ENTROPY_WINNERS_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_entropy"
    / "_master"
    / "symbol_winners_bar_entropy_latest.csv"
)

TICK_CHART_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "tick_vs_imbalance_bars"
)


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


def show_dataframe(title: str, df: pd.DataFrame, empty_message: str) -> None:
    st.subheader(title)

    if df.empty:
        st.warning(empty_message)
    else:
        st.dataframe(df, use_container_width=True)


def format_number(value, decimals: int = 2):
    try:
        if pd.isna(value):
            return "n/a"

        return f"{float(value):,.{decimals}f}"
    except Exception:
        return value


state = read_csv(STATE_PATH)
alerts = read_csv(ALERTS_PATH)
health = read_csv(HEALTH_PATH)
selection = read_csv(SELECTION_PATH)
snapshot = read_json(SNAPSHOT_PATH)

tick_comparison = read_csv(TICK_COMPARISON_PATH)
tick_efficiency = read_csv(TICK_EFFICIENCY_MASTER_PATH)
tick_winners = read_csv(TICK_WINNERS_PATH)

vol_clustering = read_csv(VOL_CLUSTERING_MASTER_PATH)
vol_winners = read_csv(VOL_CLUSTERING_WINNERS_PATH)

bar_entropy = read_csv(BAR_ENTROPY_MASTER_PATH)
entropy_winners = read_csv(BAR_ENTROPY_WINNERS_PATH)

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

tab_operator, tab_tick_research = st.tabs(
    [
        "BACQE Operator",
        "Tick Research / Microstructure",
    ]
)


with tab_operator:
    show_dataframe(
        "Current BACQE State Registry",
        state,
        "State registry not found.",
    )

    show_dataframe(
        "BACQE Alerts",
        alerts,
        "No alerts found.",
    )

    show_dataframe(
        "Adaptive Strategy Selection",
        selection,
        "Selection dashboard not found.",
    )

    show_dataframe(
        "Operational Health",
        health,
        "Health monitor not found.",
    )


with tab_tick_research:
    st.header("BACQE Tick Research / Microstructure")

    if tick_comparison.empty and tick_efficiency.empty and tick_winners.empty:
        st.warning("No tick research outputs found yet.")
    else:
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

        with metric_col1:
            if not tick_comparison.empty and "symbol" in tick_comparison.columns:
                st.metric("Tick Symbols", tick_comparison["symbol"].nunique())
            else:
                st.metric("Tick Symbols", "unknown")

        with metric_col2:
            if not tick_comparison.empty:
                st.metric("Comparison Rows", len(tick_comparison))
            else:
                st.metric("Comparison Rows", "unknown")

        with metric_col3:
            if not tick_winners.empty:
                st.metric("Symbol Winners", len(tick_winners))
            else:
                st.metric("Symbol Winners", "unknown")

        with metric_col4:
            if not tick_comparison.empty and "bar_type" in tick_comparison.columns:
                st.metric("Bar Types", tick_comparison["bar_type"].nunique())
            else:
                st.metric("Bar Types", "unknown")

        st.divider()

        st.subheader("Best Bar Type Per Symbol")

        if tick_winners.empty:
            st.warning("Symbol winner summary not found.")
        else:
            winner_cols = [
                "symbol",
                "bar_type",
                "bar_family",
                "bar_count",
                "avg_tick_count",
                "avg_duration_seconds",
                "return_std",
                "return_kurtosis",
                "lag1_return_autocorr",
                "structural_efficiency_score",
            ]

            available_winner_cols = [
                col for col in winner_cols if col in tick_winners.columns
            ]

            st.dataframe(
                tick_winners[available_winner_cols],
                use_container_width=True,
            )

            if {
                "symbol",
                "structural_efficiency_score",
            }.issubset(tick_winners.columns):
                st.bar_chart(
                    tick_winners.set_index("symbol")["structural_efficiency_score"]
                )
                
                st.divider()

        st.subheader("Volatility Clustering Winners")

        if vol_winners.empty:
            st.warning("Volatility clustering winner summary not found.")
        else:
            vol_cols = [
                "symbol",
                "bar_type",
                "bar_family",
                "bar_count",
                "return_std",
                "abs_return_autocorr_lag1",
                "squared_return_autocorr_lag1",
                "rolling_vol_25_autocorr_lag1",
                "rolling_abs_25_autocorr_lag1",
                "volatility_clustering_score",
            ]

            available_vol_cols = [
                col for col in vol_cols if col in vol_winners.columns
            ]

            st.dataframe(
                vol_winners[available_vol_cols],
                use_container_width=True,
            )

            if {
                "symbol",
                "volatility_clustering_score",
            }.issubset(vol_winners.columns):
                st.caption("Strongest volatility clustering score by symbol")
                st.bar_chart(
                    vol_winners.set_index("symbol")["volatility_clustering_score"]
                )
                
                st.divider()

        st.subheader("Entropy Structure Winners")

        if entropy_winners.empty:
            st.warning("Entropy winner summary not found.")
        else:
            entropy_cols = [
                "symbol",
                "bar_type",
                "bar_family",
                "bar_count",
                "direction_entropy_normalized",
                "return_sign_entropy_normalized",
                "direction_transition_entropy",
                "return_sign_transition_entropy",
                "return_sign_same_direction_pct",
                "entropy_structure_score",
            ]

            available_entropy_cols = [
                col for col in entropy_cols if col in entropy_winners.columns
            ]

            st.dataframe(
                entropy_winners[available_entropy_cols],
                use_container_width=True,
            )

            if {
                "symbol",
                "entropy_structure_score",
            }.issubset(entropy_winners.columns):
                st.caption("Highest entropy structure score by symbol")
                st.bar_chart(
                    entropy_winners.set_index("symbol")["entropy_structure_score"]
                )

        st.divider()

        st.subheader("Tick vs Imbalance Comparison")

        if tick_comparison.empty:
            st.warning("Tick vs imbalance comparison file not found.")
        else:
            symbols = sorted(tick_comparison["symbol"].dropna().unique().tolist())

            selected_symbol = st.selectbox(
                "Select symbol",
                symbols,
                index=0,
            )

            symbol_comparison = tick_comparison[
                tick_comparison["symbol"] == selected_symbol
            ].copy()

            st.dataframe(symbol_comparison, use_container_width=True)

            chart_cols = st.columns(2)

            with chart_cols[0]:
                if {"bar_type", "return_std"}.issubset(symbol_comparison.columns):
                    st.caption("Return standard deviation by bar type")
                    st.bar_chart(
                        symbol_comparison.set_index("bar_type")["return_std"]
                    )

            with chart_cols[1]:
                if {"bar_type", "avg_duration_seconds"}.issubset(symbol_comparison.columns):
                    st.caption("Average duration by bar type")
                    st.bar_chart(
                        symbol_comparison.set_index("bar_type")["avg_duration_seconds"]
                    )

            st.divider()

            st.subheader("Tick Research Charts")

            chart_symbol_dir = TICK_CHART_ROOT / f"symbol={selected_symbol}"

            chart_options = {
                "Return volatility": "01_return_volatility.png",
                "Return kurtosis": "02_return_kurtosis.png",
                "Average duration": "03_average_duration.png",
                "Median duration": "04_median_duration.png",
                "Average tick count": "05_average_tick_count.png",
                "Median tick count": "06_median_tick_count.png",
                "Lag-1 autocorrelation": "07_lag1_autocorrelation.png",
                "Average imbalance ratio": "08_average_imbalance_ratio.png",
                "Positive imbalance percentage": "09_positive_imbalance_percentage.png",
                "Negative imbalance percentage": "10_negative_imbalance_percentage.png",
            }

            selected_chart = st.selectbox(
                "Select chart",
                list(chart_options.keys()),
                index=0,
            )

            chart_path = chart_symbol_dir / chart_options[selected_chart]

            if chart_path.exists():
                st.image(str(chart_path), use_container_width=True)
            else:
                st.warning(f"Chart not found: {chart_path}")

        st.divider()

        st.subheader("Master Bar Efficiency Ranking")

        if tick_efficiency.empty:
            st.warning("Master bar efficiency analysis not found.")
        else:
            st.dataframe(tick_efficiency, use_container_width=True)

            if {
                "symbol",
                "bar_type",
                "structural_efficiency_score",
            }.issubset(tick_efficiency.columns):
                top_efficiency = (
                    tick_efficiency
                    .sort_values("structural_efficiency_score", ascending=False)
                    .head(15)
                    .copy()
                )

                top_efficiency["label"] = (
                    top_efficiency["symbol"].astype(str)
                    + " | "
                    + top_efficiency["bar_type"].astype(str)
                )

                st.caption("Top 15 structural efficiency scores")
                st.bar_chart(
                    top_efficiency.set_index("label")["structural_efficiency_score"]
                )
                
                st.divider()

        st.subheader("Master Volatility Clustering Ranking")

        if vol_clustering.empty:
            st.warning("Master volatility clustering analysis not found.")
        else:
            st.dataframe(vol_clustering, use_container_width=True)

            if {
                "symbol",
                "bar_type",
                "volatility_clustering_score",
            }.issubset(vol_clustering.columns):
                top_vol = (
                    vol_clustering
                    .sort_values("volatility_clustering_score", ascending=False)
                    .head(15)
                    .copy()
                )

                top_vol["label"] = (
                    top_vol["symbol"].astype(str)
                    + " | "
                    + top_vol["bar_type"].astype(str)
                )

                st.caption("Top 15 volatility clustering scores")
                st.bar_chart(
                    top_vol.set_index("label")["volatility_clustering_score"]
                )
                
                st.divider()

        st.subheader("Master Entropy Structure Ranking")

        if bar_entropy.empty:
            st.warning("Master entropy analysis not found.")
        else:
            st.dataframe(bar_entropy, use_container_width=True)

            if {
                "symbol",
                "bar_type",
                "entropy_structure_score",
            }.issubset(bar_entropy.columns):
                top_entropy = (
                    bar_entropy
                    .sort_values("entropy_structure_score", ascending=False)
                    .head(15)
                    .copy()
                )

                top_entropy["label"] = (
                    top_entropy["symbol"].astype(str)
                    + " | "
                    + top_entropy["bar_type"].astype(str)
                )

                st.caption("Top 15 entropy structure scores")
                st.bar_chart(
                    top_entropy.set_index("label")["entropy_structure_score"]
                )

        st.divider()

        st.subheader("Master Cross-Symbol Charts")

        master_chart_options = {
            "Imbalance 25 bar count": "01_cross_symbol_imbalance_25_bar_count.png",
            "Imbalance 25 average duration": "02_cross_symbol_imbalance_25_average_duration.png",
            "Imbalance 25 return volatility": "03_cross_symbol_imbalance_25_return_volatility.png",
            "Imbalance 200 positive imbalance percentage": "04_cross_symbol_imbalance_200_positive_imbalance_pct.png",
        }

        selected_master_chart = st.selectbox(
            "Select master chart",
            list(master_chart_options.keys()),
            index=0,
        )

        master_chart_path = (
            TICK_CHART_ROOT
            / "_master"
            / master_chart_options[selected_master_chart]
        )

        if master_chart_path.exists():
            st.image(str(master_chart_path), use_container_width=True)
        else:
            st.warning(f"Master chart not found: {master_chart_path}")


st.caption("BACQE Terminal is a research/operator dashboard, not a trading signal engine.")
