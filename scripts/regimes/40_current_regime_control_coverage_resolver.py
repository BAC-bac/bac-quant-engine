"""
BACQE Script 40
Current Regime Control Coverage Resolver

Purpose:
- Resolve missing control rows from Script 39 current dashboard
- Fill missing controls using:
  1. exact existing control match
  2. regime-family fallback from Script 37 family risk
  3. conservative default fallback
- Produce a complete current dashboard with no blank control recommendations

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

DASHBOARD_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_current_strategy_dashboard"
RISK_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_risk_intelligence"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_current_strategy_dashboard_resolved"

CURRENT_DASHBOARD = DASHBOARD_DIR / "current_regime_strategy_control_dashboard_latest.csv"
FAMILY_RISK = RISK_DIR / "regime_family_risk_latest.csv"


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def regime_family(regime: str) -> str:
    r = str(regime).lower()

    if "volatile_transition" in r:
        return "volatile_transition"
    if "volatile_range" in r:
        return "volatile_range"
    if "transition" in r:
        return "transition"
    if "range" in r:
        return "range"
    if "bull_trend_high_vol" in r:
        return "bull_trend_high_vol"
    if "bear_trend_high_vol" in r:
        return "bear_trend_high_vol"
    if "bull_trend_normal_vol" in r:
        return "bull_trend_normal_vol"
    if "bear_trend_normal_vol" in r:
        return "bear_trend_normal_vol"
    if "bull_trend_low_vol" in r:
        return "bull_trend_low_vol"
    if "bear_trend_low_vol" in r:
        return "bear_trend_low_vol"
    if "bull" in r:
        return "bull_other"
    if "bear" in r:
        return "bear_other"

    return "other"


def classify_timeframe_group(timeframe: str) -> str:
    tf = str(timeframe).upper()

    if tf in {"M1", "M2", "M3", "M5", "M10", "M15", "M30"}:
        return "tactical_intraday"
    if tf in {"H1", "H2", "H3", "H4", "H8", "H12"}:
        return "intraday_swing"
    if tf in {"D1", "W1"}:
        return "position"
    if tf in {"MN1"}:
        return "long_horizon"

    return "unknown"


def dashboard_status(permission: str, risk_band: str) -> str:
    permission = str(permission).lower()
    risk_band = str(risk_band).lower()

    if permission == "avoid_or_convex_only" or risk_band == "extreme":
        return "RED"
    if permission == "restricted" or risk_band == "high":
        return "AMBER"
    if permission in {"allowed", "selective"}:
        return "GREEN"

    return "UNKNOWN"


def default_control_for_family(risk_band: str, family: str) -> dict:
    band = str(risk_band).lower()
    family = str(family).lower()

    if band == "low":
        if "range" in family:
            return {
                "trade_permission": "allowed",
                "risk_multiplier": 1.00,
                "leverage_multiplier": 1.00,
                "convexity_bias": "neutral_to_short_convexity",
                "execution_mode": "normal",
                "allowed_strategy_families": "mean_reversion|range_fade|validated_core_strategies",
                "blocked_strategy_families": "aggressive_breakout|tail_chase|unvalidated_high_turnover",
                "stop_loss_profile": "standard_atr_stop",
                "trailing_stop_profile": "optional_after_profit",
                "control_commentary": "Family fallback: stable range-type regime. Standard validated mean-reversion may be allowed.",
            }

        return {
            "trade_permission": "allowed",
            "risk_multiplier": 1.00,
            "leverage_multiplier": 1.00,
            "convexity_bias": "long_convexity_optional",
            "execution_mode": "normal",
            "allowed_strategy_families": "trend_following|pullback_continuation|validated_core_strategies",
            "blocked_strategy_families": "aggressive_countertrend|unvalidated_high_turnover",
            "stop_loss_profile": "standard_or_trend_atr_stop",
            "trailing_stop_profile": "optional_or_trend_trailing",
            "control_commentary": "Family fallback: stable trend/core regime. Standard validated strategy logic may be allowed.",
        }

    if band == "medium":
        return {
            "trade_permission": "selective",
            "risk_multiplier": 0.75,
            "leverage_multiplier": 0.75,
            "convexity_bias": "mild_long_convexity",
            "execution_mode": "confirmation_required",
            "allowed_strategy_families": "validated_core_strategies|confirmed_trend|confirmed_mean_reversion",
            "blocked_strategy_families": "high_leverage_scalping|fragile_grid|unconfirmed_countertrend",
            "stop_loss_profile": "tighter_atr_stop",
            "trailing_stop_profile": "recommended_after_profit",
            "control_commentary": "Family fallback: medium risk. Trade selectively with confirmation and reduced exposure.",
        }

    if band == "high":
        if "volatile" in family or "transition" in family:
            return {
                "trade_permission": "restricted",
                "risk_multiplier": 0.40,
                "leverage_multiplier": 0.40,
                "convexity_bias": "long_convexity_preferred",
                "execution_mode": "defensive",
                "allowed_strategy_families": "convex_breakout|volatility_expansion|small_probe_trend",
                "blocked_strategy_families": "fragile_mean_reversion|grid|martingale|large_countertrend|tight_stop_scalping",
                "stop_loss_profile": "strict_atr_or_structural_stop",
                "trailing_stop_profile": "required_on_profit",
                "control_commentary": "Family fallback: high-risk transition/volatile regime. Defensive or convex logic preferred.",
            }

        return {
            "trade_permission": "selective",
            "risk_multiplier": 0.55,
            "leverage_multiplier": 0.55,
            "convexity_bias": "mild_long_convexity",
            "execution_mode": "defensive_confirmation_required",
            "allowed_strategy_families": "confirmed_trend|reduced_size_pullback|validated_core_strategies",
            "blocked_strategy_families": "large_countertrend|fragile_grid|unconfirmed_scalping",
            "stop_loss_profile": "strict_atr_stop",
            "trailing_stop_profile": "recommended",
            "control_commentary": "Family fallback: high-risk non-transition regime. Use reduced exposure and stronger confirmation.",
        }

    if band == "extreme":
        return {
            "trade_permission": "avoid_or_convex_only",
            "risk_multiplier": 0.20,
            "leverage_multiplier": 0.20,
            "convexity_bias": "strong_long_convexity_only",
            "execution_mode": "capital_preservation",
            "allowed_strategy_families": "convex_tail|volatility_breakout_probe|event_risk_probe",
            "blocked_strategy_families": "mean_reversion|grid|martingale|large_directional|fragile_scalping|countertrend",
            "stop_loss_profile": "hard_stop_required",
            "trailing_stop_profile": "required",
            "control_commentary": "Family fallback: extreme risk. Avoid normal trading unless using predefined-risk convex logic.",
        }

    return conservative_default_control()


def conservative_default_control() -> dict:
    return {
        "trade_permission": "restricted",
        "risk_multiplier": 0.35,
        "leverage_multiplier": 0.35,
        "convexity_bias": "long_convexity_preferred",
        "execution_mode": "conservative_default",
        "allowed_strategy_families": "validated_low_risk_only|small_probe_only",
        "blocked_strategy_families": "grid|martingale|large_directional|fragile_scalping|unvalidated_strategies",
        "stop_loss_profile": "hard_stop_required",
        "trailing_stop_profile": "recommended_or_required",
        "control_commentary": "Conservative default: no exact or family-level control available. Use restricted exposure.",
    }


def prepare_family_lookup(family_risk: pd.DataFrame) -> pd.DataFrame:
    df = family_risk.copy()

    df["timeframe_group"] = df["timeframe_group"].astype(str)
    df["regime_family"] = df["regime_family"].astype(str)

    for col in [
        "family_risk_score",
        "avg_leave_probability",
        "avg_segment_bars",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    keep_cols = [
        "broker",
        "timeframe_group",
        "regime_family",
        "family_risk_score",
        "family_risk_band",
        "family_strategy_posture",
        "avg_leave_probability",
        "avg_segment_bars",
    ]

    return df[[c for c in keep_cols if c in df.columns]].copy()


def resolve_controls(dashboard: pd.DataFrame, family_lookup: pd.DataFrame) -> pd.DataFrame:
    df = dashboard.copy()

    if "symbol_current" not in df.columns and "symbol" in df.columns:
        df["symbol_current"] = df["symbol"]

    if "timeframe_group" not in df.columns:
        df["timeframe_group"] = df["timeframe"].apply(classify_timeframe_group)
    else:
        df["timeframe_group"] = df["timeframe_group"].fillna("")
        df.loc[df["timeframe_group"].astype(str).str.strip().eq(""), "timeframe_group"] = (
            df.loc[df["timeframe_group"].astype(str).str.strip().eq(""), "timeframe"].apply(classify_timeframe_group))

    if "regime_family" not in df.columns:
        df["regime_family"] = df["current_regime"].apply(regime_family)
    else:
        df["regime_family"] = df["regime_family"].fillna(
            df["current_regime"].apply(regime_family)
        )

    df["control_source"] = "exact_symbol_regime_match"
    df.loc[df["trade_permission"].isna(), "control_source"] = "unresolved"

    merged = df.merge(
        family_lookup,
        on=["broker", "timeframe_group", "regime_family"],
        how="left",
        suffixes=("", "_family"),
    )

    control_cols = [
        "trade_permission",
        "risk_multiplier",
        "leverage_multiplier",
        "convexity_bias",
        "execution_mode",
        "allowed_strategy_families",
        "blocked_strategy_families",
        "stop_loss_profile",
        "trailing_stop_profile",
        "control_commentary",
    ]

    resolved_records = []

    for _, row in merged.iterrows():
        record = row.to_dict()

        has_exact = pd.notna(record.get("trade_permission"))

        if has_exact:
            record["resolved_regime_risk_score"] = record.get("regime_risk_score")
            record["resolved_regime_risk_band"] = record.get("regime_risk_band")
            record["resolved_strategy_posture"] = record.get("strategy_posture")
            record["control_source"] = "exact_symbol_regime_match"

        else:
            family_band = record.get("family_risk_band")
            family_score = record.get("family_risk_score")

            if pd.notna(family_band):
                fallback = default_control_for_family(
                    risk_band=family_band,
                    family=record.get("regime_family"),
                )

                for col in control_cols:
                    record[col] = fallback.get(col)

                record["regime_risk_score"] = family_score
                record["regime_risk_band"] = family_band
                record["strategy_posture"] = record.get("family_strategy_posture")
                record["resolved_regime_risk_score"] = family_score
                record["resolved_regime_risk_band"] = family_band
                record["resolved_strategy_posture"] = record.get("family_strategy_posture")
                record["control_source"] = "regime_family_fallback"

            else:
                fallback = conservative_default_control()

                for col in control_cols:
                    record[col] = fallback.get(col)

                record["regime_risk_score"] = 0.45
                record["regime_risk_band"] = "high"
                record["strategy_posture"] = "defensive_mode"
                record["resolved_regime_risk_score"] = 0.45
                record["resolved_regime_risk_band"] = "high"
                record["resolved_strategy_posture"] = "defensive_mode"
                record["control_source"] = "conservative_default"

        record["dashboard_status_resolved"] = dashboard_status(
            record.get("trade_permission"),
            record.get("regime_risk_band"),
        )

        resolved_records.append(record)

    return pd.DataFrame(resolved_records)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE CURRENT REGIME CONTROL COVERAGE RESOLVER")
    print("=" * 90)
    print(f"Project root:      {PROJECT_ROOT}")
    print(f"Current dashboard: {CURRENT_DASHBOARD}")
    print(f"Family risk:       {FAMILY_RISK}")
    print(f"Output dir:        {OUTPUT_DIR}")
    print("-" * 90)

    dashboard = read_required(CURRENT_DASHBOARD)
    family_risk = read_required(FAMILY_RISK)
    family_lookup = prepare_family_lookup(family_risk)

    print(f"Dashboard rows loaded:   {len(dashboard):,}")
    print(f"Family risk rows loaded: {len(family_lookup):,}")

    unresolved_before = int(dashboard["trade_permission"].isna().sum()) if "trade_permission" in dashboard.columns else len(dashboard)

    resolved = resolve_controls(dashboard, family_lookup)

    unresolved_after = int(resolved["trade_permission"].isna().sum())

    exact_rows = int(resolved["control_source"].eq("exact_symbol_regime_match").sum())
    family_rows = int(resolved["control_source"].eq("regime_family_fallback").sum())
    default_rows = int(resolved["control_source"].eq("conservative_default").sum())

    reduced_or_blocked = resolved[
        resolved["trade_permission"].isin(["restricted", "avoid_or_convex_only"])
    ].copy()

    allowed_or_selective = resolved[
        resolved["trade_permission"].isin(["allowed", "selective"])
    ].copy()

    resolved_leverage_cols = [
        "broker",
        "timeframe",
        "timeframe_group",
        "symbol_current",
        "latest_timestamp",
        "current_regime",
        "regime_family",
        "trend_state",
        "volatility_state",
        "momentum_state",
        "trend_strength_state",
        "regime_confidence",
        "regime_risk_score",
        "regime_risk_band",
        "trade_permission",
        "risk_multiplier",
        "leverage_multiplier",
        "convexity_bias",
        "execution_mode",
        "allowed_strategy_families",
        "blocked_strategy_families",
        "stop_loss_profile",
        "trailing_stop_profile",
        "dashboard_status_resolved",
        "control_source",
        "control_commentary",
    ]

    available_cols = [c for c in resolved_leverage_cols if c in resolved.columns]
    leverage_dashboard = resolved[available_cols].copy()

    summary = (
        resolved.groupby(
            [
                "broker",
                "timeframe",
                "timeframe_group",
                "control_source",
                "dashboard_status_resolved",
                "trade_permission",
                "regime_risk_band",
                "execution_mode",
            ],
            dropna=False,
        )
        .agg(
            rows=("symbol_current", "count"),
            avg_risk_score=("regime_risk_score", "mean"),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
        )
        .reset_index()
    )

    for col in ["avg_risk_score", "avg_risk_multiplier", "avg_leverage_multiplier"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    summary = summary.sort_values(
        ["dashboard_status_resolved", "control_source", "timeframe", "rows"],
        ascending=[True, True, True, False],
    )

    outputs = {
        "resolved_dashboard": OUTPUT_DIR / "current_regime_strategy_control_resolved_latest.csv",
        "resolved_leverage": OUTPUT_DIR / "current_regime_leverage_dashboard_resolved_latest.csv",
        "summary": OUTPUT_DIR / "current_regime_control_coverage_summary_latest.csv",
        "reduced_or_blocked": OUTPUT_DIR / "current_regime_control_reduced_or_blocked_resolved_latest.csv",
        "allowed_or_selective": OUTPUT_DIR / "current_regime_control_allowed_or_selective_resolved_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    resolved.to_csv(outputs["resolved_dashboard"], index=False)
    leverage_dashboard.to_csv(outputs["resolved_leverage"], index=False)
    summary.to_csv(outputs["summary"], index=False)
    reduced_or_blocked.to_csv(outputs["reduced_or_blocked"], index=False)
    allowed_or_selective.to_csv(outputs["allowed_or_selective"], index=False)

    resolved.to_csv(timestamped["resolved_dashboard"], index=False)
    leverage_dashboard.to_csv(timestamped["resolved_leverage"], index=False)
    summary.to_csv(timestamped["summary"], index=False)
    reduced_or_blocked.to_csv(timestamped["reduced_or_blocked"], index=False)
    allowed_or_selective.to_csv(timestamped["allowed_or_selective"], index=False)

    source_counts = resolved["control_source"].value_counts(dropna=False).to_dict()
    status_counts = resolved["dashboard_status_resolved"].value_counts(dropna=False).to_dict()
    permission_counts = resolved["trade_permission"].value_counts(dropna=False).to_dict()

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dashboard_rows": int(len(dashboard)),
        "resolved_rows": int(len(resolved)),
        "unresolved_before": unresolved_before,
        "unresolved_after": unresolved_after,
        "exact_symbol_regime_match_rows": exact_rows,
        "regime_family_fallback_rows": family_rows,
        "conservative_default_rows": default_rows,
        "reduced_or_blocked_rows": int(len(reduced_or_blocked)),
        "allowed_or_selective_rows": int(len(allowed_or_selective)),
        "source_counts": source_counts,
        "status_counts": status_counts,
        "permission_counts": permission_counts,
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect resolved leverage dashboard. "
            "Next script can export this resolved dashboard as a live strategy-router/watchlist file."
        ),
    }

    json_latest = OUTPUT_DIR / "current_regime_control_coverage_resolver_latest.json"
    json_ts = OUTPUT_DIR / f"current_regime_control_coverage_resolver_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Current regime control coverage resolved.")
    print(f"Rows resolved:              {len(resolved):,}")
    print(f"Unresolved before:          {unresolved_before:,}")
    print(f"Unresolved after:           {unresolved_after:,}")
    print(f"Exact control rows:         {exact_rows:,}")
    print(f"Family fallback rows:       {family_rows:,}")
    print(f"Conservative default rows:  {default_rows:,}")
    print(f"Reduced/blocked rows:       {len(reduced_or_blocked):,}")
    print(f"Allowed/selective rows:     {len(allowed_or_selective):,}")
    print(f"Resolved dashboard:         {outputs['resolved_dashboard']}")
    print(f"Resolved leverage:          {outputs['resolved_leverage']}")
    print(f"Summary:                    {outputs['summary']}")
    print(f"JSON summary:               {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()