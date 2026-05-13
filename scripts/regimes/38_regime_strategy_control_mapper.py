"""
BACQE Script 38
Regime Strategy Control Mapper

Purpose:
- Convert regime risk intelligence into strategy control rules
- Map each symbol/timeframe/regime to:
  - allowed strategy families
  - blocked strategy families
  - risk multiplier
  - leverage multiplier
  - convexity bias
  - execution mode
  - stop/trailing guidance

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_risk_intelligence"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_control"

SYMBOL_REGIME_RISK = INPUT_DIR / "regime_symbol_regime_risk_latest.csv"
SYMBOL_TIMEFRAME_RISK = INPUT_DIR / "regime_symbol_timeframe_risk_latest.csv"
REGIME_FAMILY_RISK = INPUT_DIR / "regime_family_risk_latest.csv"


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def strategy_rules(risk_band: str, regime_family: str, strategy_posture: str) -> dict:
    band = str(risk_band).lower()
    family = str(regime_family).lower()
    posture = str(strategy_posture).lower()

    rules = {
        "trade_permission": "manual_review",
        "allowed_strategy_families": "",
        "blocked_strategy_families": "",
        "risk_multiplier": 0.0,
        "leverage_multiplier": 0.0,
        "convexity_bias": "neutral",
        "execution_mode": "manual_review",
        "suggested_trade_frequency": "manual_review",
        "max_position_duration": "manual_review",
        "stop_loss_profile": "manual_review",
        "trailing_stop_profile": "manual_review",
        "control_commentary": "Manual review required.",
    }

    if band == "low":
        if "range" in family:
            rules.update({
                "trade_permission": "allowed",
                "allowed_strategy_families": "mean_reversion|range_fade|market_making_research",
                "blocked_strategy_families": "aggressive_breakout|tail_chase",
                "risk_multiplier": 1.00,
                "leverage_multiplier": 1.00,
                "convexity_bias": "neutral_to_short_convexity",
                "execution_mode": "normal",
                "suggested_trade_frequency": "normal",
                "max_position_duration": "standard",
                "stop_loss_profile": "standard_atr_stop",
                "trailing_stop_profile": "optional_after_profit",
                "control_commentary": "Stable range regime. Mean-reversion logic may be favoured if independently validated.",
            })
        elif "trend" in family:
            rules.update({
                "trade_permission": "allowed",
                "allowed_strategy_families": "trend_following|pullback_continuation|breakout_continuation",
                "blocked_strategy_families": "aggressive_countertrend|range_fade_without_confirmation",
                "risk_multiplier": 1.00,
                "leverage_multiplier": 1.00,
                "convexity_bias": "long_convexity_optional",
                "execution_mode": "normal",
                "suggested_trade_frequency": "normal",
                "max_position_duration": "standard_to_extended",
                "stop_loss_profile": "trend_atr_stop",
                "trailing_stop_profile": "trend_trailing_allowed",
                "control_commentary": "Stable trend regime. Trend-continuation logic may be favoured if validated.",
            })
        else:
            rules.update({
                "trade_permission": "allowed",
                "allowed_strategy_families": "validated_core_strategies",
                "blocked_strategy_families": "unvalidated_high_turnover",
                "risk_multiplier": 1.00,
                "leverage_multiplier": 1.00,
                "convexity_bias": "neutral",
                "execution_mode": "normal",
                "suggested_trade_frequency": "normal",
                "max_position_duration": "standard",
                "stop_loss_profile": "standard_atr_stop",
                "trailing_stop_profile": "optional",
                "control_commentary": "Low-risk regime. Standard validated strategy logic may be allowed.",
            })

    elif band == "medium":
        rules.update({
            "trade_permission": "selective",
            "allowed_strategy_families": "validated_core_strategies|confirmed_trend|confirmed_mean_reversion",
            "blocked_strategy_families": "high_leverage_scalping|fragile_grid|unconfirmed_countertrend",
            "risk_multiplier": 0.75,
            "leverage_multiplier": 0.75,
            "convexity_bias": "mild_long_convexity",
            "execution_mode": "confirmation_required",
            "suggested_trade_frequency": "reduced",
            "max_position_duration": "reduced_or_standard",
            "stop_loss_profile": "tighter_atr_stop",
            "trailing_stop_profile": "recommended_after_profit",
            "control_commentary": "Medium-risk regime. Trade selectively with confirmation and reduced exposure.",
        })

    elif band == "high":
        if "volatile" in family or "transition" in family or posture == "defensive_mode":
            rules.update({
                "trade_permission": "restricted",
                "allowed_strategy_families": "convex_breakout|volatility_expansion|small_probe_trend",
                "blocked_strategy_families": "fragile_mean_reversion|grid|martingale|large_countertrend|tight_stop_scalping",
                "risk_multiplier": 0.40,
                "leverage_multiplier": 0.40,
                "convexity_bias": "long_convexity_preferred",
                "execution_mode": "defensive",
                "suggested_trade_frequency": "low",
                "max_position_duration": "short_or_rule_based",
                "stop_loss_profile": "strict_atr_or_structural_stop",
                "trailing_stop_profile": "required_on_profit",
                "control_commentary": "High-risk unstable regime. Prefer convex or defensive strategies; avoid fragile mean-reversion.",
            })
        else:
            rules.update({
                "trade_permission": "selective",
                "allowed_strategy_families": "confirmed_trend|reduced_size_pullback|validated_core_strategies",
                "blocked_strategy_families": "large_countertrend|fragile_grid|unconfirmed_scalping",
                "risk_multiplier": 0.55,
                "leverage_multiplier": 0.55,
                "convexity_bias": "mild_long_convexity",
                "execution_mode": "defensive_confirmation_required",
                "suggested_trade_frequency": "low_to_moderate",
                "max_position_duration": "reduced",
                "stop_loss_profile": "strict_atr_stop",
                "trailing_stop_profile": "recommended",
                "control_commentary": "High-risk but non-transition regime. Use reduced exposure and stronger confirmation.",
            })

    elif band == "extreme":
        rules.update({
            "trade_permission": "avoid_or_convex_only",
            "allowed_strategy_families": "convex_tail|volatility_breakout_probe|event_risk_probe",
            "blocked_strategy_families": "mean_reversion|grid|martingale|large_directional|fragile_scalping|countertrend",
            "risk_multiplier": 0.20,
            "leverage_multiplier": 0.20,
            "convexity_bias": "strong_long_convexity_only",
            "execution_mode": "capital_preservation",
            "suggested_trade_frequency": "very_low",
            "max_position_duration": "short_or_predefined",
            "stop_loss_profile": "hard_stop_required",
            "trailing_stop_profile": "required",
            "control_commentary": "Extreme-risk regime. Avoid normal trading unless using explicitly convex, predefined-risk logic.",
        })

    return rules


def build_control_matrix(symbol_regime_risk: pd.DataFrame) -> pd.DataFrame:
    df = symbol_regime_risk.copy()

    control_rows = []

    for _, row in df.iterrows():
        rules = strategy_rules(
            risk_band=row.get("regime_risk_band"),
            regime_family=row.get("regime_family"),
            strategy_posture=row.get("strategy_posture"),
        )

        combined = row.to_dict()
        combined.update(rules)

        control_rows.append(combined)

    control = pd.DataFrame(control_rows)

    sort_cols = [
        "regime_risk_score",
        "leave_probability",
        "regime_change_rate",
    ]

    for col in sort_cols:
        if col in control.columns:
            control[col] = pd.to_numeric(control[col], errors="coerce").fillna(0)

    return control.sort_values(
        sort_cols,
        ascending=[False, False, False],
    ).reset_index(drop=True)


def build_control_summary(control: pd.DataFrame) -> pd.DataFrame:
    summary = (
        control.groupby(
            [
                "broker",
                "timeframe",
                "timeframe_group",
                "regime_family",
                "regime_risk_band",
                "trade_permission",
                "execution_mode",
            ],
            dropna=False,
        )
        .agg(
            rows=("symbol", "count"),
            avg_risk_score=("regime_risk_score", "mean"),
            avg_leave_probability=("leave_probability", "mean"),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
            avg_segment_bars=("avg_segment_bars", "mean"),
        )
        .reset_index()
    )

    for col in [
        "avg_risk_score",
        "avg_leave_probability",
        "avg_risk_multiplier",
        "avg_leverage_multiplier",
        "avg_segment_bars",
    ]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    return summary.sort_values(
        ["avg_risk_score", "rows"],
        ascending=[False, False],
    ).reset_index(drop=True)


def build_symbol_control_summary(control: pd.DataFrame) -> pd.DataFrame:
    summary = (
        control.groupby(["broker", "timeframe", "timeframe_group", "symbol"], dropna=False)
        .agg(
            regimes_mapped=("regime", "count"),
            avg_risk_score=("regime_risk_score", "mean"),
            max_risk_score=("regime_risk_score", "max"),
            min_risk_multiplier=("risk_multiplier", "min"),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            min_leverage_multiplier=("leverage_multiplier", "min"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
            restricted_or_avoid_count=(
                "trade_permission",
                lambda x: int(pd.Series(x).isin(["restricted", "avoid_or_convex_only"]).sum()),
            ),
            normal_allowed_count=(
                "trade_permission",
                lambda x: int(pd.Series(x).eq("allowed").sum()),
            ),
        )
        .reset_index()
    )

    for col in [
        "avg_risk_score",
        "max_risk_score",
        "min_risk_multiplier",
        "avg_risk_multiplier",
        "min_leverage_multiplier",
        "avg_leverage_multiplier",
    ]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    return summary.sort_values(
        ["avg_risk_score", "max_risk_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME STRATEGY CONTROL MAPPER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input dir:    {INPUT_DIR}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    symbol_regime_risk = read_required(SYMBOL_REGIME_RISK)
    symbol_timeframe_risk = read_required(SYMBOL_TIMEFRAME_RISK)
    regime_family_risk = read_required(REGIME_FAMILY_RISK)

    print(f"Symbol-regime risk rows loaded:    {len(symbol_regime_risk):,}")
    print(f"Symbol-timeframe risk rows loaded: {len(symbol_timeframe_risk):,}")
    print(f"Regime-family risk rows loaded:    {len(regime_family_risk):,}")

    control_matrix = build_control_matrix(symbol_regime_risk)
    control_summary = build_control_summary(control_matrix)
    symbol_control_summary = build_symbol_control_summary(control_matrix)

    blocked_matrix = control_matrix[
        control_matrix["trade_permission"].isin(["restricted", "avoid_or_convex_only"])
    ].copy()

    normal_allowed = control_matrix[
        control_matrix["trade_permission"].eq("allowed")
    ].copy()

    defensive_matrix = control_matrix[
        control_matrix["execution_mode"].isin([
            "defensive",
            "capital_preservation",
            "defensive_confirmation_required",
        ])
    ].copy()

    leverage_guidance = control_matrix[
        [
            "broker",
            "timeframe",
            "timeframe_group",
            "symbol",
            "regime",
            "regime_family",
            "regime_risk_score",
            "regime_risk_band",
            "trade_permission",
            "risk_multiplier",
            "leverage_multiplier",
            "convexity_bias",
            "execution_mode",
            "stop_loss_profile",
            "trailing_stop_profile",
        ]
    ].copy()

    outputs = {
        "control_matrix": OUTPUT_DIR / "regime_strategy_control_matrix_latest.csv",
        "control_summary": OUTPUT_DIR / "regime_strategy_control_summary_latest.csv",
        "symbol_control_summary": OUTPUT_DIR / "regime_symbol_strategy_control_summary_latest.csv",
        "blocked_matrix": OUTPUT_DIR / "regime_blocked_strategy_matrix_latest.csv",
        "normal_allowed": OUTPUT_DIR / "regime_normal_allowed_strategy_matrix_latest.csv",
        "defensive_matrix": OUTPUT_DIR / "regime_defensive_strategy_matrix_latest.csv",
        "leverage_guidance": OUTPUT_DIR / "regime_leverage_guidance_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    control_matrix.to_csv(outputs["control_matrix"], index=False)
    control_summary.to_csv(outputs["control_summary"], index=False)
    symbol_control_summary.to_csv(outputs["symbol_control_summary"], index=False)
    blocked_matrix.to_csv(outputs["blocked_matrix"], index=False)
    normal_allowed.to_csv(outputs["normal_allowed"], index=False)
    defensive_matrix.to_csv(outputs["defensive_matrix"], index=False)
    leverage_guidance.to_csv(outputs["leverage_guidance"], index=False)

    control_matrix.to_csv(timestamped["control_matrix"], index=False)
    control_summary.to_csv(timestamped["control_summary"], index=False)
    symbol_control_summary.to_csv(timestamped["symbol_control_summary"], index=False)
    blocked_matrix.to_csv(timestamped["blocked_matrix"], index=False)
    normal_allowed.to_csv(timestamped["normal_allowed"], index=False)
    defensive_matrix.to_csv(timestamped["defensive_matrix"], index=False)
    leverage_guidance.to_csv(timestamped["leverage_guidance"], index=False)

    permission_counts = control_matrix["trade_permission"].value_counts().to_dict()
    execution_counts = control_matrix["execution_mode"].value_counts().to_dict()
    convexity_counts = control_matrix["convexity_bias"].value_counts().to_dict()

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "control_matrix_rows": int(len(control_matrix)),
        "control_summary_rows": int(len(control_summary)),
        "symbol_control_summary_rows": int(len(symbol_control_summary)),
        "blocked_or_restricted_rows": int(len(blocked_matrix)),
        "normal_allowed_rows": int(len(normal_allowed)),
        "defensive_rows": int(len(defensive_matrix)),
        "permission_counts": permission_counts,
        "execution_counts": execution_counts,
        "convexity_counts": convexity_counts,
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect control matrix and leverage guidance. "
            "Next script can join latest current regimes to these controls to produce a live strategy control dashboard."
        ),
    }

    json_latest = OUTPUT_DIR / "regime_strategy_control_mapper_latest.json"
    json_ts = OUTPUT_DIR / f"regime_strategy_control_mapper_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Regime strategy controls created.")
    print(f"Control matrix rows:        {len(control_matrix):,}")
    print(f"Blocked/restricted rows:    {len(blocked_matrix):,}")
    print(f"Normal allowed rows:        {len(normal_allowed):,}")
    print(f"Defensive rows:             {len(defensive_matrix):,}")
    print(f"Control matrix:             {outputs['control_matrix']}")
    print(f"Leverage guidance:          {outputs['leverage_guidance']}")
    print(f"Symbol control summary:     {outputs['symbol_control_summary']}")
    print(f"JSON summary:               {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()