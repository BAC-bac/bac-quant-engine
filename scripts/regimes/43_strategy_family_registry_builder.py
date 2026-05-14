"""
BACQE Script 43
Strategy Family Registry Builder

Purpose:
- Create a central registry of BACQE strategy families
- Define each strategy family's:
  - description
  - preferred regimes
  - blocked regimes
  - minimum trade permission
  - minimum / maximum risk multipliers
  - suitable timeframe groups
  - FTMO suitability
  - notes for future strategy-router integration

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_family_registry"


STRATEGY_FAMILIES = [
    {
        "strategy_family": "trend_following",
        "description": "Directional strategy seeking continuation in established trend regimes.",
        "preferred_regime_families": "bull_trend_normal_vol|bear_trend_normal_vol|bull_trend_high_vol|bear_trend_high_vol",
        "blocked_regime_families": "volatile_transition|volatile_range",
        "minimum_trade_permission": "selective",
        "minimum_risk_multiplier": 0.55,
        "maximum_risk_multiplier": 1.00,
        "suitable_timeframe_groups": "intraday_swing|position",
        "unsuitable_timeframe_groups": "tactical_intraday",
        "ftmo_suitability": "medium",
        "router_action": "allow_when_confirmed",
        "notes": "Best used when trend regime is stable or moderately selective. Avoid blindly chasing volatile transitions.",
    },
    {
        "strategy_family": "mean_reversion",
        "description": "Range or stretched-price strategy expecting reversion toward fair value.",
        "preferred_regime_families": "range",
        "blocked_regime_families": "volatile_transition|transition|bull_trend_high_vol|bear_trend_high_vol",
        "minimum_trade_permission": "selective",
        "minimum_risk_multiplier": 0.75,
        "maximum_risk_multiplier": 1.00,
        "suitable_timeframe_groups": "intraday_swing|position",
        "unsuitable_timeframe_groups": "tactical_intraday",
        "ftmo_suitability": "medium",
        "router_action": "allow_only_in_stable_range",
        "notes": "Must avoid fragile use during transitions. Dangerous when volatility expansion begins.",
    },
    {
        "strategy_family": "breakout_convex",
        "description": "Predefined-risk breakout strategy designed for asymmetric upside.",
        "preferred_regime_families": "transition|volatile_transition|volatile_range|bull_trend_high_vol|bear_trend_high_vol",
        "blocked_regime_families": "quiet_range",
        "minimum_trade_permission": "restricted",
        "minimum_risk_multiplier": 0.20,
        "maximum_risk_multiplier": 0.75,
        "suitable_timeframe_groups": "tactical_intraday|intraday_swing|position",
        "unsuitable_timeframe_groups": "",
        "ftmo_suitability": "medium",
        "router_action": "allow_small_predefined_risk",
        "notes": "Useful for unstable regimes if losses are strictly capped and position size is small.",
    },
    {
        "strategy_family": "volatility_expansion",
        "description": "Strategy targeting expansion from compressed or unstable conditions.",
        "preferred_regime_families": "transition|volatile_transition|volatile_range",
        "blocked_regime_families": "quiet_range|range",
        "minimum_trade_permission": "restricted",
        "minimum_risk_multiplier": 0.20,
        "maximum_risk_multiplier": 0.60,
        "suitable_timeframe_groups": "tactical_intraday|intraday_swing",
        "unsuitable_timeframe_groups": "long_horizon",
        "ftmo_suitability": "medium",
        "router_action": "allow_small_probe_only",
        "notes": "Needs hard stops and preferably trailing exits. Not suitable for large exposure.",
    },
    {
        "strategy_family": "grid_mean_reversion",
        "description": "Grid-style mean reversion with multiple entries around a central price.",
        "preferred_regime_families": "range|quiet_range",
        "blocked_regime_families": "transition|volatile_transition|volatile_range|bull_trend_high_vol|bear_trend_high_vol",
        "minimum_trade_permission": "allowed",
        "minimum_risk_multiplier": 1.00,
        "maximum_risk_multiplier": 1.00,
        "suitable_timeframe_groups": "intraday_swing|position",
        "unsuitable_timeframe_groups": "tactical_intraday",
        "ftmo_suitability": "low",
        "router_action": "block_unless_stable_low_risk",
        "notes": "Potentially dangerous under FTMO drawdown rules. Only consider in highly stable range regimes with hard loss caps.",
    },
    {
        "strategy_family": "scalping",
        "description": "Short-horizon execution strategy seeking small moves.",
        "preferred_regime_families": "range|bull_trend_normal_vol|bear_trend_normal_vol",
        "blocked_regime_families": "volatile_transition|volatile_range|transition",
        "minimum_trade_permission": "selective",
        "minimum_risk_multiplier": 0.75,
        "maximum_risk_multiplier": 1.00,
        "suitable_timeframe_groups": "tactical_intraday",
        "unsuitable_timeframe_groups": "position|long_horizon",
        "ftmo_suitability": "medium",
        "router_action": "allow_only_with_spread_and_vol_filters",
        "notes": "Must include spread, slippage, session and news filters. Avoid chaotic transition states.",
    },
    {
        "strategy_family": "macro_bias",
        "description": "Directional strategy guided by macro score, rates, yields, sovereign strength or macro divergence.",
        "preferred_regime_families": "bull_trend_normal_vol|bear_trend_normal_vol|range",
        "blocked_regime_families": "volatile_transition",
        "minimum_trade_permission": "selective",
        "minimum_risk_multiplier": 0.55,
        "maximum_risk_multiplier": 1.00,
        "suitable_timeframe_groups": "position|intraday_swing",
        "unsuitable_timeframe_groups": "tactical_intraday",
        "ftmo_suitability": "medium",
        "router_action": "allow_when_macro_and_regime_align",
        "notes": "Best when macro bias agrees with technical regime and volatility is not chaotic.",
    },
    {
        "strategy_family": "event_risk_probe",
        "description": "Tiny predefined-risk strategy around major scheduled or unscheduled market events.",
        "preferred_regime_families": "transition|volatile_transition|volatile_range",
        "blocked_regime_families": "quiet_range",
        "minimum_trade_permission": "restricted",
        "minimum_risk_multiplier": 0.20,
        "maximum_risk_multiplier": 0.40,
        "suitable_timeframe_groups": "tactical_intraday|intraday_swing",
        "unsuitable_timeframe_groups": "position|long_horizon",
        "ftmo_suitability": "low",
        "router_action": "allow_tiny_probe_only",
        "notes": "Highly constrained. Should be disabled unless event framework and max-loss limits are active.",
    },
    {
        "strategy_family": "capital_preservation",
        "description": "Defensive posture where normal strategies are paused or sharply reduced.",
        "preferred_regime_families": "volatile_transition|volatile_range|transition",
        "blocked_regime_families": "",
        "minimum_trade_permission": "restricted",
        "minimum_risk_multiplier": 0.00,
        "maximum_risk_multiplier": 0.40,
        "suitable_timeframe_groups": "tactical_intraday|intraday_swing|position|long_horizon",
        "unsuitable_timeframe_groups": "",
        "ftmo_suitability": "high",
        "router_action": "reduce_or_pause",
        "notes": "Used when regime engine recommends defensive mode. Protects account survival.",
    },
]


PERMISSION_RANK = {
    "avoid_or_convex_only": 0,
    "restricted": 1,
    "selective": 2,
    "allowed": 3,
}


def build_registry() -> pd.DataFrame:
    df = pd.DataFrame(STRATEGY_FAMILIES)

    df["minimum_trade_permission_rank"] = df["minimum_trade_permission"].map(PERMISSION_RANK)
    df["minimum_risk_multiplier"] = pd.to_numeric(df["minimum_risk_multiplier"], errors="coerce")
    df["maximum_risk_multiplier"] = pd.to_numeric(df["maximum_risk_multiplier"], errors="coerce")

    df["is_convex_strategy"] = df["strategy_family"].isin([
        "breakout_convex",
        "volatility_expansion",
        "event_risk_probe",
    ])

    df["is_defensive_strategy"] = df["strategy_family"].isin([
        "capital_preservation",
    ])

    df["is_fragile_strategy"] = df["strategy_family"].isin([
        "grid_mean_reversion",
        "mean_reversion",
        "scalping",
    ])

    return df.sort_values("strategy_family").reset_index(drop=True)


def build_permission_matrix(registry: pd.DataFrame) -> pd.DataFrame:
    rows = []

    permissions = ["allowed", "selective", "restricted", "avoid_or_convex_only"]

    for _, strategy in registry.iterrows():
        min_rank = int(strategy["minimum_trade_permission_rank"])

        for permission in permissions:
            permission_rank = PERMISSION_RANK[permission]

            allowed_by_permission = permission_rank >= min_rank

            if permission == "avoid_or_convex_only":
                allowed_by_permission = bool(strategy["is_convex_strategy"] or strategy["is_defensive_strategy"])

            rows.append({
                "strategy_family": strategy["strategy_family"],
                "trade_permission": permission,
                "allowed_by_permission": allowed_by_permission,
                "router_action": strategy["router_action"],
                "ftmo_suitability": strategy["ftmo_suitability"],
                "is_convex_strategy": strategy["is_convex_strategy"],
                "is_defensive_strategy": strategy["is_defensive_strategy"],
                "is_fragile_strategy": strategy["is_fragile_strategy"],
            })

    return pd.DataFrame(rows)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE STRATEGY FAMILY REGISTRY BUILDER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    registry = build_registry()
    permission_matrix = build_permission_matrix(registry)

    outputs = {
        "registry": OUTPUT_DIR / "strategy_family_registry_latest.csv",
        "permission_matrix": OUTPUT_DIR / "strategy_family_permission_matrix_latest.csv",
        "json": OUTPUT_DIR / "strategy_family_registry_latest.json",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    registry.to_csv(outputs["registry"], index=False)
    permission_matrix.to_csv(outputs["permission_matrix"], index=False)

    registry.to_csv(timestamped["registry"], index=False)
    permission_matrix.to_csv(timestamped["permission_matrix"], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema_version": "bacqe_strategy_family_registry_v1",
        "strategy_family_count": int(len(registry)),
        "permission_matrix_rows": int(len(permission_matrix)),
        "strategy_families": registry.to_dict(orient="records"),
        "permission_matrix": permission_matrix.to_dict(orient="records"),
        "next_recommended_step": (
            "Next script can join live router export to this strategy registry "
            "to produce per-symbol strategy eligibility."
        ),
    }

    with outputs["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with timestamped["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] Strategy family registry created.")
    print(f"Strategy families:       {len(registry):,}")
    print(f"Permission matrix rows:  {len(permission_matrix):,}")
    print(f"Registry CSV:            {outputs['registry']}")
    print(f"Permission matrix CSV:   {outputs['permission_matrix']}")
    print(f"Registry JSON:           {outputs['json']}")
    print("=" * 90)


if __name__ == "__main__":
    main()