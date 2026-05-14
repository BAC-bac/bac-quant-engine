"""
BACQE Script 44
Strategy Eligibility Matrix Builder

Purpose:
- Join the live strategy router export with the strategy family registry.
- Produce per symbol/timeframe strategy eligibility.
- Show which strategy families are allowed, blocked, or conditionally allowed.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

ROUTER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_router_export"
REGISTRY_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_family_registry"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_eligibility_matrix"

ROUTER_EXPORT = ROUTER_DIR / "live_strategy_router_export_latest.csv"
REGISTRY_FILE = REGISTRY_DIR / "strategy_family_registry_latest.csv"
PERMISSION_MATRIX_FILE = REGISTRY_DIR / "strategy_family_permission_matrix_latest.csv"


def classify_timeframe_group(timeframe: str) -> str:
    tf = str(timeframe).upper()

    if tf in {"M1", "M2", "M3", "M5", "M10", "M15", "M30"}:
        return "tactical_intraday"
    if tf in {"H1", "H2", "H3", "H4", "H8", "H12"}:
        return "intraday_swing"
    if tf in {"D1", "W1"}:
        return "position"
    if tf == "MN1":
        return "long_horizon"

    return "unknown"


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def split_pipe(value: str) -> set[str]:
    if pd.isna(value):
        return set()
    return {x.strip() for x in str(value).split("|") if x.strip()}


def timeframe_allowed(router_tf_group: str, suitable: str, unsuitable: str) -> tuple[bool, str]:
    tf_group = str(router_tf_group)

    suitable_set = split_pipe(suitable)
    unsuitable_set = split_pipe(unsuitable)

    if tf_group in unsuitable_set:
        return False, "timeframe_group_unsuitable"

    if suitable_set and tf_group not in suitable_set:
        return False, "timeframe_group_not_in_suitable_list"

    return True, "timeframe_group_allowed"


def regime_allowed(regime_family: str, preferred: str, blocked: str) -> tuple[bool, str]:
    family = str(regime_family)

    preferred_set = split_pipe(preferred)
    blocked_set = split_pipe(blocked)

    if family in blocked_set:
        return False, "regime_family_blocked"

    if preferred_set and family not in preferred_set:
        return True, "regime_family_not_preferred_but_not_blocked"

    return True, "regime_family_preferred"


def risk_allowed(risk_multiplier: float, min_risk: float, max_risk: float) -> tuple[bool, str]:
    try:
        risk_multiplier = float(risk_multiplier)
        min_risk = float(min_risk)
        max_risk = float(max_risk)
    except Exception:
        return False, "risk_multiplier_invalid"

    if risk_multiplier < min_risk:
        return False, "risk_multiplier_below_strategy_minimum"

    if risk_multiplier > max_risk:
        return False, "risk_multiplier_above_strategy_maximum"

    return True, "risk_multiplier_allowed"


def determine_eligibility(router_row: pd.Series, strategy_row: pd.Series, permission_allowed: bool) -> dict:
    tf_ok, tf_reason = timeframe_allowed(
        router_row.get("timeframe_group"),
        strategy_row.get("suitable_timeframe_groups"),
        strategy_row.get("unsuitable_timeframe_groups"),
    )

    regime_ok, regime_reason = regime_allowed(
        router_row.get("regime_family"),
        strategy_row.get("preferred_regime_families"),
        strategy_row.get("blocked_regime_families"),
    )

    risk_ok, risk_reason = risk_allowed(
        router_row.get("risk_multiplier"),
        strategy_row.get("minimum_risk_multiplier"),
        strategy_row.get("maximum_risk_multiplier"),
    )

    permission_ok = bool(permission_allowed)

    reasons = []

    if not permission_ok:
        reasons.append("trade_permission_not_allowed_for_strategy")
    if not tf_ok:
        reasons.append(tf_reason)
    if not regime_ok:
        reasons.append(regime_reason)
    if not risk_ok:
        reasons.append(risk_reason)

    if permission_ok and tf_ok and regime_ok and risk_ok:
        if regime_reason == "regime_family_preferred":
            eligibility = "allowed_preferred"
        else:
            eligibility = "allowed_conditional"
    else:
        eligibility = "blocked"

    return {
        "strategy_eligibility": eligibility,
        "is_strategy_allowed": eligibility != "blocked",
        "block_reasons": "|".join(reasons),
        "timeframe_check": tf_reason,
        "regime_check": regime_reason,
        "risk_check": risk_reason,
        "permission_check": "permission_allowed" if permission_ok else "permission_blocked",
    }


def build_eligibility_matrix(
    router: pd.DataFrame,
    registry: pd.DataFrame,
    permission_matrix: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    permission_lookup = permission_matrix.set_index(
        ["strategy_family", "trade_permission"]
    )["allowed_by_permission"].to_dict()

    for _, r in router.iterrows():
        for _, s in registry.iterrows():
            strategy_family = s["strategy_family"]
            trade_permission = r["trade_permission"]

            permission_allowed = permission_lookup.get(
                (strategy_family, trade_permission),
                False,
            )

            eligibility = determine_eligibility(r, s, permission_allowed)

            row = {
                "broker": r.get("broker"),
                "symbol": r.get("symbol"),
                "timeframe": r.get("timeframe"),
                "timeframe_group": r.get("timeframe_group"),
                "latest_timestamp": r.get("latest_timestamp"),
                "current_regime": r.get("current_regime"),
                "regime_family": r.get("regime_family"),
                "regime_risk_band": r.get("regime_risk_band"),
                "trade_permission": r.get("trade_permission"),
                "risk_multiplier": r.get("risk_multiplier"),
                "leverage_multiplier": r.get("leverage_multiplier"),
                "execution_mode": r.get("execution_mode"),
                "control_source": r.get("control_source"),
                "strategy_family": strategy_family,
                "strategy_description": s.get("description"),
                "router_action": s.get("router_action"),
                "ftmo_suitability": s.get("ftmo_suitability"),
                "is_convex_strategy": s.get("is_convex_strategy"),
                "is_defensive_strategy": s.get("is_defensive_strategy"),
                "is_fragile_strategy": s.get("is_fragile_strategy"),
                **eligibility,
            }

            rows.append(row)

    return pd.DataFrame(rows)


def build_recommendations(eligibility: pd.DataFrame) -> pd.DataFrame:
    allowed = eligibility[eligibility["is_strategy_allowed"].eq(True)].copy()

    if allowed.empty:
        return allowed

    score_map = {
        "allowed_preferred": 3,
        "allowed_conditional": 2,
        "blocked": 0,
    }

    ftmo_map = {
        "high": 3,
        "medium": 2,
        "low": 1,
    }

    allowed["eligibility_score"] = allowed["strategy_eligibility"].map(score_map).fillna(0)
    allowed["ftmo_score"] = allowed["ftmo_suitability"].map(ftmo_map).fillna(0)
    allowed["risk_multiplier_num"] = pd.to_numeric(
        allowed["risk_multiplier"],
        errors="coerce",
    ).fillna(0)

    allowed["recommendation_score"] = (
        allowed["eligibility_score"] * 10
        + allowed["ftmo_score"] * 2
        + allowed["risk_multiplier_num"]
    )

    recommendations = (
        allowed.sort_values(
            ["broker", "symbol", "timeframe", "recommendation_score"],
            ascending=[True, True, True, False],
        )
        .groupby(["broker", "symbol", "timeframe"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    return recommendations


def build_summary(eligibility: pd.DataFrame) -> pd.DataFrame:
    summary = (
        eligibility.groupby(
            [
                "strategy_family",
                "strategy_eligibility",
                "trade_permission",
                "regime_risk_band",
                "timeframe_group",
            ],
            dropna=False,
        )
        .agg(
            rows=("symbol", "count"),
            avg_risk_multiplier=("risk_multiplier", lambda x: pd.to_numeric(x, errors="coerce").mean()),
            avg_leverage_multiplier=("leverage_multiplier", lambda x: pd.to_numeric(x, errors="coerce").mean()),
        )
        .reset_index()
    )

    for col in ["avg_risk_multiplier", "avg_leverage_multiplier"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    return summary.sort_values(
        ["strategy_family", "strategy_eligibility", "rows"],
        ascending=[True, True, False],
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE STRATEGY ELIGIBILITY MATRIX BUILDER")
    print("=" * 90)
    print(f"Project root:       {PROJECT_ROOT}")
    print(f"Router export:      {ROUTER_EXPORT}")
    print(f"Strategy registry:  {REGISTRY_FILE}")
    print(f"Permission matrix:  {PERMISSION_MATRIX_FILE}")
    print(f"Output dir:         {OUTPUT_DIR}")
    print("-" * 90)

    router = read_required(ROUTER_EXPORT)
    registry = read_required(REGISTRY_FILE)
    permission_matrix = read_required(PERMISSION_MATRIX_FILE)

    if "timeframe_group" not in router.columns:
        router["timeframe_group"] = router["timeframe"].apply(classify_timeframe_group)
    else:
        router["timeframe_group"] = router["timeframe_group"].fillna("")
        mask = router["timeframe_group"].astype(str).str.strip().eq("")
        router.loc[mask, "timeframe_group"] = router.loc[mask, "timeframe"].apply(classify_timeframe_group)

    permission_matrix["allowed_by_permission"] = (
        permission_matrix["allowed_by_permission"].astype(str).str.lower().map({"true": True, "false": False}).fillna(
            False))

    print(f"Router rows loaded:       {len(router):,}")
    print(f"Strategy families loaded: {len(registry):,}")
    print(f"Permission rows loaded:   {len(permission_matrix):,}")

    eligibility = build_eligibility_matrix(router, registry, permission_matrix)
    recommendations = build_recommendations(eligibility)
    summary = build_summary(eligibility)

    allowed = eligibility[eligibility["is_strategy_allowed"].eq(True)].copy()
    blocked = eligibility[eligibility["is_strategy_allowed"].eq(False)].copy()

    outputs = {
        "eligibility": OUTPUT_DIR / "strategy_eligibility_matrix_latest.csv",
        "allowed": OUTPUT_DIR / "strategy_eligibility_allowed_latest.csv",
        "blocked": OUTPUT_DIR / "strategy_eligibility_blocked_latest.csv",
        "recommendations": OUTPUT_DIR / "strategy_eligibility_recommendations_latest.csv",
        "summary": OUTPUT_DIR / "strategy_eligibility_summary_latest.csv",
        "json": OUTPUT_DIR / "strategy_eligibility_matrix_latest.json",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    eligibility.to_csv(outputs["eligibility"], index=False)
    allowed.to_csv(outputs["allowed"], index=False)
    blocked.to_csv(outputs["blocked"], index=False)
    recommendations.to_csv(outputs["recommendations"], index=False)
    summary.to_csv(outputs["summary"], index=False)

    eligibility.to_csv(timestamped["eligibility"], index=False)
    allowed.to_csv(timestamped["allowed"], index=False)
    blocked.to_csv(timestamped["blocked"], index=False)
    recommendations.to_csv(timestamped["recommendations"], index=False)
    summary.to_csv(timestamped["summary"], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "router_rows": int(len(router)),
        "strategy_family_count": int(len(registry)),
        "eligibility_rows": int(len(eligibility)),
        "allowed_rows": int(len(allowed)),
        "blocked_rows": int(len(blocked)),
        "recommendation_rows": int(len(recommendations)),
        "eligibility_counts": eligibility["strategy_eligibility"].value_counts(dropna=False).to_dict(),
        "strategy_allowed_counts": allowed["strategy_family"].value_counts(dropna=False).to_dict(),
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Review eligibility recommendations. Next script can create a compact strategy-router "
            "decision file per symbol/timeframe."
        ),
    }

    with outputs["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with timestamped["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Strategy eligibility matrix created.")
    print(f"Eligibility rows:       {len(eligibility):,}")
    print(f"Allowed rows:           {len(allowed):,}")
    print(f"Blocked rows:           {len(blocked):,}")
    print(f"Recommendation rows:    {len(recommendations):,}")
    print(f"Eligibility matrix:     {outputs['eligibility']}")
    print(f"Recommendations:        {outputs['recommendations']}")
    print(f"Summary:                {outputs['summary']}")
    print(f"JSON summary:           {outputs['json']}")
    print("=" * 90)


if __name__ == "__main__":
    main()