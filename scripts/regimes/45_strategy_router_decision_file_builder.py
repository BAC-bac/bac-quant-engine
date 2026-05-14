"""
BACQE Script 45
Strategy Router Decision File Builder

Purpose:
- Build one compact decision file per symbol/timeframe.
- Combine:
  - live router export
  - strategy eligibility recommendations
  - allowed strategy families
  - blocked strategy families
- Produce a downstream-ready decision layer for BACQE.

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

ROUTER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_router_export"
ELIGIBILITY_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_eligibility_matrix"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_router_decisions"

ROUTER_EXPORT = ROUTER_DIR / "live_strategy_router_export_latest.csv"
ELIGIBILITY_MATRIX = ELIGIBILITY_DIR / "strategy_eligibility_matrix_latest.csv"
RECOMMENDATIONS = ELIGIBILITY_DIR / "strategy_eligibility_recommendations_latest.csv"


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def join_pipe(values) -> str:
    clean = sorted({str(v) for v in values if pd.notna(v) and str(v).strip()})
    return "|".join(clean)


def build_allowed_blocked_summary(eligibility: pd.DataFrame) -> pd.DataFrame:
    grouped = []

    keys = ["broker", "symbol", "timeframe"]

    for key_values, group in eligibility.groupby(keys, dropna=False):
        broker, symbol, timeframe = key_values

        allowed = group[group["is_strategy_allowed"].astype(str).str.lower().eq("true")]
        blocked = group[~group["is_strategy_allowed"].astype(str).str.lower().eq("true")]

        preferred = allowed[allowed["strategy_eligibility"].eq("allowed_preferred")]
        conditional = allowed[allowed["strategy_eligibility"].eq("allowed_conditional")]

        grouped.append({
            "broker": broker,
            "symbol": symbol,
            "timeframe": timeframe,
            "allowed_strategy_family_count": int(len(allowed)),
            "blocked_strategy_family_count": int(len(blocked)),
            "allowed_strategy_families_resolved": join_pipe(allowed["strategy_family"]),
            "preferred_strategy_families": join_pipe(preferred["strategy_family"]),
            "conditional_strategy_families": join_pipe(conditional["strategy_family"]),
            "blocked_strategy_families_resolved": join_pipe(blocked["strategy_family"]),
            "block_reasons_summary": join_pipe(blocked["block_reasons"]),
        })

    return pd.DataFrame(grouped)


def normalise_recommendations(recs: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "broker",
        "symbol",
        "timeframe",
        "strategy_family",
        "strategy_eligibility",
        "router_action",
        "ftmo_suitability",
        "block_reasons",
        "timeframe_check",
        "regime_check",
        "risk_check",
        "permission_check",
    ]

    out = recs[[c for c in keep_cols if c in recs.columns]].copy()

    out = out.rename(columns={
        "strategy_family": "recommended_strategy_family",
        "strategy_eligibility": "recommended_strategy_eligibility",
        "router_action": "recommended_router_action",
        "ftmo_suitability": "recommended_ftmo_suitability",
        "block_reasons": "recommended_block_reasons",
    })

    return out


def build_decision_status(row: pd.Series) -> str:
    permission = str(row.get("trade_permission", "")).lower()
    recommended = str(row.get("recommended_strategy_family", "")).strip()
    risk_band = str(row.get("regime_risk_band", "")).lower()

    if permission == "avoid_or_convex_only" or risk_band == "extreme":
        return "RED_CONVEX_ONLY" if recommended else "RED_NO_STRATEGY"

    if permission == "restricted":
        return "AMBER_RESTRICTED_WITH_STRATEGY" if recommended else "AMBER_RESTRICTED_NO_STRATEGY"

    if permission == "selective":
        return "GREEN_SELECTIVE_WITH_STRATEGY" if recommended else "AMBER_SELECTIVE_NO_STRATEGY"

    if permission == "allowed":
        return "GREEN_ALLOWED_WITH_STRATEGY" if recommended else "AMBER_ALLOWED_NO_STRATEGY"

    return "UNKNOWN"


def build_decision_commentary(row: pd.Series) -> str:
    symbol = row.get("symbol")
    timeframe = row.get("timeframe")
    regime = row.get("current_regime")
    permission = row.get("trade_permission")
    strategy = row.get("recommended_strategy_family")
    risk = row.get("risk_multiplier")
    lev = row.get("leverage_multiplier")
    mode = row.get("execution_mode")

    if pd.isna(strategy) or str(strategy).strip() == "":
        return (
            f"{symbol} {timeframe}: {regime}. Permission={permission}. "
            f"No recommended strategy available; keep blocked or manual review."
        )

    return (
        f"{symbol} {timeframe}: {regime}. Permission={permission}. "
        f"Recommended={strategy}. Risk multiplier={risk}, leverage multiplier={lev}. "
        f"Execution mode={mode}."
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE STRATEGY ROUTER DECISION FILE BUILDER")
    print("=" * 90)
    print(f"Project root:       {PROJECT_ROOT}")
    print(f"Router export:      {ROUTER_EXPORT}")
    print(f"Eligibility matrix: {ELIGIBILITY_MATRIX}")
    print(f"Recommendations:    {RECOMMENDATIONS}")
    print(f"Output dir:         {OUTPUT_DIR}")
    print("-" * 90)

    router = read_required(ROUTER_EXPORT)
    eligibility = read_required(ELIGIBILITY_MATRIX)
    recommendations = read_required(RECOMMENDATIONS)

    print(f"Router rows loaded:          {len(router):,}")
    print(f"Eligibility rows loaded:     {len(eligibility):,}")
    print(f"Recommendation rows loaded:  {len(recommendations):,}")

    family_summary = build_allowed_blocked_summary(eligibility)
    recs = normalise_recommendations(recommendations)

    decision = router.merge(
        recs,
        on=["broker", "symbol", "timeframe"],
        how="left",
    )

    decision = decision.merge(
        family_summary,
        on=["broker", "symbol", "timeframe"],
        how="left",
    )

    for col in [
        "allowed_strategy_family_count",
        "blocked_strategy_family_count",
    ]:
        decision[col] = pd.to_numeric(decision[col], errors="coerce").fillna(0).astype(int)

    fill_cols = [
        "allowed_strategy_families_resolved",
        "preferred_strategy_families",
        "conditional_strategy_families",
        "blocked_strategy_families_resolved",
        "block_reasons_summary",
        "recommended_strategy_family",
        "recommended_strategy_eligibility",
        "recommended_router_action",
        "recommended_ftmo_suitability",
    ]

    for col in fill_cols:
        if col in decision.columns:
            decision[col] = decision[col].fillna("")

    decision["decision_status"] = decision.apply(build_decision_status, axis=1)
    decision["decision_commentary"] = decision.apply(build_decision_commentary, axis=1)

    compact_cols = [
        "broker",
        "symbol",
        "timeframe",
        "latest_timestamp",
        "current_regime",
        "regime_family",
        "regime_risk_band",
        "trade_permission",
        "risk_multiplier",
        "leverage_multiplier",
        "convexity_bias",
        "execution_mode",
        "control_source",
        "recommended_strategy_family",
        "recommended_strategy_eligibility",
        "recommended_router_action",
        "recommended_ftmo_suitability",
        "allowed_strategy_family_count",
        "blocked_strategy_family_count",
        "preferred_strategy_families",
        "conditional_strategy_families",
        "blocked_strategy_families_resolved",
        "decision_status",
        "decision_commentary",
    ]

    compact = decision[[c for c in compact_cols if c in decision.columns]].copy()

    compact = compact.sort_values(
        ["decision_status", "symbol", "timeframe"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    gbp_decisions = compact[
        compact["symbol"].str.contains("GBP", case=False, na=False)
    ].copy()

    defensive_decisions = compact[
        compact["decision_status"].str.contains("AMBER|RED", case=False, na=False)
    ].copy()

    clean_decisions = compact[
        compact["decision_status"].str.contains("GREEN", case=False, na=False)
    ].copy()

    summary = (
        compact.groupby(
            [
                "broker",
                "timeframe",
                "decision_status",
                "trade_permission",
                "regime_risk_band",
                "recommended_strategy_family",
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

    outputs = {
        "decision": OUTPUT_DIR / "strategy_router_decision_file_latest.csv",
        "compact": OUTPUT_DIR / "strategy_router_decision_compact_latest.csv",
        "gbp": OUTPUT_DIR / "strategy_router_decision_gbp_latest.csv",
        "defensive": OUTPUT_DIR / "strategy_router_decision_defensive_latest.csv",
        "clean": OUTPUT_DIR / "strategy_router_decision_clean_latest.csv",
        "summary": OUTPUT_DIR / "strategy_router_decision_summary_latest.csv",
        "json": OUTPUT_DIR / "strategy_router_decision_file_latest.json",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    decision.to_csv(outputs["decision"], index=False)
    compact.to_csv(outputs["compact"], index=False)
    gbp_decisions.to_csv(outputs["gbp"], index=False)
    defensive_decisions.to_csv(outputs["defensive"], index=False)
    clean_decisions.to_csv(outputs["clean"], index=False)
    summary.to_csv(outputs["summary"], index=False)

    decision.to_csv(timestamped["decision"], index=False)
    compact.to_csv(timestamped["compact"], index=False)
    gbp_decisions.to_csv(timestamped["gbp"], index=False)
    defensive_decisions.to_csv(timestamped["defensive"], index=False)
    clean_decisions.to_csv(timestamped["clean"], index=False)
    summary.to_csv(timestamped["summary"], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "router_rows": int(len(router)),
        "decision_rows": int(len(decision)),
        "compact_rows": int(len(compact)),
        "gbp_rows": int(len(gbp_decisions)),
        "defensive_rows": int(len(defensive_decisions)),
        "clean_rows": int(len(clean_decisions)),
        "decision_status_counts": compact["decision_status"].value_counts(dropna=False).to_dict(),
        "recommended_strategy_counts": compact["recommended_strategy_family"].value_counts(dropna=False).to_dict(),
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Review compact decisions. Next script can create a strategy-router daily report "
            "or integrate decisions with live/backtest strategy modules."
        ),
    }

    with outputs["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with timestamped["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Strategy router decision file created.")
    print(f"Decision rows:       {len(decision):,}")
    print(f"Compact rows:        {len(compact):,}")
    print(f"GBP rows:            {len(gbp_decisions):,}")
    print(f"Defensive rows:      {len(defensive_decisions):,}")
    print(f"Clean rows:          {len(clean_decisions):,}")
    print(f"Decision file:       {outputs['decision']}")
    print(f"Compact file:        {outputs['compact']}")
    print(f"Summary file:        {outputs['summary']}")
    print(f"JSON summary:        {outputs['json']}")
    print("=" * 90)


if __name__ == "__main__":
    main()