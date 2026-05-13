"""
BACQE Script 41
Live Strategy Router Export

Purpose:
- Convert resolved current regime strategy controls into a clean router-ready export
- Produce compact files for downstream strategy/router/EA/dashboard consumption
- Keep full audit trail while giving live systems a simple schema

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_current_strategy_dashboard_resolved"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_router_export"

RESOLVED_LEVERAGE = INPUT_DIR / "current_regime_leverage_dashboard_resolved_latest.csv"
RESOLVED_SUMMARY = INPUT_DIR / "current_regime_control_coverage_summary_latest.csv"


ROUTER_COLUMNS = [
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
    "allowed_strategy_families",
    "blocked_strategy_families",
    "stop_loss_profile",
    "trailing_stop_profile",
    "dashboard_status",
    "control_source",
]


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def normalise_router_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "symbol_current" in out.columns:
        out["symbol"] = out["symbol_current"]
    elif "symbol" not in out.columns:
        out["symbol"] = ""

    if "dashboard_status_resolved" in out.columns:
        out["dashboard_status"] = out["dashboard_status_resolved"]
    elif "dashboard_status" not in out.columns:
        out["dashboard_status"] = "UNKNOWN"

    numeric_cols = ["risk_multiplier", "leverage_multiplier", "regime_confidence"]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ROUTER_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    router = out[ROUTER_COLUMNS].copy()

    router["symbol"] = router["symbol"].astype(str)
    router["timeframe"] = router["timeframe"].astype(str)
    router["current_regime"] = router["current_regime"].astype(str)
    router["trade_permission"] = router["trade_permission"].astype(str)
    router["regime_risk_band"] = router["regime_risk_band"].astype(str)

    router = router.sort_values(
        ["dashboard_status", "timeframe", "symbol"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    return router


def build_router_subsets(router: pd.DataFrame) -> dict[str, pd.DataFrame]:
    subsets = {}

    subsets["green_allowed"] = router[
        router["trade_permission"].isin(["allowed", "selective"])
    ].copy()

    subsets["amber_restricted"] = router[
        router["trade_permission"].eq("restricted")
    ].copy()

    subsets["red_avoid_or_convex"] = router[
        router["trade_permission"].eq("avoid_or_convex_only")
    ].copy()

    subsets["defensive_watchlist"] = router[
        router["execution_mode"].isin([
            "defensive",
            "capital_preservation",
            "conservative_default",
            "defensive_confirmation_required",
        ])
    ].copy()

    subsets["gbp_related"] = router[
        router["symbol"].str.contains("GBP", case=False, na=False)
    ].copy()

    return subsets


def build_router_summary(router: pd.DataFrame) -> pd.DataFrame:
    summary = (
        router.groupby(
            [
                "broker",
                "timeframe",
                "dashboard_status",
                "trade_permission",
                "regime_risk_band",
                "execution_mode",
                "control_source",
            ],
            dropna=False,
        )
        .agg(
            symbols=("symbol", "count"),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
        )
        .reset_index()
    )

    for col in ["avg_risk_multiplier", "avg_leverage_multiplier"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).round(6)

    return summary.sort_values(
        ["dashboard_status", "timeframe", "symbols"],
        ascending=[True, True, False],
    ).reset_index(drop=True)


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    clean = df.copy()
    clean = clean.where(pd.notna(clean), None)
    return clean.to_dict(orient="records")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE LIVE STRATEGY ROUTER EXPORT")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input file:   {RESOLVED_LEVERAGE}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    resolved = read_required(RESOLVED_LEVERAGE)
    router = normalise_router_frame(resolved)
    subsets = build_router_subsets(router)
    summary = build_router_summary(router)

    print(f"Resolved rows loaded: {len(resolved):,}")
    print(f"Router rows created:  {len(router):,}")

    outputs = {
        "router_csv": OUTPUT_DIR / "live_strategy_router_export_latest.csv",
        "router_json": OUTPUT_DIR / "live_strategy_router_export_latest.json",
        "summary_csv": OUTPUT_DIR / "live_strategy_router_summary_latest.csv",
        "green_allowed": OUTPUT_DIR / "live_strategy_router_green_allowed_latest.csv",
        "amber_restricted": OUTPUT_DIR / "live_strategy_router_amber_restricted_latest.csv",
        "red_avoid_or_convex": OUTPUT_DIR / "live_strategy_router_red_avoid_or_convex_latest.csv",
        "defensive_watchlist": OUTPUT_DIR / "live_strategy_router_defensive_watchlist_latest.csv",
        "gbp_related": OUTPUT_DIR / "live_strategy_router_gbp_related_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    router.to_csv(outputs["router_csv"], index=False)
    summary.to_csv(outputs["summary_csv"], index=False)

    subsets["green_allowed"].to_csv(outputs["green_allowed"], index=False)
    subsets["amber_restricted"].to_csv(outputs["amber_restricted"], index=False)
    subsets["red_avoid_or_convex"].to_csv(outputs["red_avoid_or_convex"], index=False)
    subsets["defensive_watchlist"].to_csv(outputs["defensive_watchlist"], index=False)
    subsets["gbp_related"].to_csv(outputs["gbp_related"], index=False)

    router.to_csv(timestamped["router_csv"], index=False)
    summary.to_csv(timestamped["summary_csv"], index=False)

    subsets["green_allowed"].to_csv(timestamped["green_allowed"], index=False)
    subsets["amber_restricted"].to_csv(timestamped["amber_restricted"], index=False)
    subsets["red_avoid_or_convex"].to_csv(timestamped["red_avoid_or_convex"], index=False)
    subsets["defensive_watchlist"].to_csv(timestamped["defensive_watchlist"], index=False)
    subsets["gbp_related"].to_csv(timestamped["gbp_related"], index=False)

    router_json_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema_version": "bacqe_strategy_router_export_v1",
        "row_count": int(len(router)),
        "records": dataframe_to_records(router),
    }

    with outputs["router_json"].open("w", encoding="utf-8") as f:
        json.dump(router_json_payload, f, indent=2, default=str)

    with timestamped["router_json"].open("w", encoding="utf-8") as f:
        json.dump(router_json_payload, f, indent=2, default=str)

    status_counts = router["dashboard_status"].value_counts(dropna=False).to_dict()
    permission_counts = router["trade_permission"].value_counts(dropna=False).to_dict()
    source_counts = router["control_source"].value_counts(dropna=False).to_dict()

    overall_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema_version": "bacqe_strategy_router_export_summary_v1",
        "router_rows": int(len(router)),
        "green_allowed_rows": int(len(subsets["green_allowed"])),
        "amber_restricted_rows": int(len(subsets["amber_restricted"])),
        "red_avoid_or_convex_rows": int(len(subsets["red_avoid_or_convex"])),
        "defensive_watchlist_rows": int(len(subsets["defensive_watchlist"])),
        "gbp_related_rows": int(len(subsets["gbp_related"])),
        "status_counts": status_counts,
        "permission_counts": permission_counts,
        "source_counts": source_counts,
        "router_csv": str(outputs["router_csv"]),
        "router_json": str(outputs["router_json"]),
        "summary_csv": str(outputs["summary_csv"]),
        "next_recommended_step": (
            "Inspect router export subsets. "
            "Next script can create a human-readable watchlist/status report from this router export."
        ),
    }

    overall_latest = OUTPUT_DIR / "live_strategy_router_export_status_latest.json"
    overall_ts = OUTPUT_DIR / f"live_strategy_router_export_status_{run_ts}.json"

    with overall_latest.open("w", encoding="utf-8") as f:
        json.dump(overall_payload, f, indent=4, default=str)

    with overall_ts.open("w", encoding="utf-8") as f:
        json.dump(overall_payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Live strategy router export created.")
    print(f"Router rows:             {len(router):,}")
    print(f"Green allowed/selective: {len(subsets['green_allowed']):,}")
    print(f"Amber restricted:        {len(subsets['amber_restricted']):,}")
    print(f"Red avoid/convex-only:   {len(subsets['red_avoid_or_convex']):,}")
    print(f"Defensive watchlist:     {len(subsets['defensive_watchlist']):,}")
    print(f"GBP-related rows:        {len(subsets['gbp_related']):,}")
    print(f"Router CSV:              {outputs['router_csv']}")
    print(f"Router JSON:             {outputs['router_json']}")
    print(f"Summary CSV:             {outputs['summary_csv']}")
    print(f"Status JSON:             {overall_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()