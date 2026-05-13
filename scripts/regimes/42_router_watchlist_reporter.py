"""
BACQE Script 42
Router Watchlist Reporter

Purpose:
- Create a human-readable watchlist report from Script 41 router export
- Summarise:
  - GREEN allowed/selective symbols
  - AMBER restricted symbols
  - RED avoid/convex-only symbols
  - GBP-related controls
  - defensive watchlist
  - cleanest current trading environments

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_router_export"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_router_watchlist"

ROUTER_EXPORT = INPUT_DIR / "live_strategy_router_export_latest.csv"
ROUTER_SUMMARY = INPUT_DIR / "live_strategy_router_summary_latest.csv"


FOCUS_SYMBOLS = [
    "GBPUSD",
    "EURGBP",
    "GBPJPY",
    "GBPCHF",
    "GBPAUD",
    "GBPCAD",
    "GBPNZD",
    "EURUSD",
    "USDJPY",
    "XAUUSD",
    "US500.cash",
    "US100.cash",
    "GER40.cash",
    "UK100.cash",
]


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def safe_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    return out


def build_focus_watchlist(router: pd.DataFrame) -> pd.DataFrame:
    focus = router[router["symbol"].isin(FOCUS_SYMBOLS)].copy()

    if focus.empty:
        return focus

    focus["focus_rank"] = focus["symbol"].apply(
        lambda x: FOCUS_SYMBOLS.index(x) if x in FOCUS_SYMBOLS else 999
    )

    return focus.sort_values(
        ["focus_rank", "timeframe"]
    ).drop(columns=["focus_rank"]).reset_index(drop=True)


def build_clean_trade_candidates(router: pd.DataFrame) -> pd.DataFrame:
    clean = router[
        (router["trade_permission"].isin(["allowed", "selective"]))
        & (router["regime_risk_band"].isin(["low", "medium"]))
        & (router["risk_multiplier"] >= 0.75)
    ].copy()

    return clean.sort_values(
        ["risk_multiplier", "leverage_multiplier", "symbol", "timeframe"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)


def build_defensive_watchlist(router: pd.DataFrame) -> pd.DataFrame:
    defensive = router[
        (
            router["trade_permission"].isin(["restricted", "avoid_or_convex_only"])
            | router["execution_mode"].isin([
                "defensive",
                "capital_preservation",
                "conservative_default",
                "defensive_confirmation_required",
            ])
            | router["regime_risk_band"].isin(["high", "extreme"])
        )
    ].copy()

    return defensive.sort_values(
        ["regime_risk_band", "risk_multiplier", "symbol", "timeframe"],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)


def build_symbol_snapshot(router: pd.DataFrame) -> pd.DataFrame:
    snapshot = (
        router.groupby(["symbol"], dropna=False)
        .agg(
            timeframes=("timeframe", "count"),
            allowed_or_selective=(
                "trade_permission",
                lambda x: int(pd.Series(x).isin(["allowed", "selective"]).sum()),
            ),
            restricted_or_red=(
                "trade_permission",
                lambda x: int(pd.Series(x).isin(["restricted", "avoid_or_convex_only"]).sum()),
            ),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            min_risk_multiplier=("risk_multiplier", "min"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
            min_leverage_multiplier=("leverage_multiplier", "min"),
            dominant_status=("dashboard_status", lambda x: pd.Series(x).mode().iloc[0] if not pd.Series(x).mode().empty else "UNKNOWN"),
        )
        .reset_index()
    )

    for col in [
        "avg_risk_multiplier",
        "min_risk_multiplier",
        "avg_leverage_multiplier",
        "min_leverage_multiplier",
    ]:
        snapshot[col] = pd.to_numeric(snapshot[col], errors="coerce").fillna(0).round(4)

    return snapshot.sort_values(
        ["restricted_or_red", "min_risk_multiplier", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def line_item(row: pd.Series) -> str:
    return (
        f"- {row.get('symbol')} {row.get('timeframe')}: "
        f"{row.get('current_regime')} | "
        f"{row.get('regime_risk_band')} | "
        f"{row.get('trade_permission')} | "
        f"risk={row.get('risk_multiplier')} | "
        f"lev={row.get('leverage_multiplier')} | "
        f"{row.get('execution_mode')}"
    )


def build_text_report(router: pd.DataFrame, summary: pd.DataFrame, subsets: dict[str, pd.DataFrame]) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")

    status_counts = router["dashboard_status"].value_counts(dropna=False).to_dict()
    permission_counts = router["trade_permission"].value_counts(dropna=False).to_dict()
    source_counts = router["control_source"].value_counts(dropna=False).to_dict()

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE ROUTER WATCHLIST REPORT")
    lines.append("=" * 90)
    lines.append(f"Generated at: {generated_at}")
    lines.append("")
    lines.append("ROUTER STATUS")
    lines.append(f"Total router rows: {len(router):,}")
    lines.append(f"Status counts:     {status_counts}")
    lines.append(f"Permission counts: {permission_counts}")
    lines.append(f"Control sources:   {source_counts}")
    lines.append("")

    lines.append("GBP FOCUS")
    if subsets["gbp_focus"].empty:
        lines.append("- No GBP-related rows found.")
    else:
        for _, row in subsets["gbp_focus"].head(40).iterrows():
            lines.append(line_item(row))
    lines.append("")

    lines.append("TOP DEFENSIVE WARNINGS")
    if subsets["defensive"].empty:
        lines.append("- No defensive warnings.")
    else:
        for _, row in subsets["defensive"].head(40).iterrows():
            lines.append(line_item(row))
    lines.append("")

    lines.append("CLEAN TRADE CANDIDATES")
    if subsets["clean"].empty:
        lines.append("- No clean candidates found.")
    else:
        for _, row in subsets["clean"].head(40).iterrows():
            lines.append(line_item(row))
    lines.append("")

    lines.append("AMBER RESTRICTED")
    if subsets["amber"].empty:
        lines.append("- No amber restricted rows.")
    else:
        for _, row in subsets["amber"].head(40).iterrows():
            lines.append(line_item(row))
    lines.append("")

    lines.append("RED AVOID / CONVEX ONLY")
    if subsets["red"].empty:
        lines.append("- No red avoid/convex-only rows.")
    else:
        for _, row in subsets["red"].head(40).iterrows():
            lines.append(line_item(row))
    lines.append("")

    lines.append("RECOMMENDED NEXT STEP")
    lines.append(
        "Use this report as the human-readable regime-control watchlist. "
        "The next script can define an explicit strategy family registry and map real BACQE strategies "
        "to the router permissions."
    )
    lines.append("=" * 90)

    return "\n".join(lines)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE ROUTER WATCHLIST REPORTER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Router file:  {ROUTER_EXPORT}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    router = read_required(ROUTER_EXPORT)
    summary = read_required(ROUTER_SUMMARY)

    router = safe_numeric(router, ["risk_multiplier", "leverage_multiplier"])

    print(f"Router rows loaded:  {len(router):,}")
    print(f"Summary rows loaded: {len(summary):,}")

    subsets = {
        "gbp_focus": router[router["symbol"].str.contains("GBP", case=False, na=False)].copy(),
        "focus": build_focus_watchlist(router),
        "clean": build_clean_trade_candidates(router),
        "defensive": build_defensive_watchlist(router),
        "amber": router[router["trade_permission"].eq("restricted")].copy(),
        "red": router[router["trade_permission"].eq("avoid_or_convex_only")].copy(),
    }

    symbol_snapshot = build_symbol_snapshot(router)

    text_report = build_text_report(router, summary, subsets)

    outputs = {
        "txt": OUTPUT_DIR / "router_watchlist_report_latest.txt",
        "json": OUTPUT_DIR / "router_watchlist_report_latest.json",
        "focus": OUTPUT_DIR / "router_watchlist_focus_symbols_latest.csv",
        "gbp_focus": OUTPUT_DIR / "router_watchlist_gbp_focus_latest.csv",
        "clean": OUTPUT_DIR / "router_watchlist_clean_trade_candidates_latest.csv",
        "defensive": OUTPUT_DIR / "router_watchlist_defensive_latest.csv",
        "amber": OUTPUT_DIR / "router_watchlist_amber_restricted_latest.csv",
        "red": OUTPUT_DIR / "router_watchlist_red_avoid_or_convex_latest.csv",
        "symbol_snapshot": OUTPUT_DIR / "router_watchlist_symbol_snapshot_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    outputs["txt"].write_text(text_report, encoding="utf-8")
    timestamped["txt"].write_text(text_report, encoding="utf-8")

    subsets["focus"].to_csv(outputs["focus"], index=False)
    subsets["gbp_focus"].to_csv(outputs["gbp_focus"], index=False)
    subsets["clean"].to_csv(outputs["clean"], index=False)
    subsets["defensive"].to_csv(outputs["defensive"], index=False)
    subsets["amber"].to_csv(outputs["amber"], index=False)
    subsets["red"].to_csv(outputs["red"], index=False)
    symbol_snapshot.to_csv(outputs["symbol_snapshot"], index=False)

    subsets["focus"].to_csv(timestamped["focus"], index=False)
    subsets["gbp_focus"].to_csv(timestamped["gbp_focus"], index=False)
    subsets["clean"].to_csv(timestamped["clean"], index=False)
    subsets["defensive"].to_csv(timestamped["defensive"], index=False)
    subsets["amber"].to_csv(timestamped["amber"], index=False)
    subsets["red"].to_csv(timestamped["red"], index=False)
    symbol_snapshot.to_csv(timestamped["symbol_snapshot"], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "router_rows": int(len(router)),
        "gbp_focus_rows": int(len(subsets["gbp_focus"])),
        "focus_rows": int(len(subsets["focus"])),
        "clean_candidate_rows": int(len(subsets["clean"])),
        "defensive_rows": int(len(subsets["defensive"])),
        "amber_rows": int(len(subsets["amber"])),
        "red_rows": int(len(subsets["red"])),
        "symbol_snapshot_rows": int(len(symbol_snapshot)),
        "status_counts": router["dashboard_status"].value_counts(dropna=False).to_dict(),
        "permission_counts": router["trade_permission"].value_counts(dropna=False).to_dict(),
        "output_dir": str(OUTPUT_DIR),
        "text_report": str(outputs["txt"]),
        "next_recommended_step": (
            "Review the watchlist report. Next script can define the strategy family registry "
            "and map real BACQE strategy modules to router permissions."
        ),
    }

    with outputs["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with timestamped["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Router watchlist report created.")
    print(f"GBP focus rows:        {len(subsets['gbp_focus']):,}")
    print(f"Clean candidate rows:  {len(subsets['clean']):,}")
    print(f"Defensive rows:        {len(subsets['defensive']):,}")
    print(f"Amber rows:            {len(subsets['amber']):,}")
    print(f"Red rows:              {len(subsets['red']):,}")
    print(f"TXT report:            {outputs['txt']}")
    print(f"JSON summary:          {outputs['json']}")
    print("=" * 90)


if __name__ == "__main__":
    main()