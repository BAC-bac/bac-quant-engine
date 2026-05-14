"""
BACQE Script 46
Strategy Router Decision Reporter

Purpose:
- Create a readable command-centre report from Script 45 decision files.
- Summarise:
  - overall decision status
  - GBP focus
  - defensive / capital preservation warnings
  - clean strategy opportunities
  - strategy family counts
  - next integration steps

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_router_decisions"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "strategy_router_decision_reports"

DECISION_COMPACT = INPUT_DIR / "strategy_router_decision_compact_latest.csv"
DECISION_SUMMARY = INPUT_DIR / "strategy_router_decision_summary_latest.csv"


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


def line_item(row: pd.Series) -> str:
    return (
        f"- {row.get('symbol')} {row.get('timeframe')}: "
        f"{row.get('current_regime')} | "
        f"{row.get('trade_permission')} | "
        f"risk={row.get('risk_multiplier')} | "
        f"{row.get('recommended_strategy_family')} | "
        f"{row.get('decision_status')}"
    )


def build_report(decisions: pd.DataFrame, summary: pd.DataFrame) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")

    status_counts = decisions["decision_status"].value_counts(dropna=False).to_dict()
    strategy_counts = decisions["recommended_strategy_family"].value_counts(dropna=False).to_dict()
    permission_counts = decisions["trade_permission"].value_counts(dropna=False).to_dict()

    gbp = decisions[decisions["symbol"].str.contains("GBP", case=False, na=False)].copy()
    defensive = decisions[decisions["decision_status"].str.contains("AMBER|RED", case=False, na=False)].copy()
    clean = decisions[decisions["decision_status"].str.contains("GREEN", case=False, na=False)].copy()

    clean = clean.sort_values(
        ["risk_multiplier", "symbol", "timeframe"],
        ascending=[False, True, True],
    )

    defensive = defensive.sort_values(
        ["risk_multiplier", "symbol", "timeframe"],
        ascending=[True, True, True],
    )

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE STRATEGY ROUTER DECISION REPORT")
    lines.append("=" * 90)
    lines.append(f"Generated at: {generated_at}")
    lines.append("")
    lines.append("OVERALL DECISION STATUS")
    lines.append(f"Total decisions:     {len(decisions):,}")
    lines.append(f"Decision statuses:   {status_counts}")
    lines.append(f"Trade permissions:   {permission_counts}")
    lines.append(f"Strategy families:   {strategy_counts}")
    lines.append("")
    lines.append("GBP FOCUS")
    if gbp.empty:
        lines.append("- No GBP-related decisions found.")
    else:
        for _, row in gbp.head(80).iterrows():
            lines.append(line_item(row))
    lines.append("")
    lines.append("DEFENSIVE / CAPITAL PRESERVATION WATCHLIST")
    if defensive.empty:
        lines.append("- No defensive decisions found.")
    else:
        for _, row in defensive.head(80).iterrows():
            lines.append(line_item(row))
    lines.append("")
    lines.append("CLEAN STRATEGY OPPORTUNITIES")
    if clean.empty:
        lines.append("- No clean strategy decisions found.")
    else:
        for _, row in clean.head(80).iterrows():
            lines.append(line_item(row))
    lines.append("")
    lines.append("STRATEGY FAMILY SUMMARY")
    for strategy, count in strategy_counts.items():
        lines.append(f"- {strategy}: {count}")
    lines.append("")
    lines.append("NEXT INTEGRATION STEPS")
    lines.append("1. Rerun the full regime refresh after Sunday evening's full build.")
    lines.append("2. Rerun scripts 16 through 46 to refresh the full decision layer.")
    lines.append("3. Promote Script 45 output as the official BACQE strategy-router input.")
    lines.append("4. Connect real strategy modules to the strategy_family names.")
    lines.append("5. Add strategy-level permission checks before any backtest or live execution.")
    lines.append("6. Add risk sizing using risk_multiplier and leverage_multiplier.")
    lines.append("")
    lines.append("RETURN POINT")
    lines.append(
        "When returning to BACQE, start by running the regime ops suite, then refresh the "
        "strategy-router decision chain from Scripts 34 to 46."
    )
    lines.append("=" * 90)

    return "\n".join(lines)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE STRATEGY ROUTER DECISION REPORTER")
    print("=" * 90)
    print(f"Project root:  {PROJECT_ROOT}")
    print(f"Decision file: {DECISION_COMPACT}")
    print(f"Output dir:    {OUTPUT_DIR}")
    print("-" * 90)

    decisions = read_required(DECISION_COMPACT)
    summary = read_required(DECISION_SUMMARY)

    decisions = safe_numeric(decisions, ["risk_multiplier", "leverage_multiplier"])

    report_text = build_report(decisions, summary)

    gbp = decisions[decisions["symbol"].str.contains("GBP", case=False, na=False)].copy()
    defensive = decisions[decisions["decision_status"].str.contains("AMBER|RED", case=False, na=False)].copy()
    clean = decisions[decisions["decision_status"].str.contains("GREEN", case=False, na=False)].copy()

    outputs = {
        "txt": OUTPUT_DIR / "strategy_router_decision_report_latest.txt",
        "json": OUTPUT_DIR / "strategy_router_decision_report_latest.json",
        "gbp": OUTPUT_DIR / "strategy_router_decision_report_gbp_latest.csv",
        "defensive": OUTPUT_DIR / "strategy_router_decision_report_defensive_latest.csv",
        "clean": OUTPUT_DIR / "strategy_router_decision_report_clean_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    outputs["txt"].write_text(report_text, encoding="utf-8")
    timestamped["txt"].write_text(report_text, encoding="utf-8")

    gbp.to_csv(outputs["gbp"], index=False)
    defensive.to_csv(outputs["defensive"], index=False)
    clean.to_csv(outputs["clean"], index=False)

    gbp.to_csv(timestamped["gbp"], index=False)
    defensive.to_csv(timestamped["defensive"], index=False)
    clean.to_csv(timestamped["clean"], index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "decision_rows": int(len(decisions)),
        "gbp_rows": int(len(gbp)),
        "defensive_rows": int(len(defensive)),
        "clean_rows": int(len(clean)),
        "decision_status_counts": decisions["decision_status"].value_counts(dropna=False).to_dict(),
        "trade_permission_counts": decisions["trade_permission"].value_counts(dropna=False).to_dict(),
        "recommended_strategy_counts": decisions["recommended_strategy_family"].value_counts(dropna=False).to_dict(),
        "output_dir": str(OUTPUT_DIR),
        "text_report": str(outputs["txt"]),
        "return_point": (
            "After the Sunday full regime build, rerun Scripts 16-46. "
            "Then begin integration by promoting Script 45 compact output as the official strategy-router input."
        ),
    }

    with outputs["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with timestamped["json"].open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("[DONE] Strategy router decision report created.")
    print(f"Decision rows:  {len(decisions):,}")
    print(f"GBP rows:       {len(gbp):,}")
    print(f"Defensive rows: {len(defensive):,}")
    print(f"Clean rows:     {len(clean):,}")
    print(f"TXT report:     {outputs['txt']}")
    print(f"JSON report:    {outputs['json']}")
    print("=" * 90)


if __name__ == "__main__":
    main()