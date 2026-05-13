"""
BACQE Script 39
Current Regime Strategy Control Dashboard

Purpose:
- Extract the latest/current regime from each classified regime file
- Join current regimes to the strategy control matrix from Script 38
- Produce a current dashboard showing:
  - current regime
  - risk band
  - trade permission
  - risk/leverage multiplier
  - allowed/blocked strategy families
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

CLASSIFIED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "classified"
CONTROL_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_strategy_control"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_current_strategy_dashboard"

CONTROL_MATRIX = CONTROL_DIR / "regime_strategy_control_matrix_latest.csv"

TIMESTAMP_CANDIDATES = [
    "timestamp",
    "time",
    "datetime",
    "date",
    "bar_time",
    "open_time",
]

REGIME_CANDIDATES = [
    "composite_regime",
    "regime",
    "regime_state",
    "regime_class",
    "regime_name",
    "regime_label",
    "classified_regime",
    "market_regime",
    "final_regime",
    "primary_regime",
    "market_state",
    "state",
]

OPTIONAL_STATE_COLUMNS = [
    "trend_state",
    "volatility_state",
    "momentum_state",
    "trend_strength_state",
    "regime_confidence",
    "close",
    "rolling_vol_20",
    "atr_pct_14",
    "rsi_14",
    "adx_14",
]


def read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)


def list_classified_files() -> list[Path]:
    if not CLASSIFIED_DIR.exists():
        return []
    return sorted([p for p in CLASSIFIED_DIR.rglob("*.parquet") if p.is_file()])


def infer_metadata(path: Path) -> dict:
    try:
        rel = path.relative_to(CLASSIFIED_DIR)
        parts = rel.parts

        broker = parts[0] if len(parts) >= 3 else "unknown"
        timeframe = parts[1] if len(parts) >= 3 else "unknown"

        stem = path.stem
        symbol = stem

        for suffix in ["_classified", "_regimes"]:
            if symbol.endswith(suffix):
                symbol = symbol[: -len(suffix)]

        tf_suffix = f"_{timeframe}"
        if symbol.endswith(tf_suffix):
            symbol = symbol[: -len(tf_suffix)]

        return {
            "broker": broker,
            "timeframe": timeframe,
            "symbol": symbol,
        }

    except Exception:
        return {
            "broker": "unknown",
            "timeframe": "unknown",
            "symbol": path.stem,
        }


def find_first_existing(columns: list[str], candidates: list[str]) -> str | None:
    for col in candidates:
        if col in columns:
            return col
    return None


def extract_latest_regime(path: Path) -> dict:
    meta = infer_metadata(path)

    record = {
        **meta,
        "file_path": str(path),
        "status": "ok",
        "error": None,
        "rows": 0,
        "latest_timestamp": None,
        "current_regime": None,
        "source_timestamp_column": None,
        "source_regime_column": None,
    }

    try:
        df = pd.read_parquet(path)
        record["rows"] = len(df)

        if df.empty:
            record["status"] = "skipped"
            record["error"] = "empty_file"
            return record

        timestamp_col = find_first_existing(list(df.columns), TIMESTAMP_CANDIDATES)
        regime_col = find_first_existing(list(df.columns), REGIME_CANDIDATES)

        if timestamp_col is None:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index().rename(columns={"index": "timestamp"})
                timestamp_col = "timestamp"
            else:
                record["status"] = "skipped"
                record["error"] = "no_timestamp_column"
                return record

        if regime_col is None:
            record["status"] = "skipped"
            record["error"] = "no_regime_column"
            return record

        df = df.copy()
        df["_bacqe_timestamp"] = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
        df = df.dropna(subset=["_bacqe_timestamp"]).sort_values("_bacqe_timestamp")

        if df.empty:
            record["status"] = "skipped"
            record["error"] = "empty_after_timestamp_cleaning"
            return record

        latest = df.iloc[-1]

        record["latest_timestamp"] = latest["_bacqe_timestamp"].isoformat()
        record["current_regime"] = str(latest[regime_col])
        record["source_timestamp_column"] = timestamp_col
        record["source_regime_column"] = regime_col

        for col in OPTIONAL_STATE_COLUMNS:
            if col in df.columns:
                value = latest[col]
                record[col] = value

        return record

    except Exception as exc:
        record["status"] = "error"
        record["error"] = str(exc)
        return record


def clean_control_symbol(control: pd.DataFrame) -> pd.DataFrame:
    df = control.copy()

    if "symbol" not in df.columns:
        return df

    # Earlier summaries may contain names like GBPUSD_H1_regimes.
    # Normalise to GBPUSD for joining to classified-file metadata.
    def strip_suffix(row):
        symbol = str(row["symbol"])
        timeframe = str(row.get("timeframe", ""))

        for suffix in ["_classified", "_regimes"]:
            if symbol.endswith(suffix):
                symbol = symbol[: -len(suffix)]

        tf_suffix = f"_{timeframe}"
        if timeframe and symbol.endswith(tf_suffix):
            symbol = symbol[: -len(tf_suffix)]

        return symbol

    df["symbol_clean"] = df.apply(strip_suffix, axis=1)

    return df


def classify_dashboard_status(row) -> str:
    permission = str(row.get("trade_permission", "")).lower()
    risk_band = str(row.get("regime_risk_band", "")).lower()

    if permission == "avoid_or_convex_only" or risk_band == "extreme":
        return "RED"

    if permission == "restricted" or risk_band == "high":
        return "AMBER"

    if permission in {"allowed", "selective"}:
        return "GREEN"

    return "UNKNOWN"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE CURRENT REGIME STRATEGY CONTROL DASHBOARD")
    print("=" * 90)
    print(f"Project root:   {PROJECT_ROOT}")
    print(f"Classified dir: {CLASSIFIED_DIR}")
    print(f"Control matrix: {CONTROL_MATRIX}")
    print(f"Output dir:     {OUTPUT_DIR}")
    print("-" * 90)

    control = read_required(CONTROL_MATRIX)
    control = clean_control_symbol(control)

    files = list_classified_files()
    print(f"Classified files found: {len(files):,}")
    print(f"Control rows loaded:    {len(control):,}")

    latest_records = []

    for idx, path in enumerate(files, start=1):
        if idx == 1 or idx % 250 == 0:
            print(f"[LATEST] {idx}/{len(files)}: {path}")

        latest_records.append(extract_latest_regime(path))

    current = pd.DataFrame(latest_records)

    ok_current = current[current["status"].eq("ok")].copy()
    audit = current.copy()

    join_cols_left = ["broker", "timeframe", "symbol", "current_regime"]
    join_cols_right = ["broker", "timeframe", "symbol_clean", "regime"]

    dashboard = ok_current.merge(
        control,
        left_on=join_cols_left,
        right_on=join_cols_right,
        how="left",
        suffixes=("_current", "_control"),
    )

    dashboard["control_match"] = dashboard["trade_permission"].notna()

    dashboard["dashboard_status"] = dashboard.apply(classify_dashboard_status, axis=1)

    missing_control = dashboard[~dashboard["control_match"]].copy()

    dashboard_summary = (
        dashboard.groupby(
            [
                "broker",
                "timeframe",
                "dashboard_status",
                "trade_permission",
                "regime_risk_band",
                "execution_mode",
            ],
            dropna=False,
        )
        .agg(
            symbols=("symbol_current", "count"),
            avg_risk_score=("regime_risk_score", "mean"),
            avg_risk_multiplier=("risk_multiplier", "mean"),
            avg_leverage_multiplier=("leverage_multiplier", "mean"),
        )
        .reset_index()
    )

    for col in ["avg_risk_score", "avg_risk_multiplier", "avg_leverage_multiplier"]:
        dashboard_summary[col] = pd.to_numeric(
            dashboard_summary[col],
            errors="coerce",
        ).fillna(0).round(6)

    dashboard_summary = dashboard_summary.sort_values(
        ["dashboard_status", "timeframe", "symbols"],
        ascending=[True, True, False],
    )

    actionable = dashboard[
        dashboard["control_match"]
    ].copy()

    reduced_or_blocked = actionable[
        actionable["trade_permission"].isin(["restricted", "avoid_or_convex_only"])
    ].copy()

    allowed_or_selective = actionable[
        actionable["trade_permission"].isin(["allowed", "selective"])
    ].copy()

    leverage_dashboard_cols = [
        "broker",
        "timeframe",
        "symbol_current",
        "latest_timestamp",
        "current_regime",
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
        "dashboard_status",
        "control_commentary",
    ]

    available_leverage_cols = [c for c in leverage_dashboard_cols if c in dashboard.columns]
    leverage_dashboard = dashboard[available_leverage_cols].copy()

    outputs = {
        "dashboard": OUTPUT_DIR / "current_regime_strategy_control_dashboard_latest.csv",
        "summary": OUTPUT_DIR / "current_regime_strategy_control_summary_latest.csv",
        "audit": OUTPUT_DIR / "current_regime_strategy_control_file_audit_latest.csv",
        "missing_control": OUTPUT_DIR / "current_regime_strategy_control_missing_latest.csv",
        "reduced_or_blocked": OUTPUT_DIR / "current_regime_strategy_control_reduced_or_blocked_latest.csv",
        "allowed_or_selective": OUTPUT_DIR / "current_regime_strategy_control_allowed_or_selective_latest.csv",
        "leverage_dashboard": OUTPUT_DIR / "current_regime_leverage_dashboard_latest.csv",
    }

    timestamped = {
        key: path.with_name(path.stem.replace("_latest", f"_{run_ts}") + path.suffix)
        for key, path in outputs.items()
    }

    dashboard.to_csv(outputs["dashboard"], index=False)
    dashboard_summary.to_csv(outputs["summary"], index=False)
    audit.to_csv(outputs["audit"], index=False)
    missing_control.to_csv(outputs["missing_control"], index=False)
    reduced_or_blocked.to_csv(outputs["reduced_or_blocked"], index=False)
    allowed_or_selective.to_csv(outputs["allowed_or_selective"], index=False)
    leverage_dashboard.to_csv(outputs["leverage_dashboard"], index=False)

    dashboard.to_csv(timestamped["dashboard"], index=False)
    dashboard_summary.to_csv(timestamped["summary"], index=False)
    audit.to_csv(timestamped["audit"], index=False)
    missing_control.to_csv(timestamped["missing_control"], index=False)
    reduced_or_blocked.to_csv(timestamped["reduced_or_blocked"], index=False)
    allowed_or_selective.to_csv(timestamped["allowed_or_selective"], index=False)
    leverage_dashboard.to_csv(timestamped["leverage_dashboard"], index=False)

    status_counts = dashboard["dashboard_status"].value_counts(dropna=False).to_dict()
    permission_counts = dashboard["trade_permission"].fillna("missing_control").value_counts().to_dict()

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "classified_files_found": int(len(files)),
        "current_rows_ok": int(len(ok_current)),
        "current_rows_skipped_or_error": int(len(current) - len(ok_current)),
        "control_rows_loaded": int(len(control)),
        "dashboard_rows": int(len(dashboard)),
        "control_matches": int(dashboard["control_match"].sum()),
        "missing_control_rows": int((~dashboard["control_match"]).sum()),
        "reduced_or_blocked_rows": int(len(reduced_or_blocked)),
        "allowed_or_selective_rows": int(len(allowed_or_selective)),
        "status_counts": status_counts,
        "permission_counts": permission_counts,
        "output_dir": str(OUTPUT_DIR),
        "next_recommended_step": (
            "Inspect current leverage dashboard and reduced/blocked outputs. "
            "Next script can create a live watchlist/export for MT5 or strategy-router consumption."
        ),
    }

    json_latest = OUTPUT_DIR / "current_regime_strategy_control_dashboard_latest.json"
    json_ts = OUTPUT_DIR / f"current_regime_strategy_control_dashboard_{run_ts}.json"

    with json_latest.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    with json_ts.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Current regime strategy control dashboard created.")
    print(f"Dashboard rows:          {len(dashboard):,}")
    print(f"Control matches:         {int(dashboard['control_match'].sum()):,}")
    print(f"Missing control rows:    {int((~dashboard['control_match']).sum()):,}")
    print(f"Reduced/blocked rows:    {len(reduced_or_blocked):,}")
    print(f"Allowed/selective rows:  {len(allowed_or_selective):,}")
    print(f"Dashboard:               {outputs['dashboard']}")
    print(f"Leverage dashboard:      {outputs['leverage_dashboard']}")
    print(f"Summary:                 {outputs['summary']}")
    print(f"JSON summary:            {json_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()