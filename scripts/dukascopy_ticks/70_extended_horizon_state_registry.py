"""
BACQE DUKASCOPY EXTENDED HORIZONS 70
RESEARCH STATE REGISTRY

Purpose:
    Track extended-horizon research progress by symbol.

Stages tracked:
    EH01 target build
    EH02 feature discovery
    EH03 stability
    EH04 signal validation
    EH05 cost survival
    EH06 dynamic cost
    EH07 context conditioning
    EH08 regime edge
    EH09 regime replay
    EH10 Monte Carlo
    EH11 cross-symbol transfer
    EH12 cross-year stability
    EH13 candidate registry
"""

from pathlib import Path
import yaml
import pandas as pd


CONFIG_PATH = Path("config/dukascopy_research.yaml")

BASE_DIR = Path("E:/Quant_Lab")

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "research_state_registry"
)


STAGES = [
    {
        "stage_key": "EH01",
        "stage_name": "extended_horizon_targets",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "target_build",
        "pattern": "{symbol_lower}_extended_horizon_target_build_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH02",
        "stage_name": "feature_discovery",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "feature_discovery",
        "pattern": "{symbol_lower}_extended_horizon_feature_discovery_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH03",
        "stage_name": "stability_engine",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "stability_engine",
        "pattern": "{symbol_lower}_extended_horizon_stability_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH04",
        "stage_name": "signal_validation",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "signal_validation",
        "pattern": "{symbol_lower}_extended_horizon_signal_validation_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH05",
        "stage_name": "cost_survival",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "cost_survival",
        "pattern": "{symbol_lower}_extended_horizon_cost_survival_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH06",
        "stage_name": "dynamic_cost_survival",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "dynamic_cost_survival",
        "pattern": "{symbol_lower}_extended_horizon_dynamic_cost_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH07",
        "stage_name": "context_conditioning",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "context_conditioning",
        "pattern": "{symbol_lower}_extended_horizon_context_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH08",
        "stage_name": "regime_edge_engine",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "regime_edge_engine",
        "pattern": "{symbol_lower}_extended_horizon_regime_registry_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH09",
        "stage_name": "regime_replay",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "regime_replay",
        "pattern": "{symbol_lower}_extended_horizon_regime_replay_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
    {
        "stage_key": "EH10",
        "stage_name": "monte_carlo_robustness",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "monte_carlo_robustness",
        "pattern": "{symbol_lower}_extended_horizon_monte_carlo_ranked_latest.csv",
        "completion_type": "rows_positive",
    },
]


GLOBAL_STAGES = [
    {
        "stage_key": "EH11",
        "stage_name": "cross_symbol_transfer",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "cross_symbol_transfer",
        "pattern_contains": "cross_symbol_transfer_ranked_latest.csv",
    },
    {
        "stage_key": "EH12",
        "stage_name": "cross_year_stability",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "cross_year_stability",
        "pattern_contains": "cross_year_stability_ranked_latest.csv",
    },
    {
        "stage_key": "EH13",
        "stage_name": "candidate_registry",
        "path": BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "candidate_registry",
        "pattern_contains": "candidate_registry_latest.csv",
    },
]


def load_symbols() -> list[str]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)["dukascopy_research"]

    return [symbol.upper() for symbol in cfg["symbols"]]


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0

    try:
        return len(pd.read_csv(path))
    except Exception:
        return 0


def stage_status(path: Path, completion_type: str) -> tuple[str, int]:
    if not path.exists():
        return "missing", 0

    rows = count_csv_rows(path)

    if completion_type == "rows_positive":
        if rows > 0:
            return "complete", rows
        return "empty", rows

    return "present", rows


def build_symbol_stage_registry(symbols: list[str]) -> pd.DataFrame:
    rows = []

    for symbol in symbols:
        symbol_lower = symbol.lower()

        for stage in STAGES:
            filename = stage["pattern"].format(symbol_lower=symbol_lower)
            path = stage["path"] / filename

            status, rows_count = stage_status(
                path=path,
                completion_type=stage["completion_type"],
            )

            rows.append(
                {
                    "symbol": symbol,
                    "stage_key": stage["stage_key"],
                    "stage_name": stage["stage_name"],
                    "stage_status": status,
                    "rows": rows_count,
                    "file_path": str(path),
                }
            )

    return pd.DataFrame(rows)


def build_symbol_summary(registry: pd.DataFrame) -> pd.DataFrame:
    summary = (
        registry.groupby("symbol", dropna=False)
        .agg(
            stages_tracked=("stage_key", "count"),
            stages_complete=("stage_status", lambda x: int((x == "complete").sum())),
            stages_missing=("stage_status", lambda x: int((x == "missing").sum())),
            stages_empty=("stage_status", lambda x: int((x == "empty").sum())),
        )
        .reset_index()
    )

    summary["completion_pct"] = (
        summary["stages_complete"] / summary["stages_tracked"] * 100
    ).round(2)

    summary["next_missing_stage"] = summary["symbol"].apply(
        lambda symbol: find_next_missing_stage(registry, symbol)
    )

    summary["research_status"] = summary.apply(
        lambda row: (
            "complete"
            if row["stages_complete"] == row["stages_tracked"]
            else "in_progress"
            if row["stages_complete"] > 0
            else "not_started"
        ),
        axis=1,
    )

    return summary.sort_values(
        by=["research_status", "completion_pct", "symbol"],
        ascending=[True, False, True],
    )


def find_next_missing_stage(registry: pd.DataFrame, symbol: str) -> str:
    subset = registry[registry["symbol"] == symbol].sort_values("stage_key")

    for _, row in subset.iterrows():
        if row["stage_status"] != "complete":
            return row["stage_key"]

    return "none"


def build_global_stage_registry() -> pd.DataFrame:
    rows = []

    for stage in GLOBAL_STAGES:
        root = stage["path"]

        if not root.exists():
            matches = []
        else:
            matches = [
                path
                for path in root.rglob("*.csv")
                if stage["pattern_contains"] in path.name
            ]

        rows.append(
            {
                "stage_key": stage["stage_key"],
                "stage_name": stage["stage_name"],
                "stage_status": "complete" if matches else "missing",
                "files_found": len(matches),
                "example_file": str(matches[0]) if matches else "",
            }
        )

    return pd.DataFrame(rows)


def write_outputs(
    registry: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    global_registry: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    registry_path = REPORT_ROOT / "extended_horizon_research_state_registry_latest.csv"
    summary_path = REPORT_ROOT / "extended_horizon_research_symbol_summary_latest.csv"
    global_path = REPORT_ROOT / "extended_horizon_research_global_stage_registry_latest.csv"
    report_path = REPORT_ROOT / "extended_horizon_research_state_report_latest.txt"

    registry.to_csv(registry_path, index=False)
    symbol_summary.to_csv(summary_path, index=False)
    global_registry.to_csv(global_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS 70 - RESEARCH STATE REGISTRY\n")
        f.write("=" * 90 + "\n\n")

        f.write("SYMBOL SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(symbol_summary.to_string(index=False))
        f.write("\n\n")

        f.write("GLOBAL STAGES\n")
        f.write("-" * 90 + "\n")
        f.write(global_registry.to_string(index=False))
        f.write("\n\n")

        f.write("FULL REGISTRY\n")
        f.write("-" * 90 + "\n")
        f.write(registry.to_string(index=False))

    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS 70 - RESEARCH STATE REGISTRY")
    print("=" * 90)
    print("SYMBOL SUMMARY")
    print("-" * 90)
    print(symbol_summary.to_string(index=False))
    print("-" * 90)
    print("GLOBAL STAGES")
    print("-" * 90)
    print(global_registry.to_string(index=False))
    print("-" * 90)
    print(f"Registry:       {registry_path}")
    print(f"Symbol summary: {summary_path}")
    print(f"Global stages:  {global_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


def main() -> None:
    symbols = load_symbols()

    registry = build_symbol_stage_registry(symbols)
    symbol_summary = build_symbol_summary(registry)
    global_registry = build_global_stage_registry()

    write_outputs(
        registry=registry,
        symbol_summary=symbol_summary,
        global_registry=global_registry,
    )


if __name__ == "__main__":
    main()