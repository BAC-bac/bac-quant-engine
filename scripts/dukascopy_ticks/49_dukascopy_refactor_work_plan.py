"""
BACQE DUKASCOPY 49 - REFACTOR WORK PLAN

Purpose:
    Read Script 48 refactor audit output and produce an ordered,
    practical work plan for hardening the Dukascopy branch.
"""

from pathlib import Path
import pandas as pd


AUDIT_PATH = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_refactor_audit\audit_tables\dukascopy_refactor_audit_latest.csv"
)

OUTPUT_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_refactor_work_plan")


BATCH_RULES = {
    "batch_01_core_ingestion": [
        "07_download_dukascopy_date_range.py",
        "08_normalise_dukascopy_date_range.py",
        "42_dukascopy_config_audit.py",
        "43_dukascopy_symbol_inventory.py",
        "44_dukascopy_symbol_download_plan.py",
        "45_dukascopy_batch_downloader.py",
        "46_dukascopy_trading_day_plan.py",
        "47_dukascopy_failed_hour_retry.py",
    ],
    "batch_02_feature_pipeline": [
        "22_feature_discovery_engine.py",
        "23_build_engineered_tick_features.py",
        "24_feature_aggregation_stability_engine.py",
        "30_horizon_expansion_engine.py",
        "31_horizon_signal_validation_engine.py",
    ],
    "batch_03_signal_replay_cost": [
        "25_signal_validation_engine.py",
        "26_signal_forensics_engine.py",
        "27_signal_filter_optimizer.py",
        "28_candidate_replay_engine.py",
        "29_cost_survival_engine.py",
        "32_horizon_candidate_replay.py",
        "33_horizon_cost_survival_engine.py",
    ],
    "batch_04_context_validation_monte_carlo": [
        "34_horizon_context_optimizer.py",
        "35_horizon_context_replay.py",
        "36_context_oos_validation.py",
        "37_walk_forward_validation_engine.py",
        "38_market_structure_investigation.py",
        "39_weekend_gap_research.py",
        "40_conditional_context_engine.py",
        "41_monte_carlo_stability_engine.py",
    ],
    "batch_05_legacy_and_support": [
        "01_dukascopy_schema_spec.py",
        "02_download_dukascopy_one_day.py",
        "03_inspect_dukascopy_bi5_files.py",
        "04_normalise_dukascopy_day_to_parquet.py",
        "05_build_dukascopy_tick_bars.py",
        "06_build_dukascopy_tick_imbalance_bars.py",
        "07b_audit_download_report.py",
        "09_build_dukascopy_tick_bars_date_range.py",
        "10_build_dukascopy_tibs_date_range.py",
        "11_audit_dukascopy_range_outputs.py",
        "12_inventory_script48_eurusd_candidates.py",
        "13_prepare_dukascopy_replay_spec.py",
        "14_map_replay_spec_to_dukascopy_data.py",
        "15_replay_primary_candidates_on_dukascopy_tibs.py",
        "16_diagnose_replay_signal_pressure.py",
        "17_replay_dominant_pressure_candidates.py",
        "18_analyse_dominant_pressure_by_context.py",
        "19_context_stability_audit.py",
        "20_oos_validate_stable_contexts.py",
        "21_cost_survival_filter_refinement.py",
    ],
}


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "plans",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def assign_batch(script_name: str) -> str:
    for batch_name, scripts in BATCH_RULES.items():
        if script_name in scripts:
            return batch_name

    return "batch_99_unassigned"


def refactor_action(row: pd.Series) -> str:
    actions = []

    if not row.get("has_symbol_arg", False):
        actions.append("add --symbol argument or config symbol")

    if not row.get("has_run_function", False):
        actions.append("expose run_* function")

    if row.get("hardcoded_symbol_count", 0) > 0:
        actions.append("remove hardcoded symbol usage")

    if row.get("hardcoded_path_count", 0) > 0:
        actions.append("make output/input paths symbol-aware")

    if not actions:
        actions.append("review only")

    return "; ".join(actions)


def desired_interface(row: pd.Series) -> str:
    category = row["category"]

    if category in ["download", "normalisation"]:
        return "--symbol --start-date --end-date + run_* function"

    if category in ["bar_building", "feature_pipeline"]:
        return "--symbol + optional --start-date --end-date + run_* function"

    if category in [
        "signal_research",
        "replay",
        "cost_validation",
        "context_research",
        "validation",
    ]:
        return "--symbol + symbol-aware input/output roots + run_* function"

    if category == "utility":
        return "config/YAML-driven + safe symbol filters"

    return "manual review"


def main() -> None:
    banner("BACQE DUKASCOPY 49 - REFACTOR WORK PLAN")

    ensure_dirs()

    print(f"Audit path:  {AUDIT_PATH}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    if not AUDIT_PATH.exists():
        print("[STOP] Missing Script 48 audit CSV.")
        return

    audit = pd.read_csv(AUDIT_PATH)

    audit["refactor_batch"] = audit["script"].apply(assign_batch)
    audit["required_action"] = audit.apply(refactor_action, axis=1)
    audit["desired_interface"] = audit.apply(desired_interface, axis=1)

    batch_order = {
        batch_name: idx
        for idx, batch_name in enumerate(BATCH_RULES.keys(), start=1)
    }
    batch_order["batch_99_unassigned"] = 99

    priority_order = {
        "high": 1,
        "medium": 2,
        "low": 3,
    }

    audit["batch_order"] = audit["refactor_batch"].map(batch_order).fillna(99)
    audit["priority_order"] = audit["refactor_priority"].map(priority_order).fillna(9)

    plan = audit.sort_values(
        [
            "batch_order",
            "priority_order",
            "script",
        ]
    ).copy()

    batch_summary = (
        plan.groupby(["refactor_batch", "refactor_priority"], as_index=False)
        .agg(script_count=("script", "count"))
        .sort_values(["refactor_batch", "refactor_priority"])
    )

    plan_path = OUTPUT_ROOT / "plans" / "dukascopy_refactor_work_plan_latest.csv"
    summary_path = OUTPUT_ROOT / "plans" / "dukascopy_refactor_batch_summary_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "dukascopy_refactor_work_plan_report_latest.txt"

    plan.to_csv(plan_path, index=False)
    batch_summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY REFACTOR WORK PLAN REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Scripts planned: {len(plan):,}\n\n")

        f.write("Batch Summary\n")
        f.write("-" * 80 + "\n")
        f.write(batch_summary.to_string(index=False))

        f.write("\n\nOrdered Refactor Plan\n")
        f.write("-" * 80 + "\n")
        f.write(
            plan[
                [
                    "refactor_batch",
                    "script",
                    "category",
                    "refactor_priority",
                    "desired_interface",
                    "required_action",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nFirst Batch Focus\n")
        f.write("-" * 80 + "\n")
        first_batch = plan[plan["refactor_batch"] == "batch_01_core_ingestion"]
        f.write(
            first_batch[
                [
                    "script",
                    "refactor_priority",
                    "desired_interface",
                    "required_action",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"Plan: {plan_path}\n")
        f.write(f"Summary: {summary_path}\n")

    print("BATCH SUMMARY")
    print("-" * 90)
    print(batch_summary.to_string(index=False))
    print("-" * 90)
    print("[DONE] Refactor work plan complete.")
    print(f"Plan:   {plan_path}")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()