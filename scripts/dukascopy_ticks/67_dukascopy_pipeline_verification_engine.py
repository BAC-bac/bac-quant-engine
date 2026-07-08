"""
BACQE DUKASCOPY 67 - PIPELINE VERIFICATION ENGINE

Purpose:
    Verify that the Dukascopy pipeline has reached the expected state
    defined in config/dukascopy_pipeline_definition.yaml.

Reads:
    - Pipeline definition YAML
    - Script 65 state registry
    - Script 66 execution plan

Outputs:
    - Verification results CSV
    - Verification failures CSV
    - Text report

This script does not execute processing.
It verifies pipeline state.
"""

from pathlib import Path
import yaml
import pandas as pd
import numpy as np


PIPELINE_CONFIG_PATH = Path("config/dukascopy_pipeline_definition.yaml")


def load_pipeline_config() -> dict:
    if not PIPELINE_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing pipeline config: {PIPELINE_CONFIG_PATH}")

    with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if "dukascopy_pipeline" not in config:
        raise KeyError("Missing top-level key: dukascopy_pipeline")

    cfg = config["dukascopy_pipeline"]

    if not cfg.get("enabled", True):
        raise RuntimeError("dukascopy_pipeline.enabled is false.")

    return cfg


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"{label} is empty: {path}")

    return df


def normalise_status(value: object) -> str:
    if pd.isna(value):
        return "missing"
    return str(value).strip()


def verify_stage_for_symbol(row: pd.Series, stage: dict) -> dict:
    symbol = row["symbol"]

    stage_key = stage["stage_key"]
    status_column = stage["status_column"]
    complete_values = [str(v) for v in stage.get("complete_values", [])]
    priority = stage.get("priority", "medium")
    next_script_stage = stage.get("next_script_stage", "")

    if status_column not in row.index:
        return {
            "symbol": symbol,
            "stage_key": stage_key,
            "verification_status": "definition_error",
            "priority": "critical",
            "status_column": status_column,
            "observed_status": "missing_column",
            "expected_statuses": ", ".join(complete_values),
            "next_script_stage": next_script_stage,
            "reason": f"Status column not found in registry: {status_column}",
        }

    observed_status = normalise_status(row[status_column])

    passed = observed_status in complete_values

    return {
        "symbol": symbol,
        "stage_key": stage_key,
        "verification_status": "pass" if passed else "fail",
        "priority": priority,
        "status_column": status_column,
        "observed_status": observed_status,
        "expected_statuses": ", ".join(complete_values),
        "next_script_stage": next_script_stage,
        "reason": (
            "Stage meets configured completion rule."
            if passed
            else f"Observed {status_column}={observed_status}; expected one of {complete_values}"
        ),
    }


def build_verification_results(registry: pd.DataFrame, stages: list[dict]) -> pd.DataFrame:
    rows = []

    for _, row in registry.iterrows():
        for stage in stages:
            result = verify_stage_for_symbol(row, stage)

            context_cols = [
                "start_date",
                "end_date",
                "raw_days",
                "processed_tick_days",
                "tick_bar_files",
                "tib_files",
                "engineered_feature_days",
                "horizon_feature_days",
                "raw_coverage_pct",
                "processed_tick_coverage_pct",
                "engineered_feature_coverage_pct",
                "horizon_feature_coverage_pct",
            ]

            for col in context_cols:
                if col in row.index:
                    result[col] = row[col]

            rows.append(result)

    results = pd.DataFrame(rows)

    priority_order = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
    }

    results["priority_rank"] = results["priority"].map(priority_order).fillna(50)

    results = results.sort_values(
        by=["verification_status", "priority_rank", "symbol", "stage_key"],
        ascending=[True, True, True, True],
    )

    return results


def build_symbol_summary(results: pd.DataFrame) -> pd.DataFrame:
    summary = (
        results.groupby("symbol", dropna=False)
        .agg(
            stages_checked=("stage_key", "count"),
            stages_passed=("verification_status", lambda x: int((x == "pass").sum())),
            stages_failed=("verification_status", lambda x: int((x == "fail").sum())),
            definition_errors=("verification_status", lambda x: int((x == "definition_error").sum())),
        )
        .reset_index()
    )

    summary["symbol_verification_status"] = np.select(
        [
            summary["definition_errors"] > 0,
            summary["stages_failed"] > 0,
            summary["stages_passed"] == summary["stages_checked"],
        ],
        [
            "definition_error",
            "verification_failed",
            "verified",
        ],
        default="unknown",
    )

    return summary.sort_values(
        by=["symbol_verification_status", "symbol"],
        ascending=[True, True],
    )


def build_stage_summary(results: pd.DataFrame) -> pd.DataFrame:
    summary = (
        results.groupby(["stage_key", "verification_status"], dropna=False)
        .agg(
            symbols=("symbol", "count"),
        )
        .reset_index()
    )

    return summary.sort_values(
        by=["stage_key", "verification_status"],
        ascending=[True, True],
    )


def compare_with_decision_plan(
    results: pd.DataFrame,
    execution_plan: pd.DataFrame | None,
) -> pd.DataFrame:
    if execution_plan is None or execution_plan.empty:
        results["decision_alignment"] = "not_checked"
        return results

    plan_cols = [
        "symbol",
        "decision_status",
        "next_stage_key",
        "next_script_stage",
        "reason",
    ]

    available_cols = [col for col in plan_cols if col in execution_plan.columns]

    plan = execution_plan[available_cols].rename(
        columns={
            "reason": "decision_reason",
            "next_script_stage": "decision_next_script_stage",
        }
    )

    merged = results.merge(plan, on="symbol", how="left")

    merged["decision_alignment"] = np.select(
        [
            (merged["verification_status"] == "pass")
            & (merged["decision_status"] == "complete"),

            (merged["verification_status"] == "fail")
            & (merged["decision_status"] == "action_required")
            & (merged["stage_key"] == merged["next_stage_key"]),

            merged["decision_status"].isna(),
        ],
        [
            "aligned_complete",
            "aligned_action_required",
            "decision_missing",
        ],
        default="review",
    )

    return merged


def write_outputs(
    results: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    stage_summary: pd.DataFrame,
    output_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)

    results_path = output_root / "dukascopy_pipeline_verification_results_latest.csv"
    failures_path = output_root / "dukascopy_pipeline_verification_failures_latest.csv"
    symbol_summary_path = output_root / "dukascopy_pipeline_verification_symbol_summary_latest.csv"
    stage_summary_path = output_root / "dukascopy_pipeline_verification_stage_summary_latest.csv"
    report_path = output_root / "dukascopy_pipeline_verification_report_latest.txt"

    results.to_csv(results_path, index=False)

    failures = results[
        results["verification_status"].isin(["fail", "definition_error"])
    ].copy()

    failures.to_csv(failures_path, index=False)
    symbol_summary.to_csv(symbol_summary_path, index=False)
    stage_summary.to_csv(stage_summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY 67 - PIPELINE VERIFICATION ENGINE\n")
        f.write("=" * 90 + "\n\n")

        f.write("OVERALL STATUS\n")
        f.write("-" * 90 + "\n")
        if failures.empty:
            f.write("VERIFIED: All configured pipeline stages passed verification.\n")
        else:
            f.write(f"NOT VERIFIED: {len(failures)} failures or definition errors detected.\n")
        f.write("\n\n")

        f.write("SYMBOL SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(symbol_summary.to_string(index=False))
        f.write("\n\n")

        f.write("STAGE SUMMARY\n")
        f.write("-" * 90 + "\n")
        f.write(stage_summary.to_string(index=False))
        f.write("\n\n")

        if not failures.empty:
            display_cols = [
                "symbol",
                "stage_key",
                "verification_status",
                "priority",
                "observed_status",
                "expected_statuses",
                "next_script_stage",
                "reason",
            ]

            f.write("FAILURES\n")
            f.write("-" * 90 + "\n")
            f.write(failures[display_cols].to_string(index=False))
            f.write("\n\n")

        f.write("FULL VERIFICATION RESULTS\n")
        f.write("-" * 90 + "\n")
        f.write(results.to_string(index=False))

    print("=" * 90)
    print("BACQE DUKASCOPY 67 - PIPELINE VERIFICATION ENGINE")
    print("=" * 90)

    if failures.empty:
        print("[VERIFIED] All configured pipeline stages passed verification.")
    else:
        print(f"[NOT VERIFIED] {len(failures)} failures or definition errors detected.")

    print("-" * 90)
    print("SYMBOL SUMMARY")
    print("-" * 90)
    print(symbol_summary.to_string(index=False))
    print("-" * 90)
    print("STAGE SUMMARY")
    print("-" * 90)
    print(stage_summary.to_string(index=False))
    print("-" * 90)
    print(f"Results:        {results_path}")
    print(f"Failures:       {failures_path}")
    print(f"Symbol summary: {symbol_summary_path}")
    print(f"Stage summary:  {stage_summary_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


def main() -> None:
    cfg = load_pipeline_config()

    registry_path = Path(cfg["state_registry_path"])
    output_root = Path(cfg["output_root"]).parent / "dukascopy_pipeline_verification_engine"
    stages = cfg["stages"]

    registry = load_csv(registry_path, "pipeline state registry")

    decision_plan_path = Path(cfg["output_root"]) / "dukascopy_pipeline_execution_plan_latest.csv"

    execution_plan = None
    if decision_plan_path.exists():
        execution_plan = pd.read_csv(decision_plan_path)

    results = build_verification_results(registry, stages)
    results = compare_with_decision_plan(results, execution_plan)

    symbol_summary = build_symbol_summary(results)
    stage_summary = build_stage_summary(results)

    write_outputs(
        results=results,
        symbol_summary=symbol_summary,
        stage_summary=stage_summary,
        output_root=output_root,
    )


if __name__ == "__main__":
    main()