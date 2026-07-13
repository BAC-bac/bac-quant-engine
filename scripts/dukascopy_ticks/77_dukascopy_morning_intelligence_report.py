"""
BACQE DUKASCOPY 77 - MORNING INTELLIGENCE REPORT

Purpose:
    Produce a concise, evidence-based morning briefing covering the latest
    Dukascopy overnight run, pipeline health, research completion, recovery
    state, durable resume state, global cohort status and candidate tiers.

Reads:
    Script 65  - Pipeline state registry
    Script 67  - Pipeline verification summary
    Script 70  - Extended-horizon symbol summary
    Script 72  - Global cohort registry
    Script 74  - Symbol onboarding registry
    Script 75  - Latest overnight run state and job ledger
    Script 76  - Durable resume ledger state and resume plan
    EH13       - Latest full-cohort candidate registry outputs

Outputs:
    dukascopy_morning_intelligence_report_latest.txt
    dukascopy_morning_intelligence_summary_latest.csv
    dukascopy_morning_attention_items_latest.csv
    dukascopy_morning_candidate_tiers_latest.csv
    dukascopy_morning_intelligence_state_latest.json

This script does not execute research.
It summarises the latest verified state.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json

import pandas as pd


BASE_DIR = Path("E:/Quant_Lab")
ANALYSIS_ROOT = BASE_DIR / "data" / "analysis"

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_morning_intelligence"
)


PIPELINE_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_state_registry"
    / "dukascopy_pipeline_state_registry_latest.csv"
)

PIPELINE_VERIFICATION_SUMMARY_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_verification_engine"
    / "dukascopy_pipeline_verification_symbol_summary_latest.csv"
)

PIPELINE_FAILURES_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_verification_engine"
    / "dukascopy_pipeline_verification_failures_latest.csv"
)

EH_SYMBOL_SUMMARY_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "research_state_registry"
    / "extended_horizon_research_symbol_summary_latest.csv"
)

GLOBAL_COHORT_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "global_cohort_registry"
    / "extended_horizon_global_cohort_registry_latest.csv"
)

ONBOARDING_REGISTRY_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_symbol_onboarding"
    / "dukascopy_symbol_onboarding_registry_latest.csv"
)

OVERNIGHT_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
    / "run_state"
    / "dukascopy_overnight_master_latest.json"
)

OVERNIGHT_LEDGER_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
    / "dukascopy_overnight_job_ledger_latest.csv"
)

DURABLE_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_ledger_state_latest.json"
)

DURABLE_LEDGER_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_durable_job_ledger.csv"
)

RESUME_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_plan_latest.csv"
)

CANDIDATE_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "candidate_registry"
)


REPORT_PATH = (
    REPORT_ROOT
    / "dukascopy_morning_intelligence_report_latest.txt"
)

SUMMARY_PATH = (
    REPORT_ROOT
    / "dukascopy_morning_intelligence_summary_latest.csv"
)

ATTENTION_PATH = (
    REPORT_ROOT
    / "dukascopy_morning_attention_items_latest.csv"
)

CANDIDATE_TIERS_PATH = (
    REPORT_ROOT
    / "dukascopy_morning_candidate_tiers_latest.csv"
)

STATE_PATH = (
    REPORT_ROOT
    / "dukascopy_morning_intelligence_state_latest.json"
)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_directory() -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def safe_read_json(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        return json.loads(
            path.read_text(encoding="utf-8")
        )
    except Exception:
        return {}


def clean_text(value: object, default: str = "") -> str:
    if pd.isna(value):
        return default

    text = str(value).strip()

    if not text or text.lower() == "nan":
        return default

    return text


def clean_int(value: object, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default

        return int(float(value))
    except (TypeError, ValueError):
        return default


def clean_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default

        return float(value)
    except (TypeError, ValueError):
        return default


def find_latest_full_cohort_candidate_file() -> Path | None:
    if not CANDIDATE_ROOT.exists():
        return None

    matches = sorted(
        CANDIDATE_ROOT.glob(
            "*candidate_registry_latest.csv"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not matches:
        return None

    return matches[0]


def find_latest_candidate_symbol_summary() -> Path | None:
    if not CANDIDATE_ROOT.exists():
        return None

    matches = sorted(
        CANDIDATE_ROOT.glob(
            "*candidate_symbol_summary_latest.csv"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not matches:
        return None

    return matches[0]


def build_candidate_tier_summary(
    candidate_registry: pd.DataFrame,
) -> pd.DataFrame:
    if candidate_registry.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "candidate_tier",
                "candidate_count",
            ]
        )

    symbol_column = None

    for candidate in [
        "test_symbol",
        "symbol",
        "target_symbol",
        "candidate_symbol",
    ]:
        if candidate in candidate_registry.columns:
            symbol_column = candidate
            break

    if symbol_column is None:
        return pd.DataFrame(
            columns=[
                "symbol",
                "candidate_tier",
                "candidate_count",
            ]
        )

    if "candidate_tier" not in candidate_registry.columns:
        return pd.DataFrame(
            columns=[
                "symbol",
                "candidate_tier",
                "candidate_count",
            ]
        )

    summary = (
        candidate_registry
        .groupby(
            [
                symbol_column,
                "candidate_tier",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="candidate_count")
        .rename(columns={symbol_column: "symbol"})
    )

    summary["symbol"] = (
        summary["symbol"]
        .astype(str)
        .str.upper()
    )

    return summary.sort_values(
        by=[
            "symbol",
            "candidate_tier",
        ]
    )


def overnight_status_summary(
    overnight_state: dict,
    overnight_ledger: pd.DataFrame,
) -> dict:
    return {
        "run_id": clean_text(
            overnight_state.get("run_id"),
            "unknown",
        ),
        "status": clean_text(
            overnight_state.get("status"),
            "unknown",
        ),
        "started_at": clean_text(
            overnight_state.get("started_at"),
            "unknown",
        ),
        "finished_at": clean_text(
            overnight_state.get("finished_at"),
            "unknown",
        ),
        "elapsed_minutes": round(
            clean_float(
                overnight_state.get("elapsed_minutes"),
                0.0,
            ),
            2,
        ),
        "jobs_started": clean_int(
            overnight_state.get("jobs_started"),
            len(overnight_ledger),
        ),
        "jobs_succeeded": clean_int(
            overnight_state.get("jobs_succeeded"),
            int(
                (
                    overnight_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "ok"
                ).sum()
            )
            if not overnight_ledger.empty
            else 0,
        ),
        "jobs_failed": clean_int(
            overnight_state.get("jobs_failed"),
            int(
                (
                    overnight_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "error"
                ).sum()
            )
            if not overnight_ledger.empty
            else 0,
        ),
        "control_refreshes": clean_int(
            overnight_state.get("control_refreshes"),
            0,
        ),
        "stop_reason": clean_text(
            overnight_state.get("stop_reason"),
            "No stop reason recorded.",
        ),
        "last_command": clean_text(
            overnight_state.get("last_command"),
        ),
        "last_result": clean_text(
            overnight_state.get("last_result"),
        ),
        "log_path": clean_text(
            overnight_state.get("log_path"),
        ),
    }


def pipeline_summary(
    pipeline_state: pd.DataFrame,
    verification_summary: pd.DataFrame,
    failures: pd.DataFrame,
) -> dict:
    symbols = (
        sorted(
            pipeline_state["symbol"]
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
        )
        if not pipeline_state.empty
        and "symbol" in pipeline_state.columns
        else []
    )

    verified_symbols = 0

    if (
        not verification_summary.empty
        and "symbol_verification_status"
        in verification_summary.columns
    ):
        verified_symbols = int(
            (
                verification_summary[
                    "symbol_verification_status"
                ]
                == "verified"
            ).sum()
        )

    return {
        "symbols_tracked": len(symbols),
        "symbols": symbols,
        "verified_symbols": verified_symbols,
        "verification_failures": len(failures),
        "pipeline_verified": (
            len(failures) == 0
            and verified_symbols == len(symbols)
            and len(symbols) > 0
        ),
    }


def extended_horizon_summary(
    eh_summary: pd.DataFrame,
) -> dict:
    if eh_summary.empty:
        return {
            "symbols_tracked": 0,
            "symbols_complete": 0,
            "average_completion_pct": 0.0,
            "incomplete_symbols": [],
        }

    complete_mask = (
        eh_summary["research_status"].astype(str)
        == "complete"
    )

    incomplete = (
        eh_summary.loc[
            ~complete_mask,
            "symbol",
        ]
        .astype(str)
        .str.upper()
        .tolist()
    )

    return {
        "symbols_tracked": len(eh_summary),
        "symbols_complete": int(complete_mask.sum()),
        "average_completion_pct": round(
            pd.to_numeric(
                eh_summary["completion_pct"],
                errors="coerce",
            )
            .fillna(0.0)
            .mean(),
            2,
        ),
        "incomplete_symbols": sorted(incomplete),
    }


def global_cohort_summary(
    cohort: pd.DataFrame,
) -> dict:
    if cohort.empty:
        return {
            "stages_tracked": 0,
            "stages_current": 0,
            "stale_stages": [],
            "cohort_current": False,
        }

    current_mask = (
        cohort["stage_status"].astype(str)
        == "complete_for_current_cohort"
    )

    stale = (
        cohort.loc[
            ~current_mask,
            "stage_key",
        ]
        .astype(str)
        .tolist()
    )

    return {
        "stages_tracked": len(cohort),
        "stages_current": int(current_mask.sum()),
        "stale_stages": stale,
        "cohort_current": (
            len(cohort) > 0
            and bool(current_mask.all())
        ),
    }


def onboarding_summary(
    onboarding: pd.DataFrame,
) -> dict:
    if onboarding.empty:
        return {
            "symbols_tracked": 0,
            "fully_onboarded": 0,
            "actions_required": 0,
            "pending_symbols": [],
        }

    fully_onboarded_mask = (
        onboarding["onboarding_status"].astype(str)
        == "fully_onboarded"
    )

    action_required_mask = (
        onboarding["onboarding_status"].astype(str)
        .isin(
            [
                "action_required",
                "definition_error",
            ]
        )
    )

    pending_symbols = (
        onboarding.loc[
            action_required_mask,
            "symbol",
        ]
        .astype(str)
        .tolist()
    )

    return {
        "symbols_tracked": int(
            (
                onboarding["symbol"].astype(str)
                != "GLOBAL"
            ).sum()
        ),
        "fully_onboarded": int(
            fully_onboarded_mask.sum()
        ),
        "actions_required": int(
            action_required_mask.sum()
        ),
        "pending_symbols": sorted(pending_symbols),
    }


def durable_resume_summary(
    durable_state: dict,
    durable_ledger: pd.DataFrame,
    resume_plan: pd.DataFrame,
) -> dict:
    return {
        "ledger_rows": clean_int(
            durable_state.get("ledger_rows"),
            len(durable_ledger),
        ),
        "currently_required_jobs": clean_int(
            durable_state.get(
                "currently_required_jobs"
            ),
            int(
                durable_ledger.get(
                    "currently_required",
                    pd.Series(dtype=bool),
                )
                .astype(str)
                .str.lower()
                .isin(["true", "1", "yes"])
                .sum()
            )
            if not durable_ledger.empty
            else 0,
        ),
        "resume_plan_jobs": clean_int(
            durable_state.get("resume_plan_jobs"),
            len(resume_plan),
        ),
        "completed_jobs": clean_int(
            durable_state.get("completed_jobs"),
            int(
                (
                    durable_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "completed"
                ).sum()
            )
            if not durable_ledger.empty
            else 0,
        ),
        "failed_jobs": clean_int(
            durable_state.get("failed_jobs"),
            int(
                (
                    durable_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "failed"
                ).sum()
            )
            if not durable_ledger.empty
            else 0,
        ),
        "interrupted_jobs": clean_int(
            durable_state.get("interrupted_jobs"),
            int(
                (
                    durable_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "interrupted"
                ).sum()
            )
            if not durable_ledger.empty
            else 0,
        ),
        "retry_exhausted_jobs": clean_int(
            durable_state.get(
                "retry_exhausted_jobs"
            ),
            int(
                (
                    durable_ledger.get(
                        "status",
                        pd.Series(dtype=str),
                    )
                    == "retry_exhausted"
                ).sum()
            )
            if not durable_ledger.empty
            else 0,
        ),
    }


def candidate_summary(
    candidate_registry: pd.DataFrame,
    candidate_tiers: pd.DataFrame,
) -> dict:
    if candidate_registry.empty:
        return {
            "registry_rows": 0,
            "tier_1": 0,
            "tier_2": 0,
            "tier_3": 0,
            "reject_or_hold": 0,
        }

    counts = (
        candidate_registry["candidate_tier"]
        .astype(str)
        .value_counts()
        if "candidate_tier"
        in candidate_registry.columns
        else pd.Series(dtype=int)
    )

    return {
        "registry_rows": len(candidate_registry),
        "tier_1": clean_int(
            counts.get(
                "tier_1_priority_candidate",
                0,
            )
        ),
        "tier_2": clean_int(
            counts.get(
                "tier_2_research_candidate",
                0,
            )
        ),
        "tier_3": clean_int(
            counts.get(
                "tier_3_watchlist_candidate",
                0,
            )
        ),
        "reject_or_hold": clean_int(
            counts.get(
                "reject_or_hold",
                0,
            )
        ),
        "symbols_represented": (
            sorted(
                candidate_tiers[
                    "symbol"
                ]
                .dropna()
                .astype(str)
                .str.upper()
                .unique()
            )
            if not candidate_tiers.empty
            else []
        ),
    }


def build_attention_items(
    overnight: dict,
    pipeline: dict,
    extended: dict,
    cohort: dict,
    onboarding: dict,
    durable: dict,
) -> pd.DataFrame:
    rows: list[dict] = []

    def add(
        priority: str,
        area: str,
        issue: str,
        recommended_action: str,
    ) -> None:
        rows.append(
            {
                "priority": priority,
                "area": area,
                "issue": issue,
                "recommended_action": recommended_action,
            }
        )

    overnight_status = overnight["status"]

    if overnight_status not in {
        "complete",
        "stopped_by_limit",
    }:
        add(
            "critical",
            "overnight_orchestrator",
            (
                f"Latest overnight run status is "
                f"{overnight_status}."
            ),
            (
                "Inspect the Script 75 master log and latest "
                "run-state JSON before starting another run."
            ),
        )

    if overnight["jobs_failed"] > 0:
        add(
            "critical",
            "overnight_jobs",
            (
                f"{overnight['jobs_failed']} overnight job(s) failed."
            ),
            (
                "Review the Script 75 job ledger and per-job logs."
            ),
        )

    if not pipeline["pipeline_verified"]:
        add(
            "critical",
            "pipeline_verification",
            (
                f"{pipeline['verification_failures']} pipeline "
                "verification failure(s) remain."
            ),
            (
                "Run Scripts 65–68 and resolve the generated "
                "recovery actions."
            ),
        )

    if extended["symbols_complete"] < extended["symbols_tracked"]:
        add(
            "high",
            "extended_horizon_research",
            (
                "Extended-horizon research is incomplete for: "
                + ", ".join(
                    extended["incomplete_symbols"]
                )
            ),
            (
                "Run Scripts 70 and 71, then execute the next "
                "required EH stages."
            ),
        )

    if not cohort["cohort_current"]:
        add(
            "high",
            "global_cohort",
            (
                "Global cohort stages are stale or incomplete: "
                + ", ".join(
                    cohort["stale_stages"]
                )
            ),
            (
                "Run Scripts 72 and 73 and execute the earliest "
                "required global stage."
            ),
        )

    if onboarding["actions_required"] > 0:
        add(
            "high",
            "symbol_onboarding",
            (
                f"{onboarding['actions_required']} onboarding "
                "action(s) remain."
            ),
            (
                "Run Script 74 and follow the generated onboarding "
                "plan."
            ),
        )

    if durable["resume_plan_jobs"] > 0:
        add(
            "high",
            "durable_resume",
            (
                f"{durable['resume_plan_jobs']} job(s) require "
                "resumption."
            ),
            (
                "Allow Script 75 to resume the highest-priority "
                "eligible job."
            ),
        )

    if durable["retry_exhausted_jobs"] > 0:
        add(
            "critical",
            "durable_resume",
            (
                f"{durable['retry_exhausted_jobs']} job(s) have "
                "exhausted their retry limit."
            ),
            (
                "Investigate manually before resetting attempt "
                "counts."
            ),
        )

    if not rows:
        add(
            "none",
            "system",
            "No attention items detected.",
            "No immediate action required.",
        )

    priority_rank = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
        "none": 99,
    }

    frame = pd.DataFrame(rows)

    frame["priority_rank"] = (
        frame["priority"]
        .map(priority_rank)
        .fillna(50)
    )

    return frame.sort_values(
        by=[
            "priority_rank",
            "area",
        ]
    )


def determine_overall_status(
    attention: pd.DataFrame,
    overnight: dict,
) -> str:
    priorities = set(
        attention["priority"].astype(str)
    )

    if "critical" in priorities:
        return "critical_attention_required"

    if "high" in priorities:
        return "attention_required"

    if overnight["status"] == "stopped_by_limit":
        return "healthy_but_runtime_limited"

    if overnight["status"] == "complete":
        return "healthy"

    return "review_required"


def recommended_next_action(
    overall_status: str,
    attention: pd.DataFrame,
) -> str:
    if overall_status == "healthy":
        return (
            "No immediate Dukascopy action is required. "
            "Continue with the next scheduled BACQE research priority."
        )

    actionable = attention[
        attention["priority"] != "none"
    ]

    if actionable.empty:
        return "Review the latest BACQE control outputs."

    return clean_text(
        actionable.iloc[0]["recommended_action"],
        "Review the highest-priority attention item.",
    )


def build_summary_frame(
    overall_status: str,
    overnight: dict,
    pipeline: dict,
    extended: dict,
    cohort: dict,
    onboarding: dict,
    durable: dict,
    candidates: dict,
    next_action: str,
) -> pd.DataFrame:
    rows = [
        {
            "metric": "generated_at",
            "value": now_text(),
        },
        {
            "metric": "overall_status",
            "value": overall_status,
        },
        {
            "metric": "overnight_run_status",
            "value": overnight["status"],
        },
        {
            "metric": "overnight_elapsed_minutes",
            "value": overnight["elapsed_minutes"],
        },
        {
            "metric": "overnight_jobs_succeeded",
            "value": overnight["jobs_succeeded"],
        },
        {
            "metric": "overnight_jobs_failed",
            "value": overnight["jobs_failed"],
        },
        {
            "metric": "pipeline_verified",
            "value": pipeline["pipeline_verified"],
        },
        {
            "metric": "pipeline_symbols_verified",
            "value": pipeline["verified_symbols"],
        },
        {
            "metric": "extended_horizon_symbols_complete",
            "value": extended["symbols_complete"],
        },
        {
            "metric": "global_cohort_current",
            "value": cohort["cohort_current"],
        },
        {
            "metric": "symbols_fully_onboarded",
            "value": onboarding["fully_onboarded"],
        },
        {
            "metric": "resume_plan_jobs",
            "value": durable["resume_plan_jobs"],
        },
        {
            "metric": "candidate_registry_rows",
            "value": candidates["registry_rows"],
        },
        {
            "metric": "tier_1_candidates",
            "value": candidates["tier_1"],
        },
        {
            "metric": "tier_2_candidates",
            "value": candidates["tier_2"],
        },
        {
            "metric": "tier_3_candidates",
            "value": candidates["tier_3"],
        },
        {
            "metric": "recommended_next_action",
            "value": next_action,
        },
    ]

    return pd.DataFrame(rows)


def format_bool(value: bool) -> str:
    return "YES" if value else "NO"


def write_report(
    overall_status: str,
    overnight: dict,
    pipeline: dict,
    extended: dict,
    cohort: dict,
    onboarding: dict,
    durable: dict,
    candidates: dict,
    candidate_tiers: pd.DataFrame,
    overnight_ledger: pd.DataFrame,
    attention: pd.DataFrame,
    next_action: str,
    candidate_registry_path: Path | None,
) -> None:
    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 77 - MORNING INTELLIGENCE REPORT\n"
        )
        file.write("=" * 100 + "\n")
        file.write(f"Generated: {now_text()}\n")
        file.write(f"Overall status: {overall_status}\n")
        file.write("=" * 100 + "\n\n")

        file.write("EXECUTIVE SUMMARY\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Latest overnight run: {overnight['status']}\n"
        )
        file.write(
            f"Pipeline verified: "
            f"{format_bool(pipeline['pipeline_verified'])}\n"
        )
        file.write(
            f"EH01-EH10 symbols complete: "
            f"{extended['symbols_complete']}/"
            f"{extended['symbols_tracked']}\n"
        )
        file.write(
            f"Global cohort current: "
            f"{format_bool(cohort['cohort_current'])}\n"
        )
        file.write(
            f"Fully onboarded symbols: "
            f"{onboarding['fully_onboarded']}/"
            f"{onboarding['symbols_tracked']}\n"
        )
        file.write(
            f"Resume jobs outstanding: "
            f"{durable['resume_plan_jobs']}\n"
        )
        file.write(
            f"Candidate registry rows: "
            f"{candidates['registry_rows']}\n"
        )
        file.write("\n")

        file.write("RECOMMENDED NEXT ACTION\n")
        file.write("-" * 100 + "\n")
        file.write(next_action + "\n\n")

        file.write("OVERNIGHT RUN\n")
        file.write("-" * 100 + "\n")

        for key in [
            "run_id",
            "status",
            "started_at",
            "finished_at",
            "elapsed_minutes",
            "jobs_started",
            "jobs_succeeded",
            "jobs_failed",
            "control_refreshes",
            "stop_reason",
            "last_command",
            "last_result",
            "log_path",
        ]:
            file.write(
                f"{key}: {overnight[key]}\n"
            )

        file.write("\nOVERNIGHT JOB LEDGER\n")
        file.write("-" * 100 + "\n")

        if overnight_ledger.empty:
            file.write(
                "No heavy jobs were executed during the latest run.\n"
            )
        else:
            display_columns = [
                column
                for column in [
                    "job_number",
                    "source",
                    "symbol",
                    "stage",
                    "priority",
                    "started_at",
                    "finished_at",
                    "elapsed_seconds",
                    "return_code",
                    "status",
                ]
                if column in overnight_ledger.columns
            ]

            file.write(
                overnight_ledger[
                    display_columns
                ].to_string(index=False)
            )
            file.write("\n")

        file.write("\nPIPELINE HEALTH\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Symbols tracked: {pipeline['symbols_tracked']}\n"
        )
        file.write(
            f"Symbols verified: {pipeline['verified_symbols']}\n"
        )
        file.write(
            f"Verification failures: "
            f"{pipeline['verification_failures']}\n"
        )
        file.write(
            f"Pipeline verified: "
            f"{format_bool(pipeline['pipeline_verified'])}\n"
        )

        file.write("\nEXTENDED-HORIZON RESEARCH\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Symbols tracked: {extended['symbols_tracked']}\n"
        )
        file.write(
            f"Symbols complete: {extended['symbols_complete']}\n"
        )
        file.write(
            f"Average completion: "
            f"{extended['average_completion_pct']:.2f}%\n"
        )
        file.write(
            "Incomplete symbols: "
            + (
                ", ".join(
                    extended["incomplete_symbols"]
                )
                if extended["incomplete_symbols"]
                else "None"
            )
            + "\n"
        )

        file.write("\nGLOBAL COHORT\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Stages current: {cohort['stages_current']}/"
            f"{cohort['stages_tracked']}\n"
        )
        file.write(
            f"Cohort current: "
            f"{format_bool(cohort['cohort_current'])}\n"
        )
        file.write(
            "Stale stages: "
            + (
                ", ".join(cohort["stale_stages"])
                if cohort["stale_stages"]
                else "None"
            )
            + "\n"
        )

        file.write("\nONBOARDING\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Symbols tracked: {onboarding['symbols_tracked']}\n"
        )
        file.write(
            f"Fully onboarded: "
            f"{onboarding['fully_onboarded']}\n"
        )
        file.write(
            f"Actions required: "
            f"{onboarding['actions_required']}\n"
        )
        file.write(
            "Pending symbols: "
            + (
                ", ".join(
                    onboarding["pending_symbols"]
                )
                if onboarding["pending_symbols"]
                else "None"
            )
            + "\n"
        )

        file.write("\nDURABLE RESUME STATE\n")
        file.write("-" * 100 + "\n")

        for key, value in durable.items():
            file.write(f"{key}: {value}\n")

        file.write("\nCANDIDATE INTELLIGENCE\n")
        file.write("-" * 100 + "\n")
        file.write(
            f"Candidate file: "
            f"{candidate_registry_path or 'Not found'}\n"
        )
        file.write(
            f"Registry rows: {candidates['registry_rows']}\n"
        )
        file.write(
            f"Tier 1: {candidates['tier_1']}\n"
        )
        file.write(
            f"Tier 2: {candidates['tier_2']}\n"
        )
        file.write(
            f"Tier 3: {candidates['tier_3']}\n"
        )
        file.write(
            f"Reject or hold: "
            f"{candidates['reject_or_hold']}\n"
        )

        file.write("\nCANDIDATE TIERS BY SYMBOL\n")
        file.write("-" * 100 + "\n")

        if candidate_tiers.empty:
            file.write(
                "No candidate-tier breakdown available.\n"
            )
        else:
            file.write(
                candidate_tiers.to_string(index=False)
            )
            file.write("\n")

        file.write("\nATTENTION ITEMS\n")
        file.write("-" * 100 + "\n")
        file.write(
            attention[
                [
                    "priority",
                    "area",
                    "issue",
                    "recommended_action",
                ]
            ].to_string(index=False)
        )
        file.write("\n")


def main() -> None:
    ensure_output_directory()

    pipeline_state = safe_read_csv(
        PIPELINE_STATE_PATH
    )

    verification_summary = safe_read_csv(
        PIPELINE_VERIFICATION_SUMMARY_PATH
    )

    pipeline_failures = safe_read_csv(
        PIPELINE_FAILURES_PATH
    )

    eh_summary = safe_read_csv(
        EH_SYMBOL_SUMMARY_PATH
    )

    global_cohort = safe_read_csv(
        GLOBAL_COHORT_PATH
    )

    onboarding = safe_read_csv(
        ONBOARDING_REGISTRY_PATH
    )

    overnight_state = safe_read_json(
        OVERNIGHT_STATE_PATH
    )

    overnight_ledger = safe_read_csv(
        OVERNIGHT_LEDGER_PATH
    )

    durable_state = safe_read_json(
        DURABLE_STATE_PATH
    )

    durable_ledger = safe_read_csv(
        DURABLE_LEDGER_PATH
    )

    resume_plan = safe_read_csv(
        RESUME_PLAN_PATH
    )

    candidate_registry_path = (
        find_latest_full_cohort_candidate_file()
    )

    candidate_registry = (
        safe_read_csv(candidate_registry_path)
        if candidate_registry_path
        else pd.DataFrame()
    )

    candidate_symbol_summary_path = (
        find_latest_candidate_symbol_summary()
    )

    candidate_symbol_summary = (
        safe_read_csv(candidate_symbol_summary_path)
        if candidate_symbol_summary_path
        else pd.DataFrame()
    )

    candidate_tiers = build_candidate_tier_summary(
        candidate_registry
    )

    overnight = overnight_status_summary(
        overnight_state=overnight_state,
        overnight_ledger=overnight_ledger,
    )

    pipeline = pipeline_summary(
        pipeline_state=pipeline_state,
        verification_summary=verification_summary,
        failures=pipeline_failures,
    )

    extended = extended_horizon_summary(
        eh_summary=eh_summary
    )

    cohort = global_cohort_summary(
        cohort=global_cohort
    )

    onboarding_state = onboarding_summary(
        onboarding=onboarding
    )

    durable = durable_resume_summary(
        durable_state=durable_state,
        durable_ledger=durable_ledger,
        resume_plan=resume_plan,
    )

    candidates = candidate_summary(
        candidate_registry=candidate_registry,
        candidate_tiers=candidate_tiers,
    )

    attention = build_attention_items(
        overnight=overnight,
        pipeline=pipeline,
        extended=extended,
        cohort=cohort,
        onboarding=onboarding_state,
        durable=durable,
    )

    overall_status = determine_overall_status(
        attention=attention,
        overnight=overnight,
    )

    next_action = recommended_next_action(
        overall_status=overall_status,
        attention=attention,
    )

    summary = build_summary_frame(
        overall_status=overall_status,
        overnight=overnight,
        pipeline=pipeline,
        extended=extended,
        cohort=cohort,
        onboarding=onboarding_state,
        durable=durable,
        candidates=candidates,
        next_action=next_action,
    )

    summary.to_csv(
        SUMMARY_PATH,
        index=False,
    )

    attention.to_csv(
        ATTENTION_PATH,
        index=False,
    )

    candidate_tiers.to_csv(
        CANDIDATE_TIERS_PATH,
        index=False,
    )

    state = {
        "generated_at": now_text(),
        "overall_status": overall_status,
        "recommended_next_action": next_action,
        "overnight": overnight,
        "pipeline": pipeline,
        "extended_horizon": extended,
        "global_cohort": cohort,
        "onboarding": onboarding_state,
        "durable_resume": durable,
        "candidates": candidates,
        "attention_items": clean_int(
            (
                attention["priority"]
                != "none"
            ).sum()
        ),
        "candidate_registry_path": (
            str(candidate_registry_path)
            if candidate_registry_path
            else ""
        ),
        "candidate_symbol_summary_path": (
            str(candidate_symbol_summary_path)
            if candidate_symbol_summary_path
            else ""
        ),
        "candidate_symbol_summary_rows": len(
            candidate_symbol_summary
        ),
    }

    STATE_PATH.write_text(
        json.dumps(
            state,
            indent=2,
        ),
        encoding="utf-8",
    )

    write_report(
        overall_status=overall_status,
        overnight=overnight,
        pipeline=pipeline,
        extended=extended,
        cohort=cohort,
        onboarding=onboarding_state,
        durable=durable,
        candidates=candidates,
        candidate_tiers=candidate_tiers,
        overnight_ledger=overnight_ledger,
        attention=attention,
        next_action=next_action,
        candidate_registry_path=candidate_registry_path,
    )

    print("=" * 100)
    print(
        "BACQE DUKASCOPY 77 - MORNING INTELLIGENCE REPORT"
    )
    print("=" * 100)
    print(f"Generated:                {now_text()}")
    print(f"Overall status:           {overall_status}")
    print(f"Overnight run:            {overnight['status']}")
    print(
        f"Pipeline verified:        "
        f"{pipeline['pipeline_verified']}"
    )
    print(
        f"EH symbols complete:      "
        f"{extended['symbols_complete']}/"
        f"{extended['symbols_tracked']}"
    )
    print(
        f"Global cohort current:    "
        f"{cohort['cohort_current']}"
    )
    print(
        f"Fully onboarded symbols:  "
        f"{onboarding_state['fully_onboarded']}/"
        f"{onboarding_state['symbols_tracked']}"
    )
    print(
        f"Resume-plan jobs:         "
        f"{durable['resume_plan_jobs']}"
    )
    print(
        f"Candidate registry rows:  "
        f"{candidates['registry_rows']}"
    )
    print(
        f"Tier 1 / 2 / 3:           "
        f"{candidates['tier_1']} / "
        f"{candidates['tier_2']} / "
        f"{candidates['tier_3']}"
    )
    print("-" * 100)
    print("RECOMMENDED NEXT ACTION")
    print(next_action)
    print("-" * 100)

    actionable_attention = attention[
        attention["priority"] != "none"
    ]

    if actionable_attention.empty:
        print("[HEALTHY] No attention items detected.")
    else:
        print(
            f"[ATTENTION] "
            f"{len(actionable_attention)} item(s) detected."
        )

        print(
            actionable_attention[
                [
                    "priority",
                    "area",
                    "issue",
                    "recommended_action",
                ]
            ].to_string(index=False)
        )

    print("-" * 100)
    print(f"Report:          {REPORT_PATH}")
    print(f"Summary:         {SUMMARY_PATH}")
    print(f"Attention items: {ATTENTION_PATH}")
    print(f"Candidate tiers: {CANDIDATE_TIERS_PATH}")
    print(f"State:           {STATE_PATH}")
    print("=" * 100)


if __name__ == "__main__":
    main()