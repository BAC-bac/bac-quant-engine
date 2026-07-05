"""
BACQE DUKASCOPY 64 - PIPELINE ORCHESTRATOR

Purpose:
    YAML-driven orchestrator for Dukascopy ingestion and audit stages.

Reads:
    config/dukascopy_research.yaml

Default:
    Runs audit stage 11 only, across all YAML symbols.

Examples:
    python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py --stages 11

    python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py --stages 10 11 --symbols EURJPY USDJPY

    python scripts/dukascopy_ticks/64_dukascopy_pipeline_orchestrator.py --stages 08 09 10 11 --symbols EURJPY
"""

from pathlib import Path
import argparse
import subprocess
import sys
import time
import yaml


CONFIG_PATH = Path("config/dukascopy_research.yaml")

LOG_ROOT = Path(
    "E:/Quant_Lab/data/analysis/dukascopy_pipeline_orchestrator/logs"
)

STAGE_SCRIPTS = {
    "08": Path("scripts/dukascopy_ticks/08_normalise_dukascopy_date_range.py"),
    "09": Path("scripts/dukascopy_ticks/09_build_dukascopy_tick_bars_date_range.py"),
    "10": Path("scripts/dukascopy_ticks/10_build_dukascopy_tibs_date_range.py"),
    "11": Path("scripts/dukascopy_ticks/11_audit_dukascopy_range_outputs.py"),
}


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if "dukascopy_research" not in config:
        raise KeyError("Missing top-level key: dukascopy_research")

    return config["dukascopy_research"]


def validate_stage_scripts(stages: list[str]) -> None:
    unknown = [stage for stage in stages if stage not in STAGE_SCRIPTS]

    if unknown:
        raise ValueError(
            f"Unknown stage(s): {unknown}. Valid stages: {list(STAGE_SCRIPTS.keys())}"
        )

    missing = [
        str(STAGE_SCRIPTS[stage])
        for stage in stages
        if not STAGE_SCRIPTS[stage].exists()
    ]

    if missing:
        raise FileNotFoundError(f"Missing stage script(s): {missing}")


def run_command(command: list[str], log_path: Path) -> int:
    with open(log_path, "a", encoding="utf-8") as log:
        log.write("=" * 100 + "\n")
        log.write("COMMAND\n")
        log.write(" ".join(command) + "\n")
        log.write("=" * 100 + "\n\n")
        log.flush()

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None

        for line in process.stdout:
            print(line, end="")
            log.write(line)

        process.wait()

        log.write("\n")
        log.write(f"RETURN_CODE: {process.returncode}\n")
        log.write("\n\n")

    return int(process.returncode)


def build_command(
    script_path: Path,
    symbol: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    return [
        sys.executable,
        str(script_path),
        "--symbol",
        symbol,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]


def print_header(
    symbols: list[str],
    stages: list[str],
    start_date: str,
    end_date: str,
    log_path: Path,
    dry_run: bool,
) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 64 - PIPELINE ORCHESTRATOR")
    print("=" * 90)
    print(f"Config:     {CONFIG_PATH}")
    print(f"Symbols:    {symbols}")
    print(f"Stages:     {stages}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Dry run:    {dry_run}")
    print(f"Log:        {log_path}")
    print("-" * 90)


def main(
    stages: list[str],
    symbols: list[str] | None,
    continue_on_error: bool,
    dry_run: bool,
) -> None:
    cfg = load_config()

    if not cfg.get("enabled", True):
        raise RuntimeError("dukascopy_research.enabled is false in config.")

    yaml_symbols = [symbol.upper() for symbol in cfg["symbols"]]
    selected_symbols = [symbol.upper() for symbol in symbols] if symbols else yaml_symbols

    start_date = cfg["date_range"]["start"]
    end_date = cfg["date_range"]["end"]

    validate_stage_scripts(stages)

    LOG_ROOT.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_path = LOG_ROOT / f"dukascopy_pipeline_orchestrator_{timestamp}.log"

    print_header(
        symbols=selected_symbols,
        stages=stages,
        start_date=start_date,
        end_date=end_date,
        log_path=log_path,
        dry_run=dry_run,
    )

    results = []

    for stage in stages:
        script_path = STAGE_SCRIPTS[stage]

        print(f"\n[STAGE {stage}] {script_path}")
        print("-" * 90)

        for symbol in selected_symbols:
            command = build_command(
                script_path=script_path,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )

            print(f"\n[RUN] Stage {stage} | Symbol {symbol}")
            print(" ".join(command))
            print("-" * 90)

            if dry_run:
                return_code = 0
            else:
                return_code = run_command(command, log_path)

            status = "ok" if return_code == 0 else "error"

            results.append(
                {
                    "stage": stage,
                    "symbol": symbol,
                    "status": status,
                    "return_code": return_code,
                    "command": " ".join(command),
                }
            )

            if return_code != 0:
                print(f"[ERROR] Stage {stage} failed for {symbol}")

                if not continue_on_error:
                    print("[STOP] Pipeline stopped. Use --continue-on-error to continue.")
                    print("=" * 90)
                    return

            else:
                print(f"[OK] Stage {stage} completed for {symbol}")

    print("-" * 90)
    print("RUN SUMMARY")
    print("-" * 90)

    for row in results:
        print(
            f"stage={row['stage']} "
            f"symbol={row['symbol']} "
            f"status={row['status']} "
            f"return_code={row['return_code']}"
        )

    print("-" * 90)
    print("[DONE] Dukascopy pipeline orchestrator complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--stages",
        nargs="+",
        default=["11"],
        help="Stages to run, e.g. --stages 08 09 10 11",
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional symbol override. Defaults to YAML symbols.",
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue to the next symbol if a stage fails.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without running them.",
    )

    args = parser.parse_args()

    main(
        stages=args.stages,
        symbols=args.symbols,
        continue_on_error=args.continue_on_error,
        dry_run=args.dry_run,
    )