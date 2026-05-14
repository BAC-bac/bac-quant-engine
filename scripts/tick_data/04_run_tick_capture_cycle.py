from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import traceback

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "tick_data.yaml"
COLLECTOR_SCRIPT = PROJECT_ROOT / "scripts" / "tick_data" / "02_collect_ticks_mt5.py"


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_log(log_file: Path, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {message}\n")


def run_collector(log_file: Path) -> int:
    command = [
        sys.executable,
        str(COLLECTOR_SCRIPT),
    ]

    write_log(log_file, f"Running collector: {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        write_log(log_file, "STDOUT:")
        for line in result.stdout.strip().splitlines():
            write_log(log_file, f"  {line}")

    if result.stderr:
        write_log(log_file, "STDERR:")
        for line in result.stderr.strip().splitlines():
            write_log(log_file, f"  {line}")

    write_log(log_file, f"Collector return code: {result.returncode}")

    return result.returncode


def main():
    run_started = datetime.now(timezone.utc)

    try:
        config = load_config(CONFIG_PATH)

        log_dir = PROJECT_ROOT / config["paths"]["log_dir"]
        log_file = log_dir / "tick_capture_cycle.log"

        write_log(log_file, "=" * 80)
        write_log(log_file, "BACQE tick capture cycle started")
        write_log(log_file, f"Project root: {PROJECT_ROOT}")
        write_log(log_file, f"Config path:  {CONFIG_PATH}")
        write_log(log_file, f"Collector:    {COLLECTOR_SCRIPT}")

        return_code = run_collector(log_file)

        elapsed = datetime.now(timezone.utc) - run_started

        if return_code == 0:
            write_log(log_file, f"BACQE tick capture cycle completed successfully in {elapsed}")
            print("[DONE] Tick capture cycle completed successfully.")
        else:
            write_log(log_file, f"BACQE tick capture cycle failed in {elapsed}")
            print("[FAIL] Tick capture cycle failed. Check logs/tick_data/tick_capture_cycle.log")

        write_log(log_file, "=" * 80)

        sys.exit(return_code)

    except Exception as exc:
        fallback_log = PROJECT_ROOT / "logs" / "tick_data" / "tick_capture_cycle.log"

        write_log(fallback_log, "=" * 80)
        write_log(fallback_log, "BACQE tick capture cycle crashed before normal logging setup")
        write_log(fallback_log, f"Error: {exc}")
        write_log(fallback_log, traceback.format_exc())
        write_log(fallback_log, "=" * 80)

        print("[FAIL] Tick capture cycle crashed.")
        print(f"Check log: {fallback_log}")

        sys.exit(1)


if __name__ == "__main__":
    main()