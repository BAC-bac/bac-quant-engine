"""
================================================================================
WORLD CUP LAB
SCRIPT 23 - RUN DAILY WORLD CUP PIPELINE
================================================================================

Purpose:
    Run the full daily World Cup Lab update pipeline in sequence.

Pipeline:
    13 - Actual Results Tracker
    14 - Prediction Accuracy Tracker
    15 - Host Boost Model Comparison
    16 - Team Watchlist
    17 - Team Rating Adjustment Engine
    18 - Group Stage Dashboard
    19 - Qualification Tracker
    20 - Live Qualification Forecast
    21 - Daily Command Centre
    22 - Tournament Storylines Engine
================================================================================
"""

from pathlib import Path
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCRIPTS = [
    "13_actual_results_tracker.py",
    "14_prediction_accuracy_tracker.py",
    "15_host_boost_model_comparison.py",
    "16_team_watchlist.py",
    "17_team_rating_adjustment_engine.py",
    "18_group_stage_dashboard.py",
    "19_qualification_tracker.py",
    "20_live_qualification_forecast.py",
    "21_daily_command_centre.py",
    "22_tournament_storylines_engine.py",
    "24_prematch_intelligence_engine.py",
    "25_daily_briefing_compiler.py",
    "26_model_edge_tracker.py",
]


def run_script(script_name):
    script_path = PROJECT_ROOT / "scripts" / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Missing script: {script_path}")

    print("=" * 80)
    print(f"RUNNING: {script_name}")
    print("=" * 80)

    start = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        text=True,
    )

    elapsed = time.time() - start

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline stopped. {script_name} failed with exit code {result.returncode}."
        )

    print("-" * 80)
    print(f"COMPLETED: {script_name} in {elapsed:.2f} seconds")
    print("-" * 80)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 23 - RUN DAILY WORLD CUP PIPELINE")
    print("=" * 80)

    total_start = time.time()

    for script in SCRIPTS:
        run_script(script)

    total_elapsed = time.time() - total_start

    print("=" * 80)
    print("DAILY WORLD CUP PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Scripts run: {len(SCRIPTS)}")
    print(f"Total time:  {total_elapsed:.2f} seconds")
    print("=" * 80)
    print("Key outputs refreshed:")
    print(" - outputs/daily_command_centre_report.txt")
    print(" - outputs/tournament_storylines_report.txt")
    print(" - outputs/live_qualification_forecast.csv")
    print(" - outputs/team_watchlist.csv")
    print("=" * 80)


if __name__ == "__main__":
    main()
