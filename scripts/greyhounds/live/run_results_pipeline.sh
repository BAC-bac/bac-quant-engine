#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/mnt/quant_lab/meta/run_logs/pipeline_status.csv"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo "$TIMESTAMP,results_pipeline,STARTED" >> "$LOG_FILE"

{
    cd /home/ben/bac-quant-engine
    source /home/ben/PycharmProjects/greyhound_tips_ingest/.venv/bin/activate

    rsync -a --ignore-existing "/mnt/d_drive/Greyhound Racing/" "/mnt/quant_lab/raw/Greyhound Racing/"

    python /home/ben/bac-quant-engine/scripts/greyhounds/pipeline/01_build_results_staging.py
    python /home/ben/bac-quant-engine/scripts/greyhounds/pipeline/02_curate_results.py
    python /home/ben/bac-quant-engine/scripts/greyhounds/pipeline/04_build_race_features.py
    python /home/ben/bac-quant-engine/scripts/greyhounds/pipeline/03_merge_tips_results.py

    python /home/ben/bac-quant-engine/scripts/greyhounds/live/02_validate_live_outputs.py

    echo "$(date '+%Y-%m-%d %H:%M:%S'),results_pipeline,SUCCESS" >> "$LOG_FILE"
} || {
    echo "$(date '+%Y-%m-%d %H:%M:%S'),results_pipeline,FAILED" >> "$LOG_FILE"
    exit 1
}
