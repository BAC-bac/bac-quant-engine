#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/mnt/quant_lab/meta/run_logs/pipeline_status.csv"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo "$TIMESTAMP,results_pipeline,STARTED" >> $LOG_FILE

{
    cd /home/ben/bac-quant-engine
    source .venv/bin/activate

    rsync -a --ignore-existing "/mnt/d_drive/Greyhound Racing/" "/mnt/quant_lab/raw/Greyhound Racing/"

    python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/01_build_results_staging.py
    python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/02_curate_results.py
    python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/04_build_race_features.py
    python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/03_merge_tips_results.py

    echo "$TIMESTAMP,results_pipeline,SUCCESS" >> $LOG_FILE
} || {
    echo "$TIMESTAMP,results_pipeline,FAILED" >> $LOG_FILE
    exit 1
}
