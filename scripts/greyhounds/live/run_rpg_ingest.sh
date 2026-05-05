#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/mnt/quant_lab/meta/run_logs/pipeline_status.csv"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo "$TIMESTAMP,rpg_ingest,STARTED" >> $LOG_FILE

{
    cd /home/ben/bac-quant-engine
    source /home/ben/PycharmProjects/greyhound_tips_ingest/.venv/bin/activate

    python /home/ben/PycharmProjects/greyhound_tips_ingest/greyhound_tips_daily_ingest.py >> /home/ben/PycharmProjects/greyhound_tips_ingest/cron.log 2>&1

    echo "$TIMESTAMP,rpg_ingest,SUCCESS" >> $LOG_FILE
} || {
    echo "$TIMESTAMP,rpg_ingest,FAILED" >> $LOG_FILE
    exit 1
}
