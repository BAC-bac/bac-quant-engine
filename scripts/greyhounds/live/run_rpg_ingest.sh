#!/bin/bash

cd /home/ben/bac-quant-engine
source .venv/bin/activate
python /home/ben/PycharmProjects/greyhound_tips_ingest/greyhound_tips_daily_ingest.py >> /home/ben/PycharmProjects/greyhound_tips_ingest/cron.log 2>&1
