#!/bin/bash
cd /home/ben/bac-quant-engine || exit 1
source .venv/bin/activate
python scripts/greyhounds/live/01_check_data_lake_paths.py >> /mnt/quant_lab/greyhounds/logs/path_check_cron.log 2>&1
