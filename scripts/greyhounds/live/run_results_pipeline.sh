#!/usr/bin/env bash
set -euo pipefail

cd /home/ben/bac-quant-engine
source .venv/bin/activate

rsync -av --ignore-existing "/mnt/d_drive/Greyhound Racing/" "/mnt/quant_lab/raw/Greyhound Racing/"

python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/01_build_results_staging.py
python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/02_curate_results.py
python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/04_build_race_features.py
python /home/ben/PycharmProjects/greyhound_tips_ingest/scripts/03_merge_tips_results.py
