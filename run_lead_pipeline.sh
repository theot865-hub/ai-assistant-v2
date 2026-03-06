#!/bin/bash
set -e
cd /Users/theotaylorassistant/ai/assistant_v2
source .venv/bin/activate
python run_lead_pipeline_cli.py "$@"
