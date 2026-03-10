---
name: lead-pipeline
description: Run the local Victoria realtor lead pipeline and produce a merged CSV with validation.
---

# Lead Pipeline

## When to use this skill

Use this skill when you need to run the realtor lead collection pipeline for Victoria and get refreshed leads from RankMyAgent and REW.

## What this skill does

- Runs the existing pipeline wrapper: `/Users/theotaylorassistant/ai/assistant_v2/run_lead_pipeline.sh`
- Executes scraping, merge, and validation steps
- Produces merged output at:
  - `/Users/theotaylorassistant/ai/assistant_v2/workspace/leads/all_realtor_leads.csv`

## Output contract

This skill returns:
- Pipeline stdout from the wrapper command
- The final CSV path: `/Users/theotaylorassistant/ai/assistant_v2/workspace/leads/all_realtor_leads.csv`

## Run

```bash
/Users/theotaylorassistant/ai/assistant_v2/skills/lead-pipeline/run.sh
```
