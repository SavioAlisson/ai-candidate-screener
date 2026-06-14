---
name: screen
description: Screen candidates from Ashby — pulls from Application Review + New Lead, dedup checks, screens in parallel, writes back to Ashby
allowed-tools: Bash(python3 *) Bash(cd *) Bash(touch *) Bash(tail *) Bash(cat *)
---

Screen candidates from Ashby's Application Review and New Lead stages.

## What this does

1. Pulls unscreened candidates from Ashby (filtered by status=Active/Lead)
2. Dedup check: Sheet CSV + screening_log.csv + AI Verdict field + in-batch dedup
3. Moves candidates to AI Screening stage (processing lock)
4. Screens in parallel (15 workers): LinkedIn scrape, web research, dossier, Opus judgment
5. Writes results back to Ashby (custom fields, notes, stage moves)
6. Logs every candidate to screening_log.csv
7. Copies JSON to clipboard for Sheet import

## Instructions

1. Change to the pipeline directory:
   ```
   cd ~/Desktop/Projects/Purple\ Unicorn/lambda-screener
   ```

2. Parse the user's intent from $ARGUMENTS:
   - "all" or "everything" → `--batch-all --include-leads`
   - A number like "50" → `--limit 50 --include-leads`
   - "preview" or "how many" → `--dry-run --include-leads`
   - "rerun" or "opus only" → `--opus-only --include-leads`
   - No arguments → `--limit 50 --include-leads`

3. Run:
   ```
   python3 screen_batch.py --from-ashby --include-leads $ARGS
   ```

4. After completion, report:
   - How many candidates screened
   - Verdict breakdown (SCREEN / DECLINE / REVIEW)
   - Any failures or orphans
   - Whether JSON was copied to clipboard

## Notes

- Always include `--include-leads` and `--from-ashby`
- Stop mid-batch: `touch .stop_screening`
- ASHBY_API_KEY must be set in the environment
- Failures auto-retry on next run (orphan recovery)
