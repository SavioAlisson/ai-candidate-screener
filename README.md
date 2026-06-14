# AI Candidate Screening Pipeline

An agentic recruiting pipeline that sources, researches, and screens engineering
candidates end-to-end using Claude. It pulls candidates from an applicant tracking
system (ATS), builds a research dossier on each one from public data, runs a
structured hiring-judgment prompt, and writes the verdict back to the ATS with
plan-aware stage routing — all in parallel, with crash recovery and idempotent
writebacks.

Built and run in production to screen thousands of candidates across ~13 open roles.

> **Note:** This is a sanitized portfolio version. All candidate data, API keys,
> and organization-specific identifiers (ATS job IDs, Slack channel IDs, internal
> source records) have been removed or replaced with `REPLACE_WITH_*` placeholders.
> A few proprietary outreach/sequencing modules are referenced but intentionally
> not included.

---

## What it does

```
Intake ──▶ Research ──▶ Judgment ──▶ Writeback ──▶ Routing
(ATS /     (LinkedIn +   (Claude     (custom        (plan-aware
 Slack /    web search +  Opus,       fields +        stage moves,
 sourcing)  GitHub +      structured  notes +         best-fit
            Crunchbase)   verdict)    log)            consolidation)
```

For each candidate the pipeline runs a multi-step agentic research-then-judge flow:

1. **LinkedIn scrape** — pull the public profile (cache-first).
2. **Smart query generation** — Claude Haiku writes a targeted web-search query from the profile.
3. **Web research** — deep search via Linkup for signals not on LinkedIn.
4. **Dossier synthesis** — Haiku fuses LinkedIn + web results into a structured research dossier.
5. **Enrichment** — GitHub profile/top-repos, recent posts, and Crunchbase company data are merged in when available.
6. **Judgment** — Claude Opus renders a structured hiring verdict (`SCREEN` / `DEFER` / `DECLINE` / `INSUFFICIENT DATA`) with confidence, reasoning, concerns, and ranked best-fit roles.
7. **Writeback** — verdict + reasoning land on the ATS record as custom fields and a stage-tailored note; the candidate is routed to the correct stage based on verdict, source (inbound vs. outbound vs. referral), and the interview plan.

## Key engineering properties

- **Parallel** — up to 15 candidates screened concurrently with a thread pool.
- **Cache-first & cheap-to-resume** — the research dossier is cached *before* the
  expensive judgment call, so an Opus timeout never loses the ~$0.09 of research.
  Re-runs can reuse the dossier (`--opus-only`).
- **Crash recovery** — a file-based processing lock with TTL means a dead laptop
  mid-batch is recovered on the next run; nothing is screened twice.
- **Idempotent, durable writebacks** — failed ATS writes are queued and replayed
  with zero re-screening; permanently-gone candidates are dead-lettered, not retried forever.
- **Forward-only routing** — candidates never move backward through stages; the
  router is plan-aware (each interview plan owns its own stage IDs).
- **Append-only audit ledger** — every screening decision is logged for analysis,
  calibration, and reproducibility.

## Repository layout

| Area | Files |
|------|-------|
| **Entry point** | `screen_batch.py` — orchestrates batches, parallel workers, logging |
| **Core agentic flow** | `pipeline.py` — research → dossier → enrichment → Opus judgment |
| **ATS integration** | `ashby_bridge.py` — pull, push, stage routing, custom fields, dedup, durable writeback |
| **Research / enrichment** | `apify_linkedin.py`, `apify_posts.py`, `apify_crunchbase.py`, `linkedin_discovery.py` |
| **Sourcing** | `github_sourcer.py`, `waas_sourcer.py`, `juicebox_query_optimizer.py`, `push_to_ashby.py` |
| **Intake** | `slack_intake.py` — `/intake` slash command + agency-channel auto-monitor (Socket Mode) |
| **Judgment prompt** | `prompts/opus_body.md` — the structured hiring-evaluation prompt |
| **Evaluation harness** | `eval/eval_prompt.py` — offline replay of cached dossiers to compare prompt versions |
| **Feedback loop** | `pull_hm_feedback.py` — pulls hiring-manager notes to calibrate the prompt |
| **Utilities** | `csv_bridge.py`, `json_repair.py`, `pre_screener.py`, `movability.py`, `check_unscreened.py` |

## The `/screen` command

`.claude/skills/screen/SKILL.md` is a [Claude Code](https://claude.com/claude-code)
slash command that wraps the batch screener: it finds the latest candidate CSV,
confirms it with the user, runs the pipeline, and summarizes the results.

## Setup

```bash
pip install -r requirements.txt        # only slack-bolt; the rest is stdlib + raw HTTPS
cp .env.example .env                    # fill in your API keys
# replace the REPLACE_WITH_* placeholders in ashby_bridge.py / slack_intake.py / push_to_ashby.py
#   with your own ATS job/stage/actor IDs and Slack channel IDs
```

## Usage

```bash
# Screen a batch from a CSV export
python3 screen_batch.py

# Re-run only the judgment step on already-researched candidates (reuses cached dossiers)
python3 screen_batch.py --opus-only

# Count unscreened candidates for active roles (no screening)
python3 check_unscreened.py --count

# Source candidates from GitHub into the ATS
python3 github_sourcer.py --language TypeScript --location "San Francisco" \
  --min-stars 50 --require-linkedin --output sourced.csv
python3 push_to_ashby.py sourced.csv --source "Outbound - Github Sourced" --job "Frontend Engineer"

# Offline prompt evaluation (no ATS changes)
python3 eval/eval_prompt.py --since 2026-04-16 --prompt prompts/opus_body.md
```

## Tech

Python 3 · Claude (Opus for judgment, Haiku for research) via raw HTTPS · Apify
(LinkedIn / posts / Crunchbase) · Linkup (web search) · GitHub API · Ashby ATS API ·
Slack Bolt (Socket Mode).

## License

MIT — see [LICENSE](LICENSE).
