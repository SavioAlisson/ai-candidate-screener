"""
Parallel batch screener — CSV or Ashby mode.

CSV mode:  Reads from CSV export, screens, outputs JSON for Sheet import.
Ashby mode: Pulls from Ashby, screens, writes back to Ashby + mirrors to Sheet.

Usage:
  python3 screen_batch.py                                            # CSV mode (auto-find)
  python3 screen_batch.py --from-ashby                               # Ashby mode
  python3 screen_batch.py --from-ashby --limit 5                     # Ashby mode, 5 candidates
  python3 screen_batch.py --csv candidates.csv                       # specific CSV file
  python3 screen_batch.py --rescreen                                 # re-screen already-screened
  python3 screen_batch.py --opus-only                                # judgment rerun (reuse dossier)
  python3 screen_batch.py --rows 5,12,18                             # specific rows only
  python3 screen_batch.py --dry-run                                  # preview without screening
  python3 screen_batch.py --parallel 20                              # 20 parallel workers

Required environment variables:
  CLAUDE_API_KEY      — Anthropic API key

Optional:
  ASHBY_API_KEY       — Ashby API key (required for --from-ashby)
  LINKUP_API_KEY      — Linkup API key (needed for full research, not opus-only)
  APIFY_TOKEN         — Apify token for LinkedIn scraping
  PROMPT_VERSION      — defaults to "local-batch-v1"
  MAX_PARALLEL        — override default parallel workers (default: 15)
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline import screen_one_candidate
from csv_bridge import (
    read_candidates_csv,
    read_query_learnings,
    write_query_learning,
    save_results_json,
    copy_to_clipboard,
    VERDICT_DESTINATION,
    get_verdict_destination,
)
from pre_screener import pre_screen_candidate, load_archive_patterns

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────
DEFAULT_PARALLEL = 15
PROMPT_VERSION = os.environ.get("PROMPT_VERSION", "local-batch-v1")
SCREENING_LOG_PATH = Path(__file__).parent / "screening_log.csv"
STOP_SIGNAL_PATH = Path(__file__).parent / ".stop_screening"


ASHBY_DESTINATION_ORDER = [
    "Application Review",
    "Inbound App Review",
    "Referrals Review",
    "Outbound Screened",
    "Archived",
    "Needs Rescreen",
]


def _ashby_destination_for_result(result: dict, context: Optional[dict] = None) -> str:
    """Return the same Ashby destination stage used by write_screening_to_ashby."""
    from ashby_bridge import get_verdict_stage

    context = context or {}
    return get_verdict_stage(
        result.get("verdict", ""),
        rejection_type=result.get("rejection_type", ""),
        nurture=result.get("nurture", ""),
        confidence_score=result.get("confidence_score", ""),
        # Inbound/outbound classification keys off the PREFIXED source
        # ("Inbound — LinkedIn"), so prefer it over the bare `source_raw`
        # ("LinkedIn"). The bare title isn't in the inbound patterns, so reading
        # it first silently routes inbound SCREENs to Outbound Screened. This
        # precedence must mirror write_screening_to_ashby.
        source=(
            result.get("source")
            or context.get("source")
            or result.get("source_raw")
            or context.get("source_raw")
            or ""
        ),
    )


def _ashby_destination_counts(results: list, contexts: Optional[dict] = None) -> Dict[str, int]:
    """Count screened results by actual Ashby destination stage."""
    counts = {stage: 0 for stage in ASHBY_DESTINATION_ORDER}
    contexts = contexts or {}
    for result in results:
        name = result.get("name", "")
        stage = _ashby_destination_for_result(result, contexts.get(name, {}))
        counts[stage] = counts.get(stage, 0) + 1
    return counts


def _format_destination_counts(counts: Dict[str, int]) -> str:
    """Compact one-line destination summary for live progress output."""
    return "  |  ".join(
        f"{stage}: {counts.get(stage, 0)}"
        for stage in ASHBY_DESTINATION_ORDER
        if counts.get(stage, 0) > 0 or stage != "Needs Rescreen"
    )


def _resolve_ashby_write_workers(args, total_jobs: int) -> int:
    """Bound Ashby writeback concurrency; each candidate fans out to several API calls."""
    raw = getattr(args, "write_parallel", None) or os.environ.get("ASHBY_WRITE_PARALLEL", "5")
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 5
    return max(1, min(workers, total_jobs or 1))


def _check_stop_signal() -> bool:
    """Check if .stop_screening file exists (graceful stop request)."""
    return STOP_SIGNAL_PATH.exists()


def _clear_stop_signal():
    """Remove the stop signal file after acknowledging it."""
    try:
        STOP_SIGNAL_PATH.unlink(missing_ok=True)
    except Exception:
        pass

# ── Screening log (append-only) ─────────────────────────────────

SCREENING_LOG_COLUMNS = [
    "timestamp", "name", "linkedin", "source", "target_role", "job_title",
    "verdict", "spark", "verdict_reason", "best_fit_role", "best_fit_reason",
    "matched_level", "reasoning", "regret_test", "concerns",
    "screening_questions", "screener_brief", "defer_until",
    "outreach_1", "outreach_2", "research_output",
    "rejection_type", "nurture",
    "move_to", "prompt_version", "sonar_cost", "opus_cost", "total_cost",
    "token_log", "elapsed_sec", "ashby_candidate_id", "ashby_application_id",
    "data_confidence", "confidence_score", "mode",
]


def append_to_screening_log(result: dict, candidate: dict, mode: str = "csv"):
    """Append one row to the screening log CSV. Creates the file with headers if needed."""
    import csv

    file_exists = SCREENING_LOG_PATH.exists()

    row = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "name": result.get("name", ""),
        "linkedin": result.get("linkedin", candidate.get("linkedin", "")),
        "source": candidate.get("source", ""),
        "target_role": candidate.get("target_role", ""),
        "job_title": candidate.get("job_title", ""),
        "verdict": result.get("verdict", ""),
        "spark": result.get("spark", ""),
        "verdict_reason": result.get("verdict_reason", ""),
        "best_fit_role": result.get("best_fit_role", ""),
        "best_fit_reason": result.get("best_fit_reason", ""),
        "matched_level": result.get("matched_level", ""),
        "reasoning": (result.get("reasoning") or "")[:5000],  # truncate for CSV
        "regret_test": result.get("regret_test", ""),
        "concerns": result.get("concerns", ""),
        "screening_questions": result.get("screening_questions", ""),
        "screener_brief": result.get("screener_brief", ""),
        "defer_until": result.get("defer_until", ""),
        "outreach_1": result.get("outreach_1", ""),
        "outreach_2": result.get("outreach_2", ""),
        "research_output": (result.get("research_output") or "")[:49000],
        "rejection_type": result.get("rejection_type", ""),
        "nurture": result.get("nurture", ""),
        "move_to": result.get("move_to", ""),
        "prompt_version": result.get("prompt_version", PROMPT_VERSION),
        "sonar_cost": result.get("sonar_cost", ""),
        "opus_cost": result.get("opus_cost", ""),
        "total_cost": result.get("total_cost", ""),
        "token_log": result.get("token_log", ""),
        "elapsed_sec": f"{result.get('elapsed', 0):.1f}",
        "ashby_candidate_id": candidate.get("ashby_candidate_id", ""),
        "ashby_application_id": candidate.get("ashby_application_id", ""),
        "data_confidence": result.get("data_confidence", ""),
        "confidence_score": result.get("confidence_score", ""),
        "mode": mode,
    }

    try:
        with open(SCREENING_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SCREENING_LOG_COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        logger.warning("Failed to append screening log: %s", e)

# Known sheet tab names that appear in CSV filenames when exported from Google Sheets
CSV_TAB_PATTERNS = [
    "Candidate Screener",
    "Screen (Needs Review)",
    "Nurture",
    "Archived",
]


def auto_find_csv() -> str:
    """
    Find the most recent CSV in ~/Downloads that looks like a Google Sheets export
    of a screening tab. Google Sheets names downloads like:
      'Purple Unicorn Screening - Candidate Screener.csv'
    Returns the path, or exits with a helpful message.
    """
    downloads = Path.home() / "Downloads"
    if not downloads.exists():
        return ""

    # Find all CSVs in Downloads, sorted newest first
    csv_files = sorted(
        downloads.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not csv_files:
        return ""

    # First pass: look for files with known tab names in the filename
    for f in csv_files:
        name_lower = f.name.lower()
        for tab in CSV_TAB_PATTERNS:
            if tab.lower() in name_lower:
                return str(f)

    # Second pass: look for any CSV with "screening" or "candidate" in the name
    for f in csv_files:
        name_lower = f.name.lower()
        if "screening" in name_lower or "candidate" in name_lower or "unicorn" in name_lower:
            return str(f)

    # Third pass: just return the most recent CSV (within last 24 hours)
    import datetime
    cutoff = time.time() - 86400  # 24 hours
    for f in csv_files:
        if f.stat().st_mtime > cutoff:
            return str(f)

    return ""


# ── Email extraction ─────────────────────────────────────────────

import re

# Domains to exclude from email extraction
_EXCLUDED_EMAIL_DOMAINS = {
    "linkedin.com", "github.com", "example.com", "email.com",
    "company.com", "domain.com", "placeholder.com", "test.com",
    "klarity.com", "klaritylaw.com",
}
_GENERIC_PREFIXES = {
    "support", "admin", "info", "noreply", "no-reply", "help",
    "contact", "sales", "team", "hello", "office", "hr",
}


def extract_candidate_email(dossier: str, cv: str = "", verdict_reason: str = "") -> str:
    """Extract a personal email from research dossier, CV, or verdict reason.
    Returns the first valid personal email found, or empty string."""
    text = f"{verdict_reason}\n{dossier}\n{cv}"
    emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text)

    for email in emails:
        email = email.lower().strip(".")
        domain = email.split("@")[1] if "@" in email else ""
        prefix = email.split("@")[0] if "@" in email else ""

        # Skip excluded domains
        if any(d in domain for d in _EXCLUDED_EMAIL_DOMAINS):
            continue
        # Skip generic prefixes
        if prefix in _GENERIC_PREFIXES:
            continue
        # Skip obviously fake emails
        if "example" in domain or "placeholder" in email:
            continue

        return email
    return ""


# ── Format result for import ────────────────────────────────────

def format_result_for_import(
    result: dict,
    row: int,
    candidate_name: str,
    costs: dict,
    token_log: str,
    research_output: str,
) -> dict:
    """Convert pipeline result into a flat dict for Apps Script import."""

    # Format concerns
    concerns = result.get("concerns") or []
    concern_lines = []
    for c in concerns:
        concern_lines.append(f"[{c.get('type', 'UNKNOWN')}] {c.get('concern', '')}: {c.get('detail', '')}")

    # Format screening questions from screener_brief
    brief = result.get("screener_brief") or {}
    q_lines = []
    for h in brief.get("hypotheses") or []:
        q_lines.append(f"Q: {h.get('question', '')}")
        if h.get("green_flag"):
            q_lines.append(f"  Green: {h['green_flag']}")
        if h.get("red_flag"):
            q_lines.append(f"  Red: {h['red_flag']}")

    # Format screener brief text
    brief_text = ""
    if brief:
        if brief.get("open_with"):
            brief_text += "Open with: " + str(brief["open_with"])
        if brief.get("watch_for"):
            brief_text += ("\n\n" if brief_text else "") + "Watch for: " + str(brief["watch_for"])

    # Format outreach messages
    outreach = result.get("outreach_messages") or []
    msg1 = ""
    msg2 = ""
    if len(outreach) > 0:
        o = outreach[0]
        msg1 = f"[{o.get('angle', '')}] {o.get('message', '')}"
    if len(outreach) > 1:
        o = outreach[1]
        msg2 = f"[{o.get('angle', '')}] {o.get('message', '')}"

    # Format roles
    roles = result.get("roles") or []
    role_names = [r.get("role") for r in roles if r.get("role")]
    role_reasons = [f"{r.get('role')}: {r.get('fit_reason', '')}" for r in roles if r.get("fit_reason")]
    levels = [r.get("level") for r in roles if r.get("level")]

    verdict = result.get("verdict") or "SCREENING FAILED"
    rejection_type = result.get("rejection_type") or ""
    nurture_val = result.get("nurture") or ""
    dest = get_verdict_destination(verdict, rejection_type, nurture_val)

    # Extract personal email for SCREEN candidates
    verdict_reason_text = result.get("verdict_reason") or ""
    if verdict.upper() == "SCREEN":
        extracted_email = extract_candidate_email(
            research_output or "", result.get("cv", ""), verdict_reason_text
        )
        if extracted_email:
            verdict_reason_text = f"Email: {extracted_email}\n\n{verdict_reason_text}"

    return {
        "row": row,
        "name": candidate_name,
        "verdict": verdict,
        "spark": result.get("spark") or "",
        "verdict_reason": verdict_reason_text,
        "best_fit_role": ", ".join(role_names),
        "best_fit_reason": " | ".join(role_reasons),
        "matched_level": levels[0] if levels else "",
        "reasoning": (result.get("reasoning") or "")[:49000],
        "regret_test": result.get("regret_test") or "",
        "concerns": "\n".join(concern_lines),
        "screening_questions": "\n".join(q_lines),
        "screener_brief": brief_text,
        "defer_until": result.get("defer_until") or "",
        "outreach_1": msg1,
        "outreach_2": msg2,
        "research_output": (research_output or "")[:49000],
        "rejection_type": result.get("rejection_type") or "",
        "nurture": result.get("nurture") or "",
        "confidence_score": result.get("confidence_score", ""),
        "prompt_version": PROMPT_VERSION,
        "total_cost": f"${costs.get('total', 0):.4f}",
        "sonar_cost": f"${costs.get('sonar', 0):.4f}",
        "opus_cost": f"${costs.get('opus', 0):.4f}",
        "token_log": token_log,
        "move_to": dest,
    }


# ── Screen one candidate ─────────────────────────────────────────

def screen_one(
    candidate: Dict[str, Any],
    query_learnings: str = "",
    opus_only: bool = False,
) -> Dict[str, Any]:
    """Screen one candidate and return formatted result."""
    row = candidate["row"]
    name = candidate["name"]
    start = time.time()

    try:
        result = screen_one_candidate(
            name=name,
            linkedin=candidate.get("linkedin", ""),
            source=candidate.get("source", ""),
            source_notes=candidate.get("source_notes", ""),
            cv=candidate.get("cv", ""),
            target_role=candidate.get("target_role", ""),
            existing_dossier=candidate.get("existing_dossier", ""),
            query_learnings=query_learnings,
            opus_only=opus_only,
            candidate_id=candidate.get("ashby_candidate_id", ""),
        )
    except Exception as e:
        elapsed = time.time() - start
        logger.error("  Row %d (%s): SCREENING FAILED — %s (%.0fs)", row, name, e, elapsed)
        return {
            "row": row,
            "name": name,
            "verdict": "SCREENING FAILED",
            "verdict_reason": str(e)[:500],
            "error": str(e),
            "elapsed": elapsed,
        }

    costs = result.pop("_costs", {})
    token_log = result.pop("_token_log", "")
    research_output = result.pop("_researchOutput", "")
    learning = result.pop("_learning", "")
    data_confidence = result.pop("_data_confidence", "")

    # Cache dossier immediately so crash between here and the main loop doesn't lose it
    cid = candidate.get("ashby_candidate_id", "")
    if cid and research_output and len(research_output) > 100:
        try:
            from ashby_bridge import _save_cache
            _save_cache(cid, {
                "dossier": research_output,
                "verdict": result.get("verdict", ""),
                "screened_at": time.strftime("%Y-%m-%d %H:%M"),
            })
        except Exception:
            pass

    # Write query learning to local cache
    if learning:
        try:
            write_query_learning(name, learning, candidate.get("target_role", ""))
        except Exception:
            pass

    formatted = format_result_for_import(result, row, name, costs, token_log, research_output)
    formatted["linkedin"] = candidate.get("linkedin", "")
    formatted["elapsed"] = time.time() - start
    formatted["cost"] = costs.get("total", 0)
    formatted["data_confidence"] = data_confidence
    return formatted


# ── Main ─────────────────────────────────────────────────────────

def _run_ashby_mode(args, max_parallel: int):
    """Pull from Ashby, screen, write back to Ashby + mirror to Sheet."""
    from ashby_bridge import (
        pull_for_screening,
        drain_application_review,
        drain_lead_stages,
        drain_outbound_screened,
        drain_unconfigured_referrals,
    )

    if not os.environ.get("ASHBY_API_KEY"):
        print("\nMissing ASHBY_API_KEY. Set it: export ASHBY_API_KEY='...'")
        sys.exit(1)

    # 0a. Pre-run drain of Application Review — move candidates who were
    # screened in a prior run (AI Verdict already terminal) but got stranded
    # in AR because their stage-move failed. Honors human-move safeguard.
    print(f"{'='*60}")
    print("  PRE-RUN DRAIN — Application Review (stranded-verdict sweep)")
    print(f"{'='*60}")
    try:
        ar_counts = drain_application_review(dry_run=args.dry_run)
        print(f"  AR drain: moved={ar_counts['moved']}, "
              f"unscreened={ar_counts['stuck_unscreened']}, "
              f"skipped-human={ar_counts['skipped_human']}, "
              f"errors={ar_counts['errors']}\n")
    except Exception as e:
        print(f"  ⚠ AR drain failed: {e}\n")

    # 0c. Pre-run drain of Lead intake sub-stages — route re-applicants whose
    # candidate-level AI Verdict is already terminal (e.g. prior DECLINE)
    # out of the Lead sub-stages. Honors human-move safeguard.
    print(f"{'='*60}")
    print("  PRE-RUN DRAIN — Lead sub-stages (re-applicant verdict sweep)")
    print(f"{'='*60}")
    try:
        lead_counts = drain_lead_stages(dry_run=args.dry_run)
        print(f"  Lead drain: moved={lead_counts['moved']}, "
              f"unscreened={lead_counts['stuck_unscreened']}, "
              f"skipped-human={lead_counts['skipped_human']}, "
              f"errors={lead_counts['errors']}\n")
    except Exception as e:
        print(f"  ⚠ Lead drain failed: {e}\n")

    # 0c2. Pre-run drain of unconfigured-role referrals — referrals on OPEN but
    # non-screening-configured roles (e.g. Solution Consultant) are moved into
    # Referrals Review with no verdict, instead of sitting unseen in New Lead /
    # Application Review. The 'Other referrals' catch-all (closed roles) is left
    # alone — its plan has no Referrals Review stage. Per the recruiting lead, 2026-05-29.
    print(f"{'='*60}")
    print("  PRE-RUN DRAIN — Unconfigured-role referrals → Referrals Review")
    print(f"{'='*60}")
    try:
        uref_counts = drain_unconfigured_referrals(dry_run=args.dry_run)
        print(f"  Unconfigured-referral drain: moved={uref_counts['moved']}, "
              f"not-referral={uref_counts['skipped_not_referral']}, "
              f"active-role={uref_counts['skipped_active_role']}, "
              f"no-RR-stage={uref_counts['skipped_no_rr_stage']}, "
              f"skipped-human={uref_counts['skipped_human']}, "
              f"errors={uref_counts['errors']}\n")
    except Exception as e:
        print(f"  ⚠ Unconfigured-referral drain failed: {e}\n")

    # 0d. Pre-run drain of Outbound Screened — archive candidates whose AI
    # Verdict has flipped to DECLINE/INSUFFICIENT_DATA/DUPLICATE/DEFER after a
    # re-screen, but whose stage move didn't take. Honors human-move safeguard.
    print(f"{'='*60}")
    print("  PRE-RUN DRAIN — Outbound Screened (verdict-flip sweep)")
    print(f"{'='*60}")
    try:
        os_counts = drain_outbound_screened(dry_run=args.dry_run)
        print(f"  OS drain: moved={os_counts['moved']}, "
              f"kept-screen={os_counts['kept_screen']}, "
              f"unscreened={os_counts['stuck_unscreened']}, "
              f"skipped-human={os_counts['skipped_human']}, "
              f"errors={os_counts['errors']}\n")
    except Exception as e:
        print(f"  ⚠ OS drain failed: {e}\n")

    # 1. Pull candidates from Application Review (+ New Lead if --include-leads) → AI Screening
    force = getattr(args, 'force', False)
    include_leads = getattr(args, 'include_leads', False)
    batch_all = getattr(args, 'batch_all', False)
    stages = "New Lead + Needs Rescreen + Application Review (auto-placed)"

    if batch_all:
        # Pull ALL candidates once, then process in chunks
        batch_size = args.limit or 50
        print(f"\nPulling ALL candidates from Ashby ({stages}) — will process in batches of {batch_size}...\n")
        all_candidates = pull_for_screening(limit=0, dry_run=args.dry_run, force=force,
                                            include_leads=include_leads)
        if not all_candidates:
            print(f"  No candidates in {stages} to screen.")
            return

        print(f"\n  {len(all_candidates)} candidates pulled. Processing in batches of {batch_size}...\n")

        if args.dry_run:
            for c in all_candidates:
                li = " (LinkedIn)" if c.get("linkedin") else " (no LinkedIn)"
                print(f"    {c['name']:<35} {c.get('job_title', ''):<30}{li}")
            print("\n  DRY RUN — no screening performed.")
            return

        # Process in chunks — each chunk goes through screening + writeback
        grand_total = 0
        grand_cost = 0.0
        grand_start = time.time()
        for batch_num, chunk_start in enumerate(range(0, len(all_candidates), batch_size), 1):
            chunk = all_candidates[chunk_start:chunk_start + batch_size]
            print(f"\n{'='*60}")
            print(f"  BATCH {batch_num} — candidates {chunk_start+1}-{chunk_start+len(chunk)} of {len(all_candidates)}")
            print(f"{'='*60}")
            for c in chunk:
                li = " (LinkedIn)" if c.get("linkedin") else " (no LinkedIn)"
                print(f"    {c['name']:<35} {c.get('job_title', ''):<30}{li}")

            # Run this chunk through the normal pipeline
            args_copy = argparse.Namespace(**vars(args))
            args_copy.batch_all = False  # prevent recursion
            args_copy.limit = 0  # already sliced
            _run_ashby_batch(args_copy, max_parallel, chunk)

            grand_total += len(chunk)
            print(f"\n  Cumulative: {grand_total}/{len(all_candidates)} candidates processed")

            # Check stop signal between batches
            if _check_stop_signal():
                print(f"\n  Stop signal detected. Stopping after batch {batch_num}.")
                _clear_stop_signal()
                break

        grand_elapsed = time.time() - grand_start
        print(f"\n{'='*60}")
        print(f"  ALL BATCHES COMPLETE — {grand_total} candidates in {grand_elapsed/60:.1f} minutes")
        print(f"{'='*60}\n")

        # Post-run drain: catch any stage-move failures during the batch
        print(f"\n{'='*60}")
        print("  POST-RUN DRAIN — Application Review (stranded-verdict sweep)")
        print(f"{'='*60}")
        try:
            ar_counts = drain_application_review(dry_run=args.dry_run)
            print(f"  AR drain: moved={ar_counts['moved']}, "
                  f"unscreened={ar_counts['stuck_unscreened']}, "
                  f"skipped-human={ar_counts['skipped_human']}, "
                  f"errors={ar_counts['errors']}\n")
        except Exception as e:
            print(f"  ⚠ Post-run drain failed: {e}\n")
        return

    print(f"\nPulling candidates from Ashby ({stages})...\n")
    candidates = pull_for_screening(limit=args.limit, dry_run=args.dry_run, force=force,
                                    include_leads=include_leads)

    if not candidates:
        print(f"  No candidates in {stages} to screen.")
        return

    print(f"\n  {len(candidates)} candidates pulled for screening:\n")
    for c in candidates:
        li = " (LinkedIn)" if c.get("linkedin") else " (no LinkedIn)"
        print(f"    {c['name']:<35} {c.get('job_title', ''):<30}{li}")

    if args.dry_run:
        print("\n  DRY RUN — no screening performed.")
        return

    _run_ashby_batch(args, max_parallel, candidates)

    # Post-run drain (single-batch path)
    print(f"\n{'='*60}")
    print("  POST-RUN DRAIN — Application Review (stranded-verdict sweep)")
    print(f"{'='*60}")
    try:
        ar_counts = drain_application_review(dry_run=args.dry_run)
        print(f"  AR drain: moved={ar_counts['moved']}, "
              f"unscreened={ar_counts['stuck_unscreened']}, "
              f"skipped-human={ar_counts['skipped_human']}, "
              f"errors={ar_counts['errors']}\n")
    except Exception as e:
        print(f"  ⚠ Post-run drain failed: {e}\n")


def _run_ashby_batch(args, max_parallel: int, candidates: list):
    """Screen a batch of candidates and write results to Ashby + Sheet."""
    from ashby_bridge import (
        write_screening_to_ashby_durable,
        replay_writeback_queue,
        format_interview_prep_note,
    )

    # Replay any writebacks that failed on a previous run (e.g. an Ashby 503
    # burst). These are replayed from the saved result — no re-screening, no
    # tokens — so a transient outage never permanently orphans a screened
    # candidate. Runs before screening so recovered candidates are settled first.
    try:
        replay_writeback_queue()
    except Exception as e:
        print(f"  ⚠ Writeback replay failed (non-fatal): {e}")

    # Load query learnings
    query_learnings = read_query_learnings()

    # 2. Screen in parallel (same pipeline as CSV mode)
    print(f"\nStarting screening with {max_parallel} parallel workers...\n")
    all_results = []
    completed = 0
    failed = 0
    total_cost = 0.0
    start_time = time.time()
    verdict_counts: Dict[str, int] = {}

    # Map candidate name → ashby IDs + input fields for writeback and export
    ashby_ids = {}
    candidate_inputs = {}
    for c in candidates:
        key = c["name"]
        ashby_ids[key] = {
            "candidate_id": c["ashby_candidate_id"],
            "application_id": c["ashby_application_id"],
        }
        candidate_inputs[key] = {
            "source": c.get("source", ""),
            "source_raw": c.get("source_raw", ""),  # raw Ashby source name for routing API calls
            "source_notes": c.get("source_notes", ""),
            "cv": c.get("cv", ""),
            "target_role": c.get("job_title", "") or c.get("target_role", ""),  # full job title for Sheet
            "linkedin_url": c.get("linkedin", "") or c.get("linkedin_url", ""),
            "github": c.get("github", ""),
            "ashby_candidate_id": c.get("ashby_candidate_id", ""),
        }

    # Assign synthetic row numbers + load cached dossiers for re-screening
    from ashby_bridge import _load_cache
    for i, c in enumerate(candidates, start=1):
        c["row"] = i
        # If candidate has a cached dossier, attach it for potential reuse
        cid = c.get("ashby_candidate_id", "")
        if cid:
            cached = _load_cache(cid)
            if cached.get("dossier") and len(cached["dossier"]) > 100:
                c["existing_dossier"] = cached["dossier"]

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = {
            pool.submit(screen_one, candidate, query_learnings, args.opus_only): candidate
            for candidate in candidates
        }

        try:
            for future in as_completed(futures):
                candidate = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "row": candidate["row"],
                        "name": candidate["name"],
                        "verdict": "SCREENING FAILED",
                        "verdict_reason": str(e)[:500],
                    }

                all_results.append(result)
                result["move_to"] = _ashby_destination_for_result(result, candidate)
                append_to_screening_log(result, candidate, mode="csv")
                completed += 1
                verdict = result.get("verdict", "UNKNOWN")
                verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

                cost = result.get("cost", 0)
                total_cost += cost
                elapsed_s = result.get("elapsed", 0)

                if "error" in result:
                    failed += 1
                    status = f"FAILED: {result.get('error', '')[:60]}"
                else:
                    status = f"{verdict:<25} ${cost:.3f}  ({elapsed_s:.0f}s)"

                print(f"  [{completed:>3}/{len(candidates)}] {result.get('name', '')[:30]:<30} | {status}")

                # Check for graceful stop signal
                if _check_stop_signal():
                    print(f"\n  🛑 Stop signal detected (.stop_screening). Saving {len(all_results)} results...")
                    _clear_stop_signal()
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

                # Live progress summary every 5 candidates
                if completed % 5 == 0 or completed == len(candidates):
                    elapsed_so_far = time.time() - start_time
                    avg_per = elapsed_so_far / completed if completed else 0
                    remaining = (len(candidates) - completed) * avg_per
                    destination_counts = _ashby_destination_counts(all_results, candidate_inputs)
                    print(f"\n  ── Progress: {completed}/{len(candidates)} ({elapsed_so_far/60:.1f}m elapsed, ~{remaining/60:.1f}m left) ──")
                    print(f"     {_format_destination_counts(destination_counts)}  |  Cost: ${total_cost:.2f}\n")

        except KeyboardInterrupt:
            print(f"\n\n  Stopped by user after {completed}/{len(candidates)} candidates.")
            pool.shutdown(wait=False, cancel_futures=True)

    # ── Defensive claim cleanup ──────────────────────────────────
    # Release every candidate's claim regardless of how the loop exited
    # (normal, stop-signal, Ctrl-C, per-future exception). release_claims
    # is idempotent — IDs already released by write_screening_to_ashby on
    # the happy path are a no-op. Catches three known leak paths:
    #   1. Stop-signal mid-batch — uncompleted candidates' claims persist
    #   2. KeyboardInterrupt — same
    #   3. Per-future exception (caught at line ~717) — claim never released
    try:
        from ashby_bridge import release_claims as _release_claims
        claim_ids = [c.get("ashby_candidate_id") for c in candidates if c.get("ashby_candidate_id")]
        if claim_ids:
            n_released = _release_claims(claim_ids)
            if n_released:
                print(f"  🧹 Released {n_released} unfinished claim(s) (incomplete or failed candidates)")
    except Exception as e:
        print(f"  ⚠ Claim cleanup failed: {e}")

    # 2b. Cache dossiers for re-screening
    from ashby_bridge import _save_cache
    for result in all_results:
        name = result.get("name", "")
        ids = ashby_ids.get(name, {})
        cid = ids.get("candidate_id", "")
        if cid and result.get("verdict", "").upper() != "SCREENING FAILED":
            dossier = result.get("research_output", "")
            if dossier and len(dossier) > 100:
                _save_cache(cid, {
                    "dossier": dossier,
                    "verdict": result.get("verdict", ""),
                    "screened_at": time.strftime("%Y-%m-%d %H:%M"),
                })

    # 3. Write results back to Ashby + Sheet in parallel
    print(f"\nWriting results to Ashby...")
    ashby_ok = 0
    ashby_fail = 0
    write_jobs = []

    for result in all_results:
        name = result.get("name", "")
        ids = ashby_ids.get(name, {})
        cid = ids.get("candidate_id", "")
        app_id = ids.get("application_id", "")

        if not cid or not app_id:
            print(f"  SKIP (no Ashby IDs): {name}")
            ashby_fail += 1
            continue

        # Inject target_role/job_title and source for post-screening job routing
        inputs = candidate_inputs.get(name, {})
        if not result.get("target_role") and not result.get("job_title"):
            result["target_role"] = inputs.get("target_role", "")
        # Pass raw Ashby source name so routing can resolve the sourceId correctly
        if not result.get("source_raw"):
            result["source_raw"] = inputs.get("source_raw", "")
        # Pass the PREFIXED source ("Inbound — LinkedIn") so get_verdict_stage's
        # inbound/outbound classifier reads the full qualifier, not the bare
        # title. format_result_for_import drops `source`, so without this
        # injection write_screening_to_ashby falls back to source_raw
        # ("LinkedIn") — which isn't an inbound pattern — and inbound SCREENs
        # misroute to Outbound Screened. (internal incident, 2026-05-27.)
        if not result.get("source"):
            result["source"] = inputs.get("source", "")

        write_jobs.append((cid, app_id, result))

    if write_jobs:
        write_workers = _resolve_ashby_write_workers(args, len(write_jobs))
        print(f"  Using {write_workers} parallel Ashby write workers")

        with ThreadPoolExecutor(max_workers=write_workers) as pool:
            futures = {
                pool.submit(write_screening_to_ashby_durable, cid, app_id, result): result
                for cid, app_id, result in write_jobs
            }
            for done, future in enumerate(as_completed(futures), start=1):
                result = futures[future]
                name = result.get("name", "")
                try:
                    ok = future.result()
                except Exception as e:
                    logger.warning("Ashby write failed for %s: %s", name, e)
                    ok = False

                if ok:
                    ashby_ok += 1
                    status = "ok"
                else:
                    ashby_fail += 1
                    status = "failed"
                print(f"  [{done:>3}/{len(write_jobs)}] Ashby write {status}: {name[:40]}")

    print(f"  Ashby: {ashby_ok} written, {ashby_fail} failed")

    # Save JSON locally — comprehensive export with inputs + outputs + metadata
    export_results = []
    from datetime import datetime
    screened_ts = datetime.now().strftime("%b %d, %Y %I:%M %p")
    for r in all_results:
        name = r.get("name", "")
        ids = ashby_ids.get(name, {})
        inputs = candidate_inputs.get(name, {})
        ashby_destination = _ashby_destination_for_result(r, inputs)
        # Determine destination tab
        v = (r.get("verdict") or "").upper()
        rt = r.get("rejection_type", "")
        nr = r.get("nurture", "")
        if v == "SCREEN":
            move_to = "Screen (Needs Review)"
        elif v in ("DEFER", "INSUFFICIENT DATA"):
            move_to = "Nurture"
        elif v == "DECLINE":
            # Unified with ashby_bridge.get_verdict_stage (2026-04-21):
            # all DECLINEs go to Archived, regardless of rejection_type.
            # Nurture is reserved for DEFER / INSUFFICIENT DATA.
            move_to = "Archived"
        elif v == "INPUT ERROR":
            move_to = "Archived"
        elif v == "SCREENING FAILED":
            move_to = ""  # Not screened — stays in Application Review, skip Sheet
            continue
        elif v == "DUPLICATE":
            move_to = ""
        else:
            move_to = "Archived"

        export = {
            # Identity / inputs (from original candidate pull)
            "name": name,
            "linkedin_url": inputs.get("linkedin_url", "") or r.get("linkedin", "") or r.get("linkedin_url", ""),
            "source": inputs.get("source", "") or r.get("source", ""),
            "source_raw": inputs.get("source_raw", ""),  # raw Ashby source name for routing
            "source_notes": inputs.get("source_notes", "") or r.get("source_notes", ""),
            "cv": inputs.get("cv", "") or r.get("cv", ""),
            "target_role": inputs.get("target_role", "") or r.get("target_role", ""),
            "ashby_candidate_id": inputs.get("ashby_candidate_id", "") or ids.get("candidate_id", ""),
            "github": inputs.get("github", "") or r.get("github", ""),
            # Screening outputs
            "verdict": r.get("verdict", ""),
            "rejection_type": rt,
            "nurture": nr,
            "spark": r.get("spark", ""),
            "verdict_reason": r.get("verdict_reason", ""),
            "best_fit_role": r.get("best_fit_role", ""),
            "best_fit_reason": r.get("best_fit_reason", ""),
            "matched_level": r.get("matched_level", ""),
            "reasoning": r.get("reasoning", ""),
            "regret_test": r.get("regret_test", ""),
            "concerns": r.get("concerns", ""),
            "screening_questions": r.get("screening_questions", ""),
            "screener_brief": r.get("screener_brief", ""),
            "defer_until": r.get("defer_until", ""),
            "outreach_1": r.get("outreach_1", ""),
            "outreach_2": r.get("outreach_2", ""),
            "research_output": r.get("research_output", ""),
            # Costs & metadata
            "sonar_cost": r.get("sonar_cost", ""),
            "opus_cost": r.get("opus_cost", ""),
            "total_cost": r.get("total_cost", ""),
            "token_log": r.get("token_log", ""),
            "prompt_version": r.get("prompt_version", ""),
            "last_screened_timestamp": screened_ts,
            "date_added": screened_ts,
            # Routing
            "ashby_destination": ashby_destination,
            "move_to": move_to,
        }
        export_results.append(export)
    output_path = save_results_json(export_results, args.output or "screening_results.json")
    print(f"  Results saved to: {output_path}")

    # 5. Summary
    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"  DONE — Ashby-native screening")
    print(f"{'='*60}")
    print(f"  Screened:    {completed}/{len(candidates)}")
    print(f"  Failed:      {failed}")
    print(f"  Ashby write: {ashby_ok} ok, {ashby_fail} failed")
    print(f"  Total cost:  ${total_cost:.2f}")
    print(f"  Total time:  {elapsed_total/60:.1f} minutes")

    # Verdict breakdown
    print(f"\n  Verdicts:")
    for v, count in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        print(f"    {v:<40} {count:>4}")

    # Ashby stage routing breakdown
    destination_summary = _ashby_destination_counts(all_results, candidate_inputs)
    print(f"\n  Destination stages:")
    for stage in ASHBY_DESTINATION_ORDER:
        count = destination_summary.get(stage, 0)
        if count > 0:
            print(f"    {stage:<35} {count:>4}")
    for stage, count in destination_summary.items():
        if stage not in ASHBY_DESTINATION_ORDER and count > 0:
            print(f"    {stage:<35} {count:>4}")

    # Role breakdown
    role_counts: Dict[str, int] = {}
    for r in all_results:
        role = r.get("best_fit_role", "") or r.get("target_role", "") or "Unknown"
        role_counts[role] = role_counts.get(role, 0) + 1
    print(f"\n  By role:")
    for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
        print(f"    {role:<35} {count:>4}")

    print(f"\n{'='*60}\n")

    # ── Auto-queue rejection emails for ANY candidate routed to Archived ──
    # Rule: archived + inbound → rejection email. We pass every application
    # whose routed destination is "Archived" (DECLINE, INSUFFICIENT DATA, and
    # DEFER+outbound all land here). rejection_emailer.py applies its own
    # source-type filter (inbound only) and tag filter (skip already-tagged),
    # so outbound DECLINE/INSUFFICIENT/DEFER are auto-skipped downstream.
    archived_app_ids = []
    for r in all_results:
        name = r.get("name", "")
        # Find matching candidate context for source lookup
        candidate_ctx = next((c for c in candidates if c.get("name") == name), {}) or {}
        destination = _ashby_destination_for_result(r, candidate_ctx)
        if destination != "Archived":
            continue
        ids = ashby_ids.get(name, {}) if 'ashby_ids' in dir() else {}
        aid = ids.get("application_id") if ids else None
        if not aid:
            aid = candidate_ctx.get("ashby_application_id")
        if aid:
            archived_app_ids.append(aid)
    if archived_app_ids:
        print(f"🔁 Auto-queueing rejection emails for {len(archived_app_ids)} archived candidate(s) (inbound-only filter applies downstream)...")
        try:
            import subprocess
            res = subprocess.run(
                ["python3", "rejection_emailer.py", "--application-ids", ",".join(archived_app_ids)],
                cwd=str(Path(__file__).resolve().parent),
                capture_output=True, text=True, timeout=180,
            )
            if res.stdout:
                print(res.stdout[-2000:])
            if res.returncode != 0:
                print(f"  ⚠️  rejection_emailer exited {res.returncode}: {res.stderr[-500:]}")
        except Exception as e:
            print(f"  ⚠️  Could not run rejection_emailer: {e}")
    else:
        print("(no candidates routed to Archived in this run — nothing to queue for rejection email)")


def _save_fallback(results):
    """Save results to JSON when email bridge fails."""
    path = Path("screening_results.json")
    path.write_text(json.dumps({"results": results}, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Email failed — saved to {path} for manual import")


def main():
    parser = argparse.ArgumentParser(
        description="Parallel batch candidate screener (CSV or Ashby mode)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Workflow (CSV):
  1. In Google Sheets: File > Download > CSV
  2. python3 screen_batch.py
  3. Results auto-emailed to Sheet

Workflow (Ashby):
  1. python3 screen_batch.py --from-ashby
  2. Pulls from Application Review → screens → writes back to Ashby + Sheet
        """,
    )
    parser.add_argument("--from-ashby", action="store_true",
                        help="Pull candidates from Ashby instead of CSV")
    parser.add_argument("--csv", default=None,
                        help="Path to CSV file (default: auto-find latest in ~/Downloads)")
    parser.add_argument("--parallel", type=int, default=None,
                        help=f"Max parallel screenings (default: {DEFAULT_PARALLEL})")
    parser.add_argument("--write-parallel", type=int, default=None,
                        help="Max parallel Ashby writebacks (default: ASHBY_WRITE_PARALLEL or 5)")
    parser.add_argument("--rescreen", action="store_true",
                        help="Re-screen rows that already have a verdict")
    parser.add_argument("--opus-only", action="store_true",
                        help="Judgment rerun: skip research, reuse existing dossier")
    parser.add_argument("--rows", default=None,
                        help="Comma-separated row numbers to screen (e.g. 5,12,18)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max candidates to pull from Ashby (0=all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be screened without doing it")
    parser.add_argument("--force", action="store_true",
                        help="Bypass Sheet skip-set — re-screen even if already in Sheet")
    parser.add_argument("--include-leads", action="store_true", default=True,
                        help="Pull candidates from 'New Lead' stage (default: on)")
    parser.add_argument("--no-leads", dest="include_leads", action="store_false",
                        help="Skip 'New Lead' stage, only pull from Application Review")
    parser.add_argument("--batch-all", action="store_true",
                        help="Screen ALL unscreened candidates in continuous batches of --limit (default 50). Scans Ashby once.")
    parser.add_argument("--output", default="",
                        help="Output JSON file path (default: screening_results.json)")
    parser.add_argument("--pre-filter", action="store_true",
                        help="Run cheap Haiku pre-screen before full Opus pipeline (saves ~$0.30/filtered candidate)")
    parser.add_argument("--pre-threshold", default="medium", choices=["high", "medium"],
                        help="Pre-screen threshold: 'medium' passes HIGH+MEDIUM, 'high' passes only HIGH (default: medium)")
    args = parser.parse_args()

    max_parallel = args.parallel or int(os.environ.get("MAX_PARALLEL", str(DEFAULT_PARALLEL)))

    # ── Ashby mode ──
    if args.from_ashby:
        _run_ashby_mode(args, max_parallel)
        return

    # ── Validate environment (CSV mode) ──
    missing = []
    if not os.environ.get("CLAUDE_API_KEY"):
        missing.append("CLAUDE_API_KEY")
    if not args.opus_only and not os.environ.get("LINKUP_API_KEY"):
        missing.append("LINKUP_API_KEY")

    if missing:
        print("\nMissing required environment variables:")
        for v in missing:
            print(f"   - {v}")
        print("\nSet them before running:")
        print('   export CLAUDE_API_KEY="sk-ant-..."')
        print('   export LINKUP_API_KEY="..."')
        print('   export APIFY_TOKEN="apify_api_..."')
        sys.exit(1)

    # ── Parse specific rows ──
    specific_rows = None
    if args.rows:
        try:
            specific_rows = set(int(r.strip()) for r in args.rows.split(",") if r.strip())
        except ValueError:
            print("--rows must be comma-separated numbers (e.g. 5,12,18)")
            sys.exit(1)

    # ── Find CSV file ──
    csv_path = args.csv
    if not csv_path:
        csv_path = auto_find_csv()
        if not csv_path:
            print("\nNo CSV file found. Either:")
            print("  1. Export from Google Sheets (File > Download > CSV) — it auto-detects in ~/Downloads")
            print("  2. Specify a path: python3 screen_batch.py --csv path/to/file.csv")
            sys.exit(1)
        print(f"\nAuto-detected: {csv_path}")
        confirm_file = input("  Use this file? (y/n): ").strip().lower()
        if confirm_file not in ("y", "yes", ""):
            print("  Specify a CSV: python3 screen_batch.py --csv path/to/file.csv")
            sys.exit(0)

    # ── Read candidates from CSV ──
    need_rescreen = args.rescreen or args.opus_only
    print(f"\nReading candidates from '{csv_path}'...")
    try:
        candidates = read_candidates_csv(csv_path, rescreen=need_rescreen)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    # Filter to specific rows if requested
    if specific_rows:
        candidates = [c for c in candidates if c["row"] in specific_rows]

    # For opus-only, only include candidates that have an existing dossier
    if args.opus_only:
        before = len(candidates)
        candidates = [c for c in candidates if len((c.get("existing_dossier") or "").strip()) > 100]
        skipped = before - len(candidates)
        if skipped:
            print(f"  Skipped {skipped} candidates with no existing dossier (opus-only requires a dossier)")

    if not candidates:
        print("\nNothing to screen! All candidates already have verdicts.")
        if not need_rescreen:
            print("   Use --rescreen to re-screen candidates that already have a verdict.")
        sys.exit(0)


    # ── Pre-filter with Haiku (optional) ──
    pre_filter_savings = 0.0
    if args.pre_filter and not args.opus_only:
        print(f"\n  Running pre-screening filter (Haiku fast check)...")
        archive_patterns = load_archive_patterns()
        threshold_set = {"HIGH"} if args.pre_threshold == "high" else {"HIGH", "MEDIUM"}
        passed = []
        filtered = []
        for c in candidates:
            result = pre_screen_candidate(
                name=c["name"],
                linkedin=c.get("linkedin", ""),
                source_notes=c.get("source_notes", ""),
                cv=c.get("cv", ""),
                target_role=c.get("target_role", ""),
                archive_patterns=archive_patterns,
            )
            tier = result["confidence"]
            symbol = {"HIGH": "✅", "MEDIUM": "🟡", "LOW": "❌"}.get(tier, "?")
            print(f"    {symbol} {tier:<6} | {c['name']:<35} | {result['reason'][:55]}")

            if tier in threshold_set:
                passed.append(c)
            else:
                filtered.append(c)

        if filtered:
            pre_filter_savings = len(filtered) * 0.30
            print(f"\n  Pre-filter: {len(passed)} proceed, {len(filtered)} filtered out")
            print(f"  Estimated savings: ${pre_filter_savings:.2f}")
            candidates = passed

        if not candidates:
            print("\n  All candidates filtered out by pre-screening. Nothing to screen.")
            sys.exit(0)

    # ── Read query learnings (local cache) ──
    query_learnings = ""
    if not args.opus_only:
        print("Reading query learnings...")
        query_learnings = read_query_learnings()
        if query_learnings:
            count = len([l for l in query_learnings.split("\n") if l.strip()])
            print(f"  Loaded {count} past learnings")
        else:
            print("  No past learnings found")

    # ── Resume from partial results if available ──
    partial_path = Path("screening_results_partial.json")
    resumed_results = []
    if partial_path.exists() and not args.opus_only:
        try:
            with open(partial_path, "r") as f:
                partial_data = json.load(f)
            partial_results = partial_data.get("results", []) if isinstance(partial_data, dict) else partial_data
            if isinstance(partial_results, list) and len(partial_results) > 0:
                # Match on name+linkedin (stable) instead of row number (shifts between exports)
                def _result_key(r):
                    n = (r.get("name") or "").strip().lower()
                    li = (r.get("linkedin") or "").strip().lower().rstrip("/")
                    return f"{n}|{li}"
                done_keys = {_result_key(r) for r in partial_results if isinstance(r, dict) and r.get("verdict") and r.get("verdict") != "SCREENING FAILED"}
                def _candidate_key(c):
                    n = (c.get("name") or "").strip().lower()
                    li = (c.get("linkedin") or "").strip().lower().rstrip("/")
                    return f"{n}|{li}"
                before_count = len(candidates)
                candidates = [c for c in candidates if _candidate_key(c) not in done_keys]
                skipped = before_count - len(candidates)
                if skipped > 0:
                    resumed_results = [r for r in partial_results if isinstance(r, dict) and _result_key(r) in done_keys]
                    print(f"\n  📋 Resuming: found {len(resumed_results)} results from previous run, skipping those.")
                    print(f"  Remaining to screen: {len(candidates)}")
                    if len(candidates) == 0:
                        print("  All candidates already screened. Nothing to do.")
                        # Copy full results to clipboard
                        export = [{k: v for k, v in r.items() if k not in ("error", "elapsed", "cost")} for r in resumed_results]
                        final_json = json.dumps(export, indent=2, ensure_ascii=False)
                        try:
                            subprocess.run(["pbcopy"], input=final_json.encode(), check=True)
                            print("  ✅ Full results copied to clipboard.")
                        except Exception:
                            pass
                        sys.exit(0)
        except Exception:
            pass  # If partial file is corrupt, just screen everything fresh

    # ── Estimate cost ──
    if args.opus_only:
        est_cost_per = 0.35
        mode_label = "OPUS-ONLY (judgment rerun, reusing existing dossiers)"
    elif args.rescreen:
        with_dossier = sum(1 for c in candidates if len((c.get("existing_dossier") or "").strip()) > 100)
        without_dossier = len(candidates) - with_dossier
        est_cost_per = 0.45
        mode_label = f"RE-SCREEN ({with_dossier} with existing dossier, {without_dossier} need full research)"
    else:
        est_cost_per = 0.45
        mode_label = "FULL SCREENING"

    est_minutes = (len(candidates) / max_parallel) * 1.5
    est_cost = len(candidates) * est_cost_per

    # ── Show plan ──
    print(f"\n{'='*60}")
    print(f"  SCREENING PLAN")
    print(f"{'='*60}")
    print(f"  Mode:                  {mode_label}")
    print(f"  Candidates to screen:  {len(candidates)}")
    print(f"  Parallel workers:      {max_parallel}")
    print(f"  Estimated time:        ~{est_minutes:.0f} minutes")
    print(f"  Estimated cost:        ~${est_cost:.2f}")
    print(f"  CSV file:              {csv_path}")
    if os.environ.get("APIFY_TOKEN"):
        print(f"  LinkedIn scraping:     Enabled (Apify)")
    else:
        print(f"  LinkedIn scraping:     Disabled (no APIFY_TOKEN)")
    print(f"{'='*60}")

    show = min(len(candidates), 10)
    print(f"\n  First {show} candidates:")
    for c in candidates[:show]:
        dossier_status = "has dossier" if len((c.get("existing_dossier") or "").strip()) > 100 else "no dossier"
        print(f"    Row {c['row']:>4}: {c['name'][:35]:<35} | {c.get('target_role', '')[:20]:<20} | {dossier_status}")
    if len(candidates) > 10:
        print(f"    ... and {len(candidates) - 10} more")

    if args.dry_run:
        print("\nDRY RUN — no screening performed.")
        sys.exit(0)

    # ── Confirm ──
    print()
    confirm = input("  Proceed? (y/n): ").strip().lower()
    if confirm not in ("y", "yes"):
        print("  Cancelled.")
        sys.exit(0)

    # ── Run parallel screening ──
    print(f"\nStarting screening with {max_parallel} parallel workers...\n")

    all_results = []
    completed = 0
    failed = 0
    total_cost = 0.0
    start_time = time.time()
    verdict_counts: Dict[str, int] = {}
    move_counts: Dict[str, int] = {}

    # Incremental save path — results saved after every candidate
    incremental_path = Path(args.output) if args.output else Path("screening_results_partial.json")
    def _save_incremental():
        """Save results after each candidate so nothing is lost on crash/interrupt."""
        export = [{k: v for k, v in r.items() if k not in ("error", "elapsed", "cost")} for r in all_results]
        save_results_json(export, str(incremental_path))


    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = {
            pool.submit(screen_one, candidate, query_learnings, args.opus_only): candidate
            for candidate in candidates
        }

        try:
            for future in as_completed(futures):
                candidate = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "row": candidate["row"],
                        "name": candidate["name"],
                        "verdict": "SCREENING FAILED",
                        "error": str(e),
                    }

                all_results.append(result)
                append_to_screening_log(result, candidate, mode="ashby")
                completed += 1
                verdict = result.get("verdict", "UNKNOWN")
                verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

                move_to = result.get("move_to", "")
                if move_to:
                    move_counts[move_to] = move_counts.get(move_to, 0) + 1

                if "error" in result:
                    failed += 1
                    status = f"FAILED: {result.get('error', '')[:60]}"
                else:
                    cost = result.get("cost", 0)
                    total_cost += cost
                    elapsed_s = result.get("elapsed", 0)
                    move_info = f" -> {move_to}" if move_to else ""
                    status = f"{verdict:<30}{move_info:<25} ${cost:.3f}  ({elapsed_s:.0f}s)"

                print(f"  [{completed:>3}/{len(candidates)}] Row {result['row']:>4} | "
                      f"{result.get('name', '')[:30]:<30} | {status}")

                # Save after every candidate — crash-proof
                _save_incremental()

                # Check for graceful stop signal
                if _check_stop_signal():
                    print(f"\n  🛑 Stop signal detected (.stop_screening). Saving {len(all_results)} results...")
                    _clear_stop_signal()
                    _save_incremental()
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

        except KeyboardInterrupt:
            print(f"\n\n  Stopped by user after {completed}/{len(candidates)} candidates.")
            print(f"  Saving {len(all_results)} partial results...")
            _save_incremental()
            print(f"  ✅ Partial results saved to: {incremental_path}")
            pool.shutdown(wait=False, cancel_futures=True)

    # ── Save results ──
    # Merge resumed results with new results
    all_results = resumed_results + all_results

    # Remove internal fields before saving
    export_results = []
    for r in all_results:
        export = {k: v for k, v in r.items() if k not in ("error", "elapsed", "cost")}
        export_results.append(export)

    output_path = save_results_json(export_results, args.output)
    print(f"\n  Results saved to: {output_path}")

    # Copy to clipboard
    results_json = json.dumps({"results": export_results}, ensure_ascii=False)
    try:
        if copy_to_clipboard(results_json):
            print("  Results copied to clipboard (paste into Apps Script import dialog)")
        else:
            print("  Could not copy to clipboard — use the JSON file instead")
    except Exception:
        print("  Could not copy to clipboard — use the JSON file instead")

    # ── Summary ──
    elapsed_total = time.time() - start_time
    minutes = elapsed_total / 60

    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"{'='*60}")
    print(f"  Total screened:  {completed}/{len(candidates)}")
    print(f"  Failed:          {failed}")
    print(f"  Total cost:      ${total_cost:.2f}")
    print(f"  Total time:      {minutes:.1f} minutes")
    if completed > 0:
        print(f"  Avg per candidate: {elapsed_total/completed:.0f}s")

    print(f"\n  Verdicts:")
    for v, count in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        print(f"    {v:<40} {count:>4}")

    if move_counts:
        print(f"\n  Will move to:")
        for tab_name, count in sorted(move_counts.items(), key=lambda x: -x[1]):
            print(f"    {tab_name:<40} {count:>4}")

    print(f"\n  NEXT STEP:")
    print(f"  In Google Sheets: Python Pipeline > Import Screening Results")
    print(f"  Paste the JSON from clipboard — it writes verdicts, colors cells, and moves rows.")

    # ── Auto-analyze rejections for query improvement ──
    decline_count = sum(1 for r in export_results if (r.get("verdict") or "").upper() == "DECLINE")
    if decline_count >= 3:
        print(f"\n  💡 {decline_count} candidates were declined.")
        print(f"  Run 'optimize' to analyze rejection patterns and get better Juicebox queries.")
    print(f"{'='*60}\n")

    # ── Auto-queue rejection emails (any candidate routed to Archived) ──
    # Rule: archived + inbound → rejection email. We use the same
    # _ashby_destination_for_result helper that built the summary table above,
    # so a candidate is queued for rejection email iff their routed destination
    # is "Archived" (DECLINE, INSUFFICIENT DATA, DEFER+outbound).
    # The rejection_emailer script applies its own inbound-only and
    # already-tagged filters downstream.
    archived_app_ids = []
    for r in export_results:
        destination = _ashby_destination_for_result(r)
        if destination != "Archived":
            continue
        name = r.get("name", "")
        ids = ashby_ids.get(name, {})
        aid = ids.get("application_id")
        if aid:
            archived_app_ids.append(aid)
    if archived_app_ids:
        print(f"\n🔁 Auto-queueing rejection emails for {len(archived_app_ids)} archived candidate(s) (inbound-only filter applies downstream)...")
        try:
            import subprocess
            result = subprocess.run(
                ["python3", "rejection_emailer.py", "--application-ids", ",".join(archived_app_ids)],
                cwd=str(Path(__file__).resolve().parent),
                capture_output=True, text=True, timeout=180,
            )
            print(result.stdout[-2000:] if result.stdout else "")
            if result.returncode != 0:
                print(f"  ⚠️  rejection_emailer exited {result.returncode}: {result.stderr[-500:]}")
        except Exception as e:
            print(f"  ⚠️  Could not run rejection_emailer: {e}")


if __name__ == "__main__":
    main()
