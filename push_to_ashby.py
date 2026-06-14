"""
Universal CSV → Ashby pusher — creates candidates in the Outbound Sourced project.

Takes candidates from GitHub sourcer, Juicebox exports, or any CSV with name + LinkedIn URL,
creates them in Ashby with proper source tagging, and adds them to the Outbound Sourced project.
After screening, candidates get routed to the correct job based on best_fit_role.

## When to use --job (IMPORTANT)

There are two valid intake patterns. Pick the right one for your sourcing context:

  ROLE-SPECIFIC SOURCING ("source 50 candidates for Recruiting Lead"):
    → ALWAYS use --job "<Role Name>" so candidates land directly on that role's job.
    → Skips the Outbound Sourced intermediate — no duplicate apps, no consolidation needed.

  ROLE-AGNOSTIC SOURCING ("here are 50 AI talent profiles, let AI route them"):
    → Omit --job. Candidates land in Outbound Sourced (general pool).
    → After screening, best-fit consolidation moves SCREEN candidates to their role job.

Do NOT push without --job when you know the role — that creates an application on
Outbound Sourced when one on the target role job is what you actually want.

Usage:
  # Role-specific (preferred when you know the role):
  python3 push_to_ashby.py csv --source "Sourced: LinkedIn" --job "Recruiting Lead"
  python3 push_to_ashby.py csv --source "Outbound - Github Sourced" --job "AI Backend Engineer"

  # Role-agnostic (general pool, let AI route post-screen):
  python3 push_to_ashby.py csv --source "Juicebox Outbound"
  python3 push_to_ashby.py csv --source "Candidate Labs"

  # Other flags:
  python3 push_to_ashby.py csv --dry-run    # preview without pushing
  python3 push_to_ashby.py csv --limit 10   # push first 10 only

Flow:
  CSV → candidate.create
      → if --job:    application on that role's job → New Lead → ascreen → role-specific stage
      → if no --job: application on Outbound Sourced → New Lead → ascreen → best-fit consolidation

Required:
  ASHBY_API_KEY env var
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Locks protect shared state when pushing in parallel
_PUSH_LOG_LOCK = threading.Lock()
_ERROR_LOG_LOCK = threading.Lock()
_STATS_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
ROUTING_FILE = _DIR / ".ashby_job_routing.json"
SCREENING_LOG = _DIR / "screening_log.csv"
PUSH_LOG = _DIR / ".push_to_ashby_log.json"
ERROR_LOG = _DIR / ".push_to_ashby_errors.json"

# Default project and job for all outbound candidates
OUTBOUND_PROJECT_ID = "REPLACE_WITH_YOUR_OUTBOUND_PROJECT_ID"
OUTBOUND_JOB_ID = "REPLACE_WITH_YOUR_OUTBOUND_JOB_ID"


def _log_error(entry: dict):
    """Append a failure record to .push_to_ashby_errors.json (persisted immediately).

    Thread-safe: protected by _ERROR_LOG_LOCK so concurrent workers can't stomp
    on each other's writes. Re-reads the file under the lock in case another
    process (outside our run) has appended entries.
    """
    from datetime import datetime
    entry = {"ts": datetime.utcnow().isoformat() + "Z", **entry}
    with _ERROR_LOG_LOCK:
        existing = []
        if ERROR_LOG.exists():
            try:
                existing = json.loads(ERROR_LOG.read_text(encoding="utf-8"))
            except Exception:
                existing = []
        existing.append(entry)
        from ashby_bridge import write_json_atomic
        write_json_atomic(ERROR_LOG, existing, indent=2, ensure_ascii=False)


def _post_with_retry(endpoint: str, payload: dict, retries: int = 3) -> dict:
    """Wraps _ashby_post and also retries on success=False (not just HTTP errors).

    _ashby_post already retries 429/5xx/network. This adds retries for Ashby-side
    success=False responses (e.g. transient write conflicts) that otherwise leak
    past the HTTP-level retry. Returns the last result whether successful or not.
    """
    from ashby_bridge import _ashby_post
    last = {}
    for attempt in range(retries):
        last = _ashby_post(endpoint, payload)
        if last.get("success"):
            return last
        if attempt < retries - 1:
            time.sleep(1 + attempt)  # 1s, 2s backoff
    return last


# ── LinkedIn normalization (shared with csv_bridge + ashby_bridge) ──

def _normalize_linkedin(url: str) -> str:
    if not url:
        return ""
    s = url.strip().lower()
    s = re.sub(r"https?://(www\.)?linkedin\.com", "", s, flags=re.I)
    s = s.rstrip("/")
    s = re.sub(r"\?.*$", "", s)
    return s


# ── Job routing config ──────────────────────────────────────────

def load_routing() -> dict:
    """Load .ashby_job_routing.json → {normalized_alias: job_id}"""
    if not ROUTING_FILE.exists():
        return {}
    data = json.loads(ROUTING_FILE.read_text(encoding="utf-8"))
    lookup = {}
    for role_name, info in data.get("roles", {}).items():
        job_id = info["job_id"]
        lookup[role_name.lower()] = job_id
        for alias in info.get("aliases", []):
            lookup[alias.lower()] = job_id
    return lookup


def resolve_job_id(role_hint: str, routing: dict) -> Optional[str]:
    """Resolve a role hint (short code, name, or partial) to an Ashby job ID."""
    if not role_hint:
        return None
    hint = role_hint.strip().lower()

    if hint in routing:
        return routing[hint]

    for key, job_id in routing.items():
        if hint in key or key in hint:
            return job_id

    return None


# ── Dedup: load known LinkedIn URLs from screening log ──────────

def _load_known_linkedins() -> Set[str]:
    """Load normalized LinkedIn URLs from screening_log.csv for dedup."""
    known = set()
    if not SCREENING_LOG.exists():
        return known
    try:
        with open(SCREENING_LOG, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                li = (row.get("linkedin") or "").strip()
                if li:
                    known.add(_normalize_linkedin(li))
    except Exception as e:
        logger.warning("Could not read screening log: %s", e)
    return known


def _load_push_log() -> Dict[str, str]:
    """Load {normalized_linkedin: ashby_candidate_id} from push log."""
    if PUSH_LOG.exists():
        try:
            return json.loads(PUSH_LOG.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_push_log(log: dict):
    from ashby_bridge import write_json_atomic
    write_json_atomic(PUSH_LOG, log, indent=2, ensure_ascii=False)


# ── CSV reading (auto-detect columns) ──────────────────────────

COLUMN_ALIASES = {
    "name": ["name", "candidate name", "full name", "candidate_name"],
    "linkedin": ["linkedin_url", "linkedin url", "linkedin", "linkedin profile"],
    "email": ["email", "email address", "primary email"],
    "source": ["source"],
    "cv": ["cv_summary", "cv / resume notes", "cv", "bio", "headline", "summary"],
    "target_role": ["target_role", "target role", "role"],
    "github": ["github_url", "github", "github profile"],
    "website": ["website", "portfolio", "personal site"],
    "company": ["company", "organization", "current company"],
    "location": ["location", "city"],
    "resume_path": ["resume_path", "resume", "pdf_path", "pdf"],
}


def _find_column(headers: List[str], key: str) -> Optional[int]:
    """Find column index by checking aliases."""
    normalized = [h.strip().lower() for h in headers]
    for alias in COLUMN_ALIASES.get(key, []):
        try:
            return normalized.index(alias)
        except ValueError:
            continue
    return None


def read_csv_candidates(csv_path: str) -> List[Dict[str, str]]:
    """Read candidates from any CSV format, auto-detecting columns."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 2:
        return []

    headers = rows[0]
    col_map = {}
    for key in COLUMN_ALIASES:
        idx = _find_column(headers, key)
        if idx is not None:
            col_map[key] = idx

    if "name" not in col_map:
        raise RuntimeError(f"No 'name' column found. Headers: {headers[:10]}")

    candidates = []
    for row in rows[1:]:
        def cell(key):
            idx = col_map.get(key)
            if idx is not None and idx < len(row):
                return row[idx].strip()
            return ""

        name = cell("name")
        if not name:
            continue

        candidates.append({
            "name": name,
            "linkedin": cell("linkedin"),
            "email": cell("email"),
            "source": cell("source"),
            "cv": cell("cv"),
            "target_role": cell("target_role"),
            "github": cell("github"),
            "website": cell("website"),
            "company": cell("company"),
            "location": cell("location"),
            "resume_path": cell("resume_path"),
        })

    return candidates


# ── Main push logic ─────────────────────────────────────────────

def _candidate_has_matching_linkedin(existing_candidate: dict, target_li_norm: str) -> bool:
    """Check if an existing Ashby candidate has a LinkedIn URL matching target.

    Uses candidate.info to fetch socialLinks (search result may not include them).
    Returns True if any LinkedIn URL on the existing record normalizes to target.
    Returns False if no match OR if the check fails (caller decides whether to skip).
    """
    from ashby_bridge import _ashby_post
    cid = existing_candidate.get("id", "")
    if not cid:
        return False
    try:
        info = _ashby_post("candidate.info", {"id": cid})
        if not info.get("success"):
            return False
        cand = info.get("results", {}) or {}
        for link in cand.get("socialLinks", []) or []:
            if (link.get("type") or "").lower() == "linkedin":
                if _normalize_linkedin(link.get("url", "")) == target_li_norm:
                    return True
    except Exception:
        return False
    return False


def _resolve_resume_path(c: Dict[str, str], resume_dir: str) -> str:
    """Return an absolute path to the candidate's PDF, or '' if none available."""
    raw = (c.get("resume_path") or "").strip()
    if not raw:
        return ""
    p = Path(raw)
    if p.is_absolute() and p.exists():
        return str(p)
    if resume_dir:
        rel = Path(resume_dir) / p.name
        if rel.exists():
            return str(rel)
    if p.exists():
        return str(p.resolve())
    return ""


def _upload_resume(candidate_id: str, pdf_path: str, name: str) -> bool:
    """Upload a local PDF as the Ashby candidate's resume.

    Reuses the flow from slack_intake._upload_resume_to_ashby:
    file.createFileUploadHandle → presigned S3 POST → candidate.uploadResume.
    """
    from ashby_bridge import _ashby_post
    try:
        pdf_bytes = Path(pdf_path).read_bytes()
    except Exception as e:
        logger.warning("  Resume read failed for %s (%s): %s", name, pdf_path, e)
        return False
    if not pdf_bytes.startswith(b"%PDF"):
        logger.warning("  Skipping %s — resume at %s is not a PDF", name, pdf_path)
        return False
    safe_name = Path(pdf_path).name or f"{candidate_id[:8]}_resume.pdf"
    try:
        handle_resp = _ashby_post("file.createFileUploadHandle", {
            "fileUploadContext": "CandidateResume",
            "filename": safe_name,
            "contentType": "application/pdf",
            "contentLength": len(pdf_bytes),
        })
        if not handle_resp.get("success"):
            logger.warning("  createFileUploadHandle failed for %s: %s", name, handle_resp)
            return False
        r = handle_resp.get("results", {}) or {}
        presigned_url = r.get("uploadUrl") or r.get("url", "")
        file_handle = r.get("fileHandle") or r.get("handle", "")
        fields = r.get("fields", {}) or {}
        if not presigned_url or not file_handle:
            return False
        import requests
        form = dict(fields)
        form.setdefault("Content-Type", "application/pdf")
        files = {"file": (safe_name, pdf_bytes, "application/pdf")}
        post_resp = requests.post(presigned_url, data=form, files=files, timeout=60)
        if post_resp.status_code not in (200, 201, 204):
            logger.warning("  Presigned upload failed for %s: %s", name, post_resp.status_code)
            return False
        attach = _ashby_post("candidate.uploadResume", {
            "candidateId": candidate_id,
            "resumeHandle": file_handle,
        })
        if not attach.get("success"):
            logger.warning("  candidate.uploadResume failed for %s: %s", name, attach)
            return False
        return True
    except Exception as e:
        logger.warning("  Resume upload error for %s: %s", name, e)
        return False


def _push_one(
    c: Dict[str, str],
    index: int,
    total: int,
    source: str,
    source_id: Optional[str],
    project_id: str,
    outbound_job: str,
    csv_source: str,
    known_linkedins: Set[str],
    push_log: Dict[str, str],
    resume_dir: str = "",
) -> str:
    """Push one candidate to Ashby. Thread-safe; returns outcome tag.

    Outcomes: 'pushed', 'partial', 'skipped_dedup', 'skipped_no_linkedin',
    'skipped_name_match', 'failed'.

    The dedup pre-filter (run sequentially before dispatching to workers) already
    removed candidates caught by the local log. This function still calls
    search_candidate → LinkedIn verification because that requires Ashby round-trips
    and is cheap to parallelize.
    """
    from ashby_bridge import search_candidate

    name = c["name"]
    linkedin = c.get("linkedin", "")

    li_norm = _normalize_linkedin(linkedin)

    # Name-match dedup with LinkedIn verification (Fix 3).
    try:
        existing = search_candidate(name=name)
        if existing:
            if _candidate_has_matching_linkedin(existing, li_norm):
                logger.info("  [%d/%d] SKIP (LinkedIn match in Ashby): %s → %s",
                            index, total, name, existing.get("id", "")[:12])
                with _PUSH_LOG_LOCK:
                    push_log[li_norm] = existing.get("id", "")
                    known_linkedins.add(li_norm)
                    _save_push_log(push_log)
                return "skipped_dedup"
            else:
                logger.info("  [%d/%d] Name match in Ashby but LinkedIn differs — pushing: %s",
                            index, total, name)
    except Exception as e:
        _log_error({
            "csv": csv_source, "name": name, "linkedin": linkedin,
            "step": "search_candidate", "error": str(e),
        })

    # Build candidate payload
    email = c.get("email", "")
    social_links = [{"type": "LinkedIn", "url": linkedin}]
    github = c.get("github", "")
    if github:
        social_links.append({"type": "GitHub", "url": github})
    website = c.get("website", "")
    if website:
        social_links.append({"type": "Website", "url": website})

    payload: Dict[str, Any] = {"name": name}
    if source_id:
        payload["sourceId"] = source_id
    else:
        payload["source"] = source
    if email:
        payload["emailAddresses"] = [{"value": email, "type": "Primary"}]
    if c.get("location"):
        payload["location"] = c["location"]

    # 1. Create candidate
    result = _post_with_retry("candidate.create", payload)
    if not result.get("success"):
        logger.error("  [%d/%d] FAILED to create: %s — %s", index, total, name, result)
        _log_error({
            "csv": csv_source, "name": name, "linkedin": linkedin,
            "step": "candidate.create", "error": str(result)[:500],
        })
        return "failed"

    candidate_id = result["results"]["id"]

    # Persist push log IMMEDIATELY after create.
    with _PUSH_LOG_LOCK:
        push_log[li_norm] = candidate_id
        known_linkedins.add(li_norm)
        _save_push_log(push_log)

    partial = False

    # 1b. Attach social links (candidate.create ignores socialLinks)
    if social_links:
        r = _post_with_retry("candidate.update", {
            "candidateId": candidate_id,
            "socialLinks": social_links,
        })
        if not r.get("success"):
            partial = True
            _log_error({
                "csv": csv_source, "name": name, "linkedin": linkedin,
                "candidate_id": candidate_id,
                "step": "candidate.update/socialLinks", "error": str(r)[:500],
            })

    # 2. Create application on Outbound Sourced job
    app_payload: Dict[str, Any] = {"candidateId": candidate_id, "jobId": outbound_job}
    if source_id:
        app_payload["sourceId"] = source_id
    else:
        app_payload["source"] = source
    app_result = _post_with_retry("application.create", app_payload)
    app = app_result.get("results") if app_result.get("success") else None
    if not app:
        partial = True
        _log_error({
            "csv": csv_source, "name": name, "linkedin": linkedin,
            "candidate_id": candidate_id,
            "step": "application.create", "error": str(app_result)[:500],
        })

    # 2b. Ensure source sticks via application.changeSource
    if app and source_id:
        r = _post_with_retry("application.changeSource", {
            "applicationId": app.get("id", ""),
            "sourceId": source_id,
        })
        if not r.get("success"):
            _log_error({
                "csv": csv_source, "name": name, "linkedin": linkedin,
                "candidate_id": candidate_id,
                "step": "application.changeSource", "error": str(r)[:500],
            })

    # 3. Add to Outbound Sourced project (non-fatal if it fails)
    _post_with_retry("candidate.addProject", {
        "candidateId": candidate_id,
        "projectId": project_id,
    })

    # 3b. Upload resume PDF if we have one (WaaS/Slack flows include it)
    resume_path = _resolve_resume_path(c, resume_dir)
    if resume_path:
        if not _upload_resume(candidate_id, resume_path, name):
            _log_error({
                "csv": csv_source, "name": name, "linkedin": linkedin,
                "candidate_id": candidate_id,
                "step": "upload_resume", "error": f"failed for {resume_path}",
            })
            # Non-fatal — ascreen still has LinkedIn + web research

    # 4. Add context note
    cv = c.get("cv", "")
    company = c.get("company", "")
    note_parts = []
    if company:
        note_parts.append(f"<b>Company:</b> {company}")
    if cv:
        note_parts.append(f"<b>Summary:</b> {cv[:2000]}")
    if github:
        note_parts.append(f"<b>GitHub:</b> <a href=\"{github}\">{github}</a>")
    if note_parts:
        r = _post_with_retry("candidate.createNote", {
            "candidateId": candidate_id,
            "note": {"type": "text/html", "value": "<br>".join(note_parts)},
        })
        if not r.get("success"):
            _log_error({
                "csv": csv_source, "name": name, "linkedin": linkedin,
                "candidate_id": candidate_id,
                "step": "candidate.createNote", "error": str(r)[:500],
            })

    if partial:
        logger.warning("  [%d/%d] PARTIAL: %s → %s (see errors log)",
                       index, total, name, candidate_id[:12])
        return "partial"
    logger.info("  [%d/%d] PUSHED: %s → %s", index, total, name, candidate_id[:12])
    return "pushed"


def push_candidates(
    candidates: List[Dict[str, str]],
    source: str,
    project_id: str = OUTBOUND_PROJECT_ID,
    job_id: str = "",
    dry_run: bool = False,
    limit: int = 0,
    csv_source: str = "",
    workers: int = 10,
    resume_dir: str = "",
    prefer_source_type: str = "",
    allow_no_linkedin: bool = False,
) -> dict:
    """Push candidates to Ashby in parallel. Returns summary stats.

    Robustness fixes (2026-04-24):
      1. Push log saved after EACH successful candidate.create (not at end of loop).
      2. Every sub-step uses _post_with_retry to survive transient success=False.
         Partial records are logged to .push_to_ashby_errors.json but candidate_id
         is saved to push_log (they ARE in Ashby).
      3. Name-match dedup now verifies LinkedIn URL before skipping.
      4. All failures write to .push_to_ashby_errors.json.
      5. Parallel execution via ThreadPoolExecutor (default 5 workers). Shared
         dicts are protected by locks. Dedup pre-filter runs sequentially before
         dispatching writes.
    """
    from ashby_bridge import _resolve_source_id

    # Load dedup sets (sequential — cheap, one-time)
    known_linkedins = _load_known_linkedins()
    push_log = _load_push_log()
    known_linkedins.update(set(push_log.keys()))

    # Resolve source once (cached inside _resolve_source_id but make it explicit)
    source_id = _resolve_source_id(source, prefer_source_type=prefer_source_type or None)
    outbound_job = job_id or OUTBOUND_JOB_ID

    stats = {
        "pushed": 0,
        "skipped_dedup": 0,
        "skipped_no_linkedin": 0,
        "failed": 0,
        "partial": 0,
    }

    # ── Pre-filter phase (sequential, fast) ──
    # Drop anything missing LinkedIn or already in the local dedup set.
    # This is cheap — no API calls — so keep it serial.
    work_items = []
    for c in candidates:
        name = c["name"]
        linkedin = c.get("linkedin", "")
        if not linkedin:
            if allow_no_linkedin:
                work_items.append(c)
            else:
                logger.info("  SKIP (no LinkedIn): %s", name)
                stats["skipped_no_linkedin"] += 1
            if limit and len(work_items) >= limit:
                break
            continue
        li_norm = _normalize_linkedin(linkedin)
        if li_norm in known_linkedins:
            stats["skipped_dedup"] += 1
            continue
        work_items.append(c)
        if limit and len(work_items) >= limit:
            break

    total = len(work_items)
    mode = "project + job" if job_id else "project only"
    print(f"\n  Pre-filter: {len(candidates)} input → {total} to push "
          f"(skipped {stats['skipped_dedup']} dedup, {stats['skipped_no_linkedin']} no-LI)")
    print(f"  Pushing {total} candidates to Ashby ({mode}, {workers} workers, source: {source})...\n")

    if dry_run:
        for i, c in enumerate(work_items, 1):
            print(f"  [{i}/{total}] DRY RUN: {c['name']:<35} {c.get('linkedin', '')[:50]}")
            stats["pushed"] += 1
        return stats

    if total == 0:
        return stats

    # ── Parallel push phase ──
    # Each worker runs _push_one; shared dicts protected by locks.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _push_one, c, i + 1, total, source, source_id,
                project_id, outbound_job, csv_source,
                known_linkedins, push_log, resume_dir,
            ): c for i, c in enumerate(work_items)
        }
        for fut in as_completed(futures):
            try:
                outcome = fut.result()
            except Exception as e:
                c = futures[fut]
                logger.error("  Worker crashed on %s: %s", c.get("name", "?"), e)
                _log_error({
                    "csv": csv_source, "name": c.get("name", ""),
                    "linkedin": c.get("linkedin", ""),
                    "step": "worker", "error": str(e)[:500],
                })
                outcome = "failed"
            with _STATS_LOCK:
                if outcome == "pushed":
                    stats["pushed"] += 1
                elif outcome == "partial":
                    stats["pushed"] += 1
                    stats["partial"] += 1
                elif outcome == "skipped_dedup":
                    stats["skipped_dedup"] += 1
                elif outcome == "failed":
                    stats["failed"] += 1

    return stats


# ── CLI ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Push sourced candidates to Ashby (Outbound Sourced project)")
    parser.add_argument("csv_file", help="Path to CSV file with candidates")
    parser.add_argument("--source", default="Outbound",
                        help="Ashby source tag (e.g. 'Outbound - Github Sourced', 'Juicebox Outbound')")
    parser.add_argument("--job", default="",
                        help="Optional: also create application on this job (short code: FE, BE, P+E, etc.)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be pushed without creating anything")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max candidates to push (0 = all)")
    parser.add_argument("--workers", type=int, default=10,
                        help="Parallel worker count (default 5). Ashby handles 429s with backoff.")
    parser.add_argument("--resume-dir", default="",
                        help="Directory holding PDF files referenced by the resume_path column.")
    parser.add_argument("--source-type", default="Sourced",
                        help="Ashby sourceType to disambiguate when the source title exists under multiple types "
                             "(e.g. 'Y Combinator Work at a Startup' exists under both Inbound and Sourced). "
                             "Defaults to 'Sourced' since this script is the outbound push pipeline.")
    parser.add_argument("--allow-no-linkedin", action="store_true",
                        help="Push candidates that lack a LinkedIn URL. Screener will attempt "
                             "Linkup-based discovery during screening. Costs ~$0.007 per discovered candidate.")
    args = parser.parse_args()

    if not os.environ.get("ASHBY_API_KEY"):
        print("\nMissing ASHBY_API_KEY. Set it: export ASHBY_API_KEY='...'")
        sys.exit(1)

    # Resolve optional job
    job_id = ""
    job_title = ""
    if args.job:
        routing = load_routing()
        job_id = resolve_job_id(args.job, routing)
        if not job_id:
            print(f"\n  Could not resolve job '{args.job}'. Available roles:")
            data = json.loads(ROUTING_FILE.read_text(encoding="utf-8")) if ROUTING_FILE.exists() else {}
            for name, info in data.get("roles", {}).items():
                aliases = ", ".join(info.get("aliases", [])[:3])
                print(f"    {name:<35} aliases: {aliases}")
            sys.exit(1)
        data = json.loads(ROUTING_FILE.read_text(encoding="utf-8"))
        for name, info in data.get("roles", {}).items():
            if info["job_id"] == job_id:
                job_title = info.get("job_title", name)
                break

    # Read CSV
    candidates = read_csv_candidates(args.csv_file)
    if not candidates:
        print(f"\n  No candidates found in {args.csv_file}")
        sys.exit(1)

    li_count = sum(1 for c in candidates if c.get("linkedin"))
    print(f"\n  Read {len(candidates)} candidates from {args.csv_file} ({li_count} with LinkedIn URLs)")
    print(f"  Landing: Outbound Sourced project" + (f" + {job_title} job" if job_title else ""))
    print(f"  Source tag: {args.source}")
    if args.dry_run:
        print(f"  Mode: DRY RUN")

    # Push
    stats = push_candidates(
        candidates=candidates,
        source=args.source,
        job_id=job_id,
        dry_run=args.dry_run,
        limit=args.limit,
        csv_source=os.path.basename(args.csv_file),
        workers=args.workers,
        resume_dir=args.resume_dir,
        prefer_source_type=args.source_type,
        allow_no_linkedin=args.allow_no_linkedin,
    )

    # Summary
    print(f"\n  {'DRY RUN ' if args.dry_run else ''}Summary:")
    print(f"    Pushed:          {stats['pushed']}")
    if stats.get("partial"):
        print(f"      of which partial (see .push_to_ashby_errors.json): {stats['partial']}")
    print(f"    Skipped (dedup): {stats['skipped_dedup']}")
    print(f"    Skipped (no LI): {stats['skipped_no_linkedin']}")
    print(f"    Failed:          {stats['failed']}")
    if not args.dry_run and stats["pushed"]:
        if job_id:
            print(f"\n  Next: run `ascreen` to screen these candidates.")
        else:
            print(f"\n  Candidates are in the Outbound Sourced project.")
            print(f"  To screen: push to a job with --job, or wait for screening to pull from the project.")


if __name__ == "__main__":
    main()
