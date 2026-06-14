"""
Ashby ATS API client — pulls, screens, and writes back to Ashby.

Usage:
  python3 ashby_bridge.py --list-jobs          # show Ashby jobs + mapping
  python3 ashby_bridge.py --setup              # auto-map roles to Ashby jobs
  python3 ashby_bridge.py --push               # push SCREEN candidates to Ashby
  python3 ashby_bridge.py --push --dry-run     # preview without pushing
  python3 ashby_bridge.py --pull-inbound       # pull inbound candidates to Sheet
  python3 ashby_bridge.py --setup-stages       # cache stage IDs from Ashby
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
ROLE_MAP_FILE = _DIR / ".ashby_role_map.json"
SYNC_TRACKER_FILE = _DIR / ".ashby_sync_tracker.json"
PUSH_LOG_FILE = _DIR / ".ashby_push_log.json"
PULL_TRACKER_FILE = _DIR / ".ashby_pull_tracker.json"
STAGE_MAP_FILE = _DIR / ".ashby_stage_map.json"
JOB_ROUTING_FILE = _DIR / ".ashby_job_routing.json"
CUSTOM_FIELDS_FILE = _DIR / ".ashby_custom_fields.json"
CANDIDATE_CACHE_DIR = _DIR / ".candidate_cache"
WRITEBACK_QUEUE_FILE = _DIR / ".ashby_writeback_queue.json"
# Entries that can never succeed (the candidate/application no longer exists in
# Ashby) are moved here instead of being retried forever in the live queue.
WRITEBACK_DEADLETTER_FILE = _DIR / ".ashby_writeback_deadletter.json"

ASHBY_API_URL = os.environ.get("ASHBY_API_URL", "https://api.ashbyhq.com")
ASHBY_API_KEY = os.environ.get("ASHBY_API_KEY", "")


def write_json_atomic(path: Path, data: Any, **json_kwargs) -> None:
    """Write JSON to `path` atomically. Either the original file stays
    intact, or the new content is fully written — never a half-written
    corrupt file.

    Implementation: serialize to a sibling temp file, fsync to disk,
    then os.replace() (atomic on POSIX same-filesystem renames). On any
    error the temp file is removed and the exception propagates.

    Use this for any state file whose corruption would lose work or
    confuse the next run: .screening_claims.json, .push_to_ashby_log.json,
    .candidate_cache/{id}.json, .linkedin_cache.json, .rejection_email_log.json,
    pull/sync trackers, etc. NOT needed for diagnostic / one-shot output
    where corruption is acceptable.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        encoding = json_kwargs.pop("encoding", "utf-8")
        with open(tmp, "w", encoding=encoding) as f:
            json.dump(data, f, **json_kwargs)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                # Some filesystems (tmpfs, network mounts) don't support fsync.
                # Atomicity still holds via os.replace; we just lose the
                # extra "data hit physical disk" guarantee.
                pass
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise

# Role short names → internal names (must match ACTIVE_ROLES in optimizer)
ROLE_SHORT_TO_INTERNAL = {
    "P+E": "AI Product Engineer",
    "D+E": "AI Design Engineer",
    "FE": "AI Frontend Engineer",
    "BE": "AI Backend Engineer",
    "DevOps": "Staff DevSecOps Engineer",
    "GTM": "GTM Engineer",
    "VD": "AI Value Delivery Lead",
    "RL": "Recruiting Lead",
    "OB": "Outbound Sourced",
}

# Agency-sourced candidates for VD Lead (any level) bypass AI screening and route
# directly to Application Review for HM review (decided 2026-04-28). Reason: VD Lead
# is an exec hire — agencies don't add screening signal for it.
AGENCY_VD_JOB_TITLES = {
    "AI Value Delivery Lead",
    "Senior AI Value Delivery & Strategy Lead",
}

OUTBOUND_JOB_ID = "REPLACE_WITH_YOUR_OUTBOUND_JOB_ID"

# Technical Product Manager — treated as a P+E equivalent. Screened with the P+E
# prompt; if best_fit_role comes back as any Product Engineer variant, the
# candidate stays on the TPM job instead of being rerouted to the P+E job.
TPM_JOB_ID = "REPLACE_WITH_YOUR_TPM_JOB_ID"
PE_FAMILY_BEST_FIT_KEYS = {
    "ai product engineer",
    "senior ai product engineer",
    "staff ai product engineer",
    "p+e",
    "product engineer",
    "technical product manager",
}


# ── HTTP helpers ─────────────────────────────────────────────────

def _auth_header() -> str:
    """Build Basic Auth header from API key."""
    if not ASHBY_API_KEY:
        raise RuntimeError("ASHBY_API_KEY not set. Export it: export ASHBY_API_KEY='your-key'")
    token = base64.b64encode(f"{ASHBY_API_KEY}:".encode()).decode()
    return f"Basic {token}"


def _ashby_post(endpoint: str, payload: dict, retries: int = 5) -> dict:
    """POST to Ashby API with retry + rate-limit handling.

    Retries on HTTP 429 (honoring Retry-After) and on HTTP 5xx / transient
    network errors with EXPONENTIAL backoff + jitter (1s, 2s, 4s, 8s, capped
    at 10s, plus up to 0.5s random jitter). Ashby periodically returns bursts
    of 503 Service Unavailable; the previous flat 1s × 3 retries were not
    enough to ride through those, which stranded candidates with a logged
    verdict but no writeback. The jitter prevents parallel write workers from
    retrying in lockstep (thundering herd) and making the burst worse.
    """
    url = f"{ASHBY_API_URL}/{endpoint}"
    data = json.dumps(payload).encode("utf-8")

    def _backoff(attempt: int) -> float:
        return min(2 ** attempt, 10) + random.uniform(0, 0.5)

    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json; version=1",
                "Authorization": _auth_header(),
                "User-Agent": "curl/8.7.1",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 429:
                wait = int(e.headers.get("Retry-After", 2))
                logger.warning("Rate limited. Waiting %ds...", wait)
                time.sleep(wait)
                continue
            elif e.code >= 500 and attempt < retries - 1:
                wait = _backoff(attempt)
                logger.warning("Server error %d on %s, retry %d/%d in %.1fs...",
                               e.code, endpoint, attempt + 1, retries, wait)
                time.sleep(wait)
                continue
            else:
                logger.error("Ashby API error %d on %s: %s", e.code, endpoint, body[:500])
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt < retries - 1:
                wait = _backoff(attempt)
                logger.warning("Network error on %s (%s), retry %d/%d in %.1fs...",
                               endpoint, e, attempt + 1, retries, wait)
                time.sleep(wait)
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(_backoff(attempt))
                continue
            raise

    return {}


# ── Job operations ───────────────────────────────────────────────

def list_jobs(include_closed: bool = False) -> List[Dict]:
    """List all jobs in Ashby."""
    result = _ashby_post("job.list", {"includeArchived": include_closed})
    return result.get("results", [])


def get_open_jobs() -> List[Dict]:
    """List only open jobs."""
    jobs = list_jobs()
    return [j for j in jobs if j.get("status") == "Open"]


# ── Candidate operations ─────────────────────────────────────────

def search_candidate(email: str = "", name: str = "") -> Optional[Dict]:
    """Search for existing candidate by email or name."""
    if email:
        result = _ashby_post("candidate.search", {"email": email})
        results = result.get("results", [])
        if results:
            return results[0]

    if name:
        result = _ashby_post("candidate.search", {"name": name})
        results = result.get("results", [])
        if results:
            return results[0]

    return None


def create_candidate(
    name: str,
    linkedin_url: str = "",
    email: str = "",
    phone: str = "",
) -> Optional[Dict]:
    """Create a new candidate in Ashby."""
    payload: Dict[str, Any] = {
        "name": name,
    }

    if email:
        payload["emailAddresses"] = [{"value": email, "type": "Primary"}]
    if phone:
        payload["phoneNumbers"] = [{"value": phone, "type": "Primary"}]
    if linkedin_url:
        payload["socialLinks"] = [{"type": "LinkedIn", "url": linkedin_url}]

    result = _ashby_post("candidate.create", payload)
    if result.get("success"):
        return result.get("results")
    else:
        logger.error("Failed to create candidate %s: %s", name, result)
        return None


def add_linkedin_to_candidate(candidate_id: str, linkedin_url: str) -> bool:
    """Append a LinkedIn URL to a candidate's socialLinks, preserving existing entries.

    Used after Linkup-based LinkedIn discovery during screening so the URL lives on
    the Ashby record (not just in local cache). Idempotent: no-op if a LinkedIn
    is already present.
    """
    if not candidate_id or not linkedin_url:
        return False
    info = _ashby_post("candidate.info", {"id": candidate_id})
    if not info.get("success"):
        logger.warning("add_linkedin_to_candidate: candidate.info failed for %s", candidate_id)
        return False
    existing = info.get("results", {}).get("socialLinks", []) or []
    for link in existing:
        if (link.get("type") or "").lower() == "linkedin" and link.get("url"):
            return True
    updated = list(existing) + [{"type": "LinkedIn", "url": linkedin_url}]
    r = _ashby_post("candidate.update", {"candidateId": candidate_id, "socialLinks": updated})
    if r.get("success"):
        logger.info("add_linkedin_to_candidate: wrote %s to %s", linkedin_url, candidate_id[:12])
        return True
    logger.warning("add_linkedin_to_candidate: candidate.update failed for %s: %s", candidate_id, r)
    return False


def set_primary_email(candidate_id: str, email: str, dry_run: bool = False) -> Dict:
    """Promote `email` to be the candidate's PRIMARY email address.

    Why this exists: sourced candidates (GitHub/Juicebox) often arrive with NO
    email. Ashby's "Find Email Addresses" enrichment then adds one as a
    non-primary ("Personal") address. Outreach sequences only send to the
    PRIMARY email — so an enriched-but-non-primary candidate is un-sendable
    ("candidate does not have a primary email address" / "Start 0 Sequences").
    Recruiting Lead works precisely because its candidates were created WITH an
    email, which `candidate.create` flags Primary (see create_candidate). This
    function brings the enrichment path to parity.

    Uses candidate.update's `email` field (Primary) + optional `alternateEmail`.
    NOTE: the `emailAddresses` array is silently ignored on candidate.update
    (gating test 2026-06-10) — only `email`/`alternateEmail` take effect.
    Idempotent (no-op if already primary) and SELF-VERIFYING (re-reads
    primaryEmailAddress after the write). Never raises.

    Returns {"ok": bool, "changed": bool, "reason": str}.
    """
    if not candidate_id or not email:
        return {"ok": False, "changed": False, "reason": "missing candidate_id/email"}
    email_l = email.strip().lower()
    info = _ashby_post("candidate.info", {"id": candidate_id})
    if not info.get("success"):
        return {"ok": False, "changed": False, "reason": "candidate.info failed"}
    res = info.get("results", {}) or {}
    primary = ((res.get("primaryEmailAddress") or {}).get("value") or "").strip().lower()
    if primary == email_l:
        return {"ok": True, "changed": False, "reason": "already primary"}

    existing = res.get("emailAddresses", []) or []
    # Confirm the target email actually exists on the candidate before promoting
    # it — never invent an address.
    values = [(e.get("value") or "").strip() for e in existing if e.get("value")]
    if email_l not in [v.lower() for v in values]:
        return {"ok": False, "changed": False,
                "reason": f"email {email} not on candidate (has: {values or 'none'})"}
    # candidate.update accepts `email` (Primary) + `alternateEmail` (one more).
    # The `emailAddresses` array is SILENTLY IGNORED on update — verified by the
    # 2026-06-10 gating test on Mike Caballero (411dc4ce…): the array payload
    # returned success but left primary unchanged; the `email` field promoted it.
    # (Same class as candidate.create dropping socialLinks.) Promote `email`;
    # preserve one other address as alternateEmail so we never drop contact data.
    others = [v for v in values if v.lower() != email_l]

    if dry_run:
        return {"ok": True, "changed": True,
                "reason": f"DRY-RUN would set primary={email} (was {primary or 'none'})"}

    payload = {"candidateId": candidate_id, "email": email}
    if others:
        payload["alternateEmail"] = others[0]
    r = _ashby_post("candidate.update", payload)
    if not r.get("success"):
        logger.error("set_primary_email: candidate.update failed for %s: %s",
                     candidate_id[:12], str(r)[:160])
        return {"ok": False, "changed": False, "reason": f"update failed: {str(r)[:120]}"}

    # Self-verify: re-read and confirm the primary actually changed.
    chk = _ashby_post("candidate.info", {"id": candidate_id})
    new_primary = (((chk.get("results") or {}).get("primaryEmailAddress") or {})
                   .get("value") or "").strip().lower()
    if new_primary == email_l:
        logger.info("set_primary_email: %s → primary=%s", candidate_id[:12], email)
        return {"ok": True, "changed": True, "reason": "set primary"}
    logger.error("set_primary_email: update ran but primary is still %s for %s "
                 "(API may not accept emailAddresses on update — needs internal mutation)",
                 new_primary or "<none>", candidate_id[:12])
    return {"ok": False, "changed": False,
            "reason": f"update ran but primary={new_primary or 'none'}"}


def add_primary_email(candidate_id: str, email: str, dry_run: bool = False) -> Dict:
    """Add `email` as a candidate's PRIMARY when they currently have NONE.

    Complements set_primary_email, which only *promotes* an address already on
    the record and refuses to invent one. This handles the sourced-candidate
    backlog case: GitHub/Juicebox candidates created with no email at all, where
    we have mined+guarded a safe address and need to attach it so outreach can
    send. Verified 2026-06-12 (Gregor Martynus ddc0b5f3): candidate.update with
    the `email` field adds a brand-new primary when the candidate had none.

    SAFE BY DESIGN — never clobbers an existing primary:
      - candidate already has THIS email as primary → no-op ok.
      - candidate already has ANY primary/email → refuse (use set_primary_email
        to re-point an existing address; this function only fills an empty slot).
      - candidate has no email at all → add via candidate.update(email=...).
    Self-verifying (re-reads primaryEmailAddress). Never raises.
    Returns {"ok": bool, "changed": bool, "reason": str}.
    """
    if not candidate_id or not email or "@" not in email:
        return {"ok": False, "changed": False, "reason": "missing/invalid candidate_id/email"}
    email_l = email.strip().lower()
    info = _ashby_post("candidate.info", {"id": candidate_id})
    if not info.get("success"):
        return {"ok": False, "changed": False, "reason": "candidate.info failed"}
    res = info.get("results", {}) or {}
    primary = ((res.get("primaryEmailAddress") or {}).get("value") or "").strip().lower()
    if primary == email_l:
        return {"ok": True, "changed": False, "reason": "already primary"}
    existing = [(e.get("value") or "").strip() for e in (res.get("emailAddresses") or []) if e.get("value")]
    if primary or existing:
        return {"ok": False, "changed": False,
                "reason": f"candidate already has email(s) ({primary or existing}); use set_primary_email"}

    if dry_run:
        return {"ok": True, "changed": True, "reason": f"DRY-RUN would add primary={email}"}

    r = _ashby_post("candidate.update", {"candidateId": candidate_id, "email": email})
    if not r.get("success"):
        logger.error("add_primary_email: candidate.update failed for %s: %s",
                     candidate_id[:12], str(r)[:160])
        return {"ok": False, "changed": False, "reason": f"update failed: {str(r)[:120]}"}
    chk = _ashby_post("candidate.info", {"id": candidate_id})
    new_primary = (((chk.get("results") or {}).get("primaryEmailAddress") or {})
                   .get("value") or "").strip().lower()
    if new_primary == email_l:
        logger.info("add_primary_email: %s → primary=%s", candidate_id[:12], email)
        return {"ok": True, "changed": True, "reason": "added primary"}
    logger.error("add_primary_email: update ran but primary is %s for %s",
                 new_primary or "<none>", candidate_id[:12])
    return {"ok": False, "changed": False, "reason": f"update ran but primary={new_primary or 'none'}"}


def _resolve_source_id(source_name: str, prefer_source_type: Optional[str] = None) -> Optional[str]:
    """Resolve a source name to an Ashby source ID. Caches on first call.

    Matching order:
      1. Exact (case-insensitive) match on Ashby title. If multiple sources share
         the same title (e.g. one under sourceType "Inbound" and another under
         "Sourced"), ``prefer_source_type`` breaks the tie. Without it, the first
         non-archived match wins.
      2. Substring match — either the Ashby title is contained in ``source_name``
         (e.g. "Candidate Labs" ⊂ "Agencies: Candidate Labs") or vice versa.
         Longer Ashby titles win ties (more specific).
      3. Token-overlap score — Jaccard on lowercase word sets. Best-scoring
         source wins if score ≥ 0.5; otherwise None.
    """
    if not hasattr(_resolve_source_id, "_cache"):
        try:
            resp = _ashby_post("source.list", {})
            _resolve_source_id._cache = [
                (
                    s["title"].lower(),
                    s["title"],
                    s["id"],
                    (s.get("sourceType") or {}).get("title", ""),
                )
                for s in resp.get("results", [])
                if s.get("title") and s.get("id") and not s.get("isArchived")
            ]
        except Exception:
            _resolve_source_id._cache = []

    sources = _resolve_source_id._cache
    if not sources:
        return None
    needle = (source_name or "").lower().strip()
    if not needle:
        return None
    pref = (prefer_source_type or "").lower().strip()

    # 1. Exact (with optional sourceType tiebreaker)
    exact_hits = [(lower, title, sid, st) for lower, title, sid, st in sources if lower == needle]
    if exact_hits:
        if pref:
            for lower, title, sid, st in exact_hits:
                if st.lower() == pref:
                    return sid
        return exact_hits[0][2]

    # 2. Substring (longer Ashby titles preferred)
    substring_hits = [
        (lower, title, sid, st) for lower, title, sid, st in sources
        if lower in needle or needle in lower
    ]
    if substring_hits:
        substring_hits.sort(key=lambda t: len(t[0]), reverse=True)
        lower, title, sid, _st = substring_hits[0]
        logger.info("Source fuzzy-matched '%s' → '%s' (substring)", source_name, title)
        return sid

    # 3. Token Jaccard
    def _tokens(s: str) -> set:
        return {t for t in re.split(r"[^a-z0-9]+", s.lower()) if t}
    needle_tokens = _tokens(needle)
    if not needle_tokens:
        return None
    best = (0.0, None, None)
    for lower, title, sid, _st in sources:
        cand_tokens = _tokens(lower)
        if not cand_tokens:
            continue
        inter = len(needle_tokens & cand_tokens)
        union = len(needle_tokens | cand_tokens)
        score = inter / union if union else 0.0
        if score > best[0]:
            best = (score, title, sid)
    if best[0] >= 0.5:
        logger.info("Source fuzzy-matched '%s' → '%s' (jaccard=%.2f)",
                    source_name, best[1], best[0])
        return best[2]
    logger.warning("Source '%s' not resolved (best jaccard=%.2f vs '%s')",
                   source_name, best[0], best[1])
    return None


_KLARITY_DOMAINS = ("klarity.ai", "klaritylaw.com", "klarityintelligence.com")


def resolve_referrer_user_id(email: str = "", name: str = "") -> Optional[str]:
    """Resolve a referrer's email/name to an Ashby user ID for `creditedToUserId`.

    Matching order (first hit wins):
      1. Exact email match in Ashby user directory.
      2. Domain swap across Klarity domains (klarity.ai ↔ klaritylaw.com ↔
         klarityintelligence.com) — handles people who submitted via personal-domain
         alias of a colleague's address.
      3. Exact "First Last" name match — only if there is exactly ONE Ashby user
         with that name. Ambiguous names return None to avoid mis-credit.

    Returns the user ID, or None if no safe match. External referrers (e.g. gmail
    addresses with no name match) intentionally return None — Ashby's "Credited To"
    column is reserved for internal users.

    Builds a cached user-directory index on first call. Cache persists per process.
    """
    if not hasattr(resolve_referrer_user_id, "_cache"):
        email_idx: Dict[str, str] = {}
        name_idx: Dict[str, List[str]] = {}
        try:
            cursor = None
            while True:
                p = {"limit": 100}
                if cursor:
                    p["cursor"] = cursor
                resp = _ashby_post("user.list", p)
                for u in resp.get("results", []) or []:
                    uid = u.get("id")
                    if not uid:
                        continue
                    em = (u.get("email") or "").strip().lower()
                    if em:
                        email_idx[em] = uid
                    nm = f"{(u.get('firstName') or '').strip()} {(u.get('lastName') or '').strip()}".strip().lower()
                    if nm:
                        name_idx.setdefault(nm, []).append(uid)
                if not resp.get("moreDataAvailable"):
                    break
                cursor = resp.get("nextCursor", "")
        except Exception as e:
            logger.warning("resolve_referrer_user_id: failed to load Ashby users: %s", e)
        resolve_referrer_user_id._cache = (email_idx, name_idx)

    email_idx, name_idx = resolve_referrer_user_id._cache

    em = (email or "").strip().lower()
    if em and em in email_idx:
        return email_idx[em]
    if em and "@" in em:
        local, _, dom = em.partition("@")
        for alt in _KLARITY_DOMAINS:
            if alt == dom:
                continue
            cand = f"{local}@{alt}"
            if cand in email_idx:
                return email_idx[cand]

    nm = (name or "").strip().lower()
    if nm and nm in name_idx and len(name_idx[nm]) == 1:
        return name_idx[nm][0]

    return None


def set_application_credited_to(application_id: str, user_id: str) -> bool:
    """Set `creditedToUser` on an Ashby application. Idempotent: re-setting the same
    user is a no-op on Ashby's side. Returns True on success."""
    if not application_id or not user_id:
        return False
    res = _ashby_post("application.update", {
        "applicationId": application_id,
        "creditedToUserId": user_id,
    })
    if not res.get("success"):
        logger.warning("set_application_credited_to failed for app=%s user=%s: %s",
                       application_id[:8], user_id[:8], res.get("errors"))
        return False
    return True


def create_application(
    candidate_id: str,
    job_id: str,
    source: str = "AI Screening Pipeline",
) -> Optional[Dict]:
    """Create an application for a candidate on a job."""
    payload = {
        "candidateId": candidate_id,
        "jobId": job_id,
    }
    # Ashby requires sourceId (UUID), not source (string)
    source_id = _resolve_source_id(source)
    if source_id:
        payload["sourceId"] = source_id
    else:
        payload["source"] = source  # fallback for unknown sources

    result = _ashby_post("application.create", payload)
    if result.get("success"):
        return result.get("results")
    else:
        logger.error("Failed to create application: %s", result)
        return None


def add_note(candidate_id: str, note_html: str) -> bool:
    """Add an HTML-formatted note to a candidate."""
    result = _ashby_post("candidate.createNote", {
        "candidateId": candidate_id,
        "note": {"type": "text/html", "value": note_html},
    })
    return result.get("success", False)


def set_referrer_fields(candidate_id: str, referrer_name: str = "",
                        referrer_email: str = "") -> bool:
    """Write Referrer Name + Referrer First Name + Referrer Email custom fields.

    Used by all referral intake paths (CSV bulk push, HubSpot form, /intake modal)
    so the campaign report can pull from structured fields instead of parsing notes.

    Referrer First Name is auto-derived (first whitespace token of the full name)
    so email templates can greet an EXTERNAL referrer by first name — Ashby's
    built-in 'Candidate's Referrer' merge field only resolves for internal
    (Klarity-user) referrers, which our gmail/.edu referrers are not.
    """
    referrer_name = (referrer_name or "").strip()
    referrer_email = (referrer_email or "").strip()
    if not referrer_name and not referrer_email:
        return False

    fields = load_custom_fields()
    values = []
    if referrer_name:
        f = fields.get("Referrer Name", {})
        if f.get("id"):
            values.append({"fieldId": f["id"], "fieldValue": referrer_name})
        first = referrer_name.split()[0] if referrer_name.split() else ""
        ff = fields.get("Referrer First Name", {})
        if first and ff.get("id"):
            values.append({"fieldId": ff["id"], "fieldValue": first})
    if referrer_email:
        f = fields.get("Referrer Email", {})
        if f.get("id"):
            values.append({"fieldId": f["id"], "fieldValue": referrer_email})

    if not values:
        return False

    resp = _ashby_post("customField.setValues", {
        "objectId": candidate_id,
        "objectType": "Candidate",
        "values": values,
    })
    return resp.get("success", False)


def get_interview_stages(job_id: str) -> List[Dict]:
    """Get all pipeline stages for a job's interview plan."""
    result = _ashby_post("interviewPlan.list", {"jobId": job_id})
    stages = []
    for plan in result.get("results", []):
        for stage in plan.get("interviewStages", []):
            stages.append({
                "id": stage.get("id"),
                "title": stage.get("title", ""),
                "type": stage.get("type", ""),
                "orderInInterviewPlan": stage.get("orderInInterviewPlan", 0),
            })
    return stages


ARCHIVE_REASON_ID = "REPLACE_WITH_YOUR_ARCHIVE_REASON_ID"  # "Lacks Skills/Qualifications"


def move_to_stage(application_id: str, stage_id: str, is_archive: bool = False,
                  retries: int = 3, archive_reason_id: Optional[str] = None) -> bool:
    """Move an application to a specific interview stage.

    Retries on API success=False responses (transient Ashby failures) so the
    candidate isn't silently stranded in the wrong stage.

    `archive_reason_id` overrides the default reason (only used when is_archive).
    """
    payload: Dict[str, Any] = {
        "applicationId": application_id,
        "interviewStageId": stage_id,
    }
    # Archived stage requires an archive reason
    if is_archive:
        payload["archiveReasonId"] = archive_reason_id or ARCHIVE_REASON_ID

    for attempt in range(retries):
        try:
            result = _ashby_post("application.changeStage", payload)
            if result.get("success"):
                return True
        except Exception as e:
            if attempt == retries - 1:
                logger.error("  changeStage raised on final attempt: %s", e)
                return False
        if attempt < retries - 1:
            time.sleep(1 + attempt)  # 1s, 2s backoff
    logger.error("  changeStage failed after %d attempts (app=%s, stage=%s)",
                 retries, application_id, stage_id)
    return False


# Sheet "Actioned" value → Ashby stage name keyword match
ACTIONED_TO_STAGE = {
    "outreach": "outreach",
    "interview": "interview",
    "screen": "screen",
    "phone": "phone",
    "offer": "offer",
}


def find_matching_stage(stages: List[Dict], actioned: str) -> Optional[str]:
    """Find the Ashby stage ID that best matches the Actioned column value."""
    if not actioned:
        return None
    actioned_lower = actioned.lower()

    # Direct keyword match against stage titles
    for keyword, search_term in ACTIONED_TO_STAGE.items():
        if keyword in actioned_lower:
            for stage in stages:
                if search_term in stage["title"].lower():
                    return stage["id"]

    # Fallback: fuzzy match any word from Actioned against stage titles
    words = actioned_lower.split()
    for stage in stages:
        stage_lower = stage["title"].lower()
        for word in words:
            if len(word) > 3 and word in stage_lower:
                return stage["id"]

    return None


# ── Role mapping ─────────────────────────────────────────────────

def load_role_map() -> Dict[str, str]:
    """Load role → Ashby job ID mapping."""
    if ROLE_MAP_FILE.exists():
        try:
            return json.loads(ROLE_MAP_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_role_map(mapping: Dict[str, str]):
    """Save role → Ashby job ID mapping."""
    write_json_atomic(ROLE_MAP_FILE, mapping, indent=2, ensure_ascii=False)


def setup_role_mapping():
    """Auto-map internal roles to Ashby jobs by fuzzy matching."""
    jobs = get_open_jobs()
    if not jobs:
        print("No open jobs found in Ashby.")
        return

    print(f"\n  Found {len(jobs)} open jobs in Ashby:\n")
    for j in jobs:
        print(f"    {j['id'][:8]}...  {j['title']}")

    # Fuzzy match
    mapping = {}
    match_table = {
        "AI Backend Engineer": ["backend"],
        "AI Frontend Engineer": ["frontend"],
        "AI Product Engineer": ["product engineer", "p+e"],
        "AI Design Engineer": ["design engineer"],
        "Staff DevSecOps Engineer": ["devsecops", "devops"],
        "GTM Engineer": ["gtm"],
        "AI Value Delivery Lead": ["value delivery"],
        "Field Marketing Manager": ["field marketing", "head of field"],
    }

    for role_name, keywords in match_table.items():
        for j in jobs:
            title_lower = j["title"].lower()
            if any(kw in title_lower for kw in keywords):
                mapping[role_name] = j["id"]
                break

    if mapping:
        print(f"\n  Auto-matched {len(mapping)} roles:\n")
        for role, job_id in mapping.items():
            job_title = next((j["title"] for j in jobs if j["id"] == job_id), "?")
            print(f"    {role:<35} → {job_title}")

    save_role_map(mapping)
    print(f"\n  Saved to {ROLE_MAP_FILE.name}")
    return mapping


def get_job_id_for_role(role_name: str) -> Optional[str]:
    """Look up Ashby job ID for an internal role name."""
    mapping = load_role_map()

    # Direct match
    if role_name in mapping:
        return mapping[role_name]

    # Try short name → internal name
    for short, internal in ROLE_SHORT_TO_INTERNAL.items():
        if short.lower() == role_name.lower() or internal.lower() == role_name.lower():
            if internal in mapping:
                return mapping[internal]

    # Fuzzy partial match
    role_lower = role_name.lower()
    for key, job_id in mapping.items():
        if role_lower in key.lower() or key.lower() in role_lower:
            return job_id

    return None


# ── Sync tracker ─────────────────────────────────────────────────

def _load_tracker() -> Dict[str, str]:
    """Load {linkedin_url: ashby_candidate_id} tracker."""
    if SYNC_TRACKER_FILE.exists():
        try:
            return json.loads(SYNC_TRACKER_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_tracker(tracker: Dict[str, str]):
    write_json_atomic(SYNC_TRACKER_FILE, tracker, indent=2, ensure_ascii=False)


def _normalize_linkedin(url: str) -> str:
    """Normalize LinkedIn URL for dedup."""
    url = url.strip().rstrip("/").lower()
    url = url.replace("http://", "https://")
    if "/in/" in url:
        # Extract just the /in/username part
        idx = url.index("/in/")
        path = url[idx:]
        return f"https://www.linkedin.com{path}".rstrip("/")
    return url


def is_already_pushed(linkedin_url: str) -> bool:
    """Check if candidate was already pushed to Ashby."""
    tracker = _load_tracker()
    normalized = _normalize_linkedin(linkedin_url)
    return normalized in tracker


def mark_pushed(linkedin_url: str, ashby_candidate_id: str):
    """Mark candidate as pushed."""
    tracker = _load_tracker()
    normalized = _normalize_linkedin(linkedin_url)
    tracker[normalized] = ashby_candidate_id
    _save_tracker(tracker)


# ── Push log ─────────────────────────────────────────────────────

def _load_push_log() -> list:
    if PUSH_LOG_FILE.exists():
        try:
            return json.loads(PUSH_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def log_push(entry: dict):
    """Log a push event."""
    log = _load_push_log()
    entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    log.append(entry)
    if len(log) > 500:
        log = log[-500:]
    write_json_atomic(PUSH_LOG_FILE, log, indent=2, ensure_ascii=False)


# ── Note formatting ──────────────────────────────────────────────

def format_screening_note(candidate: dict) -> str:
    """Format screening metadata into a readable note for Ashby."""
    parts = ["## AI Screening Result\n"]

    verdict = candidate.get("verdict", "")
    spark = candidate.get("spark", "")
    best_fit = candidate.get("best_fit_role", "")
    level = candidate.get("matched_level", "")
    reason = candidate.get("verdict_reason", "")
    concerns = candidate.get("concerns", "")
    brief = candidate.get("screener_brief", "")
    questions = candidate.get("screening_questions", "")

    if verdict:
        parts.append(f"**Verdict:** {verdict}")
    if spark:
        parts.append(f"**Spark:** {spark}")
    if best_fit:
        role_line = best_fit
        if level:
            role_line += f" ({level})"
        parts.append(f"**Best Fit Role:** {role_line}")

    # Show target role if different from best fit
    target_role = candidate.get("target_role", "")
    if target_role and target_role != best_fit:
        parts.append(f"**Originally Sourced For:** {target_role}")

    source = candidate.get("source", "")
    if source:
        parts.append(f"**Source:** {source}")

    linkedin = candidate.get("linkedin_url", "")
    if linkedin:
        parts.append(f"**LinkedIn:** {linkedin}")

    if reason:
        parts.append(f"\n**Verdict Reason:** {reason}")

    if concerns:
        parts.append(f"\n### Concerns\n{concerns}")

    if brief:
        parts.append(f"\n### Screener Brief\n{brief}")

    if questions:
        parts.append(f"\n### Screening Questions\n{questions}")

    parts.append(f"\n---\nScreened by AI pipeline on {time.strftime('%Y-%m-%d %H:%M')}")

    return "\n".join(parts)


# ── Name parsing ─────────────────────────────────────────────────

def split_name(full_name: str) -> Tuple[str, str]:
    """Split full name into (first, last). Handles common patterns."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("Unknown", "Unknown")
    elif len(parts) == 1:
        return (parts[0], "")
    else:
        return (parts[0], " ".join(parts[1:]))


# ── Source mapping (Sheet → Ashby) ───────────────────────────────

# Map generic/legacy sheet values to Ashby's exact source names
SOURCE_MAP = {
    # Legacy generic values
    "OUTBOUND": "Sourced: LinkedIn",
    "INBOUND": "INBOUND: Dover Careers Page",
    "REFERRAL": "Referral: Referral",
    # Juicebox variations
    "Outbound — Juicebox": "Sourced: Juicebox",
    "Outbound - Juicebox": "Sourced: Juicebox",
    "Juicebox": "Sourced: Juicebox",
    "Juicebox (PeopleGPT)": "Sourced: Juicebox",
    # Events
    "Recruiting Event": "Sourced: Recruiting Event",
    "Event": "Sourced: Recruiting Event",
    # Already correct Ashby format — pass through
    "Sourced: Juicebox": "Sourced: Juicebox",
    "Sourced: LinkedIn": "Sourced: LinkedIn",
    "Sourced: GitHub": "Sourced: GitHub",
    "Sourced: Sourcing Form": "Sourced: Sourcing Form",
    "Sourced: Recruiting Event": "Sourced: Recruiting Event",
    "INBOUND: Dover Careers Page": "INBOUND: Dover Careers Page",
    "Referral: Referral": "Referral: Referral",
    "Agencies: Hirewell": "Agencies: Hirewell",
    "Agencies: Candidate Labs": "Agencies: Candidate Labs",
}


def _map_source_to_ashby(sheet_source: str) -> str:
    """Map sheet Source value to Ashby's exact source name."""
    if not sheet_source:
        return "Sourced: LinkedIn"

    # Exact match
    if sheet_source in SOURCE_MAP:
        return SOURCE_MAP[sheet_source]

    # Case-insensitive match
    lower = sheet_source.lower()
    for key, val in SOURCE_MAP.items():
        if key.lower() == lower:
            return val

    # If it already looks like an Ashby format (has colon), pass through
    if ":" in sheet_source:
        return sheet_source

    # Looks like a referrer name/email — treat as referral
    if "@" in sheet_source or (sheet_source[0].isupper() and len(sheet_source.split()) <= 2):
        return "Referral: Referral"

    # Unknown — default to sourced
    logger.warning("  Unknown source '%s' — defaulting to 'Sourced: LinkedIn'", sheet_source)
    return "Sourced: LinkedIn"


# ── Pull inbound logic ───────────────────────────────────────────

# Ashby job title → internal role short name (reverse of ROLE_SHORT_TO_INTERNAL)
ASHBY_TITLE_TO_ROLE = {}
for _short, _internal in ROLE_SHORT_TO_INTERNAL.items():
    ASHBY_TITLE_TO_ROLE[_internal.lower()] = _short

# Additional mappings for job titles that don't match internal names exactly
ASHBY_TITLE_TO_ROLE.update({
    "ai backend engineer": "BE",
    "senior ai backend engineer": "BE",
    "ai frontend engineer": "FE",
    "senior ai frontend engineer": "FE",
    "staff ai frontend engineer": "FE",
    "ai product engineer (p+e)": "P+E",
    "senior ai product engineer (p+e)": "P+E",
    # Technical Product Manager is treated as P+E (same role, different title in Ashby).
    # Screened with the P+E prompt; routing keeps them on the TPM job — see TPM_JOB_ID
    # special-case in route_to_best_fit_job.
    "technical product manager": "P+E",
    "senior technical product manager": "P+E",
    "staff technical product manager": "P+E",
    "ai design engineer": "D+E",
    "senior ai design engineer": "D+E",
    "ai value delivery lead": "VD",
    "senior ai value delivery & strategy lead": "VD",
    "senior devsecops engineer": "DevOps",
    "staff devsecops engineer": "DevOps",
    "gtm engineer": "GTM",
    "recruiting lead": "RL",
    # Field Marketing excluded from Ashby screening — not in active prompt
    # "head of field marketing": "FM",
    # "field marketing manager": "FM",
    # "product marketing manager": "FM",
    # Alliances Director / Solution Consultant excluded — not in active prompt
    # "alliances director": "GTM",
    # "solution consultant": "GTM",
})


# ── Candidate cache (CV, dossier, LinkedIn data) ──────────────

def _cache_path(candidate_id: str) -> Path:
    """Return path to a candidate's cache JSON file."""
    CANDIDATE_CACHE_DIR.mkdir(exist_ok=True)
    return CANDIDATE_CACHE_DIR / f"{candidate_id}.json"


def _load_cache(candidate_id: str) -> dict:
    """Load cached data for a candidate (CV text, dossier, URLs, etc.)."""
    p = _cache_path(candidate_id)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_cache(candidate_id: str, data: dict):
    """Save/update candidate cache. Merges with existing data. Atomic write —
    corruption of a per-candidate cache wastes ~$0.09 of paid Linkup+Apify
    research, so the rename-after-write pattern is worth the extra fsync."""
    existing = _load_cache(candidate_id)
    existing.update(data)
    write_json_atomic(_cache_path(candidate_id), existing, indent=2, ensure_ascii=True)


def _load_pull_tracker() -> Dict[str, dict]:
    """Load {application_id: {candidate_id, name, pulled_at}} tracker."""
    if PULL_TRACKER_FILE.exists():
        try:
            return json.loads(PULL_TRACKER_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_pull_tracker(tracker: Dict[str, dict]):
    write_json_atomic(PULL_TRACKER_FILE, tracker, indent=2, ensure_ascii=False)


def _map_job_title_to_role(title: str) -> str:
    """Map Ashby job title to internal role short name.

    Only exact matches (case-insensitive). No fuzzy/substring matching —
    that caused 'Product Analytics' to match 'ai product engineer' etc.
    """
    lower = title.strip().lower()
    if lower in ASHBY_TITLE_TO_ROLE:
        return ASHBY_TITLE_TO_ROLE[lower]
    return title  # Return original title if no match


def pull_inbound_candidates(limit: int = 0) -> List[Dict]:
    """
    Pull new inbound candidates from Ashby's Application Review stage.
    Returns list of candidates ready to write to the Sheet.
    """
    tracker = _load_pull_tracker()
    new_candidates = []
    seen_candidate_ids = set()  # Dedup: one entry per person, not per application

    # Page through all active applications
    cursor = None
    total_fetched = 0
    total_skipped = 0

    while True:
        payload: Dict[str, Any] = {"limit": 100}
        if cursor:
            payload["cursor"] = cursor

        result = _ashby_post("application.list", payload)
        apps = result.get("results", [])

        if not apps:
            break

        for app in apps:
            app_id = app.get("id", "")
            status = app.get("status", "")

            # Only active applications
            if status != "Active":
                continue

            # Only "Application Review" stage (the intake stage)
            stage = app.get("currentInterviewStage", {})
            stage_title = stage.get("title", "")
            if stage_title != "Application Review":
                continue

            # Already pulled?
            if app_id in tracker:
                total_skipped += 1
                continue

            # Get candidate details
            candidate_data = app.get("candidate", {})
            candidate_id = candidate_data.get("id", "")

            # Dedup by candidate ID (same person, multiple applications)
            if candidate_id in seen_candidate_ids:
                # Mark this app as pulled too so we don't re-check it
                tracker[app_id] = {
                    "candidate_id": candidate_id,
                    "name": candidate_data.get("name", ""),
                    "pulled_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "skipped": "duplicate_candidate",
                }
                continue
            seen_candidate_ids.add(candidate_id)

            # Get full candidate info (for LinkedIn, position, etc.)
            try:
                full_info = _ashby_post("candidate.info", {"id": candidate_id})
                if full_info.get("success"):
                    candidate_full = full_info.get("results", {})
                else:
                    logger.warning("  Could not get full info for %s, using basic data",
                                   candidate_data.get("name", ""))
                    candidate_full = candidate_data
            except Exception:
                candidate_full = candidate_data

            # Extract LinkedIn URL
            linkedin_url = ""
            for link in candidate_full.get("socialLinks", []):
                if link.get("type", "").lower() == "linkedin":
                    linkedin_url = link.get("url", "")
                    break

            # Extract job info
            job = app.get("job", {})
            job_title = job.get("title", "")
            role = _map_job_title_to_role(job_title)

            # Build candidate row for Sheet
            name = candidate_full.get("name", candidate_data.get("name", ""))
            position = candidate_full.get("position", "")
            company = candidate_full.get("company", "")
            cv_notes = ""
            if position and company:
                cv_notes = f"{position} at {company}"
            elif position:
                cv_notes = position

            # Source info
            source_info = candidate_full.get("source", {})
            source_type = source_info.get("sourceType", {}).get("title", "")
            source_title = source_info.get("title", "")
            source = f"Inbound — {source_title}" if source_title else "Inbound — Applied"

            candidate_row = {
                "name": name,
                "linkedin_url": linkedin_url,
                "source": source,
                "source_notes": f"Ashby {source_type}: {source_title}" if source_type else "",
                "target_role": role,
                "cv_notes": cv_notes,
                "ashby_application_id": app_id,
                "ashby_candidate_id": candidate_id,
                "job_title": job_title,
                "email": candidate_full.get("primaryEmailAddress", {}).get("value", ""),
                "submitted_at": app.get("createdAt", ""),
            }

            new_candidates.append(candidate_row)
            total_fetched += 1

            # Mark as pulled
            tracker[app_id] = {
                "candidate_id": candidate_id,
                "name": name,
                "pulled_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }

            # Rate limit: small delay between candidate.info calls
            time.sleep(0.2)

            if limit and total_fetched >= limit:
                break

        if limit and total_fetched >= limit:
            break

        if not result.get("moreDataAvailable"):
            break
        cursor = result.get("nextCursor")

    _save_pull_tracker(tracker)
    logger.info("Pull complete: %d new, %d already pulled", total_fetched, total_skipped)
    return new_candidates


def format_pull_for_email(candidates: List[Dict]) -> List[Dict]:
    """Format pulled candidates as screening results JSON for the email bridge.

    The Apps Script importer expects a specific format. We create minimal rows
    with just the intake fields — no screening results yet.
    """
    results = []
    for c in candidates:
        results.append({
            "candidate_name": c["name"],
            "linkedin_url": c.get("linkedin_url", ""),
            "source": c.get("source", ""),
            "source_additional_notes": c.get("source_notes", ""),
            "cv_resume_notes": c.get("cv_notes", ""),
            "target_role": c.get("target_role", ""),
            "ashby_application_id": c.get("ashby_application_id", ""),
            "ashby_candidate_id": c.get("ashby_candidate_id", ""),
        })
    return results


def _cli_pull_inbound(dry_run: bool = False, limit: int = 0):
    """Pull inbound candidates from Ashby and write to Sheet."""
    print("\n  Pulling inbound candidates from Ashby...")

    candidates = pull_inbound_candidates(limit=limit)

    if not candidates:
        print("  No new inbound candidates found.")
        return

    # Show summary
    print(f"\n  Found {len(candidates)} new inbound candidates:\n")
    role_counts: Dict[str, int] = {}
    for c in candidates:
        role = c.get("target_role", "Unknown")
        role_counts[role] = role_counts.get(role, 0) + 1
        linkedin_tag = " (has LinkedIn)" if c.get("linkedin_url") else " (NO LinkedIn)"
        print(f"    {c['name']:<35} {c.get('job_title', ''):<35} {linkedin_tag}")

    print(f"\n  By role:")
    for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
        print(f"    {role}: {count}")

    no_linkedin = [c for c in candidates if not c.get("linkedin_url")]
    if no_linkedin:
        print(f"\n  ⚠ {len(no_linkedin)} candidates have no LinkedIn URL — screening may be limited")

    if dry_run:
        print(f"\n  DRY RUN — would write {len(candidates)} candidates to Sheet")
        return

    # Write to Sheet via email bridge
    print(f"\n  Writing {len(candidates)} candidates to Sheet via email bridge...")
    try:
        from email_bridge import send_results_email
        email_data = format_pull_for_email(candidates)
        sent = send_results_email(email_data)
        if sent:
            print("  ✓ Email sent — candidates will appear in Sheet within 5 minutes")
        else:
            # Fallback: save to JSON for manual import
            output_file = _DIR / "inbound_candidates.json"
            output_file.write_text(
                json.dumps({"results": email_data}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"  Email failed. Saved to {output_file.name} for manual import.")
    except Exception as e:
        logger.error("Email bridge error: %s", e)
        # Save to JSON as fallback
        output_file = _DIR / "inbound_candidates.json"
        output_file.write_text(
            json.dumps({"results": format_pull_for_email(candidates)}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"  Saved to {output_file.name} for manual import.")


# ── Stage map + custom fields ────────────────────────────────────

def load_stage_map() -> Dict[str, str]:
    """Load stage name → stage ID mapping from .ashby_stage_map.json.

    NOTE: This returns the DEFAULT interview plan's stage IDs only. For
    multi-plan operation (VD, custom plans), use load_stages_multi().
    """
    if STAGE_MAP_FILE.exists():
        try:
            data = json.loads(STAGE_MAP_FILE.read_text(encoding="utf-8"))
            return {k: v for k, v in data.items() if not k.startswith("_")}
        except Exception:
            pass
    return {}


# ── Multi-plan stage resolution (2026-04-21) ────────────────────
# Ashby has multiple interview plans (Default + custom plans for VD, etc.).
# Each plan has its own "AI Screening" / "Application Review" / "Screened" /
# "Nurture" / "Archived" stages with DIFFERENT ids that share the same title.
# A pipeline that only knows one id per title silently misses candidates on
# custom plans. These helpers expose the full picture.

_stages_multi_cache: Optional[Dict[str, Any]] = None


def load_stages_multi(force_refresh: bool = False) -> Dict[str, Any]:
    """Return full stage topology across all interview plans.

    Returns a dict with three keys:
      - "titles_to_ids": {stage_title: set(stage_id, ...)}
      - "stage_to_plan": {stage_id: plan_id}
      - "plan_stages":   {plan_id: {stage_title: stage_id}}

    Cached in-memory for the life of the process. Pass force_refresh=True
    to re-fetch from Ashby (~4 API calls).
    """
    global _stages_multi_cache
    if _stages_multi_cache is not None and not force_refresh:
        return _stages_multi_cache

    titles_to_ids: Dict[str, Set[str]] = {}
    stage_to_plan: Dict[str, str] = {}
    plan_stages: Dict[str, Dict[str, str]] = {}

    # Plans we DO NOT screen for — their candidates must never enter this pipeline.
    # Marketing is managed separately by a different team.
    EXCLUDED_PLAN_TITLE_KEYWORDS = ("marketing",)

    plans_resp = _ashby_post("interviewPlan.list", {})
    for plan in plans_resp.get("results", []) or []:
        plan_id = plan.get("id", "")
        plan_title = (plan.get("title") or "").lower()
        if not plan_id:
            continue
        if any(kw in plan_title for kw in EXCLUDED_PLAN_TITLE_KEYWORDS):
            logger.info("Excluding plan from pipeline: %s", plan.get("title"))
            continue
        stages_resp = _ashby_post("interviewStage.list", {"interviewPlanId": plan_id})
        by_title: Dict[str, str] = {}
        for s in stages_resp.get("results", []) or []:
            title = s.get("title") or s.get("name") or ""
            sid = s.get("id", "")
            if not title or not sid:
                continue
            titles_to_ids.setdefault(title, set()).add(sid)
            stage_to_plan[sid] = plan_id
            by_title[title] = sid
        plan_stages[plan_id] = by_title

    _stages_multi_cache = {
        "titles_to_ids": titles_to_ids,
        "stage_to_plan": stage_to_plan,
        "plan_stages": plan_stages,
    }
    logger.info("Loaded stage topology: %d plans, %d unique stages",
                len(plan_stages), len(stage_to_plan))
    return _stages_multi_cache


def resolve_dest_stage_id(current_stage_id: str, dest_title: str) -> Optional[str]:
    """Given an application's current stage ID and a destination stage title
    (e.g. "Screened"), return the stage ID in the SAME interview plan.

    Falls back to the Default plan's stage if the current plan has no such
    stage (e.g. Marketing has no "AI Screening"). Returns None if not found
    anywhere.
    """
    multi = load_stages_multi()
    plan_id = multi["stage_to_plan"].get(current_stage_id)
    if plan_id:
        sid = multi["plan_stages"].get(plan_id, {}).get(dest_title)
        if sid:
            return sid
    # Fallback: Default plan (cached in .ashby_stage_map.json)
    return load_stage_map().get(dest_title)


def get_stage_ids_by_title(title: str) -> Set[str]:
    """All stage IDs across all plans with this title (e.g. all 'AI Screening'
    stages). Used for set-membership scans across intake stages."""
    return set(load_stages_multi()["titles_to_ids"].get(title, set()))


def load_custom_fields() -> Dict[str, dict]:
    """Load custom field definitions from .ashby_custom_fields.json."""
    if CUSTOM_FIELDS_FILE.exists():
        try:
            data = json.loads(CUSTOM_FIELDS_FILE.read_text(encoding="utf-8"))
            return {k: v for k, v in data.items() if not k.startswith("_")}
        except Exception:
            pass
    return {}


def setup_stages():
    """Fetch and cache stage IDs from Ashby interview plans."""
    plans = _ashby_post("interviewPlan.list", {})
    all_plans = plans.get("results", [])

    if not all_plans:
        print("  No interview plans found. Check API permissions (Interviews → Read).")
        return

    print(f"\n  Found {len(all_plans)} interview plans:\n")

    # Use Default Interview Plan
    default_plan = None
    for p in all_plans:
        print(f"    {p['title']}: {p['id']}")
        if "default" in p["title"].lower():
            default_plan = p

    if not default_plan:
        default_plan = all_plans[0]
        print(f"\n  No 'Default' plan found, using: {default_plan['title']}")

    # Get stages for this plan
    stages = _ashby_post("interviewStage.list", {"interviewPlanId": default_plan["id"]})
    stage_list = stages.get("results", [])

    stage_map = {}
    print(f"\n  Stages in {default_plan['title']}:\n")
    for s in stage_list:
        title = s.get("title", s.get("name", "?"))
        stage_map[title] = s["id"]
        print(f"    {s.get('orderInInterviewPlan', '?'):>2}. {title}: {s['id']}")

    # Save
    stage_map["_note"] = f"Stage IDs for {default_plan['title']} ({default_plan['id']}). Used by pipeline for routing."
    write_json_atomic(STAGE_MAP_FILE, stage_map, indent=2, ensure_ascii=False)
    print(f"\n  Saved {len(stage_list)} stages to {STAGE_MAP_FILE.name}")


# ── Verdict → Stage routing ─────────────────────────────────────

_INBOUND_SOURCES_FILE = _DIR / ".inbound_sources.json"
_inbound_patterns_cache: Optional[List[str]] = None
# Bare-name outbound source patterns (matched only when sourceType isn't
# already "Sourced"). Dual-type sources like "Y Combinator Work at a Startup"
# (a Sourced record used by waas_sourcer.py + a vestigial Inbound record with the
# same title) are disambiguated by sourceType, NOT by bare title — so they don't
# belong here. Direct YC applicants use "Y Combinator Job Board" [Inbound]
# (matched via the "job board" inbound pattern). Empty until a genuinely
# bare-name outbound source (no sourceType, no prefix) needs it.
OUTBOUND_SOURCE_PATTERNS: tuple = ()


def _load_inbound_patterns() -> List[str]:
    """Load inbound-source substring patterns from .inbound_sources.json.

    HARD-FAIL on missing/corrupt file or empty patterns — this is the
    file that decides Inbound App Review vs Outbound Screened routing for
    every SCREEN verdict. An empty list silently routes every inbound
    candidate to Outbound Screened (recruiter handoff instead of HM
    review), which would only be noticed days later by an empty HM bucket.
    Better to refuse to run than misroute silently.
    """
    global _inbound_patterns_cache
    if _inbound_patterns_cache is not None:
        return _inbound_patterns_cache
    try:
        data = json.loads(_INBOUND_SOURCES_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise RuntimeError(
            f"Required routing config missing: {_INBOUND_SOURCES_FILE}. "
            f"This file decides Inbound App Review vs Outbound Screened routing "
            f"and cannot be silently defaulted. Restore the file before running ascreen."
        )
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Required routing config is corrupt: {_INBOUND_SOURCES_FILE} — {e}. "
            f"Fix the JSON before running ascreen (routing would otherwise misroute "
            f"every inbound candidate to Outbound Screened)."
        )
    patterns = [p.lower() for p in (data.get("inbound_patterns") or []) if p]
    if not patterns:
        raise RuntimeError(
            f"{_INBOUND_SOURCES_FILE} has no inbound_patterns. With an empty list, "
            f"every inbound candidate would silently route to Outbound Screened. "
            f"Restore the patterns list before running ascreen."
        )
    _inbound_patterns_cache = patterns
    return _inbound_patterns_cache


def is_inbound_source(source: str, source_type: str = "") -> bool:
    """True if `source` matches any inbound pattern (case-insensitive substring).

    `source_type` is the Ashby `application.source.sourceType.title` (e.g.
    "Sourced"). When provided it is authoritative for outbound-ness, which
    correctly excludes dual-type sources whose bare title is ambiguous (e.g.
    "Y Combinator Work at a Startup" — a Sourced record sharing its title with a
    vestigial Inbound record). Without it, those titles match neither bucket."""
    if not source and not source_type:
        return False
    if is_outbound_source(source, source_type):
        return False
    s = (source or "").lower()
    return any(p in s for p in _load_inbound_patterns())


def is_outbound_source(source: str, source_type: str = "") -> bool:
    """True if Ashby source metadata identifies a sourced/outbound candidate.

    Priority order (handles 3 naming conventions in our data):
      1. Explicit "Inbound —" prefix wins → not outbound, never.
      2. Sourced sourceType OR "outbound" anywhere in the string → outbound.
      3. Bare-name fallback patterns (e.g. raw Ashby YC source).
    """
    s = (source or "").strip().lower()

    source_type_normalized = (source_type or "").strip().lower()
    if source_type_normalized == "sourced":
        return True

    # sourceType is AUTHORITATIVE: an Agencies or Inbound source is NEVER
    # outbound, even if the source string was mislabeled "Outbound — ..." (e.g.
    # an ad-hoc re-screen that mis-prefixed an agency source). Without this, the
    # "outbound" substring check below wins over the real sourceType and an
    # agency SCREEN lands in the cold-outreach lane. (Alejandro Chang / Contrario
    # mis-routed to Outbound Screened, 2026-06-12.)
    if source_type_normalized in ("agencies", "inbound"):
        return False

    if not s:
        return False

    # 1. Explicit inbound prefix overrides everything below.
    if s.startswith("inbound"):
        return False

    # 2. "outbound" anywhere in the string (handles "Outbound — X" prefix
    #    AND older "X Outbound" suffix naming, e.g. "Juicebox Outbound").
    if "outbound" in s:
        return True

    # 3. Bare-name pattern fallback (raw Ashby source values without prefix).
    return any(pattern in s for pattern in OUTBOUND_SOURCE_PATTERNS)


def _prefixed_source(source_obj: dict) -> str:
    """Rebuild the 'Inbound — X' / 'Outbound — X' string the routing classifier
    expects, from a live Ashby application.source object. is_inbound_source keys
    off the prefix; a bare title like 'LinkedIn' defeats it and silently routes
    inbound SCREENs to Outbound Screened. Mirrors the prefix logic in the pull."""
    src = source_obj or {}
    title = src.get("title", "") or ""
    if not title:
        return ""
    st = (src.get("sourceType") or {}).get("title", "") or ""
    return f"Outbound — {title}" if is_outbound_source(title, st) else f"Inbound — {title}"


def is_referral_source(source: str) -> bool:
    """True if `source` looks like a referral (Klarity employee or external).
    Referrals are fully disconnected from auto-decisioning (per 2026-05-19
    hiring retro): every terminal verdict routes to the dedicated
    'Referrals Review' stage for manual HM review (2026-05-21). Errors still
    retry via Needs Rescreen."""
    if not source:
        return False
    s = source.lower()
    return "referral" in s or "referred" in s


# Self-referrals (someone referring themselves through the referral program) are
# auto-archived. Per the recruiting team, 2026-06-01: a self-referral isn't a genuine referral,
# so the "never auto-decision a referral" safeguard (referral-archive incident, 2026-05-19) does NOT
# protect it. Reason chosen by Savio: "Fraudulent Candidate" (gamed the program).
SELF_REFERRAL_ARCHIVE_REASON_ID = "REPLACE_WITH_YOUR_FRAUD_REASON_ID"  # "Fraudulent Candidate"


def _norm_person_name(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace — for name equality."""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", (s or "").lower())).strip()


def _norm_email(s: str) -> str:
    return (s or "").strip().lower()


def is_self_referral(candidate_name: str, candidate_email: str,
                     referrer_name: str, referrer_email: str) -> bool:
    """True if the referrer and the referred candidate are the same person.

    Match on EITHER (a) identical non-empty emails, OR (b) identical normalized
    full names. Referral forms often leave the candidate email blank, so the
    name match is what catches 'typed my own name in both boxes'. Exact full-name
    equality keeps false positives (two distinct people sharing a name) vanishingly
    rare; every auto-archive is also posted to the hiring channel as a safety net.
    """
    ce, re_ = _norm_email(candidate_email), _norm_email(referrer_email)
    if ce and re_ and ce == re_:
        return True
    cn, rn = _norm_person_name(candidate_name), _norm_person_name(referrer_name)
    if cn and rn and cn == rn:
        return True
    return False


def _coerce_confidence(raw) -> Optional[int]:
    """Coerce confidence_score from Opus (int or stringified int) to int, or None."""
    if raw is None or raw == "":
        return None
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def _plan_supports_new_routing(plan_id: str) -> bool:
    """A plan is on the new architecture if it has 'Outbound Screened'.
    Inbound-capable plans also have 'Inbound App Review' (the single review
    bucket for all inbound SCREEN + DEFER candidates); outbound-only plans
    (e.g. the Outbound plan that hosts the Outbound Sourced job) only have
    the Lead-stage flow, which is fine because inbound candidates never land
    on outbound-only jobs."""
    if not plan_id:
        return False
    try:
        plan_stages = load_stages_multi()["plan_stages"].get(plan_id, {})
    except Exception:
        return False
    return "Outbound Screened" in plan_stages


def _plan_id_for_stage(stage_id: str) -> str:
    """Look up which interview plan a stage belongs to."""
    if not stage_id:
        return ""
    try:
        return load_stages_multi()["stage_to_plan"].get(stage_id, "") or ""
    except Exception:
        return ""


# Jobs where a referral DECLINE may be auto-archived (Taylor's carve-out, per the
# SC/AD reject-first screener). This is a deliberate, scoped EXCEPTION to the global
# "referrals are never auto-decisioned" safeguard (referral-archive incident): only a CLEAN
# DECLINE auto-archives, and only on these jobs. SCREEN / DEFER / INSUFFICIENT on a
# referral still go to a human (Referrals Review) — a strong candidate with a thin or
# locked profile can land INSUFFICIENT and must never be auto-killed (variance-round
# rule). EVERY other job keeps the never-auto-decision-referrals safeguard intact.
REFERRAL_AUTODECISION_JOBS = {"solution consultant", "alliances director"}

# Per-role Slack channels for SC/AD referral screening write-backs. When a REFERRAL
# for one of these jobs is screened, the verdict summary is mirrored into that role's
# dedicated hiring channel (in addition to the standard Purple Unicorn post). Keyed on
# the REFERRED job title so each referral posts to its own channel only — never both.
# Bot must be a member of both channels. (Added 2026-06-01.)
ROLE_CHANNEL_MAP = {
    "solution consultant": "REPLACE_WITH_CHANNEL_ID",   # #hiring-solutions-consultant
    "alliances director": "REPLACE_WITH_CHANNEL_ID",     # #hiring-alliances-director
}


def get_verdict_stage(verdict: str, rejection_type: str = "", nurture: str = "",
                      confidence_score=None, source: str = "",
                      current_stage_id: str = "", source_type: str = "",
                      job_title: str = "") -> str:
    """Map screening verdict to Ashby destination stage name.

    Returns stage name (not ID) — caller looks up the ID from stage_map.

    Routing is plan-aware. If the application's interview plan supports the
    new architecture (has 'Outbound Screened' + 'Inbound App Review'), the
    new routing applies; otherwise legacy routing is used.

    REFERRAL SHORT-CIRCUIT (per 2026-05-19 hiring retro, refined 2026-05-21):
    Referrals are never auto-decisioned. Every terminal verdict (SCREEN /
    DEFER / DECLINE / INSUFFICIENT) routes a referral to the dedicated
    'Referrals Review' stage for manual HM review. The screen still runs and
    writes the verdict to custom fields as advisory context — the HM is the
    decider. Errors (SCREENING_FAILED) retry via Needs Rescreen like everyone
    else. Trigger: a high-value referral was auto-archived on a DECLINE despite being a
    high-value referral; referrals later got commingled with inbound apps in
    Inbound App Review, so they got their own bucket on 2026-05-21.

    NEW ROUTING (plans: AI Value Delivery, EPD · Engineering, EPD · Product
    & Design):
    - SCREEN + OUTBOUND (any confidence) → Outbound Screened.
    - SCREEN + INBOUND/AGENCY (any confidence) → Inbound App Review.
    - INSUFFICIENT DATA → Archived (any source). No LinkedIn + no CV means a
      human reviewer has nothing more to look at than the AI did — archive
      rather than burn cycles on review. (Rule added 2026-05-12.)
    - DEFER + INBOUND → Inbound App Review.
    - DEFER + OUTBOUND → Archived.
    - DECLINE → Archived (48h reject-email delay on Ashby).
    - Errors / in-progress markers → Needs Rescreen.

    LEGACY ROUTING (Default Interview Plan, Marketing):
    - SCREEN 5/5 → Application Review.
    - SCREEN 4/5 + INBOUND → Inbound App Review. Outbound 4/5 → Application Review.
    - SCREEN (no confidence) → Application Review.
    - INSUFFICIENT DATA → Archived.
    - DEFER → Inbound App Review.
    - DECLINE → Archived.
    - Errors / in-progress markers → Needs Rescreen.
    """
    # Normalize underscores → spaces so the API form ("SCREENING_FAILED",
    # emitted by remote_screen.py / slack_intake.py) and the display form
    # ("SCREENING FAILED", emitted by screen_batch.py) compare identically
    # downstream (e.g. "INSUFFICIENT_DATA" vs "INSUFFICIENT DATA").
    v = verdict.upper().strip().replace("_", " ")

    # Errors / in-progress markers retry for everyone, including referrals —
    # this must come before the referral short-circuit so a failed screen on a
    # referral still goes to Needs Rescreen (retry), not Referrals Review with
    # no verdict. Fuzzy match: collapse to letters-only and catch ANY variant of
    # "screening failed", "screening", or "input error" regardless of spacing,
    # punctuation, or casing (SCREENING_FAILED, "Screening Failed!", SCREEN-FAIL,
    # SCREENINGFAILED, etc.). Guard: "SCREEN" is a valid PASS verdict and must
    # never be treated as an error — only "SCREENING"/"...FAIL"/"...ERROR" do.
    v_letters = re.sub(r"[^A-Z]", "", v)
    if "FAIL" in v_letters or "ERROR" in v_letters or v_letters == "SCREENING":
        return "Needs Rescreen"

    # Referral short-circuit: no auto-decisioning for referrals. See
    # docstring above for context.
    if is_referral_source(source):
        # Taylor carve-out (SC/AD jobs ONLY): a CLEAN DECLINE auto-archives so
        # management isn't hand-triaging junk referrals. Everything else on a
        # referral — SCREEN, DEFER, and crucially INSUFFICIENT DATA — still goes
        # to a human, because a strong candidate with a thin/locked profile can
        # land INSUFFICIENT (variance-round rule). All other jobs keep the global
        # never-auto-decision-referrals safeguard.
        if v == "DECLINE" and (job_title or "").strip().lower() in REFERRAL_AUTODECISION_JOBS:
            return "Archived"
        return "Referrals Review"

    conf = _coerce_confidence(confidence_score)
    inbound = is_inbound_source(source, source_type)
    # Only a GENUINELY-outbound source goes to the cold-outreach lane. An
    # unknown/untagged source (blank, "Unspecified", or a name that resolves to
    # neither bucket) is NOT outbound — and must never be auto-pushed into
    # outreach, or we risk cold-emailing someone who actually applied. Treat
    # unknown like inbound: send it to human review (Inbound App Review), not
    # Outbound Screened. `not inbound` is too broad here — it swept up unknowns.
    # `source_type` (Ashby application.source.sourceType.title, e.g. "Sourced")
    # is authoritative when present — it disambiguates dual-type sources whose
    # bare title fools the pattern classifier (e.g. "Y Combinator Work at a
    # Startup"), which would otherwise default a SCREEN to Inbound App Review.
    outbound = is_outbound_source(source, source_type)
    plan_id = _plan_id_for_stage(current_stage_id) if current_stage_id else ""
    new_routing = _plan_supports_new_routing(plan_id)

    if v == "SCREEN":
        if new_routing:
            if outbound:
                return "Outbound Screened"
            # inbound OR unknown → human review, never auto-outreach
            return "Inbound App Review"
        # Legacy
        if conf == 4 and inbound:
            return "Inbound App Review"
        return "Application Review"
    elif v == "INSUFFICIENT DATA":
        # Universal archive — unscrapeable LinkedIn + no CV means manual
        # review has nothing more to go on than the AI did.
        return "Archived"
    elif v == "DEFER":
        # Inbound or unknown → human review. Only genuinely-outbound DEFERs
        # auto-archive (thin-data on confirmed cold outbound isn't worth review).
        if not outbound:
            return "Inbound App Review"
        return "Archived"
    elif v == "DECLINE":
        return "Archived"
    elif v == "DUPLICATE":
        return "Archived"
    else:
        return "Needs Rescreen"


# ── Custom field writing ────────────────────────────────────────

# Maps screening result keys → Ashby custom field names
RESULT_TO_FIELD = {
    "verdict": "AI Verdict",
    "spark": "Spark",
    "best_fit_role": "Best Fit Roles",
    "target_role": "Target Role",
    "matched_level": "Matched Level",
    "screener_brief": "Screener Brief",
    "concerns": "Concerns",
    "regret_test": "Regret Test",
    "verdict_reason": "Verdict Reason",
    "best_fit_reason": "Best Fit Role Reason",
    "screening_questions": "Screening Questions",
    "outreach_1": "Outreach Messages",
    "research_output": "Full Dossier",
    "nurture": "Nurture Reason",
    "rejection_type": "Rejection Type",
    "confidence_score": "AI Confidence",
}

# Verdict value mapping (display text → API value)
VERDICT_TO_API = {
    "SCREEN": "SCREEN",
    "DEFER": "DEFER",
    "DECLINE": "DECLINE",
    "INSUFFICIENT DATA": "INSUFFICIENT_DATA",
    "SCREENING FAILED": "SCREENING_FAILED",
    "SCREENING": "SCREENING",
}


def set_screening_marker(candidate_id: str) -> bool:
    """Set AI Verdict to 'SCREENING' as a processing marker.
    Requires 'SCREENING' option to exist in the AI Verdict dropdown in Ashby."""
    fields = load_custom_fields()
    verdict_field = fields.get("AI Verdict", {})
    field_id = verdict_field.get("id")
    if not field_id:
        return False
    try:
        resp = _ashby_post("customField.setValue", {
            "objectId": candidate_id,
            "objectType": "Candidate",
            "fieldId": field_id,
            "fieldValue": "SCREENING",
        })
        return resp.get("success", False)
    except Exception as e:
        logger.debug("Could not set screening marker: %s", e)
        return False

# Role name → API value for Best Fit Roles and Target Role
ROLE_TO_API = {
    "AI Backend Engineer": "AI_BACKEND",
    "AI Frontend Engineer": "AI_FRONTEND",
    "AI Product Engineer": "AI_PRODUCT",
    "AI Design Engineer": "AI_DESIGN",
    "Staff DevSecOps Engineer": "DEVSECOPS",
    "GTM Engineer": "GTM",
    "AI Value Delivery Lead": "AI_VD",
    "Field Marketing Manager": "FIELD_MARKETING",
}
# Recruiting Lead uses different API values per field (Ashby auto-generated UUIDs
# when the option was added 2026-05-19). The Best Fit Roles and Target Role values
# are looked up directly from `.ashby_custom_fields.json` per-field `values` map
# in `_build_custom_field_values`, not from this flat ROLE_TO_API dict.


# ── Outreach overpromise guard ──────────────────────────────────
#
# The Opus prompt bans "we'd love to have you"-style copy that implies a hiring
# decision before any conversation (recruiting lead, 2026-05-21 / 2026-06-01). The ban
# lives in the prompt as an instruction, not a hard stop — so a prompt drift, an
# un-synced prompt copy, or a backfill could still slip banned copy into a
# candidate's profile. This is the last line of defence: NO outreach value is
# ever written to Ashby without passing through here first. Deterministic,
# zero-cost, no model call. If it fires, it logs ERROR so the drift is visible.
_OVERPROMISE_SUBS = [
    (r"we['’]?d love to have you(?: on (?:our|the) team)?", "your background caught our attention"),
    (r"we would love to have you(?: on (?:our|the) team)?", "your background caught our attention"),
    (r"you['’]?d be a (?:great|fantastic|wonderful) addition", "your background is a strong fit signal"),
    (r"you would be a (?:great|fantastic|wonderful) addition", "your background is a strong fit signal"),
    (r"we['’]?d be (?:lucky|thrilled) to have you", "we'd welcome the chance to talk"),
    (r"we want you on (?:our|the) team", "we'd like to talk"),
    (r"you['’]?d be a perfect fit", "you look like a strong fit"),
    (r"you would be a perfect fit", "you look like a strong fit"),
    (r"you['’]?d (?:thrive|fit right in) here", "this could be a strong fit"),
    (r"you would thrive here", "this could be a strong fit"),
]


def _scrub_outreach_overpromise(text: str) -> str:
    """Neutralise banned overpromise phrasing before it reaches Ashby.

    Returns the text unchanged when clean. Logs ERROR (not warning) when it has
    to rewrite, because a fire means banned copy made it past the prompt —
    something to investigate, not silently swallow.
    """
    if not text:
        return text
    scrubbed = text
    for pattern, replacement in _OVERPROMISE_SUBS:
        scrubbed = re.sub(pattern, replacement, scrubbed, flags=re.IGNORECASE)
    if scrubbed != text:
        logger.error(
            "  OVERPROMISE GUARD fired — banned outreach phrasing scrubbed before "
            "Ashby write. Original opener: %r", text[:120],
        )
    return scrubbed


def _build_custom_field_values(result: dict) -> list:
    """Build the fieldValues array for customField.setValues from a screening result."""
    fields = load_custom_fields()
    values = []

    # The CSV-batch path (screen_batch.py) flattens outreach_messages[] into
    # outreach_1 / outreach_2 string fields. The inline path (remote_screen
    # → slack_intake referrals, /intake, ✅ reactions, Candidate Labs) calls
    # screen_one_candidate directly and skips that flattening, so without
    # this fallback the Outreach Email + Outreach Messages fields are silently
    # empty for every inline-screened SCREEN candidate.
    if not result.get("outreach_1"):
        msgs = result.get("outreach_messages") or []
        if msgs:
            o0 = msgs[0] if isinstance(msgs[0], dict) else {}
            msg = (o0.get("message") or "").strip()
            angle = (o0.get("angle") or "").strip()
            if msg:
                result["outreach_1"] = f"[{angle}] {msg}" if angle else msg
            if len(msgs) > 1 and isinstance(msgs[1], dict):
                msg2 = (msgs[1].get("message") or "").strip()
                angle2 = (msgs[1].get("angle") or "").strip()
                if msg2 and not result.get("outreach_2"):
                    result["outreach_2"] = f"[{angle2}] {msg2}" if angle2 else msg2

    for result_key, field_name in RESULT_TO_FIELD.items():
        raw_value = result.get(result_key, "")
        if not raw_value:
            continue

        field_def = fields.get(field_name)
        if not field_def:
            continue

        field_id = field_def["id"]
        field_type = field_def.get("fieldType", "")

        if field_name == "AI Verdict":
            # ValueSelect — map verdict text to API value. Normalize underscores
            # → spaces so the API form "SCREENING_FAILED" (remote_screen.py /
            # slack_intake.py) maps the same as the display form "SCREENING
            # FAILED" — otherwise the dropdown is silently left empty on crash.
            v = raw_value.upper().strip().replace("_", " ")
            api_val = VERDICT_TO_API.get(v)
            if api_val:
                values.append({"fieldId": field_id, "fieldValue": api_val})

        elif field_name == "Best Fit Roles":
            # MultiValueSelect — parse comma-separated roles. Look up the API
            # value from the field's per-field `values` map (the JSON config),
            # not the flat ROLE_TO_API — because Ashby may assign different
            # API values to the same role on different fields (e.g. auto-
            # generated UUIDs for newly-added options).
            field_values_map = field_def.get("values", {}) or ROLE_TO_API
            role_values = []
            for role_name in raw_value.split(","):
                role_name = role_name.strip()
                api_val = field_values_map.get(role_name)
                if api_val:
                    role_values.append(api_val)
                else:
                    # Try partial match against the field's value map
                    for full_name, val in field_values_map.items():
                        if role_name.lower() in full_name.lower():
                            role_values.append(val)
                            break
            if role_values:
                values.append({"fieldId": field_id, "fieldValue": role_values})

        elif field_name == "Target Role":
            # ValueSelect — map role name to API value via the field's
            # per-field `values` map (see comment on Best Fit Roles above).
            field_values_map = field_def.get("values", {}) or ROLE_TO_API
            role_name = raw_value.strip()
            # Target role comes as short code (BE, FE, etc.) — resolve to full name first
            full_name = ROLE_SHORT_TO_INTERNAL.get(role_name, role_name)
            api_val = field_values_map.get(full_name)
            if api_val:
                values.append({"fieldId": field_id, "fieldValue": api_val})

        elif field_name == "Outreach Messages":
            # Combine outreach 1 + 2 into one LongText
            msg2 = result.get("outreach_2", "")
            combined = raw_value
            if msg2:
                combined += "\n\n---\n\n" + msg2
            values.append({"fieldId": field_id, "fieldValue": _scrub_outreach_overpromise(combined)})

        else:
            # String or LongText — write directly
            values.append({"fieldId": field_id, "fieldValue": str(raw_value)})

    # Outreach Email is a String field used as an email-template merge tag.
    # Write the clean message body (no `[angle]` prefix) — the prefix would
    # render literally inside outbound emails.
    outreach_email_field = fields.get("Outreach Email")
    clean_msg = ""
    msgs = result.get("outreach_messages") or []
    if msgs and isinstance(msgs[0], dict):
        clean_msg = (msgs[0].get("message") or "").strip()
    if not clean_msg and result.get("outreach_1"):
        raw = str(result["outreach_1"]).lstrip()
        if raw.startswith("["):
            import re as _re
            m = _re.match(r"^\[[^\]]*\]\s*", raw)
            clean_msg = raw[m.end():] if m else raw
        else:
            clean_msg = raw
    if outreach_email_field and clean_msg:
        values.append({
            "fieldId": outreach_email_field["id"],
            "fieldValue": _scrub_outreach_overpromise(clean_msg),
        })

    return values


def write_custom_fields(candidate_id: str, result: dict) -> bool:
    """Write all screening custom fields to a candidate in Ashby.

    The "Full Dossier" field carries an 8KB+ raw research dump whose free text
    occasionally trips Ashby's Cloudflare WAF (an intermittent edge 403) or is
    simply oversized. It is the LEAST essential field — the verdict, reasoning,
    concerns, best-fit, etc. are what dedup/routing/notes depend on — so it is
    written in a SEPARATE call and its failure is non-fatal. Bundling it used to
    take the whole writeback down, orphaning the candidate over a cosmetic field.
    (internal incident, 2026-06-01: a dossier-only setValues 403'd while all 7
    other fields wrote fine.)
    """
    field_values = _build_custom_field_values(result)
    if not field_values:
        logger.warning("  No custom field values to write for %s", result.get("name", "?"))
        return False

    name = result.get("name", "?")
    dossier_id = (load_custom_fields().get("Full Dossier", {}) or {}).get("id")
    core = [f for f in field_values if f.get("fieldId") != dossier_id]
    dossier = [f for f in field_values if f.get("fieldId") == dossier_id]

    # Core fields are load-bearing — a failure here still orphans (and queues).
    core_resp = _ashby_post("customField.setValues", {
        "objectId": candidate_id,
        "objectType": "Candidate",
        "values": core or field_values,
    })
    if not core_resp.get("success"):
        logger.error("  Failed to write custom fields: %s", core_resp)
        return False
    logger.info("  Wrote %d custom fields for %s", len(core or field_values), name)

    # Full Dossier: best-effort, isolated. A WAF 403 / oversize error here must
    # not fail the writeback (the candidate already has every essential field).
    if dossier:
        try:
            d_resp = _ashby_post("customField.setValues", {
                "objectId": candidate_id,
                "objectType": "Candidate",
                "values": dossier,
            })
            if not d_resp.get("success"):
                logger.warning("  Full Dossier write failed for %s (non-fatal): %s",
                               name, str(d_resp)[:120])
        except Exception as e:
            logger.warning("  Full Dossier write raised for %s (non-fatal): %s", name, e)
    return True


# ── Stage-tailored notes ────────────────────────────────────────

def _format_screened_note(result: dict) -> str:
    """Note for SCREEN verdict → Screened stage.

    Shows fit assessment: brief, concerns, regret test, verdict reason, best fit role reason.
    No screening questions or outreach — those come later at Initial Screen.
    Ashby notes support: <b>, <i>, <u>, <a>, <ul>, <ol>, <li>, <code>, <pre>
    """
    verdict = result.get("verdict", "SCREEN")
    lines = [f"<b>AI Screening Result: {verdict}</b>"]

    brief = result.get("screener_brief", "")
    if brief:
        lines.append("")
        lines.append(f"<b>Screener Brief</b>")
        lines.append(_html_escape(brief))

    concerns = result.get("concerns", "")
    if concerns:
        lines.append("")
        lines.append(f"<b>Concerns</b>")
        # Parse concern lines into a list
        concern_items = [c.strip() for c in concerns.split("\n") if c.strip()]
        if concern_items:
            lines.append("<ul>" + "".join(f"<li>{_html_escape(c)}</li>" for c in concern_items) + "</ul>")

    regret = result.get("regret_test", "")
    if regret:
        lines.append("")
        lines.append(f"<b>Regret Test</b>")
        lines.append(_html_escape(regret))

    bf_reason = result.get("best_fit_reason", "")
    if bf_reason:
        lines.append("")
        lines.append(f"<b>Best Fit Role Reason</b>")
        lines.append(_html_escape(bf_reason))

    reason = result.get("verdict_reason", "")
    if reason:
        lines.append("")
        lines.append(f"<b>Verdict Reason</b>")
        lines.append(_html_escape(reason))

    lines.append("")
    lines.append(f"<i>Screened by AI pipeline | {time.strftime('%Y-%m-%d')}</i>")
    return "<br>".join(lines)


def _format_nurture_note(result: dict) -> str:
    """Note for REVIEW/DEFER/soft DECLINE → Nurture stage."""
    verdict = result.get("verdict", "")
    lines = [f"<b>AI Screening Result: {verdict}</b>"]

    brief = result.get("screener_brief", "")
    if brief:
        lines.append("")
        lines.append(f"<b>Screener Brief</b>")
        lines.append(_html_escape(brief))

    concerns = result.get("concerns", "")
    if concerns:
        lines.append("")
        lines.append(f"<b>Concerns</b>")
        concern_items = [c.strip() for c in concerns.split("\n") if c.strip()]
        if concern_items:
            lines.append("<ul>" + "".join(f"<li>{_html_escape(c)}</li>" for c in concern_items) + "</ul>")

    reason = result.get("verdict_reason", "")
    if reason:
        lines.append("")
        lines.append(f"<b>Verdict Reason</b>")
        lines.append(_html_escape(reason))

    nurture = result.get("nurture", "")
    if nurture:
        lines.append("")
        lines.append(f"<b>Nurture Reason</b>")
        lines.append(_html_escape(nurture))

    defer = result.get("defer_until", "")
    if defer:
        lines.append("")
        lines.append(f"<b>Defer Until</b>")
        lines.append(_html_escape(defer))

    lines.append("")
    lines.append(f"<i>Screened by AI pipeline | {time.strftime('%Y-%m-%d')}</i>")
    return "<br>".join(lines)


def _format_archived_note(result: dict) -> str:
    """Note for hard DECLINE/DUPLICATE → Archived stage. Minimal."""
    verdict = result.get("verdict", "")
    rejection_type = result.get("rejection_type", "")
    label = f"{verdict} ({rejection_type})" if rejection_type else verdict
    lines = [f"<b>AI Screening Result: {label}</b>"]

    reason = result.get("verdict_reason", "")
    if reason:
        lines.append("")
        lines.append(f"<b>Verdict Reason</b>")
        lines.append(_html_escape(reason))

    lines.append("")
    lines.append(f"<i>Screened by AI pipeline | {time.strftime('%Y-%m-%d')}</i>")
    return "<br>".join(lines)


def format_interview_prep_note(result: dict) -> str:
    """Note added when candidate advances from Screened → Initial Screen."""
    lines = ["<b>Interview Prep</b>"]

    msg1 = result.get("outreach_1", "")
    msg2 = result.get("outreach_2", "")
    if msg1 or msg2:
        lines.append("")
        lines.append(f"<b>Outreach Messages</b>")
        if msg1:
            lines.append(f"<i>Message 1:</i>")
            lines.append(_html_escape(msg1))
        if msg2:
            lines.append("")
            lines.append(f"<i>Message 2:</i>")
            lines.append(_html_escape(msg2))

    questions = result.get("screening_questions", "")
    if questions:
        lines.append("")
        lines.append(f"<b>Screening Questions</b>")
        q_items = [q.strip() for q in questions.split("\n") if q.strip()]
        if q_items:
            lines.append("<ol>" + "".join(f"<li>{_html_escape(q)}</li>" for q in q_items) + "</ol>")

    lines.append("")
    lines.append(f"<i>Generated by AI pipeline | {time.strftime('%Y-%m-%d')}</i>")
    return "<br>".join(lines)


def _html_escape(text: str) -> str:
    """Escape HTML special chars."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_stage_note(result: dict, dest_stage: str) -> str:
    """Build the right note content based on destination stage."""
    if dest_stage == "Screened":
        return _format_screened_note(result)
    elif dest_stage == "Nurture":
        return _format_nurture_note(result)
    else:
        return _format_archived_note(result)


# ── Durable writeback retry queue ───────────────────────────────
#
# Screening costs LLM tokens, and the verdict is appended to screening_log.csv
# as a TERMINAL verdict BEFORE the Ashby writeback runs (see
# screen_batch.py:_run_ashby_batch). So if the writeback then fails — e.g. Ashby
# returns a burst of 503s on customField.setValues — the candidate is excluded
# from the next pull (terminal-verdict dedup in _load_screening_log_ids) yet
# never got their custom fields / note / stage move. They are orphaned, and the
# screening tokens are wasted. The drain_* recovery paths can't help either:
# they route on the AI Verdict custom field, which is exactly what failed to
# write.
#
# Fix: every failed writeback is persisted here with its FULL, already-computed
# result and replayed on the next run via replay_writeback_queue() — no
# re-screening, so zero extra tokens. Replays are idempotent: customField
# setValues overwrites, and the stage move resolves to the same destination. We
# only enqueue when the custom-field write fails (before the note is written),
# so a replay never produces a duplicate note.

_WRITEBACK_QUEUE_LOCK = threading.Lock()
_WRITEBACK_MAX_ATTEMPTS = 8  # keep retrying across runs while an outage persists

# Ashby error codes that mean a writeback can NEVER succeed (the target record is
# gone — deleted or merged). Retrying these wastes an API call on every batch and
# pollutes the queue, so they are dead-lettered on sight rather than retried.
_PERMANENT_WRITE_ERRORS = {"candidate_not_found", "application_not_found"}


def _candidate_missing(candidate_id: str) -> bool:
    """True only if Ashby definitively reports the candidate does not exist.

    Returns False on success AND on transient/unknown errors — we only want to
    dead-letter on a definitive not-found, never on a flaky network blip.
    """
    if not candidate_id:
        return False
    try:
        resp = _ashby_post("candidate.info", {"id": candidate_id})
    except Exception:
        return False
    if resp.get("success"):
        return False
    code = (resp.get("errorInfo") or {}).get("code", "")
    errs = resp.get("errors") or []
    return code == "candidate_not_found" or "candidate_not_found" in errs


def _deadletter_writebacks(entries: List[dict], reason: str) -> None:
    """Append unrecoverable queue entries to the dead-letter file (audit trail)."""
    if not entries:
        return
    for e in entries:
        e.setdefault("dead_reason", reason)
        e["dead_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    try:
        existing: List[dict] = []
        if WRITEBACK_DEADLETTER_FILE.exists():
            data = json.loads(WRITEBACK_DEADLETTER_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                existing = data
        write_json_atomic(WRITEBACK_DEADLETTER_FILE, existing + entries,
                          indent=2, ensure_ascii=False)
    except (PermissionError, OSError, ValueError) as e:
        logger.warning("Could not persist writeback dead-letter (%s)", e)


def _load_writeback_queue() -> List[dict]:
    if WRITEBACK_QUEUE_FILE.exists():
        try:
            data = json.loads(WRITEBACK_QUEUE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception as e:
            logger.warning("Could not read writeback queue (%s); starting empty", e)
    return []


def _save_writeback_queue(queue: List[dict]) -> None:
    try:
        write_json_atomic(WRITEBACK_QUEUE_FILE, queue, indent=2, ensure_ascii=False)
    except (PermissionError, OSError) as e:
        logger.warning("Could not persist writeback queue (%s)", e)


def enqueue_failed_writeback(candidate_id: str, application_id: str,
                             result: dict, error: str = "") -> None:
    """Persist a failed writeback so the next run replays it without re-screening.

    Keyed on (candidate_id, application_id): a newer result for the same pair
    replaces the older queued one (the latest screening wins) but keeps the
    accumulated attempt count.
    """
    if not candidate_id or not application_id:
        return
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    with _WRITEBACK_QUEUE_LOCK:
        queue = _load_writeback_queue()
        key = (candidate_id, application_id)
        existing = next(
            (e for e in queue
             if (e.get("candidate_id"), e.get("application_id")) == key),
            None,
        )
        if existing is not None:
            existing["result"] = result
            existing["last_error"] = str(error)[:300]
            existing["updated_at"] = now
        else:
            queue.append({
                "candidate_id": candidate_id,
                "application_id": application_id,
                "name": result.get("name", ""),
                "verdict": result.get("verdict", ""),
                "result": result,
                "attempts": 0,
                "last_error": str(error)[:300],
                "queued_at": now,
                "updated_at": now,
            })
        _save_writeback_queue(queue)
    logger.warning("  Queued writeback for durable retry: %s (%d in queue)",
                   result.get("name", candidate_id), len(queue))


def replay_writeback_queue(dry_run: bool = False) -> dict:
    """Replay queued failed writebacks WITHOUT re-screening (zero tokens).

    Intended to run at the START of each Ashby batch run so a previous run's
    transient Ashby outage self-heals. Outcomes per entry:
      - success                         -> dequeued
      - candidate no longer in Ashby    -> dead-lettered immediately (no retry;
                                           a deleted/merged record can never write)
      - transient failure               -> kept, attempt counter incremented
      - failed _WRITEBACK_MAX_ATTEMPTS  -> dead-lettered (moved out of the queue,
                                           so it stops costing an API call/run)
    Dead-lettered entries are appended to WRITEBACK_DEADLETTER_FILE for audit.
    """
    queue = _load_writeback_queue()
    counts = {"replayed": 0, "ok": 0, "failed": 0, "dead": 0}
    if not queue:
        return counts

    logger.info("Replaying %d queued writeback(s) from prior run(s)...", len(queue))
    remaining: List[dict] = []
    deadletter: List[dict] = []
    for entry in queue:
        cid = entry.get("candidate_id", "")
        app_id = entry.get("application_id", "")
        result = entry.get("result") or {}
        name = entry.get("name") or result.get("name", cid)
        counts["replayed"] += 1

        if dry_run:
            logger.info("  DRY RUN: would replay writeback for %s", name)
            remaining.append(entry)
            continue

        if not cid or not app_id:
            logger.error("  Dropping malformed queue entry (missing ids): %s", name)
            continue

        # Permanent failure: the candidate record is gone. Don't waste a write
        # attempt — dead-letter on sight so it never re-runs on a future batch.
        if _candidate_missing(cid):
            counts["dead"] += 1
            entry["last_error"] = "candidate_not_found"
            entry["dead_reason"] = "candidate_not_found"
            logger.error("  Writeback dead-lettered (candidate no longer in Ashby): %s", name)
            deadletter.append(entry)
            continue

        try:
            ok = write_screening_to_ashby(cid, app_id, result)
        except Exception as e:
            ok = False
            entry["last_error"] = str(e)[:300]

        if ok:
            counts["ok"] += 1
            logger.info("  Replayed writeback OK: %s", name)
            continue

        entry["attempts"] = entry.get("attempts", 0) + 1
        entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        if entry["attempts"] >= _WRITEBACK_MAX_ATTEMPTS:
            counts["dead"] += 1
            entry["dead_reason"] = "max_attempts"
            logger.error("  Writeback STILL failing after %d attempts: %s (%s) "
                         "— moved to dead-letter for manual check",
                         entry["attempts"], name, entry.get("last_error", ""))
            deadletter.append(entry)
        else:
            counts["failed"] += 1
            logger.warning("  Replay failed (attempt %d/%d): %s",
                           entry["attempts"], _WRITEBACK_MAX_ATTEMPTS, name)
            remaining.append(entry)

    if not dry_run:
        _deadletter_writebacks(deadletter, reason="replay")
        with _WRITEBACK_QUEUE_LOCK:
            _save_writeback_queue(remaining)
    logger.info("Writeback replay: %d ok, %d still failing, %d dead-lettered",
                counts["ok"], counts["failed"], counts["dead"])
    return counts


def write_screening_to_ashby_durable(candidate_id: str, application_id: str,
                                     result: dict, dry_run: bool = False) -> bool:
    """write_screening_to_ashby + enqueue-on-failure for token-free replay.

    Use this from batch callers instead of write_screening_to_ashby directly.
    Returns True only on a fully successful writeback. On any unexpected
    exception the full result is queued so the next run replays it without
    re-screening. (Custom-field failures are already enqueued inside
    write_screening_to_ashby itself; this wrapper catches anything else.)
    """
    try:
        return write_screening_to_ashby(candidate_id, application_id, result, dry_run=dry_run)
    except Exception as e:
        if not dry_run:
            logger.warning("  Writeback raised for %s: %s — queuing for retry",
                           result.get("name", candidate_id), e)
            enqueue_failed_writeback(candidate_id, application_id, result, error=e)
        return False


# ── Full Ashby writeback ────────────────────────────────────────

def write_screening_to_ashby(
    candidate_id: str,
    application_id: str,
    result: dict,
    dry_run: bool = False,
) -> bool:
    """Write full screening results to Ashby: custom fields + note + stage move.

    This is the main writeback function called after screening completes.
    Returns True on success.
    """
    name = result.get("name", "?")
    verdict = result.get("verdict", "")
    rejection_type = result.get("rejection_type", "")
    nurture = result.get("nurture", "")
    confidence_score = result.get("confidence_score")
    # Prefer the prefixed `source` ("Inbound — LinkedIn") over the bare
    # `source_raw` ("LinkedIn"). is_inbound_source keys off the "Inbound —"
    # prefix; reading the bare title defeats it, so an inbound LinkedIn SCREEN
    # would misroute to Outbound Screened instead of Inbound App Review.
    source = result.get("source", "") or result.get("source_raw", "")

    # Resolve destination stage WITHIN THIS APP'S INTERVIEW PLAN (multi-plan support
    # added 2026-04-21). Candidates on VD/Senior VD custom plans must route to their
    # plan's Screened/Nurture/Archived stage, not the Default plan's. Fetch current
    # stage id first — get_verdict_stage uses it to pick new vs legacy routing per plan.
    current_stage_id = ""
    # Live sourceType (application.source.sourceType.title, e.g. "Sourced") is
    # authoritative for outbound-ness and disambiguates dual-type sources whose
    # bare title fools the pattern classifier (e.g. "Y Combinator Work at a
    # Startup"). Without it those SCREENs misroute to Inbound App Review.
    source_type = ""
    job_title_for_routing = ""
    try:
        app_info = _ashby_post("application.info", {"applicationId": application_id})
        _results = app_info.get("results", {}) or {}
        current_stage_id = (_results.get("currentInterviewStage", {}) or {}).get("id", "") or ""
        source_type = ((_results.get("source", {}) or {}).get("sourceType", {}) or {}).get("title", "") or ""
        # Live job title — authoritative for the SC/AD referral auto-DQ carve-out
        # (reflects the actual job this application sits on, not the result dict).
        job_title_for_routing = ((_results.get("job", {}) or {}).get("title", "") or "")
    except Exception as e:
        logger.warning("  application.info failed for %s (%s); falling back to Default plan stage map",
                       name, e)
    if not job_title_for_routing:
        job_title_for_routing = result.get("target_role", "") or result.get("job_title", "")

    # Determine destination stage (plan-aware: new routing on aligned plans)
    dest_stage = get_verdict_stage(verdict, rejection_type, nurture,
                                   confidence_score=confidence_score, source=source,
                                   current_stage_id=current_stage_id, source_type=source_type,
                                   job_title=job_title_for_routing)

    dest_stage_id = resolve_dest_stage_id(current_stage_id, dest_stage) if current_stage_id else None
    if not dest_stage_id:
        # Fallback to Default-plan stage map (keeps legacy behavior if plan lookup fails)
        dest_stage_id = load_stage_map().get(dest_stage)

    if not dest_stage_id:
        logger.error("  No stage ID for '%s'. Run: python3 ashby_bridge.py --setup-stages", dest_stage)
        return False

    logger.info("  %s → %s (%s)", name, dest_stage, verdict)

    if dry_run:
        logger.info("  DRY RUN: Would write custom fields + note + move to %s", dest_stage)
        return True

    success = True

    # 1. Write custom fields — the screening verdict's source of truth and what
    #    the dedup + drain_* routing key on. If these don't land, the candidate
    #    is orphaned: the terminal verdict is already in screening_log.csv (so
    #    they're excluded from the next pull) but Ashby never saw the verdict, so
    #    drain can't route them either. Rather than abort here (which strands
    #    them with no fields, no note, no move), queue the FULL writeback for a
    #    token-free replay on the next run and stop — the replay redoes all three
    #    steps together, so the note is never duplicated.
    cf_ok = False
    try:
        cf_ok = write_custom_fields(candidate_id, result)
    except Exception as e:
        logger.warning("  Custom field write raised for %s: %s", name, e)
    if not cf_ok:
        logger.warning("  Custom field write failed for %s — queued for durable "
                       "retry (no re-screen)", name)
        enqueue_failed_writeback(candidate_id, application_id, result,
                                 error="custom field write failed")
        release_claim(candidate_id)
        return False

    # 2. Write stage-tailored note (best-effort: a transient note failure must
    #    not abort the load-bearing stage move below). The note is cosmetic
    #    relative to the custom fields, so we log + continue rather than queue —
    #    queuing here could duplicate the note on replay.
    note_html = format_stage_note(result, dest_stage)
    try:
        if not add_note(candidate_id, note_html):
            logger.warning("  Note write failed for %s (continuing)", name)
            success = False
    except Exception as e:
        logger.warning("  Note write raised for %s: %s (continuing)", name, e)
        success = False

    # 3. Move to destination stage — this is load-bearing. If it fails, the
    # candidate is stranded with a verdict but stuck in the wrong stage.
    is_archive = dest_stage == "Archived"
    if not move_to_stage(application_id, dest_stage_id, is_archive=is_archive):
        logger.error("  STAGE MOVE FAILED for %s → %s. Candidate stranded. "
                     "Next ascreen run will recover via drain_application_review.",
                     name, dest_stage)
        success = False

    # 4. Post-screening job routing: SCREEN verdicts only (inbound + outbound).
    #    Every SCREEN candidate consolidates to ONE app on their best_fit_role's job:
    #    - If best_fit_role differs from target_role → archive target app, create new on best_fit job.
    #    - If multiple best_fit roles listed, the FIRST that maps to a known job wins.
    #    - If best_fit_role IS the target_role (or in the list), keep them in place.
    #    The new app lands at the same dest_stage the routing table picked (Inbound App
    #    Review, Outbound Screened, etc.), resolved within the best-fit job's own plan.
    if verdict.upper() == "SCREEN":
        best_fit = result.get("best_fit_role", "")
        current_job = result.get("target_role", "") or result.get("job_title", "")
        # Use raw Ashby source name (not the display string) so _resolve_source_id can match it
        source = result.get("source_raw", "") or result.get("source", "")
        # Level-aware routing (2026-05-01): screener emits roles[0].level; pass it so
        # AI Frontend Engineer + Junior resolves to the baseline FE job, not the Staff FE job.
        matched_level = (result.get("matched_level") or "").strip()
        # Use the actual current application's job.id for the no-op check, so Staff/Senior
        # variants are distinct from the baseline. application.info was already fetched above.
        current_app_job_id = ""
        try:
            current_app_job_id = ((app_info.get("results") or {}).get("job") or {}).get("id") or ""
        except Exception:
            pass
        if best_fit:
            # Wrap the whole route+archive so a transient Ashby failure during
            # routing can't abort the rest of the writeback (enrichment, claim
            # release, Slack post-back). route_to_best_fit_job is now resilient
            # after create — it returns the new app id even if source/landing
            # steps soft-fail — so the archive below reliably fires and we never
            # leave a duplicate (new best-fit app + un-archived original).
            try:
                routed_app_id = route_to_best_fit_job(
                    candidate_id, best_fit, current_job, name,
                    source=source, dest_stage_title=dest_stage,
                    matched_level=matched_level,
                    current_job_id=current_app_job_id,
                )
            except Exception as e:
                logger.warning("  Best-fit routing raised for %s (continuing): %s", name, e)
                routed_app_id = None
            if routed_app_id:
                # Archive the original application so there's no duplicate.
                # Resolve Archived within the ORIGINAL app's plan (multi-plan support).
                try:
                    archive_id = resolve_dest_stage_id(current_stage_id, "Archived") if current_stage_id else None
                    if not archive_id:
                        archive_id = load_stage_map().get("Archived")
                    if archive_id:
                        move_to_stage(application_id, archive_id, is_archive=True)
                        logger.info("  %s → archived original app, routed to %s", name, best_fit)
                    else:
                        logger.info("  %s → routed to %s (could not archive original)", name, best_fit)
                except Exception as e:
                    logger.warning("  Archive-original failed for %s (routed app exists): %s", name, e)

        # 5. Best-effort email enrichment for outbound + referral SCREEN candidates.
        #    Uses Ashby's "Find Email Addresses" UI feature via captured session.
        #    Soft-fails if session is stale — pipeline continues unaffected.
        try:
            from ashby_enrich import maybe_enrich_after_screen
            maybe_enrich_after_screen(candidate_id, source=source, name=name)
        except Exception as e:
            logger.warning("  Enrichment hook failed (non-fatal) for %s: %s", name, e)

        # 6. Best-effort sequence enrollment for OUTBOUND SCREEN candidates.
        #    Auto-creates a Draft "Cold email outreach – <Role>" enrollment so
        #    the recruiting lead (or whoever owns the sequence) only has to review + Send,
        #    not also enroll. Calls Ashby's internal GraphQL mutation
        #    (ApiCreateSourcingCampaign) via captured browser session — same
        #    auth pattern as ashby_enrich.py. Soft-fails:
        #      - .ashby_session.json missing/expired → log warning, skip
        #      - job has no entry in .ashby_job_to_sequence.json → log + skip
        #      - GraphQL endpoint changes or rejects → log + skip
        #    None of these failures break the pipeline; manual UI enrollment is
        #    the fallback in all cases.
        if dest_stage == "Outbound Screened":
            try:
                from ashby_sequence_enroll import maybe_enroll_after_outbound_screen
                # Determine the FINAL job (post-routing). If best-fit routing
                # moved the candidate to a different job, enroll on that one.
                final_job_id = current_app_job_id
                # routed_app_id only exists if best_fit routing fired and succeeded.
                _routed = locals().get("routed_app_id")
                if _routed:
                    try:
                        routed_info = _ashby_post("application.info", {"applicationId": _routed})
                        final_job_id = ((routed_info.get("results") or {}).get("job") or {}).get("id") or current_app_job_id
                    except Exception:
                        pass
                # Use the candidate's primary email if available
                to_email = ""
                try:
                    cand_primary = (app_info.get("results") or {}).get("candidate", {}).get("primaryEmailAddress", {})
                    to_email = (cand_primary or {}).get("value") or ""
                except Exception:
                    pass
                if final_job_id:
                    maybe_enroll_after_outbound_screen(
                        candidate_id=candidate_id,
                        job_id=final_job_id,
                        name=name,
                        to_email=to_email or None,
                        source=source,
                        source_type=source_type,
                    )
            except Exception as e:
                logger.warning("  Sequence-enroll hook failed (non-fatal) for %s: %s", name, e)

    time.sleep(0.3)
    # Release the file-based processing claim now that writeback is complete.
    # Ashby AI Verdict field is authoritative going forward.
    release_claim(candidate_id)

    # Post-back to Slack: if this candidate has a registered Slack thread (set
    # by Candidate Labs intake or future Slack intakes), post the verdict into
    # that thread. Only fires for terminal verdicts — SCREENING_FAILED leaves
    # the entry in place so the next retry can post when it succeeds.
    if verdict.upper() not in ("SCREENING_FAILED", "SCREENING", ""):
        try:
            import pending_slack_threads as pst
            if pst.peek(candidate_id):
                ashby_url = (
                    f"https://app.ashbyhq.com/candidate-searches/new/right-side/"
                    f"candidates/{candidate_id}/applications/{application_id}/feed"
                ) if application_id else (
                    f"https://app.ashbyhq.com/candidate-searches/new/right-side/"
                    f"candidates/{candidate_id}"
                )
                # If this verdict landed via the Needs Rescreen retry path,
                # mark it so the reader knows it's a delayed post.
                retry_marker = ""
                try:
                    cur_stage_title = ""
                    for stage_name, sid in (load_stage_map() or {}).items():
                        if sid == current_stage_id:
                            cur_stage_title = stage_name
                            break
                    if cur_stage_title == "Needs Rescreen":
                        retry_marker = "_(after auto-retry)_"
                except Exception:
                    pass
                text = pst.format_verdict_text(
                    name=name,
                    verdict=verdict,
                    best_fit=result.get("best_fit_role", "") or "",
                    confidence=result.get("confidence_score"),
                    ashby_url=ashby_url,
                    retry_marker=retry_marker,
                )
                pst.post_to_thread(candidate_id, text, clear_on_success=True)
        except Exception as e:
            logger.warning("pending Slack post-back failed for %s: %s", name, e)

        # Per-role channel mirror: for an SC/AD REFERRAL, also post the verdict
        # summary into that role's dedicated hiring channel. Keyed on the REFERRED
        # job title (not best_fit — the cross-role rescue can change best_fit) so
        # an AD referral posts only to the AD channel and an SC referral only to
        # the SC channel. Top-level message (no thread). Isolated try/except so a
        # channel post failure never affects the primary write-back.
        try:
            jt = (job_title_for_routing or "").strip().lower()
            role_channel = ROLE_CHANNEL_MAP.get(jt)
            if role_channel and is_referral_source(source):
                import pending_slack_threads as pst
                ashby_url = (
                    f"https://app.ashbyhq.com/candidate-searches/new/right-side/"
                    f"candidates/{candidate_id}"
                )
                role_text = "*Referral screening result*\n" + pst.format_verdict_text(
                    name=name,
                    verdict=verdict,
                    best_fit=result.get("best_fit_role", "") or "",
                    confidence=result.get("confidence_score"),
                    ashby_url=ashby_url,
                    retry_marker="",
                )
                pst._slack_post(role_channel, "", role_text)
        except Exception as e:
            logger.warning("role-channel Slack mirror failed for %s: %s", name, e)

    return success


def _load_job_routing() -> Dict[str, str]:
    """Load .ashby_job_routing.json → {normalized_alias: job_id} lookup.

    Back-compat flat lookup. For level-aware resolution, use
    `_resolve_role_with_level(role_name, level)` instead.
    """
    if not JOB_ROUTING_FILE.exists():
        return {}
    try:
        data = json.loads(JOB_ROUTING_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    lookup: Dict[str, str] = {}
    for role_name, info in data.get("roles", {}).items():
        job_id = info.get("job_id", "")
        if not job_id:
            continue
        lookup[role_name.lower()] = job_id
        for alias in info.get("aliases", []):
            lookup[alias.lower()] = job_id
    return lookup


def _load_job_routing_full() -> dict:
    """Load .ashby_job_routing.json as the full nested structure (with
    level_variants, family_role, etc.). Used by `_resolve_role_with_level`.
    """
    if not JOB_ROUTING_FILE.exists():
        return {}
    try:
        return json.loads(JOB_ROUTING_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _resolve_role_with_level(role_name: str, level: str = "") -> Optional[str]:
    """Resolve (role_name, level) → specific Ashby job_id.

    Level-aware lookup. Examples:
      ("AI Frontend Engineer", "Junior") → d08520bc... (baseline FE)
      ("AI Frontend Engineer", "Senior") → cac1d1ae... (Senior FE)
      ("AI Frontend Engineer", "Staff")  → d2079b80... (Staff FE)

    Falls back to plain role lookup if:
      - level is empty
      - role doesn't have level_variants in config (e.g. GTM, Field Marketing)
      - the requested level isn't in level_variants (uses default_job_id)
    """
    if not role_name:
        return None
    cfg = _load_job_routing_full()
    roles = cfg.get("roles", {}) or {}

    # Build a lowercase index of role_name + aliases → role entry
    name_to_entry: Dict[str, dict] = {}
    for canonical_name, info in roles.items():
        name_to_entry[canonical_name.lower()] = info
        for alias in info.get("aliases", []) or []:
            name_to_entry[alias.lower()] = info

    key = role_name.strip().lower()
    entry = name_to_entry.get(key)
    if not entry:
        # Partial match (e.g. "Staff AI Frontend Engineer" → "AI Frontend Engineer")
        for k, v in name_to_entry.items():
            if key in k or k in key:
                entry = v
                break
    if not entry:
        return None

    # If the entry points to a family_role, use that family's level_variants
    if entry.get("family_role"):
        family_entry = name_to_entry.get(entry["family_role"].lower())
        if family_entry:
            entry = family_entry

    # Level-aware: prefer level_variants[level], fall back to default job_id
    if level:
        lv = entry.get("level_variants") or {}
        # Direct match (case-insensitive)
        for k, v in lv.items():
            if k.lower() == level.strip().lower():
                return v
    return entry.get("job_id")


def _existing_candidate_source_id(candidate_id: str, exclude_app_id: str = "") -> Optional[str]:
    """Return a sourceId borrowed from the candidate's existing applications.

    When best-fit routing creates a new application but the incoming source
    string doesn't resolve to a known source record, the new app would land
    blank ("Unspecified") — which breaks attribution and (pre-fix) defaulted the
    candidate toward the wrong routing lane. Inherit the source from the first
    other application that carries one, so the routed app is never sourceless.
    """
    try:
        info = _ashby_post("candidate.info", {"id": candidate_id}).get("results", {})
    except Exception:
        return None
    for aid in info.get("applicationIds") or []:
        if aid == exclude_app_id:
            continue
        try:
            ai = _ashby_post("application.info", {"applicationId": aid}).get("results", {})
        except Exception:
            continue
        src = ai.get("source") or {}
        title = src.get("title") or ""
        if not title:
            continue
        st = (src.get("sourceType") or {}).get("title") or ""
        prefer = "Sourced" if is_outbound_source(title, st) else \
                 ("Inbound" if is_inbound_source(title) else None)
        sid = _resolve_source_id(title, prefer_source_type=prefer)
        if sid:
            return sid
    return None


def route_to_best_fit_job(
    candidate_id: str,
    best_fit_role: str,
    current_job_title: str,
    name: str = "",
    source: str = "",
    dest_stage_title: str = "",
    matched_level: str = "",
    current_job_id: str = "",
) -> Optional[str]:
    """If best_fit_role + matched_level maps to a different job than the current one,
    create a new application on that job. Returns the new app ID if routed, else None.

    LEVEL-AWARE (added 2026-05-01): the same role family can map to multiple Ashby
    jobs by level (e.g. AI Frontend Engineer / Senior AI Frontend Engineer / Staff
    AI Frontend Engineer are three separate jobs). The screener emits a primary
    `roles[0].role` plus `roles[0].level`; this function picks the level-specific
    job_id via `_resolve_role_with_level(role, level)`.

    `current_job_id` should be the candidate's actual application's job.id (not
    derived from the title), so the no-op check compares apples-to-apples. If
    omitted, we fall back to resolving from the title — but that loses the
    Junior/Senior/Staff distinction for the current job.

    `dest_stage_title` is the stage name the routing table picked (e.g.
    "Inbound App Review", "Outbound Screened", "Application Review"). The
    new app lands there, resolved within the best-fit job's own plan.
    """
    if not best_fit_role:
        return None

    routing_flat = _load_job_routing()
    if not routing_flat:
        return None

    def _resolve_plain(role: str) -> Optional[str]:
        """Back-compat plain lookup (no level)."""
        key = role.strip().lower()
        if not key:
            return None
        jid = routing_flat.get(key)
        if jid:
            return jid
        for k, j in routing_flat.items():
            if key in k or k in key:
                return j
        return None

    # Parse all best-fit roles (comma/pipe/semicolon separated).
    best_fit_roles = [r.strip() for r in re.split(r"[,|;]", best_fit_role) if r.strip()]

    # Resolve current job_id. Prefer the explicit param (real application's job.id);
    # fall back to title-resolution for legacy callers.
    if not current_job_id:
        current_job_id = _resolve_plain(current_job_title) if current_job_title else ""

    # TPM keep-in-place: Technical Product Manager is treated as a P+E equivalent.
    # If they were screened on the TPM job and best_fit is any P+E variant, leave
    # them on TPM — don't reroute to the P+E job.
    if current_job_id == TPM_JOB_ID:
        for r in best_fit_roles:
            if r.strip().lower() in PE_FAMILY_BEST_FIT_KEYS:
                logger.info("  %s: TPM job, best_fit '%s' is P+E equivalent, keeping in place",
                            name or candidate_id[:12], r)
                return None

    # Preferred: if the candidate's current job is the level-specific match for
    # any best_fit role, keep them in place. With matched_level passed, "Staff
    # AI Frontend Engineer" job is no longer treated as same as "AI Frontend
    # Engineer" Junior — different job_ids, different routing.
    if current_job_id:
        for r in best_fit_roles:
            jid = _resolve_role_with_level(r, matched_level) if matched_level else _resolve_plain(r)
            if jid and jid == current_job_id:
                logger.info("  %s: target job already matches best_fit '%s' (level=%s), keeping in place",
                            name or candidate_id[:12], r, matched_level or "n/a")
                return None

    # Otherwise: route to the FIRST best-fit role that maps to a known job (level-aware).
    target_job_id = None
    chosen_role = ""
    for r in best_fit_roles:
        jid = _resolve_role_with_level(r, matched_level) if matched_level else _resolve_plain(r)
        if jid:
            target_job_id = jid
            chosen_role = r
            break
    if not target_job_id:
        logger.debug("  No job mapping for best_fit_role '%s' (level=%s)", best_fit_role, matched_level or "n/a")
        return None

    if current_job_id == target_job_id:
        return None  # Already on the right job (and right level)

    # Check if candidate already has an application on the target job (prevents duplicates on re-screen)
    # Note: application.list ignores candidateId param and returns ALL apps — never use it for per-candidate checks.
    # Instead, use candidate.info → applicationIds, then application.info per app to check the job.
    try:
        cand_info = _ashby_post("candidate.info", {"id": candidate_id})
        cand_apps = cand_info.get("results", {}).get("applicationIds", [])
        for app_id in cand_apps:
            app_info = _ashby_post("application.info", {"applicationId": app_id})
            app_data = app_info.get("results", {})
            if app_data.get("job", {}).get("id") == target_job_id:
                app_status = app_data.get("status", "")
                if app_status != "Archived":
                    logger.info("  %s already has active app on %s, skipping route", name or candidate_id[:12], chosen_role)
                    return None
    except Exception as e:
        logger.debug("  Dedup check failed for %s: %s (proceeding with route)", name or candidate_id[:12], e)

    # Create new application on the best-fit job and move to Screened
    # Clean up source string: strip "Outbound — " or "Inbound — " prefix added during pull
    app_source = source or "AI Screening Pipeline"
    # Derive prefer_st from the pull-time "Outbound — "/"Inbound — " prefix BEFORE
    # stripping it: the prefix already encodes the sourceType-based classification.
    # Re-classifying by bare title is ambiguous when two source records share a
    # title (e.g. the Sourced vs vestigial Inbound "Y Combinator Work at a Startup").
    prefer_st: Optional[str] = None
    if app_source.startswith("Outbound — "):
        prefer_st = "Sourced"
    elif app_source.startswith("Inbound — "):
        prefer_st = "Inbound"
    for prefix in ("Outbound — ", "Inbound — "):
        if app_source.startswith(prefix):
            app_source = app_source[len(prefix):]
            break
    # Fallback for un-prefixed sources: classify by bare title.
    if prefer_st is None:
        if is_inbound_source(app_source):
            prefer_st = "Inbound"
        elif is_outbound_source(app_source):
            prefer_st = "Sourced"
    app = create_application(candidate_id, target_job_id, source=app_source)
    if app:
        app_id = app.get("id", "")
        # Ensure source sticks via changeSource (Ashby drops source set at
        # create time). If the incoming source doesn't resolve to a known
        # record, inherit one from the candidate's other applications so the
        # routed app is never left blank/"Unspecified" — which breaks
        # attribution and (pre-fix) defaulted the candidate into the wrong
        # routing lane. (Shraddha A's routed P+E app, 2026-05-27.)
        #
        # BEST-EFFORT: a transient failure here (e.g. an Ashby 503 on
        # changeSource) must NOT abort routing. If it did, the freshly-created
        # best-fit app would be stranded in its New Lead landing stage AND the
        # original app would never get archived — producing a duplicate where
        # the stale original looks like the live one. (Mike Jonas → Senior AI
        # Backend Engineer and Tobias Patella → AI Value Delivery Lead both hit
        # this during the 2026-06-01 Ashby 503 storm.) So we swallow the error
        # and still land the app + let the caller archive the original.
        try:
            source_id = _resolve_source_id(app_source, prefer_source_type=prefer_st)
            if not source_id:
                source_id = _existing_candidate_source_id(candidate_id, exclude_app_id=app_id)
            if source_id:
                # #15 guard: never silently move a layoff candidate onto a
                # non-layoff source. Re-applying the same (layoff) source is fine.
                try:
                    from layoff_cohort import guarded_change_source
                    guarded_change_source(app_id, source_id, candidate_id=candidate_id)
                except Exception:
                    _ashby_post("application.changeSource", {"applicationId": app_id, "sourceId": source_id})
            else:
                logger.warning("  Routed app for %s has no resolvable source — left Unspecified",
                               name or candidate_id[:12])
        except Exception as e:
            logger.warning("  Source-set failed for routed %s (continuing to land app): %s",
                           name or candidate_id[:12], e)
        # Land the new application on the same stage the routing table picked,
        # resolved within the best-fit job's own plan. Wrapped best-effort for
        # the same reason as the source-set above: even if we cannot move it out
        # of New Lead right now, we MUST still return app_id so the caller
        # archives the original (no duplicate). move_to_stage already retries +
        # soft-fails; a stranded New-Lead app is recoverable on a later pass.
        try:
            dest_id: Optional[str] = None
            target_plan_id = ""
            try:
                job_info = _ashby_post("job.info", {"id": target_job_id})
                target_plan_id = (job_info.get("results", {}) or {}).get("defaultInterviewPlanId", "")
            except Exception as e:
                logger.debug("  plan-aware landing lookup failed: %s", e)
            if target_plan_id:
                target_plan_stages = load_stages_multi()["plan_stages"].get(target_plan_id, {})
                # 1. Prefer the explicit dest_stage_title from the routing table
                if dest_stage_title:
                    dest_id = target_plan_stages.get(dest_stage_title)
                # 2. Fallback when dest_stage_title doesn't resolve in the target
                #    plan. CRITICAL: only an *outbound* routing may default into the
                #    outreach lane (Outbound Screened). A non-outbound routing
                #    (Inbound App Review / Application Review / DEFER) must NEVER
                #    fall through to Outbound Screened — that silently pushed
                #    agency/inbound SCREENs into outreach when the best-fit job's
                #    plan lacked an "Inbound App Review" stage (e.g. the Outbound
                #    plan bf792ead). Land them in a human-review stage instead.
                #    (agency-leak fix, 2026-06-11.)
                if not dest_id and _plan_supports_new_routing(target_plan_id):
                    if dest_stage_title == "Outbound Screened":
                        dest_id = target_plan_stages.get("Outbound Screened")
                    else:
                        dest_id = (target_plan_stages.get("Inbound App Review")
                                   or target_plan_stages.get("Application Review"))
            # 3. Last-resort fallback: Default-plan stage map
            if not dest_id:
                stage_map = load_stage_map()
                dest_id = (stage_map.get(dest_stage_title) if dest_stage_title else None) \
                          or stage_map.get("Application Review") or stage_map.get("Screened")
            if dest_id:
                move_to_stage(app_id, dest_id)
        except Exception as e:
            logger.warning("  Landing-stage move failed for routed %s (app stays in New "
                           "Lead, recoverable): %s", name or candidate_id[:12], e)
        logger.info("  Routed %s → %s (app %s, stage=%s)",
                    name or candidate_id[:12], chosen_role, app_id[:12],
                    dest_stage_title or "default")
        return app_id
    else:
        logger.warning("  Failed to route %s → %s", name or candidate_id[:12], chosen_role)
        return None


# ── Pull for screening (Application Review → AI Screening) ──────

def _resolve_redirects(urls: List[str], timeout: int = 3) -> Dict[str, str]:
    """Resolve HTTP redirects for a list of URLs in parallel. Returns
    {original_url: final_url}. On any failure, the original URL is kept.

    Needed so URL shorteners in resumes (bit.ly, lnkd.in, t.co, custom
    domains) resolve to the real LinkedIn/GitHub profile — otherwise the
    classifier misses them.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _one(u: str) -> str:
        headers = {"User-Agent": "Mozilla/5.0"}
        for method in ("HEAD", "GET"):
            try:
                req = urllib.request.Request(u, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.geturl() or u
            except Exception:
                continue
        return u

    if not urls:
        return {}
    out: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(10, len(urls))) as pool:
        for original, final in zip(urls, pool.map(_one, urls)):
            out[original] = final
    return out


def extract_text_from_pdf_bytes(pdf_bytes: bytes, name: str = "") -> dict:
    """Extract text + URLs from PDF bytes. Reusable across Ashby resume download and Slack file upload.

    Returns {"text": str, "linkedin": str, "github": str, "other_urls": [str]}
    """
    import io
    import re

    result = {"text": "", "linkedin": "", "github": "", "other_urls": []}

    if len(pdf_bytes) > 5 * 1024 * 1024:
        logger.warning("  PDF too large for %s (%d bytes), skipping", name, len(pdf_bytes))
        return result

    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
        result["text"] = "\n".join(pages_text).strip()
    except Exception as e:
        logger.warning("  PDF text extraction failed for %s: %s", name, e)
        return result

    # Extract URLs from PDF annotations + text
    all_urls = set()
    try:
        for page in reader.pages:
            annots = page.get("/Annots")
            if not annots:
                continue
            for annot in annots:
                try:
                    obj = annot.get_object() if hasattr(annot, "get_object") else annot
                    a = obj.get("/A", {})
                    if hasattr(a, "get_object"):
                        a = a.get_object()
                    uri = str(a.get("/URI", "")).strip()
                    if uri.startswith("http"):
                        all_urls.add(uri)
                except Exception:
                    continue
    except Exception:
        pass

    url_pattern = re.compile(r'https?://[^\s<>"\')\]]+', re.IGNORECASE)
    all_urls.update(url_pattern.findall(result["text"]))

    # Bare-domain pass: catch LinkedIn/GitHub references WITHOUT http(s):// prefix.
    # PDFs often render hyperlinks as plain text like "linkedin.com/in/scottabutler"
    # or "LinkedIn: linkedin.com/in/foo" — the URL regex above misses these.
    # Normalize to full URLs so the downstream classifier treats them uniformly.
    bare_li_pattern = re.compile(
        r'(?<![\w/])(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_%]+', re.IGNORECASE
    )
    for m in bare_li_pattern.findall(result["text"]):
        clean = m.strip().rstrip('.,;:)')
        if not clean.lower().startswith("http"):
            clean = "https://" + clean.lstrip("/")
        all_urls.add(clean)

    bare_gh_pattern = re.compile(
        r'(?<![\w/])(?:www\.)?github\.com/[A-Za-z0-9\-_.]+(?:/[A-Za-z0-9\-_.]+)?', re.IGNORECASE
    )
    for m in bare_gh_pattern.findall(result["text"]):
        clean = m.strip().rstrip('.,;:)')
        if not clean.lower().startswith("http"):
            clean = "https://" + clean.lstrip("/")
        all_urls.add(clean)

    # Labeled-slug pass: catch "LinkedIn: bamitee-aikins" style references
    # where no domain is present and the next token is a LinkedIn username.
    labeled_li_pattern = re.compile(
        r'\blinkedin\s*[:\-–—]\s*([a-z0-9][a-z0-9\-_]{2,39})(?=[\s|/,;)\]]|$)',
        re.IGNORECASE
    )
    _STOPWORDS = {"profile", "linkedin", "redacted", "above", "below",
                  "here", "available", "email", "same", "see", "text",
                  "info", "contact", "link", "page", "account", "name"}
    for slug in labeled_li_pattern.findall(result["text"]):
        slug_lower = slug.strip().lower()
        if slug_lower in _STOPWORDS:
            continue
        # Require hyphen, digit, or underscore to distinguish slugs from prose
        if not re.search(r'[\-_\d]', slug_lower):
            continue
        all_urls.add(f"https://linkedin.com/in/{slug_lower}")
        break  # one labeled LinkedIn is enough

    IRRELEVANT_HOSTS = (
        "fonts.google", "w3.org", "schemas.openxml", "purl.org",
        "creativecommons.org", "microsoft.com/office",
    )

    # Pass 1: classify by literal substring match
    unresolved: List[str] = []
    for url in all_urls:
        url_lower = url.lower()
        if "linkedin.com/in/" in url_lower and not result["linkedin"]:
            clean = re.sub(r'\?.*$', '', url)
            clean = re.sub(r'/overlay/.*$', '', clean)
            clean = clean.rstrip("/")
            result["linkedin"] = clean
        elif "github.com/" in url_lower and not result["github"]:
            result["github"] = url.rstrip("/")
        elif not any(skip in url_lower for skip in IRRELEVANT_HOSTS) and \
                "linkedin.com" not in url_lower and "github.com" not in url_lower:
            unresolved.append(url)

    # Pass 2: resolve redirects on unclassified URLs (shorteners like bit.ly,
    # lnkd.in, t.co etc. often redirect to the real LinkedIn/GitHub profile).
    resolved_map = _resolve_redirects(unresolved[:15]) if unresolved else {}

    for url in unresolved:
        final = resolved_map.get(url, url)
        final_lower = final.lower()
        if "linkedin.com/in/" in final_lower and not result["linkedin"]:
            clean = re.sub(r'\?.*$', '', final)
            clean = re.sub(r'/overlay/.*$', '', clean)
            clean = clean.rstrip("/")
            result["linkedin"] = clean
        elif "github.com/" in final_lower and not result["github"]:
            result["github"] = final.rstrip("/")
        elif not any(skip in final_lower for skip in IRRELEVANT_HOSTS):
            result["other_urls"].append(final)

    if result["text"]:
        logger.info("  PDF extracted for %s (%d chars, %d URLs found)",
                    name, len(result["text"]), len(all_urls))

    return result


_BARE_LI_RE = re.compile(
    r'(?<![\w/])(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_%]+', re.IGNORECASE
)
_BARE_GH_RE = re.compile(
    r'(?<![\w/])(?:www\.)?github\.com/[A-Za-z0-9\-_.]+(?:/[A-Za-z0-9\-_.]+)?', re.IGNORECASE
)
# "LinkedIn: some-slug" — label followed by a bare LinkedIn username.
# Valid slugs are 3-100 chars, letters/digits/hyphens/underscores, lowercase-leaning.
# Require a boundary (end of string, whitespace, |, or /) after the slug to avoid
# greedy matches into the next word. Case-insensitive on the label, but the slug
# itself must start with a lowercase alnum (LinkedIn slugs are case-insensitive
# but rendered lowercase — this filters out prose like "LinkedIn: Profile").
_LABELED_LI_RE = re.compile(
    r'\blinkedin\s*[:\-–—]\s*([a-z0-9][a-z0-9\-_]{2,39})(?=[\s|/,;)\]]|$)',
    re.IGNORECASE
)


def _rescan_cv_for_social(cv_text: str) -> tuple:
    """Scan CV text for LinkedIn and GitHub references in multiple formats.

    Catches:
      1. Bare domain references: ``linkedin.com/in/foo`` (no http prefix).
      2. Labeled slugs: ``LinkedIn: foo-bar`` (no domain, just username).
      3. GitHub bare domain: ``github.com/foo``.

    Returns (linkedin_url, github_url). Empty strings if not found. Used to
    backfill older cached CVs and to augment PDF text extraction.
    """
    if not cv_text:
        return "", ""
    li = ""
    gh = ""

    # Pattern 1: bare domain
    for m in _BARE_LI_RE.findall(cv_text):
        clean = m.strip().rstrip('.,;:)').lstrip('/')
        if not clean.lower().startswith("http"):
            clean = "https://" + clean
        li = clean
        break

    # Pattern 2: labeled slug ("LinkedIn: bamitee-aikins")
    # Be conservative to avoid false positives like "LinkedIn - text" or
    # "LinkedIn: profile below": require the slug to contain a hyphen, digit, OR
    # underscore (the hallmarks of a real LinkedIn username). Pure-alpha single
    # words like "text" or "profile" almost never appear as real slugs in the
    # "Label: slug" format — if they do, they're rare, and the Linkup fallback
    # in pipeline.py will recover them. Prefer missing a real slug over
    # injecting a bad one (which poisons the dossier).
    if not li:
        for m in _LABELED_LI_RE.findall(cv_text):
            slug = m.strip().lower()
            if slug in {"profile", "linkedin", "redacted", "above", "below",
                        "here", "available", "email", "same", "see", "text",
                        "info", "contact", "link", "page", "account", "name"}:
                continue
            if not re.search(r'[\-_\d]', slug):
                continue  # pure-alpha word, likely not a real slug
            li = f"https://linkedin.com/in/{slug}"
            break

    for m in _BARE_GH_RE.findall(cv_text):
        clean = m.strip().rstrip('.,;:)').lstrip('/')
        if not clean.lower().startswith("http"):
            clean = "https://" + clean
        if clean.lower().rstrip("/") in ("https://github.com", "https://www.github.com"):
            continue
        gh = clean
        break
    return li, gh


def _extract_resume(file_handle: str, name: str = "") -> dict:
    """Download a resume PDF from Ashby and extract text + URLs.

    Uses file.info to get a pre-signed download URL, downloads the PDF,
    then delegates to extract_text_from_pdf_bytes() for parsing.

    Returns {"text": str, "linkedin": str, "github": str, "other_urls": [str]}
    """
    result = {"text": "", "linkedin": "", "github": "", "other_urls": []}

    try:
        # Step 1: Get pre-signed download URL
        file_info = _ashby_post("file.info", {"fileHandle": file_handle})
        if not file_info or not file_info.get("success"):
            logger.warning("  file.info failed for %s", name)
            return result

        download_url = file_info.get("results", {}).get("url", "")
        if not download_url:
            logger.warning("  No download URL for %s", name)
            return result

        # Step 2: Download PDF
        import urllib.request
        req = urllib.request.Request(download_url, headers={"User-Agent": "curl/8.7.1"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            pdf_bytes = resp.read()

        # Step 3: Extract text + URLs
        result = extract_text_from_pdf_bytes(pdf_bytes, name)

        if result["text"]:
            logger.info("  Resume extracted for %s (%d chars)",
                        name, len(result["text"]))

    except Exception as e:
        logger.warning("  Resume extraction failed for %s: %s", name, e)

    return result


# Markers that identify a note we must NOT feed back into screening.
# AI notes = our own prior verdict (reading it makes the screener echo itself).
_AI_NOTE_MARKERS = (
    "AI Screening Result",
    "Screened by AI pipeline",
    "<b>Company:</b>",
    "<b>Summary:</b>",
)
# HM notes = a hiring-manager's opinion. That opinion is the OUTCOME the
# screener's verdict is later compared against in the HM-feedback calibration
# loop (pull_hm_feedback.py); feeding it to the screener makes that comparison
# circular and turns the model into an echo of the HM. Keep it out.
_HM_NOTE_MARKERS = ("[HM Feedback –", "[HM Feedback -", "AUTHOR: Hiring Manager")


def _note_is_excluded(content: str, author: dict) -> bool:
    """True if a note is AI-authored or HM-authored (must not enter screening)."""
    if not content:
        return True
    if any(m in content for m in _AI_NOTE_MARKERS):
        return True
    if any(m in content for m in _HM_NOTE_MARKERS):
        return True
    # Live UI notes carry a real author — exclude if the author is on the
    # hiring team (reuse the canonical roster; lazy import avoids an import cycle
    # since pull_hm_feedback imports this module).
    try:
        from pull_hm_feedback import _is_hiring_team
        if isinstance(author, dict) and author and _is_hiring_team(author):
            return True
    except Exception:
        pass
    return False


def _extract_candidate_documents(candidate_full: dict, name: str = "") -> str:
    """Text of every NON-resume file attached to the candidate.

    The resume is pulled separately into the CV. Any other file in `fileHandles`
    (interview/call transcripts, take-homes, portfolios) is primary evidence the
    screener should see, labeled by filename so the model knows what it is.
    Returns one labeled block per document, or "" if there are none.
    """
    file_handles = candidate_full.get("fileHandles") or []
    resume_id = (candidate_full.get("resumeFileHandle") or {}).get("id", "")
    blocks = []
    for fh in file_handles:
        if not isinstance(fh, dict):
            continue
        handle = fh.get("handle", "")
        fname = fh.get("name", "document")
        if not handle or (resume_id and fh.get("id") == resume_id):
            continue  # skip the resume — already used as the CV
        text = (_extract_resume(handle, f"{name} · {fname}").get("text") or "").strip()
        if text:
            blocks.append(f"=== ADDITIONAL DOCUMENT: {fname} ===\n{text}")
    if blocks:
        logger.info("  Pulled %d additional document(s) for %s", len(blocks), name)
    return "\n\n".join(blocks)


def _gather_screening_notes(candidate_id: str, name: str = "") -> str:
    """Human notes on the candidate, for screening context.

    Excludes AI-pipeline notes and hiring-manager notes (see _note_is_excluded);
    keeps recruiter / agency / intake notes, which are factual context. Returns
    one labeled block, or "".
    """
    if not candidate_id:
        return ""
    try:
        result = _ashby_post("candidate.listNotes", {"candidateId": candidate_id})
        notes = result.get("results", []) or []
    except Exception as e:
        logger.warning("  listNotes failed for %s: %s", name, e)
        return ""
    kept = []
    for note in notes:
        content = note.get("content") or note.get("note") or ""
        if not str(content).strip():
            continue
        if _note_is_excluded(str(content), note.get("author") or {}):
            continue
        clean = re.sub(r"<[^>]+>", " ", str(content))
        clean = re.sub(r"\s+", " ", clean).strip()
        if clean:
            kept.append(clean)
    if not kept:
        return ""
    logger.info("  Pulled %d recruiter/intake note(s) for %s", len(kept), name)
    return "=== RECRUITER / INTAKE NOTES ===\n" + "\n\n".join(kept)


def _load_sheet_ashby_ids() -> Set[str]:
    """Load all Ashby Candidate IDs from Sheet CSV exports in backfill_csvs/.

    These are candidates already in the Sheet — skip them during screening.
    """
    ids: Set[str] = set()
    csv_dir = _DIR / "backfill_csvs"
    if not csv_dir.exists():
        logger.info("No backfill_csvs/ directory — skip-set empty")
        return ids
    for csv_file in sorted(csv_dir.glob("*.csv")):
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                content = f.read().replace("\x00", "")
            reader = csv.DictReader(io.StringIO(content))
            col = "Ashby Candidate ID"
            if col not in (reader.fieldnames or []):
                continue
            count = 0
            for row in reader:
                aid = (row.get(col) or "").strip()
                if aid:
                    ids.add(aid)
                    count += 1
            logger.info("  %s: %d Ashby IDs", csv_file.name, count)
        except Exception as e:
            logger.warning("  Error reading %s: %s", csv_file.name, e)
    logger.info("Skip-set loaded: %d Ashby Candidate IDs from Sheet CSVs", len(ids))
    return ids


def _load_screening_log_ids() -> Tuple[Set[str], Set[str]]:
    """Load Ashby Candidate IDs and LinkedIn URLs from screening_log.csv.

    Only IDs whose LATEST log entry is a TERMINAL verdict count as "already screened."
    Non-terminal verdicts (SCREENING / SCREENING FAILED) mean the screening crashed
    or is still in progress — those must be retried on the next pull, not skipped.

    Forward-only rule (2026-04-21): a SCREENING FAILED entry must never block re-screening.
    If the candidate's LATEST log row is SCREENING FAILED, they stay eligible for pull.

    Returns (ashby_ids, normalized_linkedin_urls).
    """
    NON_TERMINAL = {"SCREENING", "SCREENING FAILED", "SCREENING_FAILED", ""}

    # Walk rows keeping LATEST verdict per candidate ID (CSV is chronological append-only).
    latest_verdict_by_id: Dict[str, str] = {}
    latest_verdict_by_linkedin: Dict[str, str] = {}

    log_path = _DIR / "screening_log.csv"
    if not log_path.exists():
        return set(), set()
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                aid = (row.get("ashby_candidate_id") or "").strip()
                li_raw = (row.get("linkedin") or "").strip()
                verdict = (row.get("verdict") or "").strip().upper()
                if aid:
                    latest_verdict_by_id[aid] = verdict
                if li_raw:
                    latest_verdict_by_linkedin[_normalize_linkedin(li_raw)] = verdict
    except Exception as e:
        logger.warning("Error reading screening_log.csv: %s", e)
        return set(), set()

    ids = {aid for aid, v in latest_verdict_by_id.items() if v not in NON_TERMINAL}
    linkedins = {li for li, v in latest_verdict_by_linkedin.items() if v not in NON_TERMINAL}

    retry_count = sum(1 for v in latest_verdict_by_id.values() if v in NON_TERMINAL and v)
    logger.info(
        "Screening log: %d terminal IDs, %d terminal LinkedIn URLs loaded "
        "(excluded %d non-terminal entries — eligible for retry)",
        len(ids), len(linkedins), retry_count
    )
    return ids, linkedins


# Columns in screening_log.csv that map 1:1 to screening-result keys, so a
# logged row can rebuild the `result` dict a writeback needs (no re-screen).
_LOG_RESULT_KEYS = (
    "name", "verdict", "spark", "verdict_reason", "best_fit_role",
    "best_fit_reason", "matched_level", "reasoning", "regret_test", "concerns",
    "screening_questions", "screener_brief", "defer_until", "outreach_1",
    "outreach_2", "research_output", "rejection_type", "nurture", "move_to",
    "source", "target_role", "job_title", "linkedin", "confidence_score",
)


def _result_from_log_row(row: dict) -> dict:
    """Rebuild a screening `result` dict from a screening_log.csv row so a
    failed writeback can be replayed WITHOUT re-screening (the verdict + all
    supporting fields were persisted to the log when the candidate was screened)."""
    result = {k: (row.get(k) or "") for k in _LOG_RESULT_KEYS}
    # get_verdict_stage / _build_custom_field_values expect a numeric confidence.
    cs = (row.get("confidence_score") or "").strip()
    if cs:
        try:
            result["confidence_score"] = float(cs)
        except ValueError:
            result["confidence_score"] = None
    else:
        result["confidence_score"] = None
    # write_screening_to_ashby reads `source` (prefixed) then falls back to
    # source_raw; the log only keeps the prefixed source, so mirror it.
    result["source_raw"] = row.get("source") or ""
    return result


def reconcile_orphaned_writebacks(dry_run: bool = True, limit: int = 0,
                                  workers: int = 5, recent: int = 0) -> dict:
    """Find candidates with a TERMINAL verdict in screening_log.csv whose Ashby
    AI Verdict field is still EMPTY (i.e. the writeback never landed — the
    "orphan" failure mode) and re-push them from the logged result. No
    re-screening, so zero tokens.

    This is the one-off sweep for orphans created BEFORE the durable writeback
    queue existed (the queue only catches failures from runs after it shipped).

    `recent` limits the LIVE scan to the N most recently logged terminal
    candidates (by timestamp). Strongly recommended: checking the full history
    is thousands of candidate.info calls and Ashby rate-limits aggressively.
    Orphans are almost always from a recent failed run, so `recent=400` covers
    them cheaply. `workers` is kept low (5) for the same reason.

    dry_run=True (default) only reports what it would do — nothing is written.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    NON_TERMINAL = {"SCREENING", "SCREENING FAILED", "SCREENING_FAILED", ""}

    log_path = _DIR / "screening_log.csv"
    if not log_path.exists():
        logger.info("No screening_log.csv — nothing to reconcile.")
        return {"checked": 0, "orphans": 0, "fixed": 0, "queued": 0, "errors": 0}

    # Keep the LATEST full row per candidate id (CSV is append-only/chronological).
    latest_row_by_id: Dict[str, dict] = {}
    with open(log_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            aid = (row.get("ashby_candidate_id") or "").strip()
            if aid:
                latest_row_by_id[aid] = row

    # Candidates whose latest logged verdict is terminal AND have an application id.
    terminal = [
        (aid, row) for aid, row in latest_row_by_id.items()
        if (row.get("verdict") or "").strip().upper() not in NON_TERMINAL
        and (row.get("ashby_application_id") or "").strip()
    ]
    logger.info("Reconcile: %d candidates with a terminal verdict in the log.",
                len(terminal))

    # Bound the live scan to the most recently logged candidates (Ashby
    # rate-limits hard; orphans are essentially always from a recent run).
    if recent and len(terminal) > recent:
        terminal.sort(key=lambda t: (t[1].get("timestamp") or ""), reverse=True)
        terminal = terminal[:recent]
        logger.info("Reconcile: scanning the %d most recently logged terminal "
                    "candidates only.", recent)

    # Resolve the AI Verdict custom field id so we can read the live value.
    verdict_field_id = (load_custom_fields().get("AI Verdict", {}) or {}).get("id")
    if not verdict_field_id:
        logger.error("Reconcile: AI Verdict field id not found in "
                     ".ashby_custom_fields.json. Run --setup-stages / field setup first.")
        return {"checked": 0, "orphans": 0, "fixed": 0, "queued": 0, "errors": 0}

    def _live_verdict(cid: str) -> Optional[str]:
        """Return the candidate's current AI Verdict value in Ashby ('' if unset, None on error)."""
        try:
            info = _ashby_post("candidate.info", {"id": cid})
            for cf in (info.get("results") or {}).get("customFields", []) or []:
                if cf.get("id") == verdict_field_id:
                    return (cf.get("value") or "").strip()
            return ""
        except Exception as e:
            logger.warning("  candidate.info failed for %s: %s", cid, e)
            return None

    # Parallel read-only scan to classify orphans (live AI Verdict empty/non-terminal).
    orphans: List[Tuple[str, dict]] = []
    checked = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_live_verdict, aid): (aid, row) for aid, row in terminal}
        for fut in as_completed(futs):
            aid, row = futs[fut]
            checked += 1
            live = fut.result()
            if live is None:
                continue  # transient read error — leave for a later run
            if live.upper() in NON_TERMINAL:
                orphans.append((aid, row))

    logger.info("Reconcile: %d/%d candidates are orphans (terminal in log, "
                "empty AI Verdict in Ashby).", len(orphans), checked)

    if limit and len(orphans) > limit:
        orphans = orphans[:limit]
        logger.info("Reconcile: limiting to first %d orphans.", limit)

    counts = {"checked": checked, "orphans": len(orphans),
              "fixed": 0, "queued": 0, "errors": 0}

    for aid, row in orphans:
        name = (row.get("name") or aid)
        verdict = (row.get("verdict") or "").strip()
        app_id = (row.get("ashby_application_id") or "").strip()
        if dry_run:
            logger.info("  WOULD re-push: %-32s verdict=%s", name[:32], verdict)
            continue
        result = _result_from_log_row(row)
        try:
            # Durable wrapper: on failure it parks the result in the writeback
            # queue, so even if Ashby is flaky now it self-heals on the next run.
            ok = write_screening_to_ashby_durable(aid, app_id, result)
            if ok:
                counts["fixed"] += 1
                logger.info("  Re-pushed OK: %-32s verdict=%s", name[:32], verdict)
            else:
                counts["queued"] += 1
                logger.warning("  Re-push failed (queued for retry): %s", name)
        except Exception as e:
            counts["errors"] += 1
            logger.error("  Re-push errored for %s: %s", name, e)

    logger.info("Reconcile complete: %s", counts)
    return counts


# File to cache candidate IDs we've confirmed are already screened in Ashby
_KNOWN_SCREENED_FILE = _DIR / ".known_screened_ids.json"
_SCREENING_CLAIMS_FILE = _DIR / ".screening_claims.json"
_CLAIM_MAX_AGE_SEC = 2 * 60 * 60  # 2h — a run that hasn't finished in 2h is crashed
_CLAIMS_LOCK = threading.Lock()

def _load_claims() -> Dict[str, float]:
    if _SCREENING_CLAIMS_FILE.exists():
        try:
            return dict(json.loads(_SCREENING_CLAIMS_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return {}

def _save_claims(claims: Dict[str, float]):
    try:
        write_json_atomic(_SCREENING_CLAIMS_FILE, claims)
    except (PermissionError, OSError) as e:
        logger.warning("Could not persist screening claims (%s).", e)

def claim_for_screening(candidate_id: str) -> None:
    with _CLAIMS_LOCK:
        claims = _load_claims()
        claims[candidate_id] = time.time()
        _save_claims(claims)

def release_claim(candidate_id: str) -> None:
    with _CLAIMS_LOCK:
        claims = _load_claims()
        if claims.pop(candidate_id, None) is not None:
            _save_claims(claims)


def release_claims(candidate_ids: List[str]) -> int:
    """Release multiple claims in a single file rewrite. Idempotent — IDs that
    are not currently claimed are silently ignored. Returns the number of
    claims that were actually released. Cheaper than calling release_claim N
    times because we only touch the file once."""
    if not candidate_ids:
        return 0
    with _CLAIMS_LOCK:
        claims = _load_claims()
        released = 0
        for cid in candidate_ids:
            if cid and claims.pop(cid, None) is not None:
                released += 1
        if released > 0:
            _save_claims(claims)
        return released

def is_claimed(candidate_id: str) -> bool:
    with _CLAIMS_LOCK:
        claims = _load_claims()
        ts = claims.get(candidate_id)
        if not ts:
            return False
        if time.time() - ts > _CLAIM_MAX_AGE_SEC:
            # Stale claim — a prior run crashed. Drop it so this run can re-screen.
            claims.pop(candidate_id, None)
            _save_claims(claims)
            return False
        return True


def _load_known_screened() -> Set[str]:
    """Load candidate IDs we've already confirmed have a verdict in Ashby."""
    if _KNOWN_SCREENED_FILE.exists():
        try:
            return set(json.loads(_KNOWN_SCREENED_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return set()


def _save_known_screened(ids: Set[str]):
    """Save candidate IDs confirmed as already screened.

    Fail-soft: the cache is a perf optimization only. Ashby's AI Verdict field
    is the real dedup layer, so losing this cache never causes wrong behavior —
    it just means the next run re-fetches customFields for those candidates.

    macOS can block writes from detached processes when files have inherited
    `com.apple.provenance` xattr from a sandboxed parent shell. Catch and log
    rather than crash the whole run."""
    try:
        write_json_atomic(_KNOWN_SCREENED_FILE, sorted(ids), ensure_ascii=False)
    except (PermissionError, OSError) as e:
        logger.warning(
            "Could not save known-screened cache (%s). Continuing without cache — "
            "Ashby AI Verdict field remains the authoritative dedup.", e
        )


def pull_for_screening(limit: int = 0, dry_run: bool = False, force: bool = False,
                       include_leads: bool = True) -> List[Dict]:
    """Pull unscreened candidates from the Lead sub-stages AND Application Review.

    Updated 2026-04-23: Application Review is pulled because Ashby hard-wires inbound
    applications to land there (PreInterviewScreen type is the only valid landing
    stage for submitted applications — confirmed via Ashby docs). For AR candidates
    with an empty/in-progress AI Verdict, AR is functionally intake, not progression.

    Pull sources:
      - New Lead        (all unscreened intake — outbound, inbound-manual, referrals)
      - Needs Rescreen  (errored previously, retry)
      - Application Review (inbound app submissions that Ashby auto-routed here,
                            filtered to empty-verdict + non-human-moved)

    The human-move safeguard applies to AR: if a human moved the candidate into AR
    (actorId != automation), we leave them alone — they've been deliberately placed
    there post-screen.

    Single-gate dedup: Ashby AI Verdict field is the only authoritative check.
    Terminal verdicts (SCREEN/DECLINE/etc.) skip; empty / SCREENING / SCREENING_FAILED
    are pulled.
    """
    lead_stage_titles = ["New Lead", "Needs Rescreen"]
    lead_stage_ids: Set[str] = set()
    for title in lead_stage_titles:
        ids = get_stage_ids_by_title(title)
        if ids:
            lead_stage_ids.update(ids)
        else:
            logger.warning("No '%s' stages found across plans.", title)

    if not lead_stage_ids:
        logger.error("No Lead sub-stage IDs found. Run: python3 ashby_bridge.py --setup-stages")
        return []

    # Application Review stage IDs (across all active plans) — second pull source
    ar_stage_ids: Set[str] = set(get_stage_ids_by_title("Application Review"))
    if not ar_stage_ids:
        logger.warning("No 'Application Review' stages found — AR pull disabled")
    AUTOMATION_ACTOR_ID = "REPLACE_WITH_YOUR_AUTOMATION_ACTOR_ID"

    # Still track candidates who get discovered as already-screened during the pull
    # (their Ashby AI Verdict is terminal) — used to warm .known_screened_ids.json cache.
    known_screened = _load_known_screened() if not force else set()

    # Rescreen-pending set: candidates explicitly queued for re-screening by
    # rescreen_outbound.py. Kept so we can clear the file after they're pulled.
    _rescreen_path = _DIR / ".rescreen_pending.json"
    rescreen_ids: Set[str] = set()
    if _rescreen_path.exists():
        try:
            rescreen_ids = set(json.loads(_rescreen_path.read_text(encoding="utf-8")))
        except Exception:
            pass

    newly_discovered_screened: Set[str] = set()
    skipped_inactive_role = 0
    skipped_already_screened = 0
    skipped_agency_vd_lead = 0

    # Active roles we screen for (short codes)
    active_roles = set(ROLE_SHORT_TO_INTERNAL.keys())  # BE, FE, P+E, D+E, DevOps, GTM, VD, FM

    # Load push log for LinkedIn URL recovery (candidate_id → LinkedIn URL)
    # Ashby's candidate.info may not return socialLinks, so we fall back to local push data
    _push_log_path = _DIR / ".push_to_ashby_log.json"
    push_log_reverse: Dict[str, str] = {}  # candidate_id → full LinkedIn URL
    if _push_log_path.exists():
        try:
            _pl = json.loads(_push_log_path.read_text(encoding="utf-8"))
            for li_path, cid in _pl.items():
                push_log_reverse[cid] = f"https://linkedin.com{li_path}" if li_path.startswith("/in/") else li_path
        except Exception:
            pass

    # AI Verdict custom field ID for checking already-screened candidates
    custom_fields_path = os.path.join(os.path.dirname(__file__), ".ashby_custom_fields.json")
    verdict_field_id = None
    if os.path.exists(custom_fields_path):
        with open(custom_fields_path) as _f:
            _cf = json.load(_f)
            verdict_field_id = _cf.get("AI Verdict", {}).get("id")

    # ── Filtered scan: Lead sub-stages only ──
    # status="Lead" covers all Lead sub-stages (New Lead, Needs Rescreen, etc.).
    # Processing lock is now file-based (.screening_claims.json) — candidates stay in
    # their Lead sub-stage during screening instead of being moved to AI Screening.
    intake_apps = []  # (app, candidate_data, candidate_id) tuples

    pages_scanned = 0
    logger.info("Scanning Ashby applications (status=Lead)...")
    cursor = None
    while True:
        payload: Dict[str, Any] = {"limit": 100, "status": "Lead"}
        if cursor:
            payload["cursor"] = cursor

        result = _ashby_post("application.list", payload)
        apps = result.get("results", [])
        if not apps:
            break
        pages_scanned += 1

        for app in apps:
            stage = app.get("currentInterviewStage", {})
            stage_id = stage.get("id", "")
            candidate_data = app.get("candidate", {})
            candidate_id = candidate_data.get("id", "")

            if stage_id in lead_stage_ids:
                intake_apps.append((app, candidate_data, candidate_id))

        if not result.get("moreDataAvailable"):
            break
        cursor = result.get("nextCursor")

    lead_count = len(intake_apps)
    logger.info("Lead scan complete: %d candidates from Lead sub-stages, %d pages",
                lead_count, pages_scanned)

    # ── Second scan: Application Review (status=Active) ──
    # Ashby hard-wires inbound apps to AR. We pull them if their verdict is empty,
    # regardless of who placed them. Rationale (2026-04-24): human-placed AR
    # candidates without a verdict still need AI screening — the human has just
    # surfaced them to AR, not pre-decided their outcome. Post-screening, normal
    # verdict routing applies (SCREEN 5/5 stays in AR, DECLINE → Archived, etc.).
    #
    # Perf: two stages, .known_screened_ids.json is the one cache we still need.
    #   1. Skip instantly if candidate has a known terminal verdict.
    #   2. For the remainder: parallel candidate.info verdict check (20 workers).
    # The listHistory / human-actor check is gone — we include human-placed apps.
    ar_scanned = 0
    ar_skipped_verdict = 0
    ar_skipped_cache = 0
    TERMINAL_VERDICTS = {"SCREEN", "DECLINE", "DEFER", "INSUFFICIENT_DATA"}
    if ar_stage_ids:
        logger.info("Scanning Application Review (status=Active) for unscreened apps...")
        # Stage 1: paginate AR list, apply verdict cache skip
        ar_to_check = []  # (app, candidate_data, candidate_id)
        cursor = None
        ar_pages = 0
        while True:
            payload = {"limit": 100, "status": "Active"}
            if cursor:
                payload["cursor"] = cursor
            result = _ashby_post("application.list", payload)
            apps = result.get("results", []) or []
            if not apps:
                break
            ar_pages += 1

            for app in apps:
                stage = app.get("currentInterviewStage", {})
                if stage.get("id", "") not in ar_stage_ids:
                    continue
                ar_scanned += 1
                candidate_data = app.get("candidate", {}) or {}
                candidate_id = candidate_data.get("id", "")

                if candidate_id in known_screened:
                    ar_skipped_cache += 1
                    continue

                ar_to_check.append((app, candidate_data, candidate_id))

            if not result.get("moreDataAvailable"):
                break
            cursor = result.get("nextCursor")

        # Stage 2: parallel verdict check for uncached candidates.
        from concurrent.futures import ThreadPoolExecutor

        def _fetch_verdict(cid: str) -> str:
            try:
                info = _ashby_post("candidate.info", {"id": cid})
                for cf in (info.get("results") or {}).get("customFields", []) or []:
                    if cf.get("id") == verdict_field_id:
                        return (cf.get("value") or "").strip().upper()
            except Exception:
                return ""
            return ""

        verdicts_by_cid: Dict[str, str] = {}
        if ar_to_check and verdict_field_id:
            cids = [t[2] for t in ar_to_check]
            with ThreadPoolExecutor(max_workers=20) as pool:
                for cid, v in zip(cids, pool.map(_fetch_verdict, cids)):
                    verdicts_by_cid[cid] = v

        # Stage 3: filter. Terminal verdict → skip + warm cache. Else include.
        for app, candidate_data, candidate_id in ar_to_check:
            v = verdicts_by_cid.get(candidate_id, "")
            if v in TERMINAL_VERDICTS:
                ar_skipped_verdict += 1
                newly_discovered_screened.add(candidate_id)
                continue

            intake_apps.append((app, candidate_data, candidate_id))

        logger.info("AR scan complete: %d in AR | cached-skip (verdict): %d | uncached-skip (verdict): %d | added: %d | %d pages",
                    ar_scanned, ar_skipped_cache, ar_skipped_verdict,
                    len(intake_apps) - lead_count, ar_pages)

    logger.info("Total intake: %d candidates (Lead: %d, AR: %d)",
                len(intake_apps), lead_count, len(intake_apps) - lead_count)

    # Filter intake apps using skip-sets (Sheet CSV + AI Verdict field check)
    candidates = []
    seen_candidate_ids = set()
    total = 0

    # Pre-filter: drop apps whose job maps to an inactive role. This must run
    # BEFORE the per-candidate dedup below — otherwise a candidate with one
    # inactive app (e.g. Product Analytics Engineer) and one active app can
    # have the inactive one win dedup and get silently dropped. After this
    # filter, any remaining app is a valid screening target for its candidate.
    def _app_has_active_role(app):
        job_title_peek = (app.get("job") or {}).get("title", "")
        role_peek = _map_job_title_to_role(job_title_peek)
        return role_peek in active_roles

    pre_count = len(intake_apps)
    intake_apps = [t for t in intake_apps if _app_has_active_role(t[0])]
    dropped_inactive = pre_count - len(intake_apps)
    if dropped_inactive:
        logger.info("Dropped %d apps (inactive role) before dedup", dropped_inactive)

    # ── Serial pre-pass: per-candidate dedup (cheap set lookup) ──
    unique_apps: List[tuple] = []
    for app, candidate_data, candidate_id in intake_apps:
        if candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)
        unique_apps.append((app, candidate_data, candidate_id))

    # ── Parallel metadata fetch (2026-04-24) ──
    # Each worker does the expensive per-candidate work: is_claimed check,
    # candidate.info, verdict check, cache load, resume PDF extract, claim + marker.
    # Serial before: ~15 min for 464 candidates.
    # With 15 workers: ~1-2 min.
    #
    # Thread-safety:
    #   - _intake_state_lock protects newly_discovered_screened (set), counters,
    #     and the candidates result list.
    #   - Each worker only reads immutable closure vars (push_log_reverse, verdict_field_id,
    #     active_roles) — no shared writes to those.
    #   - File I/O (_load_cache, _save_cache, claim_for_screening, set_screening_marker)
    #     uses per-candidate files/paths so inter-candidate write conflicts are impossible.
    NON_TERMINAL_VERDICTS = {"SCREENING", "SCREENING_FAILED"}
    _intake_state_lock = threading.Lock()

    def _fetch_one_candidate(triple):
        """Fetch full metadata + resume for one candidate. Returns:
          - dict candidate_row on success
          - {'_skip': 'reason'} on skip (reason determines which counter bumps)
          - None on hard error (caller logs)
        """
        nonlocal_app, candidate_data, candidate_id = triple[0], triple[1], triple[2]
        app = nonlocal_app
        name_peek = candidate_data.get("name", "?")

        # Skip if another live run claimed this candidate
        if not dry_run and is_claimed(candidate_id):
            logger.info("  ⏭ %s claimed by another run, skipping", name_peek)
            return {"_skip": "claimed"}

        # Fetch full info
        if dry_run:
            candidate_full = candidate_data
        else:
            try:
                full_info = _ashby_post("candidate.info", {"id": candidate_id})
                candidate_full = full_info.get("results", {}) if full_info.get("success") else candidate_data
            except Exception:
                candidate_full = candidate_data

            # Terminal-verdict dedup
            if verdict_field_id and candidate_full.get("customFields"):
                for cf in candidate_full.get("customFields", []):
                    if cf.get("id") == verdict_field_id and cf.get("value"):
                        val_upper = str(cf["value"]).strip().upper()
                        if val_upper in NON_TERMINAL_VERDICTS:
                            break  # still needs screening
                        logger.info("  ⏭ %s already has AI Verdict '%s', skipping",
                                    name_peek, cf["value"])
                        return {"_skip": "already_screened",
                                "candidate_id": candidate_id}

        # Extract social links
        linkedin_url = ""
        github_url = ""
        other_links: List[str] = []
        social_links_raw = candidate_full.get("socialLinks", [])
        for link in social_links_raw:
            link_type = link.get("type", "").lower()
            link_url = link.get("url", "")
            if link_type == "linkedin" or "linkedin.com" in (link_url or "").lower():
                linkedin_url = link_url
            elif link_type == "github" or "github.com" in (link_url or "").lower():
                github_url = link_url
            elif link_url:
                other_links.append(link_url)

        if not linkedin_url:
            linkedin_url = (candidate_full.get("linkedInUrl") or "").strip()

        if not linkedin_url and candidate_id in push_log_reverse:
            linkedin_url = push_log_reverse[candidate_id]
            logger.info("  📎 Recovered LinkedIn for %s from push log: %s",
                        name_peek, linkedin_url)

        # Role / active-role filter
        job = app.get("job", {})
        job_title = job.get("title", "")
        role = _map_job_title_to_role(job_title)
        if role not in active_roles and role == job_title:
            logger.debug("  ⏭ %s — unrecognized role '%s', skipping", name_peek, job_title)
            return {"_skip": "inactive_role"}

        name = candidate_full.get("name", name_peek)
        position = candidate_full.get("position", "")
        company = candidate_full.get("company", "")

        source_info = candidate_full.get("source") or {}
        source_title = source_info.get("title", "")
        source_type_title = ((source_info.get("sourceType") or {}).get("title") or "").strip()
        is_outbound = (role == "OB") or is_outbound_source(source_title, source_type_title)
        effective_role = "" if is_outbound else role

        if is_outbound:
            source = f"Outbound — {source_title}" if source_title else "Outbound"
        else:
            source = f"Inbound — {source_title}" if source_title else "Inbound — Applied"
        source_notes = f"Ashby Inbound: {job_title}" if job_title and not is_outbound else ""

        # Agency + VD Lead exclusion (2026-04-28, fixed 2026-04-29): bypass AI
        # screening, route to AR. Ashby returns source.title as the bare name
        # (e.g. "Quantum") and the category as source.sourceType.title (e.g.
        # "Agencies"). The UI shows the two combined as "Agencies: Quantum",
        # but the API does NOT prefix the title — earlier startswith("agencies:")
        # check never matched any real candidate. Karan Rami slipped because of
        # this bug.
        source_type_title_lower = source_type_title.lower()
        is_agency_source = source_type_title_lower == "agencies"
        if is_agency_source and job_title in AGENCY_VD_JOB_TITLES:
            if not dry_run:
                app_id = app.get("id", "")
                current_stage_id = (app.get("currentInterviewStage") or {}).get("id", "")
                ar_stage_id = resolve_dest_stage_id(current_stage_id, "Application Review")
                if ar_stage_id and current_stage_id != ar_stage_id:
                    move_to_stage(app_id, ar_stage_id)
                add_note(candidate_id, (
                    "<b>Routed to HM Review — AI screening skipped</b><br>"
                    f"Agency-sourced ({source_title}) candidate referred for {job_title}. "
                    "Per policy, agency-referred VD Lead candidates (any level) bypass "
                    "AI screening and go directly to HM review."
                ))
                logger.info("  ⏭ %s — agency VD Lead, routed to AR (no screening)", name_peek)
            return {"_skip": "agency_vd_lead"}

        # Resume + CV URL recovery
        resume_handle = ""
        resume_text = ""
        rfh = candidate_full.get("resumeFileHandle")
        if rfh and isinstance(rfh, dict):
            resume_handle = rfh.get("handle", "")

        if not dry_run:
            cached = _load_cache(candidate_id)
            if cached.get("cv_text"):
                logger.info("  📦 Cache hit for %s (CV + URLs)", name)
                resume_text = cached["cv_text"]
                if not linkedin_url:
                    linkedin_url = cached.get("linkedin", "")
                if not github_url:
                    github_url = cached.get("github", "")
                other_links.extend(cached.get("other_urls", []))

                if not linkedin_url or not github_url:
                    rescanned_li, rescanned_gh = _rescan_cv_for_social(resume_text)
                    if not linkedin_url and rescanned_li:
                        linkedin_url = rescanned_li
                        logger.info("  📎 Recovered LinkedIn from cached CV rescan for %s: %s",
                                    name, linkedin_url)
                    if not github_url and rescanned_gh:
                        github_url = rescanned_gh
                    if (rescanned_li or rescanned_gh):
                        _save_cache(candidate_id, {
                            "cv_text": resume_text,
                            "linkedin": linkedin_url,
                            "github": github_url,
                            "other_urls": cached.get("other_urls", []),
                            "name": name,
                            "cached_at": time.strftime("%Y-%m-%d %H:%M"),
                        })
            elif resume_handle:
                extracted = _extract_resume(resume_handle, name)
                resume_text = extracted.get("text", "")
                if not linkedin_url:
                    linkedin_url = extracted.get("linkedin", "")
                if not github_url:
                    github_url = extracted.get("github", "")
                other_links.extend(extracted.get("other_urls", []))
                _save_cache(candidate_id, {
                    "cv_text": resume_text,
                    "linkedin": linkedin_url,
                    "github": github_url,
                    "other_urls": extracted.get("other_urls", []),
                    "name": name,
                    "cached_at": time.strftime("%Y-%m-%d %H:%M"),
                })

        if not linkedin_url:
            logger.warning("  ⚠ %s has no LinkedIn after all fallbacks "
                           "(socialLinks/push log/resume) — will try Linkup discovery",
                           name)

        header = f"{position} at {company}" if position and company else position or ""
        cv_parts = []
        if header:
            cv_parts.append(header)
        if resume_text:
            cv_parts.append(resume_text)
        # Gather everything else on the profile AT SCREEN TIME — non-resume
        # documents (transcripts, take-homes) and recruiter/intake notes — so the
        # screener sees the full context, not just the resume. Pulled fresh each
        # screen (not from the CV cache) so late-arriving docs/notes are picked up
        # on a rescreen. AI/HM notes are filtered out inside the helpers.
        if not dry_run:
            extra_docs = _extract_candidate_documents(candidate_full, name)
            if extra_docs:
                cv_parts.append(extra_docs)
            extra_notes = _gather_screening_notes(candidate_id, name)
            if extra_notes:
                cv_parts.append(extra_notes)
        if github_url:
            cv_parts.append(f"GitHub: {github_url}")
        if other_links:
            cv_parts.append("Other links: " + ", ".join(other_links))
        cv_notes = "\n\n".join(cv_parts) if cv_parts else ""
        if len(cv_notes) > 45000:
            cv_notes = cv_notes[:45000]

        candidate_row = {
            "name": name,
            "linkedin": linkedin_url,
            "linkedin_url": linkedin_url,
            "source": source,
            "source_raw": source_title,
            "source_notes": source_notes,
            "target_role": effective_role,
            "cv": cv_notes,
            "ashby_application_id": app.get("id", ""),
            "ashby_candidate_id": candidate_id,
            "job_title": job_title,
            "email": candidate_full.get("primaryEmailAddress", {}).get("value", ""),
            "resume_handle": resume_handle,
            "github": github_url,
        }

        if not dry_run:
            claim_for_screening(candidate_id)
            set_screening_marker(candidate_id)

        return candidate_row

    # Dispatch to worker pool
    from concurrent.futures import ThreadPoolExecutor, as_completed
    INTAKE_WORKERS = 15
    logger.info("Parallel intake: %d candidates across %d workers...",
                len(unique_apps), INTAKE_WORKERS)

    # Workers claim + set SCREENING marker on EVERY candidate they fetch — that
    # closes the race with any concurrent ascreen process. But when `--limit N`
    # is in play, only the first N actually get screened. We collect the rest
    # (pre-claimed but limit-rejected) and release their claims here so they
    # don't sit blocking new runs for 2h. The SCREENING marker on those Ashby
    # records is left as-is — non-terminal, so the next pull picks them up
    # normally.
    stragglers_to_release: List[str] = []

    with ThreadPoolExecutor(max_workers=INTAKE_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_candidate, t): t for t in unique_apps}
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as e:
                t = futures[fut]
                logger.error("  Intake worker crashed on %s: %s",
                             t[1].get("name", "?"), e)
                continue

            if not result:
                continue

            if "_skip" in result:
                with _intake_state_lock:
                    reason = result["_skip"]
                    if reason == "already_screened":
                        skipped_already_screened += 1
                        cid = result.get("candidate_id")
                        if cid:
                            newly_discovered_screened.add(cid)
                    elif reason == "inactive_role":
                        skipped_inactive_role += 1
                    elif reason == "agency_vd_lead":
                        skipped_agency_vd_lead += 1
                    # 'claimed' is logged but not counted
                continue

            # Successful candidate_row — append (honor limit)
            with _intake_state_lock:
                if limit and total >= limit:
                    # Limit hit — release the claim the worker just acquired
                    # so this candidate is available for the next run.
                    cid = result.get("ashby_candidate_id")
                    if cid and not dry_run:
                        stragglers_to_release.append(cid)
                    continue
                candidates.append(result)
                total += 1

    # Release the stragglers pre-claimed by workers but rejected by --limit.
    # Idempotent batch-release — single file rewrite for N candidates.
    if stragglers_to_release:
        released = release_claims(stragglers_to_release)
        if released:
            logger.info("Released %d straggler claim(s) (pre-claimed by intake workers, rejected by --limit=%d)",
                        released, limit or 0)

    # Save newly discovered screened IDs so future runs skip them locally
    if newly_discovered_screened:
        updated = known_screened | newly_discovered_screened
        _save_known_screened(updated)
        logger.info("Cached %d newly discovered screened candidate IDs", len(newly_discovered_screened))

    if skipped_already_screened:
        logger.info("Skipped %d candidates already screened in Ashby (verdict field)", skipped_already_screened)
    if skipped_inactive_role:
        logger.info("Skipped %d candidates for unrecognized roles", skipped_inactive_role)
    if skipped_agency_vd_lead:
        logger.info("Routed %d agency-sourced VD Lead candidates to Application Review (skipped AI screening)", skipped_agency_vd_lead)
    logger.info("Pulled %d candidates for screening", total)

    # Clean up rescreen pending: remove candidates that were successfully pulled
    if rescreen_ids and candidates and _rescreen_path.exists():
        pulled_ids = {c["ashby_candidate_id"] for c in candidates}
        remaining = rescreen_ids - pulled_ids
        if remaining:
            write_json_atomic(_rescreen_path, list(remaining), indent=2)
        else:
            _rescreen_path.unlink(missing_ok=True)
        logger.info("Rescreen pending: %d pulled, %d remaining", len(rescreen_ids & pulled_ids), len(remaining))

    return candidates


# ── Drain Application Review (forward-only sweep) ────────────────

def drain_application_review(dry_run: bool = False) -> Dict[str, int]:
    """Sweep Application Review: move any candidate with a TERMINAL verdict
    already set in Ashby forward to their correct final stage. This covers
    candidates who were screened in a prior run but whose stage-move failed
    (verdict written, candidate stranded in AR).

    Human-move safeguard: if the candidate was moved INTO Application Review
    by a human (actorId != automation), leave them alone. Humans override.

    Forward-only: candidates with non-terminal verdicts (SCREENING /
    SCREENING_FAILED / empty) stay put and get picked up by the pull.

    Returns dict: {scanned, moved, stuck_unscreened, skipped_human, errors}
    """
    # Multi-plan: match Application Review across all non-excluded plans.
    ar_ids = get_stage_ids_by_title("Application Review")
    if not ar_ids:
        logger.error("No Application Review stages found")
        return {"scanned": 0, "moved": 0, "stuck_unscreened": 0, "skipped_human": 0, "errors": 0}

    cf_file = _DIR / ".ashby_custom_fields.json"
    verdict_field_id = None
    rejection_field_id = None
    confidence_field_id = None
    if cf_file.exists():
        cfs = json.load(open(cf_file))
        verdict_field_id = cfs.get("AI Verdict", {}).get("id")
        rejection_field_id = cfs.get("Rejection Type", {}).get("id")
        confidence_field_id = cfs.get("AI Confidence", {}).get("id")

    # Terminal API verdict codes — safe to route forward
    TERMINAL_VERDICTS = {
        "SCREEN", "DECLINE", "DEFER", "INSUFFICIENT_DATA",
    }
    AUTOMATION_ACTOR_ID = "REPLACE_WITH_YOUR_AUTOMATION_ACTOR_ID"
    API_TO_DISPLAY = {v: k for k, v in VERDICT_TO_API.items()}

    counts = {"scanned": 0, "moved": 0, "stuck_unscreened": 0, "skipped_human": 0,
              "skipped_inactive": 0, "errors": 0}

    logger.info("Draining Application Review stages (across %d plans)...", len(ar_ids))

    # Collect all AR apps (Active + Lead statuses) across all plans
    ar_apps = []
    for status in ("Active", "Lead"):
        cursor = None
        while True:
            payload: Dict[str, Any] = {"limit": 100, "status": status}
            if cursor:
                payload["cursor"] = cursor
            res = _ashby_post("application.list", payload)
            apps = res.get("results", []) or []
            if not apps:
                break
            for app in apps:
                sid = app.get("currentInterviewStage", {}).get("id", "")
                if sid in ar_ids:
                    ar_apps.append(app)
            if not res.get("moreDataAvailable"):
                break
            cursor = res.get("nextCursor")

    logger.info("  %d candidates currently in Application Review (across all plans)", len(ar_apps))

    # Inactive-role guard: drop apps whose job maps to a closed/inactive role
    # (e.g. Solution Consultant) BEFORE routing. Inactive roles are out of scope
    # for this pipeline — their legacy plans may lack the destination stage the
    # candidate-level verdict routes to (Solution Consultant's plan has no
    # "Inbound App Review"), which otherwise surfaces as a spurious
    # "no valid destination" error on every drain run. The candidate's ACTIVE
    # apps (e.g. AI Value Delivery) are handled normally. Mirrors the same gate
    # the screening pull applies (`_app_has_active_role`).
    _active_roles = set(ROLE_SHORT_TO_INTERNAL.keys())
    pre_inactive = len(ar_apps)
    ar_apps = [a for a in ar_apps
               if _map_job_title_to_role((a.get("job") or {}).get("title", "")) in _active_roles]
    counts["skipped_inactive"] = pre_inactive - len(ar_apps)
    if counts["skipped_inactive"]:
        logger.info("  Skipped %d inactive-role apps (out of scope, e.g. Solution Consultant)",
                    counts["skipped_inactive"])

    # Parallel prefetch: for each app do candidate.info + application.listHistory
    # up-front in a thread pool so the per-candidate decision loop becomes CPU-bound.
    # Prior shape was sequential (~800 API calls × ~0.5s = 6+ min) and caused the
    # live run to stall for 8+ min before screening even started.
    from concurrent.futures import ThreadPoolExecutor

    def _prefetch(app):
        cand = app.get("candidate", {}) or {}
        cid = cand.get("id", "")
        app_id = app.get("id", "")
        verdict_value = ""
        rejection_type = ""
        confidence_value = None
        actor = None
        err = None
        try:
            info = _ashby_post("candidate.info", {"id": cid})
            for cf in info.get("results", {}).get("customFields", []) or []:
                fid = cf.get("id")
                if fid == verdict_field_id:
                    verdict_value = (cf.get("value") or "").strip().upper()
                elif fid == rejection_field_id:
                    rejection_type = (cf.get("value") or "").strip().lower()
                elif fid == confidence_field_id:
                    confidence_value = cf.get("value")
        except Exception as e:
            err = f"info: {e}"
        # Only need history if verdict is terminal (else we skip this app anyway)
        if verdict_value in TERMINAL_VERDICTS:
            try:
                hist = _ashby_post("application.listHistory", {"applicationId": app_id})
                entries = hist.get("results", []) or []
                current = [e for e in entries
                           if e.get("stageId") in ar_ids and not e.get("leftStageAt")]
                actor = current[0].get("actorId") if current else None
            except Exception as e:
                err = (err + "; " if err else "") + f"history: {e}"
        return {
            "app": app,
            "verdict": verdict_value,
            "rejection": rejection_type,
            "confidence": confidence_value,
            "actor": actor,
            "err": err,
        }

    prefetched = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        for result in pool.map(_prefetch, ar_apps):
            prefetched.append(result)

    for pf in prefetched:
        app = pf["app"]
        counts["scanned"] += 1
        candidate = app.get("candidate", {}) or {}
        name = candidate.get("name", "?")
        app_id = app.get("id", "")

        if pf["err"] and not pf["verdict"]:
            logger.warning("  error fetching %s: %s", name, pf["err"])
            counts["errors"] += 1
            continue

        verdict_value = pf["verdict"]
        rejection_type = pf["rejection"]
        confidence_value = pf["confidence"]

        # Non-terminal → will be picked up by the pull, leave alone
        if verdict_value not in TERMINAL_VERDICTS:
            counts["stuck_unscreened"] += 1
            continue

        # Human-move safeguard: skip if a human placed them here.
        if pf["actor"] and pf["actor"] != AUTOMATION_ACTOR_ID:
            logger.info("  SKIP %s — human moved into AR, leaving alone", name)
            counts["skipped_human"] += 1
            continue

        current_sid = app.get("currentInterviewStage", {}).get("id", "")

        # Source lives on the application (for Inbound App Review routing)
        app_source = _prefixed_source(app.get("source"))

        # Map API verdict → display form → destination stage title, then
        # resolve the concrete stage ID within THIS application's plan.
        display_verdict = API_TO_DISPLAY.get(verdict_value, verdict_value)
        dest_stage_name = get_verdict_stage(display_verdict, rejection_type,
                                            confidence_score=confidence_value,
                                            source=app_source,
                                            current_stage_id=current_sid)
        dest_stage_id = resolve_dest_stage_id(current_sid, dest_stage_name)

        # SCREEN verdicts whose destination IS Application Review are already
        # in the right place — silent no-op, not a warning or error.
        if dest_stage_id and dest_stage_id in ar_ids:
            counts["already_in_dest"] = counts.get("already_in_dest", 0) + 1
            continue

        if not dest_stage_id:
            logger.warning("  %s: no valid destination for verdict '%s' (dest=%s)",
                           name, verdict_value, dest_stage_name)
            counts["errors"] += 1
            continue

        if dry_run:
            logger.info("  DRY RUN: %s → %s (verdict: %s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
            continue

        is_archive = (dest_stage_name == "Archived")
        if move_to_stage(app_id, dest_stage_id, is_archive=is_archive):
            logger.info("  MOVED: %s → %s (%s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
        else:
            logger.error("  FAILED to move %s → %s", name, dest_stage_name)
            counts["errors"] += 1

        time.sleep(0.1)

    logger.info("AR drain complete: %s", counts)
    return counts


# ── Drain Lead sub-stages (forward-only sweep) ───────────────────

def drain_lead_stages(dry_run: bool = False) -> Dict[str, int]:
    """Sweep Lead intake sub-stages: move any candidate with a TERMINAL
    verdict already set in Ashby forward to their correct final stage.

    Covers re-applicants whose old verdict (e.g. DECLINE from a prior
    application) is attached at the candidate level — the intake loop
    correctly skips them (dedup), but they'd otherwise sit in a Lead
    sub-stage indefinitely. This drain routes them out.

    Scans: New Lead, Needs Rescreen. Inbound App Review is NOT scanned — it's
    a human-drained bucket, not an intake stage.

    Human-move safeguard: if a human placed them here (actorId != automation),
    leave them alone.

    Forward-only: candidates with non-terminal verdicts stay put; the pull
    will pick them up.

    Returns dict: {scanned, moved, stuck_unscreened, skipped_human, errors}
    """
    lead_stage_titles = ["New Lead", "Needs Rescreen"]
    lead_ids: Set[str] = set()
    for title in lead_stage_titles:
        lead_ids.update(get_stage_ids_by_title(title))
    if not lead_ids:
        logger.error("No Lead intake sub-stages found")
        return {"scanned": 0, "moved": 0, "stuck_unscreened": 0, "skipped_human": 0, "errors": 0}

    cf_file = _DIR / ".ashby_custom_fields.json"
    verdict_field_id = None
    rejection_field_id = None
    confidence_field_id = None
    if cf_file.exists():
        cfs = json.load(open(cf_file))
        verdict_field_id = cfs.get("AI Verdict", {}).get("id")
        rejection_field_id = cfs.get("Rejection Type", {}).get("id")
        confidence_field_id = cfs.get("AI Confidence", {}).get("id")

    TERMINAL_VERDICTS = {
        "SCREEN", "DECLINE", "DEFER", "INSUFFICIENT_DATA",
    }
    AUTOMATION_ACTOR_ID = "REPLACE_WITH_YOUR_AUTOMATION_ACTOR_ID"
    API_TO_DISPLAY = {v: k for k, v in VERDICT_TO_API.items()}

    counts = {"scanned": 0, "moved": 0, "stuck_unscreened": 0, "skipped_human": 0, "errors": 0}

    logger.info("Draining Lead intake sub-stages (across %d stage ids)...", len(lead_ids))

    # Collect all Lead apps (status=Lead) across all plans, filtered to intake stages.
    lead_apps = []
    cursor = None
    while True:
        payload: Dict[str, Any] = {"limit": 100, "status": "Lead"}
        if cursor:
            payload["cursor"] = cursor
        res = _ashby_post("application.list", payload)
        apps = res.get("results", []) or []
        if not apps:
            break
        for app in apps:
            sid = app.get("currentInterviewStage", {}).get("id", "")
            if sid in lead_ids:
                lead_apps.append(app)
        if not res.get("moreDataAvailable"):
            break
        cursor = res.get("nextCursor")

    logger.info("  %d candidates currently in Lead intake sub-stages (across all plans)", len(lead_apps))

    # Parallel prefetch: candidate.info + listHistory per app up-front.
    from concurrent.futures import ThreadPoolExecutor

    def _prefetch_lead(app):
        cand = app.get("candidate", {}) or {}
        cid = cand.get("id", "")
        app_id = app.get("id", "")
        verdict_value = ""
        rejection_type = ""
        confidence_value = None
        actor = None
        err = None
        try:
            info = _ashby_post("candidate.info", {"id": cid})
            for cf in info.get("results", {}).get("customFields", []) or []:
                fid = cf.get("id")
                if fid == verdict_field_id:
                    verdict_value = (cf.get("value") or "").strip().upper()
                elif fid == rejection_field_id:
                    rejection_type = (cf.get("value") or "").strip().lower()
                elif fid == confidence_field_id:
                    confidence_value = cf.get("value")
        except Exception as e:
            err = f"info: {e}"
        if verdict_value in TERMINAL_VERDICTS:
            try:
                hist = _ashby_post("application.listHistory", {"applicationId": app_id})
                entries = hist.get("results", []) or []
                current = [e for e in entries
                           if e.get("stageId") in lead_ids and not e.get("leftStageAt")]
                actor = current[0].get("actorId") if current else None
            except Exception as e:
                err = (err + "; " if err else "") + f"history: {e}"
        return {
            "app": app, "verdict": verdict_value, "rejection": rejection_type,
            "confidence": confidence_value, "actor": actor, "err": err,
        }

    prefetched = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        for result in pool.map(_prefetch_lead, lead_apps):
            prefetched.append(result)

    for pf in prefetched:
        app = pf["app"]
        counts["scanned"] += 1
        candidate = app.get("candidate", {}) or {}
        name = candidate.get("name", "?")
        app_id = app.get("id", "")

        if pf["err"] and not pf["verdict"]:
            logger.warning("  error fetching %s: %s", name, pf["err"])
            counts["errors"] += 1
            continue

        verdict_value = pf["verdict"]
        rejection_type = pf["rejection"]
        confidence_value = pf["confidence"]

        # Non-terminal → will be picked up by the pull, leave alone
        if verdict_value not in TERMINAL_VERDICTS:
            counts["stuck_unscreened"] += 1
            continue

        # Human-move safeguard: respect deliberate human placements.
        if pf["actor"] and pf["actor"] != AUTOMATION_ACTOR_ID:
            logger.info("  SKIP %s — human moved into Lead, leaving alone", name)
            counts["skipped_human"] += 1
            continue

        current_sid = app.get("currentInterviewStage", {}).get("id", "")
        app_source = _prefixed_source(app.get("source"))

        display_verdict = API_TO_DISPLAY.get(verdict_value, verdict_value)
        dest_stage_name = get_verdict_stage(display_verdict, rejection_type,
                                            confidence_score=confidence_value,
                                            source=app_source,
                                            current_stage_id=current_sid)
        dest_stage_id = resolve_dest_stage_id(current_sid, dest_stage_name)
        if not dest_stage_id or dest_stage_id == current_sid:
            logger.warning("  %s: no valid destination for verdict '%s' (dest=%s)",
                           name, verdict_value, dest_stage_name)
            counts["errors"] += 1
            continue

        if dry_run:
            logger.info("  DRY RUN: %s → %s (verdict: %s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
            continue

        is_archive = (dest_stage_name == "Archived")
        if move_to_stage(app_id, dest_stage_id, is_archive=is_archive):
            logger.info("  MOVED: %s → %s (%s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
        else:
            logger.error("  FAILED to move %s → %s", name, dest_stage_name)
            counts["errors"] += 1

        time.sleep(0.1)

    logger.info("Lead drain complete: %s", counts)
    return counts


def drain_unconfigured_referrals(dry_run: bool = False) -> Dict[str, int]:
    """Move REFERRAL candidates on an OPEN but non-screening-configured role
    (e.g. Solution Consultant) into the dedicated 'Referrals Review' stage,
    with NO AI verdict written. Pure routing.

    Context (2026-05-29, per the recruiting lead): some roles are open in Ashby but the Opus
    screener isn't configured for them (Solution Consultant, Alliances Director,
    Product Marketing Manager, etc.). Referrals for these roles are pushed in
    but never screened, so they sit unseen in New Lead / Application Review. The
    HM wants every referral surfaced in Referrals Review for manual triage.

    Scope (all conditions must hold to move):
      - REFERRAL source — is_referral_source on the application's source.
      - Role NOT in the active screening set — configured roles are screened
        normally and routed by get_verdict_stage; this drain never touches them.
      - The application's OWN interview plan has a 'Referrals Review' stage.
        We look it up in that plan only — never the cross-plan fallback — so the
        'Other referrals' catch-all (Outbound plan, holds CLOSED roles, has no
        Referrals Review) is left exactly where it is.
      - Not already in Referrals Review.
      - Current stage was NOT set by a human (human-move safeguard).

    Scans: New Lead, Needs Rescreen (status=Lead) + Application Review
    (status=Active). Covers both new arrivals and existing stuck referrals.

    Returns dict: {scanned, moved, skipped_not_referral, skipped_active_role,
                   skipped_no_rr_stage, skipped_human, errors}
    """
    counts = {"scanned": 0, "moved": 0, "skipped_not_referral": 0,
              "skipped_active_role": 0, "skipped_no_rr_stage": 0,
              "skipped_human": 0, "errors": 0}

    lead_ids = get_stage_ids_by_title("New Lead") | get_stage_ids_by_title("Needs Rescreen")
    ar_ids = get_stage_ids_by_title("Application Review")
    intake_ids = lead_ids | ar_ids
    if not intake_ids:
        logger.error("drain_unconfigured_referrals: no intake stage ids found")
        return counts

    active_roles = set(ROLE_SHORT_TO_INTERNAL.keys())
    plan_stages = load_stages_multi()["plan_stages"]
    AUTOMATION_ACTOR_ID = "REPLACE_WITH_YOUR_AUTOMATION_ACTOR_ID"

    # Collect intake apps across both statuses (Lead sub-stages + AR).
    intake_apps = []
    for status, want_ids in (("Lead", lead_ids), ("Active", ar_ids)):
        if not want_ids:
            continue
        cursor = None
        while True:
            payload: Dict[str, Any] = {"limit": 100, "status": status}
            if cursor:
                payload["cursor"] = cursor
            res = _ashby_post("application.list", payload)
            apps = res.get("results", []) or []
            for app in apps:
                sid = app.get("currentInterviewStage", {}).get("id", "")
                if sid in want_ids:
                    intake_apps.append(app)
            if not res.get("moreDataAvailable"):
                break
            cursor = res.get("nextCursor")

    logger.info("drain_unconfigured_referrals: scanning %d intake apps", len(intake_apps))

    # First pass: cheap filters (no extra API calls). Build the move list.
    to_move = []  # (app, name, current_sid, rr_sid)
    for app in intake_apps:
        counts["scanned"] += 1
        name = (app.get("candidate", {}) or {}).get("name", "?")
        app_source = _prefixed_source(app.get("source"))
        if not is_referral_source(app_source):
            counts["skipped_not_referral"] += 1
            continue
        job_title = (app.get("job") or {}).get("title", "")
        if _map_job_title_to_role(job_title) in active_roles:
            # Configured role — leave it for the normal screening pull.
            counts["skipped_active_role"] += 1
            continue
        cur = app.get("currentInterviewStage", {}) or {}
        current_sid = cur.get("id", "")
        plan_id = cur.get("interviewPlanId", "")
        rr_sid = plan_stages.get(plan_id, {}).get("Referrals Review")
        if not rr_sid:
            # App's own plan has no Referrals Review (e.g. 'Other referrals' on
            # the Outbound plan — closed roles). Leave it alone; never move
            # across plans into a foreign stage id.
            counts["skipped_no_rr_stage"] += 1
            continue
        if current_sid == rr_sid:
            continue  # already there
        to_move.append((app, name, current_sid, rr_sid))

    # Second pass: human-move safeguard (one listHistory call per candidate,
    # parallelized) — only for the small set that passed the cheap filters.
    from concurrent.futures import ThreadPoolExecutor

    def _actor_for(app):
        app_id = app.get("id", "")
        try:
            hist = _ashby_post("application.listHistory", {"applicationId": app_id})
            entries = hist.get("results", []) or []
            current = [e for e in entries
                       if e.get("stageId") == app.get("currentInterviewStage", {}).get("id")
                       and not e.get("leftStageAt")]
            return current[0].get("actorId") if current else None
        except Exception as e:
            return f"__ERR__{e}"

    actors = {}
    if to_move:
        with ThreadPoolExecutor(max_workers=15) as pool:
            for (app, _, _, _), actor in zip(to_move, pool.map(lambda t: _actor_for(t[0]), to_move)):
                actors[app.get("id", "")] = actor

    for app, name, current_sid, rr_sid in to_move:
        app_id = app.get("id", "")
        actor = actors.get(app_id)
        if isinstance(actor, str) and actor.startswith("__ERR__"):
            logger.warning("  error fetching history for %s: %s", name, actor[7:])
            counts["errors"] += 1
            continue
        if actor and actor != AUTOMATION_ACTOR_ID:
            logger.info("  SKIP %s — human placed in current stage, leaving alone", name)
            counts["skipped_human"] += 1
            continue

        if dry_run:
            logger.info("  DRY RUN: %s → Referrals Review (unconfigured-role referral)", name)
            counts["moved"] += 1
            continue

        if move_to_stage(app_id, rr_sid):
            logger.info("  MOVED: %s → Referrals Review (unconfigured-role referral)", name)
            counts["moved"] += 1
        else:
            logger.error("  FAILED to move %s → Referrals Review", name)
            counts["errors"] += 1
        time.sleep(0.1)

    logger.info("Unconfigured-referral drain complete: %s", counts)
    return counts


def _queue_os_rescreen(candidate_id: str, app_id: str, verdict_field_id: str,
                       current_stage_id: str, name: str, dry_run: bool) -> None:
    """Queue an Outbound Screened candidate for re-screening.

    Used when a candidate sits in Outbound Screened without an Outreach Email
    populated (the stage-automation merge tag). Clears the candidate-level
    AI Verdict, adds the candidate id to .rescreen_pending.json, and moves the
    app to Needs Rescreen so the next `ascreen` run picks them up.
    """
    if dry_run:
        logger.info("  DRY RUN: would rescreen %s (no Outreach Email)", name)
        return

    # 1. Clear AI Verdict so pull_for_screening sees them.
    cleared = False
    for clear_val in [None, ""]:
        try:
            resp = _ashby_post("customField.setValue", {
                "objectId": candidate_id,
                "objectType": "Candidate",
                "fieldId": verdict_field_id,
                "fieldValue": clear_val,
            })
            if resp.get("success"):
                cleared = True
                break
        except Exception as e:
            logger.warning("  clear-verdict error for %s: %s", name, e)
            break
    if not cleared:
        logger.warning("  %s: failed to clear AI Verdict; skipping rescreen queue", name)
        return

    # 2. Add to .rescreen_pending.json so pull bypasses local skip-sets.
    pending_path = _DIR / ".rescreen_pending.json"
    pending: Set[str] = set()
    if pending_path.exists():
        try:
            pending = set(json.loads(pending_path.read_text(encoding="utf-8")))
        except Exception:
            pending = set()
    pending.add(candidate_id)
    write_json_atomic(pending_path, sorted(pending), indent=2)

    # 3. Prune known-screened cache so this id is not filtered out.
    known_path = _DIR / ".known_screened_ids.json"
    if known_path.exists():
        try:
            known = set(json.loads(known_path.read_text(encoding="utf-8")))
            if candidate_id in known:
                known.discard(candidate_id)
                write_json_atomic(known_path, sorted(known), indent=2)
        except Exception:
            pass

    # 4. Move app to Needs Rescreen (plan-aware).
    dest_id = resolve_dest_stage_id(current_stage_id, "Needs Rescreen")
    if not dest_id:
        logger.warning("  %s: could not resolve Needs Rescreen for current plan", name)
        return
    if move_to_stage(app_id, dest_id):
        logger.info("  QUEUED for rescreen: %s (no Outreach Email) → Needs Rescreen", name)
    else:
        logger.error("  %s: failed to move to Needs Rescreen", name)


def drain_outbound_screened(dry_run: bool = False) -> Dict[str, int]:
    """Sweep Outbound Screened to keep it in a clean, ready-to-outreach state.

    Three branches:
      1. Verdict-flip → Archived. Catches stranded candidates whose AI Verdict
         flipped to DECLINE / DEFER / INSUFFICIENT_DATA / DUPLICATE after a
         re-screen but the stage move didn't land. Human-move safeguard
         applies — humans who deliberately placed a candidate here are
         respected.
      2. SCREEN but no Outreach Email → Needs Rescreen. The stage-automation
         email merges from the Outreach Email field; if it's empty the
         candidate would receive an empty outreach (or none at all). Clears
         AI Verdict + queues the candidate id + moves the app to Needs
         Rescreen so the next `ascreen` run rebuilds the data. This branch
         does NOT honor the human-move safeguard, because filling missing
         outreach data completes the human's intent rather than reversing it.
      3. No verdict yet + no Outreach Email → same rescreen path as (2).

    Returns dict: {scanned, moved, kept_screen, stuck_unscreened,
                   rescreen_queued, skipped_human, errors}
    """
    os_ids: Set[str] = set(get_stage_ids_by_title("Outbound Screened"))
    if not os_ids:
        logger.error("No Outbound Screened stages found")
        return {"scanned": 0, "moved": 0, "kept_screen": 0,
                "stuck_unscreened": 0, "skipped_human": 0, "errors": 0}

    cf_file = _DIR / ".ashby_custom_fields.json"
    verdict_field_id = None
    rejection_field_id = None
    confidence_field_id = None
    outreach_field_id = None
    if cf_file.exists():
        cfs = json.load(open(cf_file))
        verdict_field_id = cfs.get("AI Verdict", {}).get("id")
        rejection_field_id = cfs.get("Rejection Type", {}).get("id")
        confidence_field_id = cfs.get("AI Confidence", {}).get("id")
        outreach_field_id = cfs.get("Outreach Email", {}).get("id")

    # Verdicts that should NOT remain in Outbound Screened.
    TERMINAL_NON_SCREEN = {"DECLINE", "DEFER", "INSUFFICIENT_DATA", "DUPLICATE"}
    AUTOMATION_ACTOR_ID = "REPLACE_WITH_YOUR_AUTOMATION_ACTOR_ID"
    API_TO_DISPLAY = {v: k for k, v in VERDICT_TO_API.items()}

    counts = {"scanned": 0, "moved": 0, "kept_screen": 0,
              "stuck_unscreened": 0, "skipped_human": 0,
              "rescreen_queued": 0, "errors": 0}

    logger.info("Draining Outbound Screened (across %d stage ids)...", len(os_ids))

    # Collect apps currently in Outbound Screened across plans.
    # Scan BOTH Lead and Active — Outbound Screened is a Lead sub-stage, so a
    # status=Active-only scan would miss every candidate sitting there.
    os_apps = []
    for status in ("Lead", "Active"):
        cursor = None
        while True:
            payload: Dict[str, Any] = {"limit": 100, "status": status}
            if cursor:
                payload["cursor"] = cursor
            res = _ashby_post("application.list", payload)
            apps = res.get("results", []) or []
            if not apps:
                break
            for app in apps:
                sid = app.get("currentInterviewStage", {}).get("id", "")
                if sid in os_ids:
                    os_apps.append(app)
            if not res.get("moreDataAvailable"):
                break
            cursor = res.get("nextCursor")

    logger.info("  %d candidates currently in Outbound Screened (across all plans)", len(os_apps))

    from concurrent.futures import ThreadPoolExecutor

    def _prefetch_os(app):
        cand = app.get("candidate", {}) or {}
        cid = cand.get("id", "")
        app_id = app.get("id", "")
        verdict_value = ""
        rejection_type = ""
        confidence_value = None
        outreach_value = ""
        actor = None
        err = None
        try:
            info = _ashby_post("candidate.info", {"id": cid})
            for cf in info.get("results", {}).get("customFields", []) or []:
                fid = cf.get("id")
                if fid == verdict_field_id:
                    verdict_value = (cf.get("value") or "").strip().upper()
                elif fid == rejection_field_id:
                    rejection_type = (cf.get("value") or "").strip().lower()
                elif fid == confidence_field_id:
                    confidence_value = cf.get("value")
                elif fid == outreach_field_id:
                    outreach_value = (cf.get("value") or "").strip() if isinstance(cf.get("value"), str) else ""
        except Exception as e:
            err = f"info: {e}"
        # Fetch history when we might mutate the app — non-SCREEN terminal moves
        # OR SCREEN-without-outreach rescreens (to honor human-move safeguard).
        needs_history = (
            verdict_value in TERMINAL_NON_SCREEN
            or (verdict_value == "SCREEN" and not outreach_value)
        )
        if needs_history:
            try:
                hist = _ashby_post("application.listHistory", {"applicationId": app_id})
                entries = hist.get("results", []) or []
                current = [e for e in entries
                           if e.get("stageId") in os_ids and not e.get("leftStageAt")]
                actor = current[0].get("actorId") if current else None
            except Exception as e:
                err = (err + "; " if err else "") + f"history: {e}"
        return {
            "app": app, "verdict": verdict_value, "rejection": rejection_type,
            "confidence": confidence_value, "outreach": outreach_value,
            "actor": actor, "err": err,
        }

    prefetched = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        for result in pool.map(_prefetch_os, os_apps):
            prefetched.append(result)

    for pf in prefetched:
        app = pf["app"]
        counts["scanned"] += 1
        candidate = app.get("candidate", {}) or {}
        name = candidate.get("name", "?")
        app_id = app.get("id", "")

        if pf["err"] and not pf["verdict"]:
            logger.warning("  error fetching %s: %s", name, pf["err"])
            counts["errors"] += 1
            continue

        verdict_value = pf["verdict"]
        rejection_type = pf["rejection"]
        confidence_value = pf["confidence"]
        current_sid = app.get("currentInterviewStage", {}).get("id", "")

        # SCREEN verdicts belong in Outbound Screened — but only if they have
        # an Outreach Email populated. Stage automation auto-sends from that
        # field, so an empty one means no outreach goes out. Queue for rescreen
        # regardless of who placed the candidate: this completes the intent
        # (candidate in OS with outreach ready), it doesn't reverse it.
        if verdict_value == "SCREEN":
            if outreach_field_id and not pf["outreach"]:
                _queue_os_rescreen(candidate.get("id", ""), app_id,
                                   verdict_field_id, current_sid, name, dry_run)
                counts["rescreen_queued"] += 1
                continue
            counts["kept_screen"] += 1
            continue

        # No verdict yet (rare in OS) — same logic: if no Outreach Email,
        # rescreen so the field gets populated.
        if verdict_value not in TERMINAL_NON_SCREEN:
            if outreach_field_id and not pf["outreach"]:
                _queue_os_rescreen(candidate.get("id", ""), app_id,
                                   verdict_field_id, current_sid, name, dry_run)
                counts["rescreen_queued"] += 1
                continue
            counts["stuck_unscreened"] += 1
            continue

        # Human-move safeguard
        if pf["actor"] and pf["actor"] != AUTOMATION_ACTOR_ID:
            logger.info("  SKIP %s — human placed into Outbound Screened, leaving alone", name)
            counts["skipped_human"] += 1
            continue

        app_source = _prefixed_source(app.get("source"))

        display_verdict = API_TO_DISPLAY.get(verdict_value, verdict_value)
        dest_stage_name = get_verdict_stage(display_verdict, rejection_type,
                                            confidence_score=confidence_value,
                                            source=app_source,
                                            current_stage_id=current_sid)
        dest_stage_id = resolve_dest_stage_id(current_sid, dest_stage_name)
        if not dest_stage_id or dest_stage_id == current_sid:
            logger.warning("  %s: no valid destination for verdict '%s' (dest=%s)",
                           name, verdict_value, dest_stage_name)
            counts["errors"] += 1
            continue

        if dry_run:
            logger.info("  DRY RUN: %s → %s (verdict: %s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
            continue

        is_archive = (dest_stage_name == "Archived")
        if move_to_stage(app_id, dest_stage_id, is_archive=is_archive):
            logger.info("  MOVED: %s → %s (%s)", name, dest_stage_name, verdict_value)
            counts["moved"] += 1
        else:
            logger.error("  FAILED to move %s → %s", name, dest_stage_name)
            counts["errors"] += 1

        time.sleep(0.1)

    logger.info("Outbound Screened drain complete: %s", counts)
    return counts


# ── Main push logic ──────────────────────────────────────────────

def push_candidate_to_ashby(
    name: str,
    linkedin_url: str,
    role_name: str,
    screening_data: dict,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Push a single screened candidate to Ashby.
    Returns Ashby candidate ID on success, None on failure.
    """
    # Dedup check 1: local tracker
    if linkedin_url and is_already_pushed(linkedin_url):
        logger.info("  SKIP (already in local tracker): %s", name)
        return None

    # Dedup check 2: Ashby ID from CSV
    ashby_id_from_sheet = screening_data.get("ashby_id", "").strip()
    if ashby_id_from_sheet:
        logger.info("  SKIP (Ashby ID already exists): %s → %s", name, ashby_id_from_sheet[:12])
        if linkedin_url:
            mark_pushed(linkedin_url, ashby_id_from_sheet)
        return None

    # Find job
    job_id = get_job_id_for_role(role_name)
    if not job_id:
        logger.warning("  SKIP (no Ashby job for role '%s'): %s", role_name, name)
        return None

    # Dedup check 3: search Ashby by name (if API allows it)
    try:
        existing = search_candidate(name=name)
        if existing:
            logger.info("  SKIP (already in Ashby): %s → %s", name, existing.get("id", "")[:12])
            if linkedin_url:
                mark_pushed(linkedin_url, existing.get("id", ""))
            return None
    except Exception:
        pass  # API key may not have search permission — skip this check

    if dry_run:
        logger.info("  DRY RUN: Would push %s → %s", name, role_name)
        return "dry-run"

    # Create candidate
    candidate = create_candidate(
        name=name,
        linkedin_url=linkedin_url,
    )

    if not candidate:
        logger.error("  FAILED to create candidate: %s", name)
        return None

    candidate_id = candidate.get("id", "")
    logger.info("  Created candidate: %s → %s", name, candidate_id[:12])

    # Create application — map sheet source to Ashby's exact source names
    source = _map_source_to_ashby(screening_data.get("source", "").strip())
    app = create_application(candidate_id, job_id, source=source)
    logger.info("  Source: %s", source)
    app_id = None
    if app:
        app_id = app.get("id", "")
        logger.info("  Created application for %s on %s", name, role_name)
    else:
        logger.warning("  Failed to create application for %s", name)

    # Move to correct pipeline stage based on Actioned column
    actioned = screening_data.get("actioned", "").strip()
    if app_id and actioned:
        stages = get_interview_stages(job_id)
        stage_id = find_matching_stage(stages, actioned)
        if stage_id:
            if move_to_stage(app_id, stage_id):
                stage_name = next((s["title"] for s in stages if s["id"] == stage_id), "?")
                logger.info("  Moved to stage: %s (from Actioned: %s)", stage_name, actioned)
            else:
                logger.warning("  Failed to move to stage for Actioned: %s", actioned)
        else:
            logger.info("  No matching Ashby stage for Actioned: %s (staying in default stage)", actioned)

    # Add screening note
    note = format_screening_note(screening_data)
    if add_note(candidate_id, note):
        logger.info("  Added screening note for %s", name)

    # Track
    if linkedin_url:
        mark_pushed(linkedin_url, candidate_id)

    # Log
    log_push({
        "name": name,
        "candidate_id": candidate_id,
        "role": role_name,
        "linkedin": linkedin_url,
    })

    # Small delay to avoid rate limits
    time.sleep(0.3)

    return candidate_id


# ── CLI ──────────────────────────────────────────────────────────

def _cli_list_jobs():
    """List all Ashby jobs and current role mapping."""
    jobs = get_open_jobs()
    mapping = load_role_map()
    reverse = {v: k for k, v in mapping.items()}

    print(f"\n  Ashby Open Jobs ({len(jobs)}):\n")
    for j in jobs:
        mapped = reverse.get(j["id"], "")
        tag = f"  ← {mapped}" if mapped else ""
        print(f"    {j['title']:<45} {j['id'][:12]}...{tag}")

    unmapped = [r for r in ROLE_SHORT_TO_INTERNAL.values() if r not in mapping]
    if unmapped:
        print(f"\n  Unmapped roles: {', '.join(unmapped)}")


def _read_screen_tab_full(csv_path: str = "") -> List[Dict[str, str]]:
    """Read Screen tab from a local CSV file.

    Auto-detects the most recent Screen CSV in ~/Downloads/ if no path given.
    Falls back to Google Sheets API if no CSV found (requires OAuth).
    """
    import csv as csv_mod
    import glob

    # Find CSV file
    if not csv_path:
        downloads = os.path.expanduser("~/Downloads")
        candidates_csvs = sorted(
            glob.glob(os.path.join(downloads, "*Screen*Review*.csv")) +
            glob.glob(os.path.join(downloads, "*Screen*.csv")) +
            glob.glob(os.path.join(downloads, "*screen*.csv")),
            key=os.path.getmtime,
            reverse=True,
        )
        if candidates_csvs:
            csv_path = candidates_csvs[0]
            logger.info("  Using CSV: %s", os.path.basename(csv_path))

    if csv_path and os.path.exists(csv_path):
        # Read from CSV
        with open(csv_path, "rb") as f:
            raw = f.read().replace(b"\x00", b"")

        import io
        reader = csv_mod.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")))

        # Map CSV headers to our internal keys
        HEADER_TO_KEY = {
            "Candidate Name": "NAME",
            "LinkedIn URL": "LINKEDIN",
            "Source": "SOURCE",
            "Source Additional Notes": "SOURCE_NOTES",
            "CV / Resume Notes": "CV",
            "Target Role": "TARGET_ROLE",
            "AI Screening Verdict": "VERDICT",
            "Actioned": "ACTIONED",
            "Feedback": "FEEDBACK",
            "Best Fit Role": "BEST_FIT_ROLE",
            "Matched Level": "MATCHED_LEVEL",
            "Verdict Reason": "VERDICT_REASON",
            "Rejection Type": "REJECTION_TYPE",
            "Spark": "SPARK",
            "Concerns": "CONCERNS",
            "Screener Brief": "SCREENER_BRIEF",
            "Screening Questions": "SCREENING_QUESTIONS",
            "Ashby ID": "ASHBY_ID",
        }

        candidates = []
        for row_num, row in enumerate(reader, start=2):
            name = row.get("Candidate Name", "").strip()
            if not name:
                continue

            candidate = {"row": row_num, "name": name}
            for header, key in HEADER_TO_KEY.items():
                if header == "Candidate Name":
                    continue
                val = row.get(header, "").strip()
                candidate[key.lower()] = val

            # Normalize field names for compatibility
            candidate["linkedin_url"] = candidate.pop("linkedin", "")
            candidate["source_notes"] = candidate.pop("source_notes", "")
            candidate["target_role"] = candidate.pop("target_role", "")
            candidate["best_fit_role"] = candidate.pop("best_fit_role", "")
            candidate["matched_level"] = candidate.pop("matched_level", "")
            candidate["verdict_reason"] = candidate.pop("verdict_reason", "")
            candidate["rejection_type"] = candidate.pop("rejection_type", "")
            candidate["screener_brief"] = candidate.pop("screener_brief", "")
            candidate["screening_questions"] = candidate.pop("screening_questions", "")
            candidate["ashby_id"] = candidate.pop("ashby_id", "")

            candidates.append(candidate)

        return candidates

    # Fallback: try Google Sheets API
    try:
        from google_auth_helper import sheets_read_tab
        from sheets_bridge import SPREADSHEET_ID, HEADER_MAP

        rows = sheets_read_tab(SPREADSHEET_ID, "Screen")
        if not rows or len(rows) < 2:
            return []

        headers = rows[0]
        reverse = {}
        for key, header_text in HEADER_MAP.items():
            for i, h in enumerate(headers):
                if h.strip().lower() == header_text.lower():
                    reverse[key] = i
                    break

        candidates = []
        for row_num, row in enumerate(rows[1:], start=2):
            while len(row) < len(headers):
                row.append("")

            def _get(key: str) -> str:
                idx = reverse.get(key)
                if idx is not None and idx < len(row):
                    return str(row[idx]).strip()
                return ""

            name = _get("NAME")
            if not name:
                continue

            candidates.append({
                "row": row_num,
                "name": name,
                "linkedin_url": _get("LINKEDIN"),
                "source": _get("SOURCE"),
                "source_notes": _get("SOURCE_NOTES"),
                "cv": _get("CV"),
                "target_role": _get("TARGET_ROLE"),
                "verdict": _get("VERDICT"),
                "actioned": _get("ACTIONED"),
                "feedback": _get("FEEDBACK"),
                "best_fit_role": _get("BEST_FIT_ROLE"),
                "matched_level": _get("MATCHED_LEVEL"),
                "verdict_reason": _get("VERDICT_REASON"),
                "rejection_type": _get("REJECTION_TYPE"),
                "spark": _get("SPARK"),
                "concerns": _get("CONCERNS"),
                "screener_brief": _get("SCREENER_BRIEF"),
                "screening_questions": _get("SCREENING_QUESTIONS"),
            })

        return candidates
    except Exception as e:
        logger.error("Cannot read Screen tab: %s", e)
        logger.error("Download the Screen tab as CSV to ~/Downloads/ and try again.")
        return []


def _cli_push(dry_run: bool = False):
    """Push SCREEN candidates with outreach sent from Sheet to Ashby."""
    mapping = load_role_map()
    if not mapping:
        print("  No role mapping found. Run: python3 ashby_bridge.py --setup")
        return

    # Read Screen tab with all columns
    print("\n  Reading Screen tab...")
    candidates = _read_screen_tab_full()

    if not candidates:
        print("  No candidates found in Screen tab.")
        return

    # Filter: all SCREEN candidates in the Screen tab are HM-approved
    # Being in the Screen tab = HM approved. Push all with SCREEN verdict.
    screen_candidates = []
    for c in candidates:
        verdict = (c.get("verdict") or "").upper().strip()
        if verdict == "SCREEN":
            screen_candidates.append(c)

    print(f"  Found {len(screen_candidates)} SCREEN candidates in Screen tab (all HM-approved)")

    if not screen_candidates:
        print("  Nothing to push.")
        return

    pushed = 0
    skipped = 0
    failed = 0

    for c in screen_candidates:
        name = c.get("name", "")
        linkedin = c.get("linkedin_url", "")
        best_fit = c.get("best_fit_role", "")
        target = c.get("target_role", "")
        role = best_fit or target

        if not name:
            continue

        result = push_candidate_to_ashby(
            name=name,
            linkedin_url=linkedin,
            role_name=role,
            screening_data=c,
            dry_run=dry_run,
        )

        if result == "dry-run":
            pushed += 1
        elif result:
            pushed += 1
        elif linkedin and is_already_pushed(linkedin):
            skipped += 1
        else:
            failed += 1

    action = "Would push" if dry_run else "Pushed"
    print(f"\n  Done: {action} {pushed}, skipped {skipped}, failed {failed}")


# ── Backfill: Sheet → Ashby ────────────────────────────────────

BACKFILL_TRACKER_FILE = _DIR / ".ashby_backfill_tracker.json"

# Sheet tab → Ashby stage mapping for backfill
SHEET_TAB_TO_ASHBY_STAGE = {
    "Screen": "Screened",
    "Screen (Needs Review)": "Screened",
    "Nurture": "Nurture",
    "Archived": "Archived",
    "Candidate Screener": None,  # uses verdict routing
}


def _load_backfill_tracker() -> dict:
    """Load {ashby_candidate_id: {tab, timestamp}} tracker."""
    if BACKFILL_TRACKER_FILE.exists():
        return json.loads(BACKFILL_TRACKER_FILE.read_text())
    return {}


def _save_backfill_tracker(tracker: dict):
    write_json_atomic(BACKFILL_TRACKER_FILE, tracker, indent=2)


def _fetch_all_application_ids() -> Dict[str, str]:
    """Fetch all applications from Ashby and return {candidate_id: application_id} map."""
    logger.info("Loading all applications from Ashby for backfill...")
    cid_to_appid: Dict[str, str] = {}
    cursor = None
    page = 0

    while True:
        payload: Dict[str, Any] = {"limit": 100}
        if cursor:
            payload["cursor"] = cursor

        result = _ashby_post("application.list", payload)
        if not result or not result.get("success"):
            logger.error("application.list failed")
            break

        apps = result.get("results", [])
        for app in apps:
            candidate = app.get("candidate", {})
            cid = candidate.get("id", "")
            app_id = app.get("id", "")
            if cid and app_id:
                cid_to_appid[cid] = app_id

        page += 1
        if not result.get("moreDataAvailable"):
            break
        cursor = result.get("nextCursor")
        time.sleep(0.2)

    logger.info("  Loaded %d applications from Ashby", len(cid_to_appid))
    return cid_to_appid


BACKFILL_CSV_DIR = _DIR / "backfill_csvs"

# CSV header → Python key mapping (reverse of _ASHBY_FIELD_MAP in Apps Script)
_CSV_HEADER_TO_KEY = {
    "candidate name": "name",
    "linkedin url": "linkedin_url",
    "source": "source",
    "source additional notes": "source_notes",
    "cv / resume notes": "cv",
    "target role": "target_role",
    "ai screening verdict": "verdict",
    "rejection type": "rejection_type",
    "nurture": "nurture",
    "spark": "spark",
    "verdict reason": "verdict_reason",
    "best fit role": "best_fit_role",
    "best fit role reason": "best_fit_reason",
    "matched level": "matched_level",
    "reasoning": "reasoning",
    "regret test": "regret_test",
    "concerns": "concerns",
    "screening questions": "screening_questions",
    "screener brief": "screener_brief",
    "defer until": "defer_until",
    "outreach message 1": "outreach_1",
    "outreach message 2": "outreach_2",
    "research output": "research_output",
    "ashby candidate id": "ashby_candidate_id",
    "actioned": "actioned",
}

# Actioned column → Ashby stage override (for Screen tab backfill)
ACTIONED_TO_STAGE = {
    "Outreach Sent": "Initial Screen",
    "Interview Scheduled": "First Round",
}

# Tab name → CSV filename mapping
_TAB_TO_CSV = {
    "Screen (Needs Review)": "Screen (Needs Review)",
    "Nurture": "Nurture",
    "Archived": "Archived",
}


def _fetch_backfill_candidates(tab: str, limit: int = 0, offset: int = 0) -> Tuple[List[dict], int]:
    """Read candidates from a local CSV export of a Sheet tab.

    Returns (candidates_list, total_count).
    """
    import csv

    # Find the CSV file — try exact name and common download patterns
    csv_dir = BACKFILL_CSV_DIR
    candidates_file = None
    tab_base = _TAB_TO_CSV.get(tab, tab)

    for pattern in [f"{tab_base}.csv", f"{tab_base} *.csv", f"*- {tab_base} (*.csv", f"*- {tab_base} *.csv", f"*- {tab_base}.csv"]:
        matches = list(csv_dir.glob(pattern))
        if matches:
            candidates_file = matches[0]
            break

    if not candidates_file:
        logger.error("No CSV found for tab '%s' in %s", tab, csv_dir)
        return [], 0

    logger.info("  Reading %s", candidates_file.name)

    candidates = []
    import io
    with open(candidates_file, "r", encoding="utf-8-sig", errors="replace") as f:
        content = f.read().replace("\x00", "")
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        if True:  # keeps indentation consistent
            # Map CSV headers to Python keys
            mapped = {}
            for csv_header, value in row.items():
                key = _CSV_HEADER_TO_KEY.get(csv_header.strip().lower(), "")
                if key:
                    mapped[key] = value.strip() if value else ""

            # Skip rows without Ashby Candidate ID or verdict
            ashby_id = mapped.get("ashby_candidate_id", "")
            verdict = mapped.get("verdict", "")
            if not ashby_id or not verdict:
                continue

            # Skip misaligned rows — detect by checking if Source contains a role name
            # instead of a real source value (e.g. "Inbound", "Sourced", etc.)
            source = mapped.get("source", "")
            known_roles = ["AI Backend", "AI Frontend", "AI Product", "AI Design",
                           "DevSecOps", "GTM", "AI Value Delivery", "Field Marketing"]
            if any(role in source for role in known_roles):
                logger.warning("  SKIP misaligned row: %s (Source='%s')",
                               mapped.get("name", "?"), source[:50])
                continue

            mapped["ashby_candidate_id"] = ashby_id
            candidates.append(mapped)

    total = len(candidates)
    if offset:
        candidates = candidates[offset:]
    if limit:
        candidates = candidates[:limit]

    return candidates, total


def run_backfill(
    tabs: Optional[List[str]] = None,
    limit: int = 0,
    dry_run: bool = False,
):
    """Backfill existing Sheet candidates into Ashby.

    Reads each Sheet tab, finds candidates with Ashby Candidate IDs,
    writes custom fields + notes + moves to correct Ashby stage.
    """
    if tabs is None:
        tabs = ["Screen (Needs Review)", "Nurture", "Archived"]

    tracker = _load_backfill_tracker()
    cid_to_appid = _fetch_all_application_ids()

    total_written = 0
    total_skipped = 0
    total_failed = 0
    total_already = 0

    for tab in tabs:
        print(f"\n{'='*60}")
        print(f"  Backfilling tab: {tab}")
        print(f"{'='*60}")

        # Determine Ashby destination stage
        dest_stage = SHEET_TAB_TO_ASHBY_STAGE.get(tab)

        # Paginate through all candidates
        offset = 0
        batch_size = 100
        tab_count = 0

        while True:
            candidates, total = _fetch_backfill_candidates(tab, limit=batch_size, offset=offset)
            if not candidates:
                if offset == 0:
                    print(f"  No candidates with Ashby IDs in {tab}")
                break

            if offset == 0:
                print(f"  Found {total} candidates with Ashby IDs")

            for c in candidates:
                ashby_cid = c.get("ashby_candidate_id", "")
                name = c.get("name", "?")

                # Skip if already backfilled
                if ashby_cid in tracker:
                    total_already += 1
                    continue

                # Find application ID
                app_id = cid_to_appid.get(ashby_cid, "")
                if not app_id:
                    print(f"    SKIP (no application in Ashby): {name}")
                    total_skipped += 1
                    continue

                # For Candidate Screener tab, use verdict-based routing
                if dest_stage is None:
                    candidate_dest = get_verdict_stage(
                        c.get("verdict", ""),
                        c.get("rejection_type", ""),
                        c.get("nurture", ""),
                    )
                else:
                    # Check Actioned column for stage override
                    actioned = c.get("actioned", "").strip()
                    candidate_dest = ACTIONED_TO_STAGE.get(actioned, dest_stage)

                if dry_run:
                    print(f"    DRY RUN: {name:<35} → {candidate_dest}")
                    tab_count += 1
                    total_written += 1
                else:
                    if write_screening_to_ashby_durable(ashby_cid, app_id, c):
                        tracker[ashby_cid] = {
                            "tab": tab,
                            "stage": candidate_dest,
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        }
                        _save_backfill_tracker(tracker)
                        tab_count += 1
                        total_written += 1
                    else:
                        print(f"    FAILED: {name}")
                        total_failed += 1

                if limit and total_written >= limit:
                    break

            offset += batch_size
            if limit and total_written >= limit:
                break

        print(f"  Tab {tab}: {tab_count} backfilled")

    print(f"\n{'='*60}")
    print(f"  Backfill complete:")
    print(f"    Written:  {total_written}")
    print(f"    Skipped:  {total_skipped} (no application in Ashby)")
    print(f"    Already:  {total_already} (previously backfilled)")
    print(f"    Failed:   {total_failed}")
    print(f"{'='*60}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ashby ATS integration")
    parser.add_argument("--list-jobs", action="store_true", help="List Ashby jobs + mapping")
    parser.add_argument("--setup", action="store_true", help="Auto-map roles to Ashby jobs")
    parser.add_argument("--push", action="store_true", help="Push SCREEN candidates to Ashby")
    parser.add_argument("--pull-inbound", action="store_true", help="Pull new inbound candidates from Ashby to Sheet")
    parser.add_argument("--setup-stages", action="store_true", help="Cache stage IDs from Ashby")
    parser.add_argument("--pull-for-screening", action="store_true", help="Pull Application Review → AI Screening")
    parser.add_argument("--backfill", action="store_true", help="Backfill Sheet candidates to Ashby")
    parser.add_argument("--replay-writebacks", action="store_true",
                        help="Replay queued failed writebacks (no re-screening, no tokens)")
    parser.add_argument("--reconcile-orphans", action="store_true",
                        help="Re-push candidates with a terminal verdict in the log but an "
                             "empty AI Verdict in Ashby (no re-screening). Use --dry-run to preview.")
    parser.add_argument("--recent", type=int, default=400,
                        help="For --reconcile-orphans: scan only the N most recently logged "
                             "terminal candidates (0 = all; default 400). Keeps the live scan cheap.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without pushing/pulling")
    parser.add_argument("--limit", type=int, default=0, help="Max candidates to process (0=all)")
    args = parser.parse_args()

    if args.list_jobs:
        _cli_list_jobs()
    elif args.setup:
        setup_role_mapping()
    elif args.setup_stages:
        setup_stages()
    elif args.push:
        _cli_push(dry_run=args.dry_run)
    elif args.pull_inbound:
        _cli_pull_inbound(dry_run=args.dry_run, limit=args.limit)
    elif args.pull_for_screening:
        candidates = pull_for_screening(limit=args.limit, dry_run=args.dry_run)
        if candidates:
            print(f"\n  Pulled {len(candidates)} candidates for screening:")
            for c in candidates:
                li = " (LinkedIn)" if c.get("linkedin_url") else " (no LinkedIn)"
                print(f"    {c['name']:<35} {c.get('job_title', ''):<30}{li}")
        else:
            print("\n  No candidates in Application Review to screen.")
    elif args.backfill:
        run_backfill(limit=args.limit, dry_run=args.dry_run)
    elif args.replay_writebacks:
        counts = replay_writeback_queue(dry_run=args.dry_run)
        print(f"\n  Writeback replay: {counts['ok']} ok, {counts['failed']} still "
              f"failing, {counts['dead']} dead-lettered "
              f"({counts['replayed']} attempted)")
    elif args.reconcile_orphans:
        counts = reconcile_orphaned_writebacks(dry_run=args.dry_run, limit=args.limit,
                                               recent=args.recent)
        mode = "DRY RUN — would re-push" if args.dry_run else "Re-pushed"
        print(f"\n  Reconcile: checked {counts['checked']}, found {counts['orphans']} "
              f"orphans. {mode} {counts['orphans'] if args.dry_run else counts['fixed']} "
              f"(queued={counts.get('queued', 0)}, errors={counts.get('errors', 0)}).")
    else:
        parser.print_help()
