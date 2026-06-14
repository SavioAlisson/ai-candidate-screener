"""
LinkedIn URL discovery — last-resort step when Ashby/CV/push-log all miss.

Pipeline order inside screen_one_candidate:
  1. ashby_bridge.py fallback chain (socialLinks → linkedInUrl → PDF → push log)
  2. If still empty, this module runs (Linkup search → Apify verify → numeric score)
  3. If a confident match is found (score ≥ 60), Apify scrape is reused downstream.

Conservative by design: false matches (wrong person, same name) are worse than
no match because they poison the dossier. We verify each candidate URL by
scraping it and comparing the LinkedIn name + company against the candidate
identity we already know.

Confidence score (0–100):
  + 40  name tokens overlap by ≥2  (else 20 for ≥1 overlap, else 0)
  + 40  current company matches the target company (substring, case-insensitive)
  + 20  any past role mentions the target company  (only if current=0)
Threshold to accept: 60
"""

from __future__ import annotations

import logging
import os
import re
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

_LI_URL_RE = re.compile(r"^https?://(www\.)?linkedin\.com/in/[^/?#\s]+/?$", re.I)
_LI_EXTRACT_RE = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[^/?#\s\)\]]+", re.I)

LINKUP_COST = 0.006
APIFY_VERIFY_COST = 0.005   # per Apify scrape during verification
SCORE_THRESHOLD = 60
MAX_VERIFY = 5              # cap Apify calls per candidate


def _name_tokens(name: str) -> List[str]:
    return [t.lower() for t in re.findall(r"[A-Za-z]{2,}", name or "")]


def _guess_company_from_cv(cv: str) -> str:
    """Cheap heuristic: first 'at <Company>' phrase, or 'Company: <X>' line."""
    if not cv:
        return ""
    # Explicit "Company: X" marker (used by GitHub sourcer push log)
    m = re.search(r"^\s*Company\s*:\s*([^\n]{2,80})", cv, re.I | re.M)
    if m:
        return m.group(1).strip()
    # "at <Company>" inline
    m = re.search(r"\bat\s+([A-Z][A-Za-z0-9&.\-]{2,40}(?:\s+[A-Z][A-Za-z0-9&.\-]{1,40}){0,2})", cv)
    return m.group(1).strip() if m else ""


def _extract_linkedin_candidates(sources: List[dict], raw_text: str) -> List[Dict]:
    """Collect unique linkedin.com/in/* URLs (up to 5)."""
    seen, out = set(), []

    for s in sources or []:
        url = (s.get("url") or "").strip()
        title = (s.get("title") or "").strip()
        if not url or "linkedin.com/in/" not in url.lower():
            continue
        clean = url.split("?", 1)[0].rstrip("/")
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": clean, "title": title})
        if len(out) >= MAX_VERIFY:
            break

    if len(out) < MAX_VERIFY and raw_text:
        for m in _LI_EXTRACT_RE.finditer(raw_text):
            clean = m.group(0).split("?", 1)[0].rstrip("/")
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({"url": clean, "title": ""})
            if len(out) >= MAX_VERIFY:
                break

    return out


# ── Confidence scoring ────────────────────────────────────────────────────────

def _name_match_pts(li_name: str, target_name: str) -> int:
    li_tok = set(_name_tokens(li_name))
    tg_tok = set(_name_tokens(target_name))
    if not tg_tok:
        return 0
    overlap = len(li_tok & tg_tok)
    if overlap >= 2:
        return 40
    if overlap == 1:
        return 20
    return 0


def _company_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return a.lower() in b.lower() or b.lower() in a.lower()


def _past_company_match(profile: dict, target_company: str) -> bool:
    if not target_company:
        return False
    text = (profile.get("fullText") or "").lower()
    return target_company.lower() in text


def _score(profile: dict, target_name: str, target_company: str) -> Dict:
    li_name = profile.get("name") or ""
    li_company = profile.get("company") or ""
    name_pts = _name_match_pts(li_name, target_name)
    cur_pts = 40 if _company_match(li_company, target_company) else 0
    past_pts = 20 if (cur_pts == 0 and _past_company_match(profile, target_company)) else 0
    return {
        "li_name": li_name,
        "li_company": li_company,
        "name_pts": name_pts,
        "current_company_pts": cur_pts,
        "past_company_pts": past_pts,
        "total": name_pts + cur_pts + past_pts,
    }


# ── Main entry point ─────────────────────────────────────────────────────────

def discover_linkedin_url(
    *,
    name: str,
    cv: str,
    target_role: str,
    linkup_key: str,
    claude_key: str,           # kept for backwards compatibility (unused)
    linkup_search_fn,
    haiku_call_fn=None,        # kept for backwards compatibility (unused)
    apify_token: str = "",
    company_hint: str = "",
) -> Tuple[str, float]:
    """Return (url_or_empty_string, cost_usd).

    Flow:
      1. Build anchored Linkup query: "<name>" "<company>" site:linkedin.com/in/
      2. Extract LinkedIn URL candidates from results.
      3. Scrape each (Apify, ≤ MAX_VERIFY).
      4. Score by name + current/past company match.
      5. Return URL of highest-scoring candidate if total ≥ SCORE_THRESHOLD.
    """
    if not name or not linkup_key:
        return "", 0.0

    target_company = (company_hint or _guess_company_from_cv(cv) or "").strip()
    apify_token = apify_token or os.environ.get("APIFY_TOKEN", "")

    # 1. Build anchored query — company in quotes if we have one
    bits = [f'"{name}"']
    if target_company:
        bits.append(f'"{target_company}"')
    bits.append("site:linkedin.com/in/")
    if target_role and not target_company:
        # Only add role as fallback anchor when we have no company
        bits.insert(1, target_role)
    query = " ".join(bits)
    logger.info("DISCOVERY query: %s", query)

    cost = 0.0
    try:
        text, sources = linkup_search_fn(linkup_key, query)
        cost += LINKUP_COST
    except Exception as e:
        logger.warning("DISCOVERY: Linkup call failed for %s: %s", name, e)
        return "", cost

    candidates = _extract_linkedin_candidates(sources, text or "")
    valid = [c for c in candidates if _LI_URL_RE.match(c["url"])]
    if not valid:
        logger.info("DISCOVERY: no valid /in/ URLs for %s", name)
        return "", cost

    if not apify_token:
        logger.warning("DISCOVERY: no APIFY_TOKEN — cannot verify candidates for %s", name)
        return "", cost

    # 3. Scrape + score each candidate
    from apify_linkedin import fetch_linkedin_profile  # local import to avoid cycle

    scored = []
    for c in valid[:MAX_VERIFY]:
        try:
            res = fetch_linkedin_profile(c["url"], apify_token)
            cost += APIFY_VERIFY_COST
        except Exception as e:
            logger.info("DISCOVERY: Apify raised for %s: %s", c["url"], e)
            continue
        if not res.get("success"):
            logger.info("DISCOVERY: Apify failed for %s — %s", c["url"], res.get("error"))
            continue
        s = _score(res["data"], name, target_company)
        s["url"] = c["url"]
        s["profile"] = res["data"]
        scored.append(s)
        logger.info("DISCOVERY: %s → name=%d cur=%d past=%d TOTAL=%d (li=%s @ %s)",
                    c["url"], s["name_pts"], s["current_company_pts"],
                    s["past_company_pts"], s["total"],
                    s["li_name"], s["li_company"] or "—")

    if not scored:
        return "", cost

    scored.sort(key=lambda x: -x["total"])
    best = scored[0]
    if best["total"] < SCORE_THRESHOLD:
        logger.info("DISCOVERY: best score %d < threshold %d — declining for %s",
                    best["total"], SCORE_THRESHOLD, name)
        return "", cost

    # Cache the verified profile so the downstream Apify step doesn't re-scrape.
    try:
        from apify_linkedin import cache_write
        cache_write(best["url"], {"success": True, "error": None,
                                  "actor": "discovery_verify", "data": best["profile"]})
    except Exception as e:
        logger.info("DISCOVERY: cache write failed (non-fatal): %s", e)

    logger.info("DISCOVERY: matched %s → %s (score=%d)", name, best["url"], best["total"])
    return best["url"], cost
