"""Crunchbase company data via vulnv/crunchbase-scraper-pro (Apify).

Cache-first: looks up `.crunchbase_cache.json` by company slug before calling Apify.
On cache miss, fetches in a single batched API call and writes back to cache.

Public function:
  fetch_crunchbase_for_companies(companies: list[str]) -> dict[str, dict]
      Given a list of company names (from a candidate's LinkedIn experience),
      returns {company_name: crunchbase_record_or_None}.

      Companies in SKIP_CRUNCHBASE (well-known mega-caps) are short-circuited
      to None so Opus uses its training knowledge instead.

      Errors are swallowed silently — the dossier just shows "(no Crunchbase
      profile)" for that company. Never blocks screening.
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
CACHE_FILE = _DIR / ".crunchbase_cache.json"
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")

# Well-known companies — Opus knows them from training. Don't waste Apify calls.
SKIP_CRUNCHBASE = {
    "google", "alphabet", "meta", "facebook", "instagram", "whatsapp", "apple", "amazon",
    "amazon web services", "aws", "microsoft", "netflix", "uber", "airbnb", "linkedin",
    "salesforce", "oracle", "ibm", "intel", "cisco", "adobe", "stripe", "paypal", "tesla",
    "twitter", "x", "spotify", "doordash", "walmart", "disney", "walt disney company",
    "deloitte", "accenture", "ey", "pwc", "kpmg", "mckinsey", "bcg", "bain",
    "openai", "anthropic", "cursor",
}


def co_slug(company: str) -> str:
    """Normalize a company name into a Crunchbase URL slug."""
    s = company.lower()
    s = re.sub(r"[^a-z0-9 -]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return re.sub(r"-inc$|-llc$|-the$", "", s)


def should_skip(company: str) -> bool:
    return company.lower().strip() in SKIP_CRUNCHBASE


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        from ashby_bridge import write_json_atomic
        write_json_atomic(CACHE_FILE, cache, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning("Crunchbase cache save failed: %s", e)


def _slug_from_url(url: str) -> str:
    return (url or "").split("/organization/")[-1].rstrip("/").split("?")[0]


def _apify_batch_fetch(slugs: List[str], attempt: int = 1) -> Dict[str, Optional[dict]]:
    """Call the Apify Crunchbase actor for a batch of slugs."""
    if not APIFY_TOKEN:
        logger.warning("APIFY_TOKEN not set — skipping Crunchbase")
        return {s: None for s in slugs}

    urls = [{"url": f"https://www.crunchbase.com/organization/{s}"} for s in slugs]
    api_url = (
        f"https://api.apify.com/v2/acts/vulnv~crunchbase-scraper-pro/"
        f"run-sync-get-dataset-items?token={APIFY_TOKEN}&timeout=300"
    )
    payload = {"company_urls": urls}
    try:
        req = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=360) as r:
            data = json.loads(r.read())
    except Exception as e:
        logger.warning("Crunchbase API call failed (attempt %d): %s", attempt, e)
        if attempt < 2:
            return _apify_batch_fetch(slugs, attempt + 1)
        return {s: None for s in slugs}

    if isinstance(data, dict) and "error" in data:
        if attempt < 2:
            return _apify_batch_fetch(slugs, attempt + 1)
        return {s: None for s in slugs}

    out: Dict[str, Optional[dict]] = {s: None for s in slugs}
    for rec in (data if isinstance(data, list) else []):
        if not rec or not rec.get("name"):
            continue
        rec_slug = _slug_from_url(rec.get("url", ""))
        if rec_slug not in out:
            continue
        acq = rec.get("acquired_by") or {}
        out[rec_slug] = {
            "name": rec.get("name"),
            "size_bracket": rec.get("num_employees"),
            "ipo_status": rec.get("ipo_status"),
            "num_funds": rec.get("number_of_funds"),
            "acquired_by": acq.get("acquirer") if acq else None,
            "industries": [i.get("value") for i in (rec.get("industries") or [])][:4],
        }

    # Retry once if 0 hits in batch — likely rate limit
    if attempt < 2 and not any(v for v in out.values()):
        import time
        time.sleep(5)
        return _apify_batch_fetch(slugs, attempt + 1)
    return out


def fetch_crunchbase_for_companies(companies: List[str]) -> Dict[str, Optional[dict]]:
    """Return {company_name: crunchbase_record} for a list of companies.

    Skip-listed companies map to None. Cached companies use cached data.
    Uncached companies are fetched in one batched Apify call. Errors return
    None for that company — never raise.
    """
    if not companies:
        return {}

    cache = _load_cache()
    result: Dict[str, Optional[dict]] = {}
    to_fetch: List[str] = []  # slugs that need Apify
    slug_to_company: Dict[str, str] = {}

    for co in companies:
        if not co or not co.strip():
            continue
        if should_skip(co):
            result[co] = None  # caller uses training knowledge
            continue
        slug = co_slug(co)
        if slug in cache:
            result[co] = cache[slug]
            continue
        to_fetch.append(slug)
        slug_to_company[slug] = co

    if to_fetch:
        logger.info("Crunchbase: fetching %d companies (cache miss)", len(to_fetch))
        fetched = _apify_batch_fetch(to_fetch)
        for slug, rec in fetched.items():
            cache[slug] = rec
            co_name = slug_to_company.get(slug)
            if co_name:
                result[co_name] = rec
        _save_cache(cache)

    return result


def extract_companies_from_linkedin(linkedin_full_text: str, limit: int = 8) -> List[str]:
    """Pull the candidate's employer names from the LinkedIn `--- Experience ---` block.

    Returns up to `limit` unique companies in order of appearance (most recent first).
    """
    if not linkedin_full_text:
        return []
    m = re.search(r"--- Experience ---\n(.+?)(\n--- |\Z)", linkedin_full_text, re.S)
    if not m:
        return []
    seen: set = set()
    out: List[str] = []
    for line in m.group(1).split("\n"):
        mm = re.match(r"- .+? at (.+?) \(", line)
        if mm:
            co = mm.group(1).strip()
            if co and co not in seen:
                seen.add(co)
                out.append(co)
        if len(out) >= limit:
            break
    return out


def format_for_dossier(companies: List[str], crunchbase_map: Dict[str, Optional[dict]]) -> str:
    """Render the Crunchbase section to append to the dossier."""
    lines = ["=== CRUNCHBASE COMPANY DATA ==="]
    for co in companies:
        if should_skip(co):
            lines.append(f"  {co} → (well-known company — use training knowledge for size/stage)")
            continue
        cb = crunchbase_map.get(co)
        if cb and cb.get("name"):
            line = (
                f"  {co} → size={cb.get('size_bracket')}, "
                f"rounds={cb.get('num_funds')}, ipo={cb.get('ipo_status')}"
            )
            if cb.get("acquired_by"):
                line += f", acquired_by={cb['acquired_by']}"
            if cb.get("industries"):
                line += f", industries={cb['industries'][:3]}"
            lines.append(line)
        else:
            lines.append(f"  {co} → (no Crunchbase profile — likely small/stealth/obscure)")
    lines.append(
        "Note: Crunchbase actor returns current size bracket + acquisition status + "
        "funding round count, not per-round dates. Use this data plus Opus's training "
        "knowledge of well-known companies (Looker raised Series E 2018, Pendo Series F "
        "2019, etc.) to infer stage-at-tenure."
    )
    return "\n".join(lines)
