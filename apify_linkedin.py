"""
Apify LinkedIn integration — Python port of apify_integration.gs

Cache-first LinkedIn profile scraping:
  1. Check LinkedIn Cache tab via Apps Script web app
  2. If cache miss, scrape via Apify (primary actor + backup)
  3. Write result to cache
  4. Return structured profile data for pipeline consumption

Reuses cached data automatically — no redundant scrapes.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Apify Configuration ──────────────────────────────────────────
APIFY_PRIMARY_ACTOR = "apimaestro/linkedin-profile-detail"   # $5/1k, rich data + email
APIFY_BACKUP_ACTOR = "supreme_coder/linkedin-profile-scraper"  # $3/1k fallback
APIFY_TIMEOUT_SEC = 90
APIFY_POLL_INTERVAL_SEC = 5
APIFY_COST_PER_SCRAPE = 0.005


# ── URL Helpers ──────────────────────────────────────────────────

def normalize_linkedin_url(url: str) -> str:
    """Normalize LinkedIn URL for cache comparison."""
    if not url:
        return ""
    s = url.strip().lower()
    s = re.sub(r"https?://(www\.)?linkedin\.com", "", s, flags=re.I)
    s = s.rstrip("/")
    s = re.sub(r"\?.*$", "", s)
    return s


def extract_linkedin_username(url: str) -> str:
    """Extract username from LinkedIn URL."""
    m = re.search(r"linkedin\.com/in/([^/?#]+)", url, re.I)
    return m.group(1) if m else ""


# ── LinkedIn Cache (via Apps Script web app) ─────────────────────

def cache_read(linkedin_url: str) -> Optional[Dict[str, Any]]:
    """Check LinkedIn cache (local JSON file)."""
    try:
        from csv_bridge import read_linkedin_cache
        return read_linkedin_cache(linkedin_url)
    except Exception as e:
        logger.warning("LinkedIn cache read failed: %s — will scrape fresh", e)
        return None


def cache_write(linkedin_url: str, result: dict) -> None:
    """Write scrape result to LinkedIn cache (local JSON file)."""
    try:
        from csv_bridge import write_linkedin_cache
        write_linkedin_cache(linkedin_url, result)
    except Exception as e:
        logger.warning("LinkedIn cache write failed: %s", e)


# ── Apify Actor Calls ───────────────────────────────────────────

def call_apify_actor(linkedin_url: str, actor_id: str, token: str) -> dict:
    """
    Start an Apify actor run, poll for completion, return result.
    Returns: {success: bool, error: str|None, actor: str, data: dict|None}
    """
    # Build payload based on actor
    if actor_id == APIFY_PRIMARY_ACTOR:
        username = extract_linkedin_username(linkedin_url)
        if not username:
            return {"success": False, "error": "INVALID_URL", "data": None, "actor": actor_id}
        payload = {"username": username, "includeEmail": True}
    else:
        payload = {"startUrls": [{"url": linkedin_url}], "profileUrls": [linkedin_url]}

    # Start actor run — Apify API uses ~ instead of / in actor IDs
    safe_actor_id = actor_id.replace("/", "~")
    start_url = f"https://api.apify.com/v2/acts/{safe_actor_id}/runs?token={token}"
    try:
        req_data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            start_url, data=req_data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 201:
                return {"success": False, "error": "START_FAILED", "data": None, "actor": actor_id}
            run_data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"success": False, "error": f"START_HTTP_{e.code}", "data": None, "actor": actor_id}
    except Exception as e:
        return {"success": False, "error": f"EXCEPTION: {e}", "data": None, "actor": actor_id}

    run_id = run_data["data"]["id"]
    dataset_id = run_data["data"]["defaultDatasetId"]

    # Poll for completion
    poll_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}"
    start_time = time.time()

    while time.time() - start_time < APIFY_TIMEOUT_SEC:
        time.sleep(APIFY_POLL_INTERVAL_SEC)
        try:
            req = urllib.request.Request(poll_url, method="GET")
            with urllib.request.urlopen(req, timeout=15) as resp:
                poll_data = json.loads(resp.read().decode("utf-8"))
            status = poll_data["data"]["status"]
        except Exception:
            continue

        if status == "SUCCEEDED":
            # Fetch results from dataset
            data_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"
            try:
                req = urllib.request.Request(data_url, method="GET")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    items = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                return {"success": False, "error": f"FETCH_FAILED: {e}", "data": None, "actor": actor_id}

            if not items:
                return {"success": False, "error": "NO_DATA", "data": None, "actor": actor_id}

            profile = parse_apify_profile(items[0])
            logger.info("APIFY: SUCCESS for %s — %s", linkedin_url, profile.get("name", "Unknown"))
            return {"success": True, "error": None, "actor": actor_id, "data": profile}

        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return {"success": False, "error": status, "data": None, "actor": actor_id}

    return {"success": False, "error": "TIMEOUT", "data": None, "actor": actor_id}


def fetch_linkedin_profile(linkedin_url: str, token: str) -> dict:
    """Fetch profile with primary actor, fallback to backup."""
    if not linkedin_url or "linkedin.com" not in linkedin_url:
        return {"success": False, "error": "INVALID_URL", "data": None, "actor": "none"}

    url = linkedin_url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    result = call_apify_actor(url, APIFY_PRIMARY_ACTOR, token)

    if not result["success"] and result["error"] not in ("PRIVATE", "INVALID_URL"):
        logger.info("APIFY: Primary actor failed (%s), trying backup...", result["error"])
        result = call_apify_actor(url, APIFY_BACKUP_ACTOR, token)
        if result["success"]:
            result["actor"] = APIFY_BACKUP_ACTOR

    return result


# ── Profile Parsing ──────────────────────────────────────────────

def parse_apify_profile(raw: dict) -> dict:
    """Route to correct parser based on actor format."""
    if raw.get("basic_info"):
        return parse_apimaestro_profile(raw)
    return parse_legacy_profile(raw)


def parse_apimaestro_profile(raw: dict) -> dict:
    """Parse apimaestro/linkedin-profile-detail format."""
    info = raw.get("basic_info") or {}
    name = info.get("fullname") or ""
    headline = info.get("headline") or ""
    company = info.get("current_company") or ""
    loc = info.get("location", {})
    location = loc.get("full") if isinstance(loc, dict) else str(loc or "")
    email = info.get("email") or ""

    # Experience
    experiences = raw.get("experience") or []
    exp_text = ""
    for exp in experiences:
        exp_text += f"- {exp.get('title', '')} at {exp.get('company', '')}"
        if exp.get("duration"):
            exp_text += f" ({exp['duration']})"
        if exp.get("location"):
            exp_text += f" [{exp['location']}]"
        if exp.get("employment_type"):
            exp_text += f" — {exp['employment_type']}"
        exp_text += "\n"
        if exp.get("description"):
            exp_text += f"  {exp['description']}\n"

    # Skills
    top_skills = info.get("top_skills") or []
    all_skills = list(top_skills)
    for exp in experiences:
        for skill in exp.get("skills") or []:
            if skill not in all_skills:
                all_skills.append(skill)
    skills_text = ", ".join(all_skills)

    # Education
    education = raw.get("education") or []
    edu_text = ""
    for edu in education:
        degree = edu.get("degree_name") or edu.get("degree") or ""
        field = edu.get("field_of_study") or ""
        school = edu.get("school") or ""
        duration = edu.get("duration") or ""
        edu_text += f"- {degree}"
        if field:
            edu_text += f" in {field}"
        edu_text += f" at {school}"
        if duration:
            edu_text += f" ({duration})"
        edu_text += "\n"

    # Certifications
    certs = raw.get("certifications") or []
    cert_text = ""
    for c in certs:
        cert_text += f"- {c.get('name', '')}"
        if c.get("issuer"):
            cert_text += f" — {c['issuer']}"
        if c.get("issued_date"):
            cert_text += f" ({c['issued_date']})"
        cert_text += "\n"

    about = info.get("about") or ""

    # Build full text
    full_text = f"Name: {name}\nHeadline: {headline}\n"
    if company:
        full_text += f"Current Company: {company}\n"
    if location:
        full_text += f"Location: {location}\n"
    if email:
        full_text += f"Email: {email}\n"
    if info.get("connection_count"):
        full_text += f"Connections: {info['connection_count']}\n"
    if info.get("is_premium"):
        full_text += "LinkedIn Premium: Yes\n"
    if info.get("open_to_work"):
        full_text += "Open to Work: Yes\n"
    if about:
        full_text += f"\n--- About ---\n{about}\n"
    full_text += f"\n--- Experience ---\n{exp_text or 'Not available'}\n"
    full_text += f"\n--- Skills ---\n{skills_text or 'Not available'}\n"
    full_text += f"\n--- Education ---\n{edu_text or 'Not available'}\n"
    if cert_text:
        full_text += f"\n--- Certifications ---\n{cert_text}"

    return {
        "name": name,
        "headline": headline,
        "company": company,
        "location": location,
        "email": email,
        "fullText": full_text,
        "experienceCount": len(experiences),
        "skillCount": len(all_skills),
    }


def parse_legacy_profile(raw: dict) -> dict:
    """Parse supreme_coder / dev_fusion / legacy actor format."""
    name = raw.get("name") or raw.get("fullName") or ""
    if not name and raw.get("firstName"):
        name = f"{raw['firstName']} {raw.get('lastName', '')}".strip()
    headline = raw.get("headline") or raw.get("title") or ""
    location = raw.get("location") or raw.get("addressLocality") or ""

    experiences = raw.get("experience") or raw.get("experiences") or raw.get("positions") or []
    company = ""
    exp_text = ""
    if experiences:
        company = experiences[0].get("company") or experiences[0].get("companyName") or experiences[0].get("organization") or ""
        for exp in experiences:
            title = exp.get("title") or exp.get("role") or exp.get("position") or ""
            comp = exp.get("company") or exp.get("companyName") or exp.get("organization") or ""
            exp_text += f"- {title} at {comp}"
            dur = exp.get("duration") or exp.get("timePeriod") or ""
            if dur:
                exp_text += f" ({dur})"
            exp_text += "\n"
            if exp.get("description"):
                exp_text += f"  {exp['description'][:500]}\n"

    skills = raw.get("skills") or []
    skills_text = ""
    if isinstance(skills, list):
        skill_names = [s if isinstance(s, str) else (s.get("name") or s.get("skill") or "") for s in skills]
        skills_text = ", ".join(s for s in skill_names if s)

    education = raw.get("education") or raw.get("educations") or []
    edu_text = ""
    if isinstance(education, list):
        for edu in education:
            degree = edu.get("degree") or edu.get("degreeName") or ""
            field = edu.get("field") or edu.get("fieldOfStudy") or ""
            school = edu.get("school") or edu.get("schoolName") or edu.get("institution") or ""
            edu_text += f"- {degree}"
            if field:
                edu_text += f" in {field}"
            edu_text += f" at {school}\n"

    full_text = f"Name: {name}\nHeadline: {headline}\n"
    if company:
        full_text += f"Current Company: {company}\n"
    if location:
        full_text += f"Location: {location}\n"
    full_text += f"\n--- Experience ---\n{exp_text or 'Not available'}\n"
    full_text += f"\n--- Skills ---\n{skills_text or 'Not available'}\n"
    full_text += f"\n--- Education ---\n{edu_text or 'Not available'}\n"

    about = raw.get("about") or raw.get("summary") or ""
    if about:
        full_text += f"\n--- About ---\n{about[:1000]}\n"

    return {
        "name": name,
        "headline": headline,
        "company": company,
        "location": location,
        "email": "",
        "fullText": full_text,
        "experienceCount": len(experiences),
        "skillCount": len(skills),
    }


# ── Formatting for Pipeline ─────────────────────────────────────

def build_identity_anchors(profile: dict) -> str:
    """Build pipe-separated identity line for logging."""
    if not profile:
        return ""
    parts = [f"Confirmed via LinkedIn: {profile.get('name', '')}"]
    if profile.get("headline"):
        parts.append(profile["headline"])
    if profile.get("company"):
        parts.append(f"at {profile['company']}")
    if profile.get("location"):
        parts.append(profile["location"])
    return " | ".join(parts)


def build_linkedin_dossier_block(profile: Optional[dict]) -> str:
    """Wrap profile text in tagged block for dossier insertion."""
    if not profile:
        return (
            "\n--- LINKEDIN PROFILE DATA ---\n"
            "LinkedIn data NOT available. Scrape failed or profile not found. "
            "Evaluate based on web research only. Do NOT infer or hallucinate LinkedIn content.\n"
            "--- END LINKEDIN DATA ---\n"
        )
    return (
        "\n--- LINKEDIN PROFILE DATA (via Apify API — verified source) ---\n"
        + profile.get("fullText", "")
        + "\n--- END LINKEDIN DATA ---\n"
    )


# ── Main Entry Point ────────────────────────────────────────────

def get_linkedin_data(
    linkedin_url: str,
    candidate_name: str,
    apify_token: str = "",
) -> Optional[dict]:
    """
    Cache-first LinkedIn data retrieval — mirrors getLinkedInDataForScreening_.

    1. Check cache via Sheets API
    2. If miss, scrape via Apify
    3. Write to cache
    4. Return profile dict or None
    """
    if not linkedin_url or "linkedin.com" not in linkedin_url:
        logger.info("APIFY: No valid LinkedIn URL for %s — skipping", candidate_name)
        return None

    # Step 1: Check cache
    cached = cache_read(linkedin_url)
    if cached:
        logger.info("APIFY: Cache HIT for %s (%s)", candidate_name, linkedin_url)
        return cached

    # Step 2: Scrape
    token = apify_token or os.environ.get("APIFY_TOKEN", "")
    if not token:
        logger.warning("APIFY: No APIFY_TOKEN — cannot scrape %s", candidate_name)
        return None

    logger.info("APIFY: Cache MISS for %s — scraping inline...", candidate_name)
    result = fetch_linkedin_profile(linkedin_url, token)

    # Step 3: Write to cache
    cache_write(linkedin_url, result)

    if result["success"]:
        logger.info("APIFY: Scraped %s — %s at %s",
                     candidate_name,
                     result["data"].get("name", ""),
                     result["data"].get("company", ""))
        return result["data"]

    logger.warning("APIFY: FAILED for %s (%s): %s — proceeding without LinkedIn data",
                    candidate_name, linkedin_url, result["error"])
    return None
