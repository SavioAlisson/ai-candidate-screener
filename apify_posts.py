"""LinkedIn posts via harvestapi/linkedin-profile-posts (Apify).

Cache-first: looks up `.posts_cache.json` by normalized LinkedIn URL.
On miss, fetches via Apify and writes back.

Public functions:
  fetch_posts(linkedin_url: str, max_posts: int = 15) -> list[dict]
      Returns a list of post dicts (own posts + reposts/engagements).
      On any error or no posts, returns [].

  summarize_posts_for_dossier(name, posts) -> str
      Renders the LinkedIn Posts section to append to the dossier.
      Highlights "AI-native receipts" — personal building vs employer affiliation.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.request
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
CACHE_FILE = _DIR / ".posts_cache.json"
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")

# Words that signal personal AI-build / automation (vs just working at an AI co)
AI_RECEIPT_PATTERN = re.compile(
    r"\b(ai|gpt|claude|chatgpt|llm|openai|anthropic|gemini|prompt|automation|"
    r"automate|workflow|n8n|zapier|cursor|copilot|agent|loom|efficiency|"
    r"hours? saved|10x|vibe)\b",
    re.I,
)


def normalize_url(url: str) -> str:
    """Normalize a LinkedIn URL for cache lookup."""
    if not url:
        return ""
    url = url.strip()
    if "linkedin.com" in url and "www." not in url:
        url = url.replace("linkedin.com", "www.linkedin.com")
    return url.rstrip("/")


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
        logger.warning("Posts cache save failed: %s", e)


def _apify_fetch(linkedin_url: str, max_posts: int = 15) -> List[dict]:
    """Call the Apify LinkedIn Posts actor for a single profile URL."""
    if not APIFY_TOKEN:
        logger.warning("APIFY_TOKEN not set — skipping Posts")
        return []

    payload = {"profileUrls": [linkedin_url], "maxPosts": max_posts}
    try:
        # Kick off the run
        start_req = urllib.request.Request(
            f"https://api.apify.com/v2/acts/harvestapi~linkedin-profile-posts/runs?token={APIFY_TOKEN}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(start_req, timeout=30) as r:
            run_data = json.loads(r.read())
        run_id = run_data.get("data", {}).get("id")
        if not run_id:
            return []

        # Poll for completion
        dataset_id = None
        for _ in range(36):  # ~6 min max
            time.sleep(10)
            poll_req = urllib.request.Request(
                f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}",
                method="GET",
            )
            with urllib.request.urlopen(poll_req, timeout=15) as r:
                poll_data = json.loads(r.read())
            status = poll_data.get("data", {}).get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED"):
                dataset_id = poll_data.get("data", {}).get("defaultDatasetId")
                if status != "SUCCEEDED":
                    logger.warning("Posts actor ended with status=%s for %s", status, linkedin_url)
                break

        if not dataset_id:
            return []

        # Fetch the dataset
        ds_req = urllib.request.Request(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&limit=200",
            method="GET",
        )
        with urllib.request.urlopen(ds_req, timeout=30) as r:
            items = json.loads(r.read())
        return items if isinstance(items, list) else []
    except Exception as e:
        logger.warning("Posts API call failed for %s: %s", linkedin_url, e)
        return []


def fetch_posts(linkedin_url: str, max_posts: int = 15) -> List[dict]:
    """Cache-first fetch of a candidate's LinkedIn posts.

    Returns [] on error or if no posts. Never raises.
    """
    if not linkedin_url:
        return []
    key = normalize_url(linkedin_url)
    cache = _load_cache()
    if key in cache:
        return cache[key] or []

    logger.info("Posts: fetching %s (cache miss)", key)
    posts = _apify_fetch(key, max_posts=max_posts)
    cache[key] = posts
    _save_cache(cache)
    return posts


def summarize_posts_for_dossier(name: str, posts: List[dict]) -> str:
    """Render the LinkedIn Posts section for the dossier.

    Distinguishes own posts (personal AI building) from reposts/engagements
    (employer affiliation or amplification).
    """
    if not posts:
        return (
            "=== LINKEDIN POSTS (last 15-20) ===\n"
            "No posts data available for this candidate."
        )

    name_l = (name or "").lower()
    own = [
        p for p in posts
        if (p.get("author") or {}).get("name", "").lower() == name_l
    ]
    others = [
        p for p in posts
        if (p.get("author") or {}).get("name", "").lower() != name_l
    ]

    lines = ["=== LINKEDIN POSTS (last 15-20) ==="]
    lines.append(f"Total posts pulled: {len(posts)} ({len(own)} own, {len(others)} engaged-with).")

    own_ai = [
        (p.get("content") or "")[:300]
        for p in own
        if AI_RECEIPT_PATTERN.search(p.get("content") or "")
    ]
    if own_ai:
        lines.append(f"OWN posts with AI signal ({len(own_ai)}/{len(own)}):")
        for t in own_ai[:5]:
            lines.append(f"  AI ► {t.replace(chr(10), ' ')}")
    else:
        lines.append(f"OWN posts with AI signal: 0/{len(own)}")
        for p in own[:3]:
            sample = (p.get("content") or "")[:200].replace("\n", " ")
            lines.append(f"  (sample) {sample}")

    if others:
        eng_ai = sum(1 for p in others if AI_RECEIPT_PATTERN.search(p.get("content") or ""))
        lines.append(f"Engaged-with posts mentioning AI: {eng_ai}/{len(others)}")

    lines.append(
        "Note: 'OWN posts with AI signal' indicates personal building/tooling. "
        "Pure employer-affiliation posts ('we're hiring at our AI co', reposts "
        "of company milestones) are NOT personal AI building — distinguish carefully."
    )
    return "\n".join(lines)
