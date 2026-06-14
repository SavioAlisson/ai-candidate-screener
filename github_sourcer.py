"""
GitHub Developer Sourcer — finds front-end engineers with public proof of building.

Searches GitHub for developers by tech stack, extracts profile data, repos, LinkedIn URLs,
emails, and other contact info. Outputs CSV ready for the screening pipeline.

Usage:
  python3 github_sourcer.py                                    # default: React/TypeScript FE devs
  python3 github_sourcer.py --query "nextjs developer"         # custom search query
  python3 github_sourcer.py --language TypeScript --location "San Francisco"
  python3 github_sourcer.py --topic react --min-stars 50       # devs with popular React repos
  python3 github_sourcer.py --org vercel                       # all public members + contributors
  python3 github_sourcer.py --repo vercel/next.js --contributors  # contributors to a specific repo
  python3 github_sourcer.py --limit 200                        # max candidates to collect
  python3 github_sourcer.py --output sourced_fe.csv            # custom output file

Modes:
  1. User search (default): Search GitHub users by language, location, followers, bio keywords
  2. Topic search (--topic): Find devs who own repos tagged with a topic (react, nextjs, etc.)
  3. Org mining (--org): Pull members and top contributors from a GitHub organization
  4. Repo contributors (--repo + --contributors): Mine contributors from a specific repo

Required:
  GITHUB_TOKEN env var (personal access token — needed for 5000 req/hr vs 60 unauthenticated)

Output CSV columns:
  name, github_url, linkedin_url, email, website, bio, company, location,
  followers, public_repos, top_repos (JSON), languages, hireable, twitter,
  total_stars, cv_summary
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GITHUB_API = "https://api.github.com"
TOKEN = os.environ.get("GITHUB_TOKEN", "")

ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = ROOT / "github_query_configs"

# Rate limit tracking
_rate_remaining = 5000
_rate_reset = 0


# ── Quality signal lists (ported from WaaS) ──────────────────────
# Match against company / bio / top-repo text. Substring match (case-insensitive).

# Calibration learning (2026-04-28): "any YC alum" is too generous for Klarity's bar.
# Split into AI-first (recruit immediately) vs broader YC (screen but lower priority).
YC_AI_FIRST = {
    "anthropic", "openai", "perplexity", "cohere", "hugging face", "sierra",
    "pinecone", "langchain", "vercel", "cursor", "replit", "modal",
    "cartesia", "paradigm", "topo", "lindy", "inventive ai",
    "scale ai", "scale",
}
YC_OTHER = {
    "airbnb", "stripe", "doordash", "coinbase", "instacart", "dropbox", "reddit",
    "twitch", "brex", "gusto", "flexport", "faire", "rippling", "whatnot",
    "retool", "vanta", "deel", "mercury", "razorpay",
    "cruise", "zapier", "segment", "amplitude", "notion", "figma", "linear",
    "ramp", "workos", "clerk", "resend", "supabase", "neon", "planetscale",
}
YC_ALUM = YC_AI_FIRST | YC_OTHER  # union, kept for backward compat
# AI / ML / document / enterprise patterns — must appear alongside big-tech senior
# title for that to count as a Tier-0 signal. Keeps "Senior at Microsoft Word team"
# from clearing the bar while letting "Senior at Google AI" through.
AI_SIGNAL_PATTERNS = [
    "ai", "ml", "machine learning", "llm", "language model", "agents",
    "rag", "embedding", "transformer", "diffusion", "generative",
    "document", "ocr", "extraction", "nlp",
]
# Design-engineer archetype titles — for senior_design_engineer signal
DESIGN_ENGINEER_TITLE_PATTERNS = [
    "design engineer", "ui engineer", "design systems engineer",
    "design system engineer", "interface engineer", "front-end designer",
    "frontend designer",
]
BIG_TECH = {
    "google", "meta", "facebook", "apple", "amazon", "microsoft", "netflix",
    "nvidia", "tesla", "uber", "lyft", "palantir", "snowflake", "databricks",
    "mongodb", "shopify", "atlassian", "cloudflare", "datadog", "elastic",
    "twilio",
}

# Title patterns — penalty (down-rank, not hard reject — bio text is noisy)
NEGATIVE_TITLE_PATTERNS = [
    "qa engineer", "test engineer", "test automation", "automation engineer", "sdet",
    "java full stack", "java developer", "java backend", "j2ee", "spring boot developer",
    "salesforce developer", "salesforce admin", "sap consultant",
    ".net developer", "asp.net", "wordpress developer", "drupal developer",
    "android developer", "ios developer",
    "devrel", "developer relations", "developer advocate",
    "data scientist", "ml researcher", "data analyst",
    "it support", "system administrator", "network engineer",
    "student", "intern", "looking for", "open to work",
]
# Company-name substrings that signal IT consulting / freelance / agency / generic offshore body-shop.
NEGATIVE_COMPANY_PATTERNS = [
    "consult", "freelanc", "upwork", "fiverr", "remoterep", "contractor",
    "services pvt", "solutions pvt", "infotech", "systems ltd", "it services",
    "java full stack", "wordpress", "drupal", ".net",
    "infosys", "tcs", "tata consultancy", "wipro", "cognizant", "accenture", "capgemini", "hcl",
    "agency", "creative studio",
]
POSITIVE_TITLE_PATTERNS = [
    "founding", "staff", "principal", "tech lead", "design engineer",
    "first engineer", "co-founder", "founder", "engineering lead",
]
SENIOR_TITLE_PATTERNS = ["staff", "principal", "senior", "lead"]
# Executive / leadership titles — strong signal at a named co even without "staff/principal"
EXEC_TITLE_PATTERNS = [
    "vp of", "svp of", "evp of", "vp ", "svp ", "evp ",
    "head of engineering", "head of product", "director of engineering",
    "cto", "ceo", "chief technology", "chief product",
]


def _word_match(needle: str, haystack: str) -> bool:
    """Substring match with word boundaries — avoids 'scale' matching 'scalable'."""
    if not needle or not haystack:
        return False
    # Allow phrases (with spaces) and tokens with dots ('next.js'). Escape, then ensure boundary.
    pattern = r"(?:^|[^a-z0-9])" + re.escape(needle.lower()) + r"(?:[^a-z0-9]|$)"
    return re.search(pattern, haystack.lower()) is not None


# ── HTTP helpers ──────────────────────────────────────────────────

def _github_get(endpoint: str, params: Optional[dict] = None, accept: str = "application/vnd.github+json") -> Tuple[int, Any]:
    """Make authenticated GET request to GitHub API with rate limit handling."""
    global _rate_remaining, _rate_reset

    url = endpoint if endpoint.startswith("http") else f"{GITHUB_API}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    headers = {
        "Accept": accept,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"

    # Respect rate limits
    if _rate_remaining < 5 and _rate_reset > time.time():
        wait = _rate_reset - time.time() + 1
        logger.warning(f"Rate limit near zero, waiting {wait:.0f}s...")
        time.sleep(wait)

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _rate_remaining = int(resp.headers.get("X-RateLimit-Remaining", 5000))
            _rate_reset = int(resp.headers.get("X-RateLimit-Reset", 0))
            body = resp.read().decode("utf-8", errors="replace")
            if not body:
                return resp.status, {}
            # Raw content requests return plain text, not JSON
            if "raw" in accept:
                return resp.status, body
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body  # return as string if not JSON
    except urllib.error.HTTPError as e:
        _rate_remaining = int(e.headers.get("X-RateLimit-Remaining", _rate_remaining))
        _rate_reset = int(e.headers.get("X-RateLimit-Reset", _rate_reset))
        body = e.read().decode("utf-8", errors="replace")
        if e.code == 403 and "rate limit" in body.lower():
            wait = max(_rate_reset - time.time(), 60)
            logger.warning(f"Rate limited. Waiting {wait:.0f}s...")
            time.sleep(wait)
            return _github_get(endpoint, params, accept)  # retry once
        if e.code == 404:
            return e.code, {}  # expected for missing profile READMEs etc.
        logger.error(f"GitHub API {e.code}: {body[:200]}")
        return e.code, {}
    except Exception as e:
        logger.error(f"Request failed: {e}")
        return 0, {}


def _paginate(endpoint: str, params: dict, max_items: int, key: Optional[str] = None) -> List[dict]:
    """Paginate through GitHub API results."""
    items = []
    params = {**params, "per_page": min(100, max_items)}
    page = 1

    while len(items) < max_items:
        params["page"] = page
        status, data = _github_get(endpoint, params)
        if status != 200:
            break

        batch = data.get(key, []) if key else data
        if not batch:
            break

        items.extend(batch)
        page += 1

        # GitHub search API caps at 1000 results
        if key and data.get("total_count", 0) <= len(items):
            break
        if len(batch) < params["per_page"]:
            break

    return items[:max_items]


# ── URL extraction helpers ────────────────────────────────────────

def _extract_linkedin_url(text: str) -> str:
    """Extract LinkedIn URL from any text (bio, blog, README)."""
    if not text:
        return ""
    m = re.search(r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]+/?", text)
    return m.group(0).rstrip("/") if m else ""


def _extract_urls(text: str) -> Dict[str, str]:
    """Extract categorized URLs from text."""
    urls = {"linkedin": "", "twitter": "", "website": "", "other": []}
    if not text:
        return urls

    for m in re.finditer(r"https?://[^\s\)\]\"'>]+", text):
        url = m.group(0).rstrip(".,;)")
        lower = url.lower()
        if "linkedin.com/in/" in lower:
            urls["linkedin"] = url.rstrip("/")
        elif "twitter.com/" in lower or "x.com/" in lower:
            urls["twitter"] = url
        elif "github.com" not in lower:
            if not urls["website"]:
                urls["website"] = url
            else:
                urls["other"].append(url)
    return urls


# ── US location detection ────────────────────────────────────────

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
}

US_STATE_ABBREVS = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi",
    "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi",
    "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc",
    "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut",
    "vt", "va", "wa", "wv", "wi", "wy", "dc",
}

US_CITIES = {
    "san francisco", "new york", "los angeles", "chicago", "seattle",
    "austin", "boston", "denver", "portland", "atlanta", "miami",
    "dallas", "houston", "phoenix", "san diego", "san jose",
    "philadelphia", "minneapolis", "nashville", "raleigh", "charlotte",
    "detroit", "pittsburgh", "brooklyn", "manhattan", "palo alto",
    "mountain view", "sunnyvale", "cupertino", "menlo park",
    "redwood city", "oakland", "berkeley", "santa monica", "santa clara",
    "ann arbor", "boulder", "salt lake city", "des moines",
    "sf bay area", "bay area", "silicon valley", "nyc",
    "washington d.c.", "washington, d.c.",
}

US_COUNTRY_KEYWORDS = {"usa", "united states", "u.s.a.", "u.s.", "us"}


def _is_us_location(location: str) -> bool:
    """Check if a GitHub location string indicates a US-based person."""
    if not location:
        return False
    loc = location.strip().lower()

    # Direct country match
    for kw in US_COUNTRY_KEYWORDS:
        if kw in loc:
            return True

    # City match
    for city in US_CITIES:
        if city in loc:
            return True

    # State match (full name)
    for state in US_STATES:
        if state in loc:
            return True

    # State abbreviation match (e.g. "SF, CA" or "Austin, TX")
    # Look for ", XX" or " XX" at the end where XX is a state abbreviation
    parts = re.split(r"[,\s]+", loc)
    for part in parts:
        if part in US_STATE_ABBREVS:
            return True

    return False


# ── Website scraping for LinkedIn URLs ───────────────────────────

def _scrape_website_for_linkedin(url: str) -> str:
    """Fetch a personal website and extract LinkedIn URL from it."""
    if not url:
        return ""
    # Skip known non-personal sites
    skip_domains = ["github.com", "twitter.com", "x.com", "youtube.com",
                    "medium.com", "dev.to", "npmjs.com", "pypi.org"]
    for d in skip_domains:
        if d in url.lower():
            return ""

    try:
        if not url.startswith("http"):
            url = "https://" + url
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="replace")[:50000]
        return _extract_linkedin_url(html)
    except Exception:
        return ""


# ── Email extraction from commits ─────────────────────────────────

def _get_email_from_commits(username: str) -> str:
    """Try to get email from user's recent public commits."""
    status, events = _github_get(f"/users/{username}/events/public", {"per_page": 30})
    if status != 200 or not isinstance(events, list):
        return ""

    for event in events:
        if event.get("type") != "PushEvent":
            continue
        for commit in event.get("payload", {}).get("commits", []):
            email = commit.get("author", {}).get("email", "")
            if email and "noreply" not in email.lower() and "@" in email:
                return email
    return ""


# ── Profile README extraction ─────────────────────────────────────

def _get_profile_readme(username: str) -> str:
    """Fetch the user's profile README (username/username repo)."""
    # Try raw content first
    status, data = _github_get(
        f"/repos/{username}/{username}/readme",
        accept="application/vnd.github.raw+json"
    )
    if status != 200:
        return ""
    if isinstance(data, str):
        return data[:3000]
    # Fallback: base64-encoded content in JSON response
    if isinstance(data, dict):
        import base64
        content = data.get("content", "")
        if content:
            try:
                return base64.b64decode(content).decode("utf-8", errors="replace")[:3000]
            except Exception:
                pass
    return ""


# ── Core profile builder ──────────────────────────────────────────

def _build_profile(user: dict, fetch_extras: bool = True) -> Dict[str, Any]:
    """Build a complete developer profile from GitHub data."""
    username = user.get("login", "")
    profile = {
        "name": user.get("name", "") or username,
        "github_url": f"https://github.com/{username}",
        "github_username": username,
        "email": user.get("email", ""),
        "linkedin_url": "",
        "website": user.get("blog", ""),
        "bio": user.get("bio", "") or "",
        "company": user.get("company", "") or "",
        "location": user.get("location", "") or "",
        "followers": user.get("followers", 0),
        "public_repos": user.get("public_repos", 0),
        "hireable": user.get("hireable", False),
        "twitter": user.get("twitter_username", "") or "",
        "top_repos": [],
        "languages": set(),
        "total_stars": 0,
        "cv_summary": "",
    }

    # Extract LinkedIn from blog URL or bio
    for text in [profile["website"], profile["bio"]]:
        li = _extract_linkedin_url(text)
        if li:
            profile["linkedin_url"] = li
            break

    if not fetch_extras:
        return profile

    # Full user profile (search results are partial)
    if "followers" not in user or user.get("followers") is None:
        status, full_user = _github_get(f"/users/{username}")
        if status == 200:
            profile["name"] = full_user.get("name", "") or username
            profile["email"] = full_user.get("email", "") or profile["email"]
            profile["bio"] = full_user.get("bio", "") or profile["bio"]
            profile["company"] = full_user.get("company", "") or profile["company"]
            profile["location"] = full_user.get("location", "") or profile["location"]
            profile["followers"] = full_user.get("followers", 0)
            profile["public_repos"] = full_user.get("public_repos", 0)
            profile["hireable"] = full_user.get("hireable", False)
            profile["twitter"] = full_user.get("twitter_username", "") or profile["twitter"]
            profile["website"] = full_user.get("blog", "") or profile["website"]

            # Re-check for LinkedIn
            for text in [profile["website"], profile["bio"]]:
                li = _extract_linkedin_url(text)
                if li:
                    profile["linkedin_url"] = li
                    break

    # Profile README — rich source of LinkedIn, portfolio, CV info
    readme = _get_profile_readme(username)
    if readme:
        urls = _extract_urls(readme)
        if not profile["linkedin_url"] and urls["linkedin"]:
            profile["linkedin_url"] = urls["linkedin"]
        if not profile["website"] and urls["website"]:
            profile["website"] = urls["website"]
        if not profile["twitter"] and urls["twitter"]:
            profile["twitter"] = urls["twitter"]

    # Scrape personal website for LinkedIn URL if still missing
    if not profile["linkedin_url"] and profile["website"]:
        li = _scrape_website_for_linkedin(profile["website"])
        if li:
            profile["linkedin_url"] = li

    # Email from commits if not in profile
    if not profile["email"]:
        profile["email"] = _get_email_from_commits(username)

    # Top repos by stars
    status, repos = _github_get(f"/users/{username}/repos", {
        "sort": "stars", "direction": "desc", "per_page": 10, "type": "owner"
    })
    if status == 200 and isinstance(repos, list):
        for repo in repos:
            stars = repo.get("stargazers_count", 0)
            lang = repo.get("language", "")
            if lang:
                profile["languages"].add(lang)
            profile["total_stars"] += stars
            if len(profile["top_repos"]) < 5:
                profile["top_repos"].append({
                    "name": repo.get("name", ""),
                    "description": (repo.get("description", "") or "")[:200],
                    "stars": stars,
                    "language": lang,
                    "url": repo.get("html_url", ""),
                    "fork": repo.get("fork", False),
                    "topics": repo.get("topics", []),
                })

    # Build CV summary from available data
    profile["cv_summary"] = _build_cv_summary(profile, readme)

    return profile


def _build_cv_summary(profile: dict, readme: str = "") -> str:
    """Build a text CV summary from profile data for the screening pipeline."""
    parts = []

    if profile["name"]:
        parts.append(f"Name: {profile['name']}")
    if profile["company"]:
        parts.append(f"Company: {profile['company']}")
    if profile["location"]:
        parts.append(f"Location: {profile['location']}")
    if profile["bio"]:
        parts.append(f"Bio: {profile['bio']}")

    # GitHub stats
    stats = []
    if profile["total_stars"]:
        stats.append(f"{profile['total_stars']} total stars")
    if profile["followers"]:
        stats.append(f"{profile['followers']} followers")
    if profile["public_repos"]:
        stats.append(f"{profile['public_repos']} public repos")
    if stats:
        parts.append(f"GitHub: {', '.join(stats)}")

    # Languages
    langs = profile.get("languages", set())
    if isinstance(langs, set):
        langs = sorted(langs)
    if langs:
        parts.append(f"Languages: {', '.join(langs)}")

    # Top repos
    repos = profile.get("top_repos", [])
    if repos:
        repo_lines = []
        for r in repos[:5]:
            line = f"  - {r['name']}"
            if r.get("stars"):
                line += f" ({r['stars']}★)"
            if r.get("description"):
                line += f": {r['description']}"
            if r.get("topics"):
                line += f" [{', '.join(r['topics'][:5])}]"
            repo_lines.append(line)
        parts.append("Top repos:\n" + "\n".join(repo_lines))

    # README excerpt (first meaningful paragraph)
    if readme:
        # Strip badges, images, links-only lines
        lines = []
        for line in readme.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("![") or stripped.startswith("<img"):
                continue
            if re.match(r"^[\[\!<].*$", stripped) and len(stripped) < 20:
                continue
            if stripped.startswith("#"):
                continue
            lines.append(stripped)
        if lines:
            excerpt = " ".join(lines[:5])[:500]
            parts.append(f"README: {excerpt}")

    # URLs
    urls = []
    if profile.get("linkedin_url"):
        urls.append(f"LinkedIn: {profile['linkedin_url']}")
    if profile.get("website"):
        urls.append(f"Website: {profile['website']}")
    if profile.get("twitter"):
        twitter = profile["twitter"]
        if not twitter.startswith("http"):
            twitter = f"https://twitter.com/{twitter}"
        urls.append(f"Twitter: {twitter}")
    if urls:
        parts.append("\n".join(urls))

    return "\n".join(parts)


# ── Scoring & tier ──────────────────────────────────────────────
# Tier 0 = at least one strong builder/employer signal (recruit-immediately bucket)
# Tier 1 = passed filters but no standout signal (screen if Tier 0 doesn't fill batch)

def _company_clean(raw: str) -> str:
    """GitHub `company` often comes as '@Stripe' — strip the @, lowercase."""
    return (raw or "").lstrip("@").strip().lower()


def _profile_text_blob(profile: dict) -> str:
    """All searchable text on a profile (bio + repo descriptions + topics + repo names)."""
    parts = [profile.get("bio", "") or "", profile.get("company", "") or ""]
    for r in profile.get("top_repos", []) or []:
        parts.append(r.get("name", "") or "")
        parts.append(r.get("description", "") or "")
        parts.extend(r.get("topics", []) or [])
    return " ".join(parts).lower()


def quality_signals(profile: dict) -> Dict[str, bool]:
    """Identify builder/employer signals from a GitHub profile.

    v3 (2026-04-28): split into strict (Tier 0) vs weak (Tier 1) signals.
    Strict signals predict SCREEN; weak signals predict 'worth a look'.
    Calibrated against the 5/5 DECLINE batch on 2026-04-28 — senior@Framer/LEGO
    cleared the v2 bar but failed Klarity's outbound bar."""
    company = _company_clean(profile.get("company", ""))
    bio = (profile.get("bio", "") or "").lower()
    blob = _profile_text_blob(profile)
    is_negative_co = any(_word_match(pat, company) for pat in NEGATIVE_COMPANY_PATTERNS) if company else False

    company_is_yc_ai = bool(company) and any(_word_match(yc, company) for yc in YC_AI_FIRST)
    company_is_yc_other = bool(company) and any(_word_match(yc, company) for yc in YC_OTHER)
    company_is_big_tech = bool(company) and any(_word_match(bt, company) for bt in BIG_TECH)

    has_senior_title = (
        any(_word_match(t, bio) for t in SENIOR_TITLE_PATTERNS)
        or any(et in bio for et in EXEC_TITLE_PATTERNS)
    )
    has_founding_title = any(_word_match(t, bio) for t in ("founding", "first engineer", "co-founder", "founder"))
    has_ai_signal = any(_word_match(p, blob) for p in AI_SIGNAL_PATTERNS)

    # ── Tier 0 (strict — recruit-immediately quality) ──
    yc_ai_alum = company_is_yc_ai
    big_tech_senior_ai = company_is_big_tech and has_senior_title and has_ai_signal
    founding_at_real_co = has_founding_title and bool(company) and not is_negative_co
    popular_oss_strong = (profile.get("total_stars") or 0) >= 5000

    # ── Tier 1 (weak — worth screening, lower priority) ──
    yc_alum_other = company_is_yc_other
    senior_at_named_co = has_senior_title and bool(company) and not is_negative_co
    big_tech_senior = company_is_big_tech and has_senior_title  # without AI signal
    popular_oss_medium = 1000 <= (profile.get("total_stars") or 0) < 5000
    is_design_engineer = (
        any(pat in bio for pat in DESIGN_ENGINEER_TITLE_PATTERNS)
        and bool(company) and not is_negative_co
    )

    return {
        # Tier 0
        "yc_ai_alum": yc_ai_alum,
        "founding_at_real_co": founding_at_real_co,
        "big_tech_senior_ai": big_tech_senior_ai,
        "popular_oss_strong": popular_oss_strong,
        # Tier 1
        "yc_alum_other": yc_alum_other,
        "big_tech_senior": big_tech_senior,
        "senior_at_named_co": senior_at_named_co,
        "popular_oss_medium": popular_oss_medium,
        "design_engineer_role": is_design_engineer,
    }


# Which signals trigger Tier 0 vs Tier 1. Anything not in either is ignored for tiering.
TIER0_SIGNALS = {"yc_ai_alum", "founding_at_real_co", "big_tech_senior_ai", "popular_oss_strong"}
TIER1_SIGNALS = {"yc_alum_other", "big_tech_senior", "senior_at_named_co", "popular_oss_medium", "design_engineer_role"}


def score_profile(
    profile: dict,
    boost_keywords: Optional[List[str]] = None,
    negative_co: Optional[List[str]] = None,
) -> Tuple[float, int, Dict[str, bool]]:
    """Return (score, tier, signals)."""
    boost_keywords = boost_keywords or []
    negative_co = negative_co or []
    s = 0.0

    company = _company_clean(profile.get("company", ""))
    bio = (profile.get("bio", "") or "").lower()
    blob = _profile_text_blob(profile)

    is_negative_co = bool(company) and (
        any(_word_match(pat, company) for pat in NEGATIVE_COMPANY_PATTERNS)
        or any(_word_match(pat, company) for pat in negative_co)
    )

    # Employer signals — v3 weights skewed toward AI-first YC + AI-adjacent big tech
    blob_lc = _profile_text_blob(profile)
    has_ai_signal = any(_word_match(p, blob_lc) for p in AI_SIGNAL_PATTERNS)
    is_senior = (
        any(_word_match(sr, bio) for sr in SENIOR_TITLE_PATTERNS)
        or any(et in bio for et in EXEC_TITLE_PATTERNS)
    )
    if company:
        if any(_word_match(yc, company) for yc in YC_AI_FIRST):
            s += 6
        elif any(_word_match(yc, company) for yc in YC_OTHER):
            s += 3
        elif any(_word_match(bt, company) for bt in BIG_TECH):
            if is_senior and has_ai_signal:
                s += 5  # tier-0 quality
            elif is_senior:
                s += 2  # tier-1
            else:
                s += 1
        if is_negative_co:
            s -= 4

    # Title signals (read from bio, since GitHub has no positions)
    has_positive_title = (
        any(_word_match(kw, bio) for kw in POSITIVE_TITLE_PATTERNS)
        or any(et in bio for et in EXEC_TITLE_PATTERNS)
    )
    if has_positive_title and not is_negative_co:
        s += 1.5
    if any(_word_match(neg, bio) for neg in NEGATIVE_TITLE_PATTERNS):
        s -= 4
    if any(_word_match(neg, company) for neg in NEGATIVE_TITLE_PATTERNS):
        s -= 4

    # Builder proof — v3: 5k+ is the new "strong" threshold (was 1k).
    stars = profile.get("total_stars") or 0
    if stars >= 5000:
        s += 3.0
    elif stars >= 1000:
        s += 1.5
    elif stars >= 200:
        s += 0.5

    # Followers — weak proxy for reach
    followers = profile.get("followers") or 0
    if followers >= 1000:
        s += 0.5

    # Boost keywords (tech stack match)
    for bk in (boost_keywords or []):
        if bk.lower() in blob:
            s += 0.6

    sigs = quality_signals(profile)
    if any(sigs.get(k) for k in TIER0_SIGNALS):
        tier = 0
    elif any(sigs.get(k) for k in TIER1_SIGNALS):
        tier = 1
    else:
        tier = 2  # no signal — sort to the bottom
    return round(s, 2), tier, sigs


# ── Config loader ────────────────────────────────────────────────

def load_config(path_or_name: str) -> Dict[str, Any]:
    """Load a JSON archetype config. Accepts a filename, a role_key, or a full path."""
    p = Path(path_or_name)
    if not p.exists():
        candidate = CONFIGS_DIR / path_or_name
        if candidate.exists():
            p = candidate
        elif (CONFIGS_DIR / f"{path_or_name}.json").exists():
            p = CONFIGS_DIR / f"{path_or_name}.json"
        else:
            raise FileNotFoundError(f"Config not found: {path_or_name}")
    return json.loads(p.read_text())


# ── Search modes ──────────────────────────────────────────────────

def search_users(
    query: str = "",
    language: str = "",
    location: str = "",
    min_followers: int = 0,
    min_repos: int = 0,
    limit: int = 100,
) -> List[dict]:
    """Search GitHub users by criteria."""
    q_parts = []
    if query:
        q_parts.append(query)
    if language:
        q_parts.append(f"language:{language}")
    if location:
        q_parts.append(f"location:{location}")
    if min_followers:
        q_parts.append(f"followers:>={min_followers}")
    if min_repos:
        q_parts.append(f"repos:>={min_repos}")

    if not q_parts:
        q_parts = ["language:TypeScript", "language:JavaScript"]

    q = " ".join(q_parts)
    logger.info(f"Searching users: {q} (limit={limit})")

    users = _paginate(
        "/search/users",
        {"q": q, "sort": "followers", "order": "desc"},
        max_items=limit,
        key="items",
    )
    logger.info(f"Found {len(users)} users")
    return users


def search_by_topic(
    topic: str,
    language: str = "",
    min_stars: int = 10,
    limit: int = 100,
) -> List[dict]:
    """Find developers who own repos tagged with a specific topic."""
    q_parts = [f"topic:{topic}"]
    if language:
        q_parts.append(f"language:{language}")
    q_parts.append(f"stars:>={min_stars}")

    q = " ".join(q_parts)
    logger.info(f"Searching repos by topic: {q}")

    repos = _paginate(
        "/search/repositories",
        {"q": q, "sort": "stars", "order": "desc"},
        max_items=limit * 2,  # fetch more repos since we dedupe by owner
        key="items",
    )

    # Dedupe by owner
    seen: Set[str] = set()
    owners = []
    for repo in repos:
        owner = repo.get("owner", {})
        login = owner.get("login", "")
        if login and login not in seen and owner.get("type") == "User":
            seen.add(login)
            owners.append(owner)
        if len(owners) >= limit:
            break

    logger.info(f"Found {len(owners)} unique developers from {len(repos)} repos")
    return owners


def search_org_members(org: str, limit: int = 100) -> List[dict]:
    """Get public members of a GitHub organization."""
    logger.info(f"Fetching members of org: {org}")
    members = _paginate(f"/orgs/{org}/members", {"per_page": 100}, max_items=limit)
    logger.info(f"Found {len(members)} public members")
    return members


def search_repo_contributors(repo_full_name: str, limit: int = 100) -> List[dict]:
    """Get contributors to a specific repository."""
    logger.info(f"Fetching contributors for: {repo_full_name}")
    contributors = _paginate(
        f"/repos/{repo_full_name}/contributors",
        {"per_page": 100},
        max_items=limit,
    )
    # Filter out bots
    contributors = [c for c in contributors if c.get("type") == "User"]
    logger.info(f"Found {len(contributors)} contributors")
    return contributors


# ── Multi-query front-end search ──────────────────────────────────

# Pre-built front-end search strategies
FE_SEARCH_STRATEGIES = [
    # Topic-based: find owners of popular FE repos
    {"mode": "topic", "topic": "react", "min_stars": 20},
    {"mode": "topic", "topic": "nextjs", "min_stars": 15},
    {"mode": "topic", "topic": "frontend", "min_stars": 15},
    {"mode": "topic", "topic": "design-system", "min_stars": 10},
    {"mode": "topic", "topic": "tailwindcss", "min_stars": 10},
    {"mode": "topic", "topic": "typescript", "min_stars": 30},
    # Org mining: prolific FE orgs
    {"mode": "org", "org": "vercel"},
    {"mode": "org", "org": "facebook"},  # React core team alumni
    {"mode": "org", "org": "radix-ui"},
    {"mode": "org", "org": "shadcn-ui"},
    # Repo contributors: foundational FE projects
    {"mode": "contributors", "repo": "vercel/next.js"},
    {"mode": "contributors", "repo": "facebook/react"},
    {"mode": "contributors", "repo": "tailwindlabs/tailwindcss"},
    {"mode": "contributors", "repo": "shadcn-ui/ui"},
    {"mode": "contributors", "repo": "radix-ui/primitives"},
    {"mode": "contributors", "repo": "TanStack/query"},
    # User search: FE devs by language + bio keywords (US-focused)
    {"mode": "user", "query": "react frontend engineer", "language": "TypeScript", "location": "US"},
    {"mode": "user", "query": "frontend developer", "language": "JavaScript", "location": "San Francisco"},
    {"mode": "user", "query": "frontend developer", "language": "JavaScript", "location": "New York"},
    {"mode": "user", "query": "react developer", "language": "TypeScript", "location": "Seattle"},
    {"mode": "user", "query": "frontend engineer", "language": "TypeScript", "location": "Austin"},
]


def run_fe_search(limit: int = 200, strategies: Optional[List[dict]] = None) -> List[dict]:
    """Run multiple search strategies and dedupe results. Returns raw user objects."""
    strategies = strategies or FE_SEARCH_STRATEGIES
    seen: Set[str] = set()
    all_users = []
    per_strategy = max(30, limit // len(strategies))

    for i, strat in enumerate(strategies):
        mode = strat.get("mode", "user")
        logger.info(f"Strategy {i+1}/{len(strategies)}: {strat}")

        try:
            if mode == "topic":
                users = search_by_topic(
                    strat["topic"],
                    language=strat.get("language", ""),
                    min_stars=strat.get("min_stars", 10),
                    limit=per_strategy,
                )
            elif mode == "org":
                users = search_org_members(strat["org"], limit=per_strategy)
            elif mode == "contributors":
                users = search_repo_contributors(strat["repo"], limit=per_strategy)
            elif mode == "user":
                users = search_users(
                    query=strat.get("query", ""),
                    language=strat.get("language", ""),
                    location=strat.get("location", ""),
                    min_followers=strat.get("min_followers", 0),
                    limit=per_strategy,
                )
            else:
                continue

            for u in users:
                login = u.get("login", "")
                if login and login not in seen:
                    seen.add(login)
                    all_users.append(u)

        except Exception as e:
            logger.error(f"Strategy {strat} failed: {e}")
            continue

        if len(all_users) >= limit:
            break

        # Brief pause between strategies to be respectful
        time.sleep(0.5)

    logger.info(f"Total unique candidates: {len(all_users)} (limit={limit})")
    return all_users[:limit]


# ── CSV output ────────────────────────────────────────────────────

CSV_COLUMNS = [
    "tier", "score", "signals",
    "name", "github_url", "linkedin_url", "email", "website", "bio",
    "company", "location", "followers", "public_repos", "top_repos",
    "languages", "hireable", "twitter", "total_stars", "cv_summary",
]


def write_csv(profiles: List[dict], output_path: str) -> str:
    """Write profiles to CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for p in profiles:
            row = {**p}
            # Serialize complex fields
            if isinstance(row.get("top_repos"), list):
                row["top_repos"] = json.dumps(row["top_repos"])
            if isinstance(row.get("languages"), (set, list)):
                row["languages"] = ", ".join(sorted(row["languages"])) if row["languages"] else ""
            writer.writerow(row)
    return output_path


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GitHub Developer Sourcer — find front-end engineers")
    parser.add_argument("--query", "-q", default="", help="Search query for GitHub users")
    parser.add_argument("--language", "-l", default="", help="Programming language filter")
    parser.add_argument("--location", default="", help="Location filter")
    parser.add_argument("--topic", "-t", default="", help="Search by repo topic (react, nextjs, etc.)")
    parser.add_argument("--org", default="", help="Mine members from a GitHub organization")
    parser.add_argument("--repo", default="", help="Repository (owner/name) for contributor mining")
    parser.add_argument("--contributors", action="store_true", help="Mine contributors from --repo")
    parser.add_argument("--min-stars", type=int, default=10, help="Min stars for topic search repos")
    parser.add_argument("--min-followers", type=int, default=0, help="Min followers for user search")
    parser.add_argument("--limit", type=int, default=200, help="Max candidates to collect")
    parser.add_argument("--output", "-o", default="", help="Output CSV path")
    parser.add_argument("--fe", action="store_true", help="Run pre-built front-end search strategies")
    parser.add_argument("--config", default="", help="Archetype config name or path (e.g. frontend_engineer or design_engineer). Runs all strategies in the config and scores+ranks results.")
    parser.add_argument("--us-only", action="store_true", help="Only keep candidates with US-based locations")
    parser.add_argument("--require-linkedin", action="store_true", help="Only output candidates with LinkedIn URLs")
    parser.add_argument("--tier-zero-only", action="store_true", help="Drop Tier 1 candidates (no quality signal) entirely")
    parser.add_argument("--skip-extras", action="store_true", help="Skip email/readme extraction (faster)")
    parser.add_argument("--dry-run", action="store_true", help="Search only, don't fetch full profiles")
    args = parser.parse_args()

    if not TOKEN:
        logger.warning("GITHUB_TOKEN not set — rate limit is 60 req/hr (vs 5000 with token)")
        logger.warning("Set: export GITHUB_TOKEN=ghp_your_token_here")

    # Load archetype config if provided — overrides default --fe behavior, applies its
    # boost/negative lists during scoring, and (when set) auto-applies require_linkedin.
    cfg: Dict[str, Any] = {}
    boost_keywords: List[str] = []
    negative_co: List[str] = []
    if args.config:
        cfg = load_config(args.config)
        boost_keywords = cfg.get("boost_keywords", []) or []
        negative_co = cfg.get("negative_company_substrings", []) or []
        cfg_filters = cfg.get("filters", {}) or {}
        if cfg_filters.get("require_linkedin"):
            args.require_linkedin = True
        logger.info(f"Loaded config: {cfg.get('role_key', args.config)} "
                    f"({len(cfg.get('strategies', []))} strategies, "
                    f"{len(boost_keywords)} boost keywords)")

    # Determine search mode
    if cfg:
        raw_users = run_fe_search(limit=args.limit, strategies=cfg.get("strategies", []))
    elif args.fe or (not args.query and not args.topic and not args.org and not args.repo):
        logger.info("Running pre-built front-end search strategies...")
        raw_users = run_fe_search(limit=args.limit)
    elif args.topic:
        raw_users = search_by_topic(args.topic, args.language, args.min_stars, args.limit)
    elif args.org:
        raw_users = search_org_members(args.org, args.limit)
    elif args.repo and args.contributors:
        raw_users = search_repo_contributors(args.repo, args.limit)
    else:
        raw_users = search_users(
            query=args.query,
            language=args.language,
            location=args.location,
            min_followers=args.min_followers,
            limit=args.limit,
        )

    if not raw_users:
        logger.error("No candidates found. Try different search criteria.")
        sys.exit(1)

    if args.dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN: Found {len(raw_users)} candidates")
        print(f"{'='*60}")
        for i, u in enumerate(raw_users[:20], 1):
            login = u.get("login", "?")
            print(f"  {i:3}. {login:30} https://github.com/{login}")
        if len(raw_users) > 20:
            print(f"  ... and {len(raw_users) - 20} more")
        return

    # Build full profiles
    profiles = []
    fetch_extras = not args.skip_extras
    total = len(raw_users)

    print(f"\nBuilding profiles for {total} candidates...")
    for i, user in enumerate(raw_users, 1):
        login = user.get("login", "?")
        try:
            profile = _build_profile(user, fetch_extras=fetch_extras)
            profiles.append(profile)

            # Progress
            li = " +LI" if profile["linkedin_url"] else ""
            em = " +email" if profile["email"] else ""
            stars = f" {profile['total_stars']}★" if profile["total_stars"] else ""
            print(f"  [{i}/{total}] {profile['name']:30}{stars}{li}{em}  ({_rate_remaining} API calls left)")

        except Exception as e:
            logger.error(f"  [{i}/{total}] {login}: {e}")
            continue

    # Filter: US-only
    if args.us_only:
        before = len(profiles)
        profiles = [p for p in profiles if _is_us_location(p.get("location", ""))]
        filtered_loc = before - len(profiles)
        print(f"\n  US filter: kept {len(profiles)}, removed {filtered_loc} non-US/unknown")

    # Filter: require LinkedIn
    if args.require_linkedin:
        before = len(profiles)
        profiles = [p for p in profiles if p.get("linkedin_url")]
        filtered_li = before - len(profiles)
        print(f"  LinkedIn filter: kept {len(profiles)}, removed {filtered_li} without LinkedIn")

    if not profiles:
        print("\n  No candidates passed filters. Try broader search or remove filters.")
        sys.exit(0)

    # Score every profile (tier 0/1 + numeric score + signal flags)
    for p in profiles:
        s, t, sigs = score_profile(p, boost_keywords=boost_keywords, negative_co=negative_co)
        p["score"] = s
        p["tier"] = t
        # Compact signal string for the CSV (only the True ones)
        p["signals"] = ",".join(k for k, v in sigs.items() if v) or "—"

    if args.tier_zero_only:
        before = len(profiles)
        profiles = [p for p in profiles if p["tier"] == 0]
        print(f"  Tier-0 filter: kept {len(profiles)}, dropped {before - len(profiles)} Tier-1+")

    if not profiles:
        print("\n  No Tier-0 candidates. Drop --tier-zero-only or broaden the strategies.")
        sys.exit(0)

    # Tier first (0 → 1 → 2), then score desc, then stars desc as tiebreaker
    profiles.sort(key=lambda p: (p.get("tier", 2), -p.get("score", 0.0), -p.get("total_stars", 0)))

    # Output
    role_slug = (cfg.get("role_key") if cfg else "fe") or "fe"
    output_path = args.output or f"sourced_{role_slug}_{time.strftime('%Y%m%d_%H%M')}.csv"
    write_csv(profiles, output_path)

    # Summary
    with_linkedin = sum(1 for p in profiles if p.get("linkedin_url"))
    with_email = sum(1 for p in profiles if p.get("email"))
    tier0 = sum(1 for p in profiles if p.get("tier") == 0)
    tier1 = sum(1 for p in profiles if p.get("tier") == 1)
    tier2 = sum(1 for p in profiles if p.get("tier") == 2)
    total_found = len(profiles)

    print(f"\n{'='*60}")
    print(f"SOURCING COMPLETE")
    print(f"{'='*60}")
    print(f"  Candidates:    {total_found}")
    print(f"  Tier 0 (recruit-now): {tier0}")
    print(f"  Tier 1 (worth screening): {tier1}")
    print(f"  Tier 2 (no signal):       {tier2}")
    print(f"  With LinkedIn: {with_linkedin} ({100*with_linkedin//max(total_found,1)}%)")
    print(f"  With email:    {with_email} ({100*with_email//max(total_found,1)}%)")
    print(f"  Output:        {output_path}")
    print(f"{'='*60}")

    # Top 10 preview
    print(f"\nTop 10 by tier+score:")
    for i, p in enumerate(profiles[:10], 1):
        name = p["name"][:25]
        stars = p.get("total_stars", 0)
        li = "LI" if p.get("linkedin_url") else "--"
        em = "EM" if p.get("email") else "--"
        print(f"  {i:2}. T{p['tier']} s{p['score']:>5.1f}  {name:25} {stars:5}★  [{li}][{em}]  {p['signals'][:40]}  {p['github_url']}")


if __name__ == "__main__":
    main()
