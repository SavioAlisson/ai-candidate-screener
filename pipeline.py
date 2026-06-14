"""
Mirrors the Apps Script screening path (Pipeline B + Opus), with Apify LinkedIn integration.

Flow:
  A) Existing dossier path (re-screen / opus-only):
     Existing dossier → Apify LinkedIn enrich → GitHub enrich → data confidence → Opus.

  B) Full research path:
     1. Apify LinkedIn scrape (cache-first)
     2. Haiku smart query (uses LinkedIn text + query learnings)
     3. Linkup deep search
     4. Haiku dossier synthesis
     5. Query learning eval + write-back
     6. Dossier fix (if insufficient data but LinkedIn exists)
     7. Apify LinkedIn enrichment (append to dossier)
     8. GitHub enrichment
     9. Data confidence assessment
    10. Opus judgment
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apify_linkedin import (
    build_linkedin_dossier_block,
    get_linkedin_data,
)
from json_repair import repair_json_string
from linkedin_discovery import discover_linkedin_url

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5-20251001")
OPUS_MODEL = os.environ.get("OPUS_MODEL", "claude-opus-4-6")
ANTHROPIC_VERSION = "2023-06-01"

PROMPT_VERSION = os.environ.get("PROMPT_VERSION", "local-batch-v1")

_DIR = Path(__file__).resolve().parent
_OPUS_BODY = _DIR / "prompts" / "opus_body.md"
_OPUS_FALLBACK = _DIR / "prompts" / "opus_body_fallback.md"
# Dedicated reject-first screener for the Alliances org (Solution Consultant +
# Alliances Director). These roles have a different bar than the engineering/GTM
# agent (DQ-first, per the hiring manager), so they load their own prompt body.
_OPUS_ALLIANCES = _DIR / "prompts" / "opus_body_alliances.md"
_ALLIANCES_ROLES = {
    "solution consultant", "solutions consultant",
    "alliances director", "alliance director",
}


def _is_alliances_role(target_role: str) -> bool:
    return (target_role or "").strip().lower() in _ALLIANCES_ROLES

# ── Insufficient data markers (same as Apps Script) ──────────────
INSUFFICIENT_MARKERS = [
    "INSUFFICIENT DATA",
    "COULD NOT FIND SUFFICIENT INFORMATION",
    "COULD NOT CONFIDENTLY IDENTIFY",
    "UNABLE TO FIND SUFFICIENT",
    "NO INFORMATION FOUND",
    "CANNOT CONFIRM",
]


def _strip_lone_surrogates(obj):
    if isinstance(obj, str):
        return "".join(ch for ch in obj if not 0xD800 <= ord(ch) <= 0xDFFF)
    if isinstance(obj, list):
        return [_strip_lone_surrogates(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _strip_lone_surrogates(v) for k, v in obj.items()}
    return obj


def _http_post_json(url: str, headers: Dict[str, str], payload: dict, timeout: int = 900) -> Tuple[int, dict]:
    data = json.dumps(_strip_lone_surrogates(payload)).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, {"_raw": body}


def anthropic_messages(
    api_key: str, model: str, max_tokens: int, user_text: str, temperature: float = 1.0, **extra
) -> Tuple[dict, str]:
    payload: Dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": user_text}],
    }
    payload.update(extra)
    code, data = _http_post_json(
        "https://api.anthropic.com/v1/messages",
        {
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "Content-Type": "application/json",
        },
        payload,
    )
    if code != 200:
        raise RuntimeError(f"Anthropic HTTP {code}: {data}")
    text = ""
    for block in data.get("content") or []:
        if block.get("type") == "text":
            text += block.get("text") or ""
    return data, text


def load_opus_body(target_role: str = "") -> str:
    # Alliances org (SC + AD) uses its own dedicated reject-first prompt.
    if _is_alliances_role(target_role) and _OPUS_ALLIANCES.is_file() and _OPUS_ALLIANCES.stat().st_size > 500:
        return _OPUS_ALLIANCES.read_text(encoding="utf-8")
    if _OPUS_BODY.is_file() and _OPUS_BODY.stat().st_size > 500:
        return _OPUS_BODY.read_text(encoding="utf-8")
    return _OPUS_FALLBACK.read_text(encoding="utf-8")


def build_opus_prompt(
    research_output: str,
    name: str,
    linkedin: str,
    source: str,
    source_notes: str,
    cv: str,
    target_role: str,
) -> str:
    base = load_opus_body(target_role)
    return (
        base.rstrip()
        + "\n\n<research_dossier>\n"
        + research_output
        + "\n</research_dossier>\n\n<candidate_metadata>\n"
        + f"Name: {name}\nLinkedIn: {linkedin}\nSource: {source}\n"
        + f"Source Additional Notes: {source_notes}\nTarget Role: {target_role}\n"
        + f"CV/Resume: {cv}\n</candidate_metadata>"
    )


def _is_referral_src(source: str) -> bool:
    s = (source or "").lower()
    return "referral" in s or "referred" in s


# Verdicts from the SC/AD screener that should NOT trigger a cross-role second
# look: universal knockouts that disqualify a candidate for EVERY role, not just
# SC/AD. A non-US or serial-job-hopper candidate is a no everywhere, so there is
# nothing to rescue. (Kept conservative — if unsure, we still run the second pass;
# the main prompt's own US-auth / loyalty gates will re-decline a true knockout.)
def maybe_cross_role_rescue(
    pass1_result: Dict[str, Any],
    *,
    name: str,
    linkedin: str,
    source: str,
    cv: str,
    dossier: str,
) -> Optional[Dict[str, Any]]:
    """Second-look cross-role check for REFERRALS the SC/AD screener declined.

    Fires only when: pass-1 verdict is DECLINE AND the source is a referral.
    Re-runs the judgment against the MAIN prompt (all standard roles) reusing the
    EXISTING dossier (opus_only — no new research, ~one model call). If the main
    prompt finds the candidate is a SCREEN/DEFER for some other role, returns that
    rescue result (so the candidate routes to the better-fit role's job for human
    review). Returns None when no rescue applies (decline stands).

    Scoped to referrals by design: low volume, high value, someone vouched for them
    — we don't want to lose a Kai (wrong for SC, strong for Product Engineer).
    """
    verdict = (pass1_result.get("verdict") or "").upper().strip()
    if verdict != "DECLINE" or not _is_referral_src(source):
        return None
    if not dossier or len(dossier.strip()) < 100:
        return None  # nothing to re-judge

    logger.info("CROSS-ROLE RESCUE: re-judging declined referral %s against all standard roles", name)
    # target_role="" → build_opus_prompt loads the MAIN prompt (not the alliances one),
    # which evaluates the candidate against every standard active role.
    pass2 = screen_one_candidate(
        name=name, linkedin=linkedin, source=source, source_notes="",
        cv=cv, target_role="", existing_dossier=dossier, opus_only=True,
    )
    v2 = (pass2.get("verdict") or "").upper().strip()
    roles = pass2.get("roles") or []
    if v2 in ("SCREEN", "DEFER") and roles:
        rescued_role = roles[0].get("role", "")
        logger.info("CROSS-ROLE RESCUE: %s rescued → %s (%s) on second pass", name, rescued_role, v2)
        pass2["_rescued_from"] = "Solution Consultant/Alliances Director"
        pass2["_rescued_role"] = rescued_role
        pass2["_pass1_verdict"] = pass1_result.get("verdict")
        pass2["_pass1_reason"] = pass1_result.get("verdict_reason")
        return pass2
    logger.info("CROSS-ROLE RESCUE: %s — no other-role fit (pass2=%s); decline stands", name, v2)
    return None


def assess_data_confidence(dossier: str) -> str:
    lower = dossier.lower()
    linkedin_ok = False
    if "linkedin" in lower:
        i = lower.index("linkedin")
        snip = lower[i : i + 200]
        linkedin_ok = not any(
            x in snip for x in ("not accessible", "unavailable", "not found", "blocked")
        )
    github_ok = "github.com/" in lower and "github profile not found" not in lower
    other_kw = (
        "blog", "article", "news", "conference",
        "twitter.com", "x.com", "substack", "medium",
    )
    has_other = any(k in lower for k in other_kw)
    sources = (1 if linkedin_ok else 0) + (1 if github_ok else 0) + (1 if has_other else 0)
    if sources >= 2 and linkedin_ok:
        return "Full"
    if sources >= 1:
        return "Partial"
    return "Minimal"


def _gh_fetch(url: str, headers: dict, timeout: int = 15):
    """Fetch a GitHub API URL; returns parsed JSON or None on error."""
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _gh_fetch_text(url: str, headers: dict, timeout: int = 15) -> str:
    """Fetch raw text from GitHub API (e.g., README). Returns empty string on error."""
    try:
        raw_headers = {**headers, "Accept": "application/vnd.github.v3.raw"}
        req = urllib.request.Request(url, headers=raw_headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _gh_contributor_count(username: str, repo_name: str, headers: dict) -> str:
    """Get contributor count for a repo via the Link header pagination trick."""
    try:
        url = f"https://api.github.com/repos/{username}/{repo_name}/contributors?per_page=1&anon=true"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            link = resp.getheader("Link") or ""
            if "last" in link:
                # Extract page count from Link: <...?page=N>; rel="last"
                m = re.search(r'page=(\d+)>;\s*rel="last"', link)
                if m:
                    return m.group(1)
            # No pagination = small number, count the array
            data = json.loads(resp.read().decode())
            return str(len(data)) if isinstance(data, list) else "1"
    except Exception:
        return "?"


_URL_RE = re.compile(r"https?://[^\s<>\"'\)\]]+", re.I)
_GITHUB_USER_RE = re.compile(r"https?://(?:www\.)?github\.com/([a-zA-Z0-9](?:[a-zA-Z0-9\-]){0,38})(?:/|$|\?|#)", re.I)
_PORTFOLIO_DOMAINS = (
    "behance.net", "dribbble.com", "codepen.io", "figma.com", "framer.",
    "webflow.io", "notion.site", "medium.com", "substack.com", "personal-site",
    "vercel.app", "netlify.app", "gitlab.com", "bitbucket.org", "kaggle.com",
    "huggingface.co", "twitter.com", "x.com", "youtube.com", "youtu.be",
    "devpost.com", "producthunt.com", "stackoverflow.com",
)


def _extract_cv_urls(cv_text: str) -> Dict[str, List[str]]:
    """Extract and classify URLs embedded in CV/resume text.

    Returns {"github": [...], "portfolio": [...], "other": [...]}.
    Ignores linkedin (handled separately) and dedupes case-insensitively.
    """
    out: Dict[str, List[str]] = {"github": [], "portfolio": [], "other": []}
    if not cv_text:
        return out
    seen = set()
    for raw in _URL_RE.findall(cv_text):
        url = raw.rstrip(".,;:)]}>\"'")
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        lower = url.lower()
        if "linkedin.com/" in lower:
            continue
        if _GITHUB_USER_RE.search(url):
            out["github"].append(url)
        elif any(d in lower for d in _PORTFOLIO_DOMAINS):
            out["portfolio"].append(url)
        else:
            out["other"].append(url)
    return out


def _format_cv_urls_for_prompt(cv_urls: Dict[str, List[str]]) -> str:
    """Compact bullet list of CV-extracted URLs for Haiku/Opus prompts."""
    lines = []
    if cv_urls.get("github"):
        lines.append("GitHub: " + ", ".join(cv_urls["github"][:5]))
    if cv_urls.get("portfolio"):
        lines.append("Portfolio / public work: " + ", ".join(cv_urls["portfolio"][:8]))
    if cv_urls.get("other"):
        lines.append("Other links: " + ", ".join(cv_urls["other"][:8]))
    return "\n".join(lines)


def github_enrich_block(dossier: str, cv_text: str = "") -> str:
    m = re.search(r"https?://(?:www\.)?github\.com/([a-zA-Z0-9](?:[a-zA-Z0-9\-]){0,38})", dossier, re.I)
    if not m and cv_text:
        m = re.search(r"https?://(?:www\.)?github\.com/([a-zA-Z0-9](?:[a-zA-Z0-9\-]){0,38})", cv_text, re.I)
    if not m:
        return ""
    username = m.group(1)
    headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "Klarity-Lambda-Screener"}
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    if gh_token:
        headers["Authorization"] = f"token {gh_token}"

    try:
        # 1. Profile
        profile = _gh_fetch(f"https://api.github.com/users/{username}", headers)
        if not profile:
            return f"\n\n--- GITHUB API ENRICHMENT ---\nGitHub profile not found via API for {username}\n--- END GITHUB API ---"

        bio = profile.get("bio") or "None"
        company = profile.get("company") or "None"
        followers = profile.get("followers") or 0
        public_repos = profile.get("public_repos") or 0
        created = (profile.get("created_at") or "")[:10]
        website = profile.get("blog") or ""

        section = (
            f"\n\n--- GITHUB API ENRICHMENT ---\nGitHub: https://github.com/{username}\n"
            f"Bio: {bio}\nCompany: {company}\nFollowers: {followers} | Public repos: {public_repos}\n"
            f"Account created: {created}\n"
        )
        if website:
            section += f"Website: {website}\n"

        # 2. Repos (top 10 by stars)
        repos = _gh_fetch(
            f"https://api.github.com/users/{username}/repos?sort=stars&per_page=10&direction=desc",
            headers,
        )
        if not repos or not isinstance(repos, list):
            section += "Repo data unavailable\n--- END GITHUB API ---"
            return section

        originals = [r for r in repos if not r.get("fork")]
        forks = [r for r in repos if r.get("fork")]
        section += f"\nOriginal repos: {len(originals)} | Forked repos: {len(forks)}\n"

        # 3. Top 3 original repos — deep enrichment
        if originals:
            section += "\nTop original repos by stars:\n"
            for r in originals[:3]:
                repo_name = r.get("name", "")
                stars = r.get("stargazers_count", 0)
                forks_count = r.get("forks_count", 0)
                lang = r.get("language") or "unknown"
                desc = (r.get("description") or "")[:150]
                updated = (r.get("updated_at") or "")[:10]

                # Contributor count
                contribs = _gh_contributor_count(username, repo_name, headers)

                section += f"- {repo_name}: {stars} stars, {forks_count} forks, {contribs} contributors, last updated {updated}, {lang}\n"
                if desc:
                    section += f"  Description: {desc}\n"

                # README excerpt (first 1500 chars)
                readme = _gh_fetch_text(
                    f"https://api.github.com/repos/{username}/{repo_name}/readme",
                    headers,
                )
                if readme:
                    readme_clean = readme[:1500].strip()
                    section += f"  README excerpt: {readme_clean}\n"

        # 4. Adoption summary
        adopted = [r for r in originals if r.get("stargazers_count", 0) > 10 or r.get("forks_count", 0) > 5]
        if adopted:
            names = ", ".join(r.get("name", "") for r in adopted[:5])
            section += f"\nAdoption signal: {len(adopted)} repo(s) with >10 stars or >5 forks ({names})\n"
        else:
            section += "\nAdoption summary: no repos with significant external adoption\n"

        # 5. Fork summary
        if forks:
            fork_names = ", ".join(r.get("name", "") for r in forks[:5])
            extra = f", (and {len(forks) - 5} more)" if len(forks) > 5 else ""
            section += f"Forked repos ({len(forks)}): {fork_names}{extra}\n"

        section += "--- END GITHUB API ---"
        return section
    except Exception as e:
        logger.warning("GitHub enrich failed: %s", e)
        return f"\n\n--- GITHUB API ENRICHMENT ---\nskipped: {e}\n--- END GITHUB API ---"


def _has_insufficient_markers(dossier: str) -> bool:
    """Check if dossier contains insufficient data markers."""
    upper = dossier.upper()
    return any(m in upper for m in INSUFFICIENT_MARKERS)


def _build_marketing_hint(target_role: str, linkedin_text: str) -> str:
    """Return marketing-specific search hint if applicable."""
    rl = (target_role or "").lower()
    ll = (linkedin_text or "").lower()
    if any(x in rl or x in ll for x in ("marketing", "field", "demand gen", "event marketing")):
        return (
            "\nFor this marketing candidate, specifically search for: "
            "(1) pipeline or revenue metrics from events/campaigns vs just attendance/registrations, "
            "(2) marketing team size at each company — sole marketer or large team, "
            "(3) whether employers sell to business buyers (CFOs, CIOs) or developers — "
            "NOTE: data infrastructure companies (Snowflake, Databricks, Airbyte) often sell to BOTH "
            "developers and enterprise data leaders, so research the actual buyer persona, "
            "(4) event ownership scope — end-to-end or one piece of a large event, "
            "(5) sales partnership evidence — ABM, account-based programs, AE collaboration, "
            "(6) company employee count and funding stage for each employer — "
            "this is critical for evaluating scrappy ownership claims.\n"
        )
    return ""


def _build_design_hint(target_role: str, linkedin_text: str) -> str:
    """Return design-specific search hint if applicable."""
    rl = (target_role or "").lower()
    ll = (linkedin_text or "").lower()
    if any(x in rl or x in ll for x in ("design engineer", "design", "product designer", "ux designer", "ui designer")):
        return (
            "\nFor this design candidate, specifically search for: "
            "(1) design portfolio — personal website, Dribbble, Behance, case study pages — "
            "the HM requires visual work review before a screening call, "
            "(2) evidence of production code shipped (not just mockups or prototypes), "
            "(3) whether they design OR develop — look for GitHub repos, CodePen, "
            "personal projects with live demos.\n"
        )
    return ""


def _build_marketing_synthesis_hint(target_role: str, linkedin_text: str) -> str:
    """Return marketing-specific synthesis context if applicable."""
    rl = (target_role or "").lower()
    ll = (linkedin_text or "").lower()
    if any(x in rl or x in ll for x in ("marketing", "field", "demand gen", "event marketing")):
        return (
            "MARKETING ROLE additional context per employer: "
            "(1) marketing team size if discoverable, "
            "(2) events — types, volume, ownership scope (end-to-end vs piece), "
            "(3) metrics — classify as pipeline/SQL/revenue or registrations/attendance/impressions, "
            "(4) company audience — business buyers vs developers, "
            "(5) sales partnership — ABM, AE collaboration, territory alignment, "
            "(6) budget/vendor ownership.\n\n"
        )
    return ""


def linkup_search(api_key: str, query: str) -> Tuple[str, List[dict]]:
    code, data = _http_post_json(
        "https://api.linkup.so/v1/search",
        {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        {"q": query, "depth": "deep", "outputType": "sourcedAnswer"},
        timeout=120,
    )
    if code != 200:
        return f"Linkup error HTTP {code}: {data}", []
    answer = data.get("answer") or data.get("text") or json.dumps(data)[:5000]
    sources = []
    for s in data.get("sources") or []:
        url = s.get("url") or s.get("link") or ""
        if url:
            sources.append({"url": url, "title": s.get("title") or s.get("name") or ""})
    return str(answer), sources


def run_research_pipeline(
    *,
    claude_key: str,
    linkup_key: str,
    name: str,
    linkedin: str,
    source: str,
    source_notes: str,
    cv: str,
    target_role: str,
    linkedin_profile_text: str = "",
    query_learnings: str = "",

) -> Tuple[str, float, str]:
    """
    Full research pipeline (Pipeline B).
    Returns (dossier_markdown, estimated_research_cost_usd, learning_text).
    """
    linkedin_full_text = linkedin_profile_text or ""
    total_cost = 0.0

    # Pull URLs embedded in the CV (GitHub, portfolio, personal sites) so they
    # can seed the search query + synthesis. Without this step, Linkup never
    # sees a candidate's design portfolio or GitHub that only lives in their PDF.
    cv_urls = _extract_cv_urls(cv)
    cv_urls_block = _format_cv_urls_for_prompt(cv_urls)
    if cv_urls_block:
        logger.info("CV URLs found — github:%d portfolio:%d other:%d",
                    len(cv_urls["github"]), len(cv_urls["portfolio"]), len(cv_urls["other"]))

    # ── Step 1: Haiku smart query ────────────────────────────────
    logger.info("=== STEP 1: Haiku smart query for %s ===", name)
    fm_hint = _build_marketing_hint(target_role, linkedin_full_text)
    de_hint = _build_design_hint(target_role, linkedin_full_text)
    cv_urls_hint = (
        f"\n\nURLs found in the candidate's CV — investigate these directly "
        f"(GitHub activity, portfolio pieces, public work):\n{cv_urls_block}"
    ) if cv_urls_block else ""

    q_prompt = (
        f"Generate ONE search query (max 300 words) to find public info about: {name} | "
        f"Role: {target_role} | LinkedIn: {(linkedin_full_text or 'Not available')[:2000]}"
        f"{fm_hint}{de_hint}{cv_urls_hint}\nPast learnings: {query_learnings or 'None'}"
    )
    try:
        _, smart_query = anthropic_messages(claude_key, HAIKU_MODEL, 1000, q_prompt, temperature=0.3)
        smart_query = (smart_query or f"{name} {target_role} professional background").strip()
        total_cost += 0.01
        logger.info("SMART QUERY: %s...", smart_query[:200])
    except Exception as e:
        logger.warning("Smart query failed: %s — using fallback", e)
        smart_query = f"{name} {target_role} professional background"

    # ── Step 2: Linkup deep search ───────────────────────────────
    # Append CV URLs to the query so Linkup crawls them directly (portfolio
    # sites, personal blogs, GitHub profiles) instead of only matching on
    # generic keyword recall.
    if cv_urls_block:
        smart_query = smart_query + "\n\nAlso investigate these URLs from the CV:\n" + cv_urls_block
    logger.info("=== STEP 2: Linkup search ===")
    combined, sources = linkup_search(linkup_key, smart_query[:3000])
    total_cost += 0.055

    # Build source index for attribution
    source_index_lines = []
    for sx, src in enumerate(sources):
        u = src["url"]
        dom = re.sub(r"^https?://([^/]+).*$", r"\1", u)
        t = src.get("title") or ""
        source_index_lines.append(f"[{sx+1}] {dom} — {t}\n    {u}")
    source_index = "\n".join(source_index_lines) if source_index_lines else ""
    logger.info("LINKUP: %d sources extracted", len(sources))

    # ── Step 3: Haiku dossier synthesis ──────────────────────────
    logger.info("=== STEP 3: Haiku synthesis ===")
    fm_synth = _build_marketing_synthesis_hint(target_role, linkedin_full_text)

    cv_urls_synth = (
        f"URLs extracted from the candidate's CV — confirm or discuss what they reveal:\n"
        f"{cv_urls_block}\n\n"
    ) if cv_urls_block else ""

    synth_prompt = (
        "Write a candidate research dossier.\n\n"
        f"LinkedIn profile (PRIMARY):\n{linkedin_full_text or 'Not available'}\n\n"
        f"Web search results (SUPPLEMENTARY):\n{combined[:30000]}\n\n"
        + cv_urls_synth
        + (f"Source index (URLs found during web search):\n{source_index}\n\n" if source_index else "")
        + "Cover: Identity, Career Timeline (with company context for each employer), "
        "Career Trajectory Summary, Education, Independent Work, Public Presence, "
        "Work Authorization, Engagement Signals.\n\n"
        + fm_synth
        + "## Source attribution (REQUIRED)\n"
        "Every factual claim MUST include its source URL inline in parentheses, "
        "right after the sentence making the claim.\n"
        "Use the source index above to find the correct full URL for each claim.\n\n"
        "Format examples:\n"
        '- "She co-founded Vara, an AI sustainability compliance platform (https://vara.ai)."\n'
        '- "Built 30+ connectors at Lilt including Figma, GitHub, Shopify (https://lilt.com)."\n'
        '- "Previously worked as Software Engineer at Illumio (LinkedIn)."\n'
        '- "Holds a B.S. in Computer Science from UC San Diego (LinkedIn)."\n\n'
        "Rules:\n"
        "- Web claims: use the FULL URL from the source index in parentheses\n"
        "- LinkedIn claims: use (LinkedIn)\n"
        "- CV-only facts: use (CV)\n"
        "- GitHub: use the full GitHub URL\n"
        "- If no URL available: use (Web) as last resort\n"
        "- Section headers and 'No information found' lines can skip attribution.\n"
        "This is critical — reviewers need to click through and verify each claim."
    )
    try:
        _, dossier = anthropic_messages(claude_key, HAIKU_MODEL, 8000, synth_prompt, temperature=0.3)
        dossier = dossier or ""
        total_cost += 0.02
    except Exception as e:
        logger.warning("Synthesis failed: %s", e)
        if linkedin_full_text and len(linkedin_full_text) > 100:
            dossier = "No web research. LinkedIn only:\n" + linkedin_full_text
        else:
            dossier = "INSUFFICIENT DATA"

    # Append raw LinkedIn text to dossier
    if linkedin_full_text and len(linkedin_full_text) > 10:
        dossier += "\n\n--- LINKEDIN PROFILE DATA ---\n" + linkedin_full_text + "\n--- END ---"
    if linkedin:
        dossier += "\nLinkedIn: " + linkedin

    logger.info("DOSSIER: %d chars | Cost: $%.3f", len(dossier), total_cost)

    # ── Step 4: Query learning (eval + write-back) ───────────────
    logger.info("=== STEP 4: Query learning ===")
    learning_text = ""
    try:
        eval_prompt = (
            "Score 0-20. Respond: LEARNING: [advice]\nSCORE: [n]/20\n\n"
            f"Dossier: {dossier[:3000]}"
        )
        _, learning_text = anthropic_messages(claude_key, HAIKU_MODEL, 500, eval_prompt, temperature=0.3)
        learning_text = learning_text or ""
        logger.info("LEARNING: %s", learning_text[:200])
    except Exception as e:
        logger.warning("Learning eval failed: %s", e)

    return dossier, total_cost, learning_text


def enrich_dossier_with_linkedin(dossier: str, linkedin_profile: Optional[dict]) -> str:
    """Append LinkedIn dossier block to research output."""
    if not linkedin_profile:
        return dossier
    block = build_linkedin_dossier_block(linkedin_profile)
    # Don't duplicate if already has LinkedIn data section
    if "--- LINKEDIN PROFILE DATA (via Apify API" in dossier:
        return dossier
    return dossier + block


def enrich_dossier_with_universal_signals(
    dossier: str,
    linkedin_profile: Optional[dict],
    linkedin_url: str,
    name: str,
) -> str:
    """Append universal enrichment signals to the dossier:

      - Movability tier (🟢/🟡/🔴) computed from LinkedIn data — no external call
      - Crunchbase per-company data (size bracket, funding, acquisitions)
      - LinkedIn Posts last 15-20 (AI-native receipts signal)

    All three are appended for EVERY candidate (not role-conditional). Failures
    in any single enrichment fall back to a "(no data available)" note in the
    dossier — never block screening.

    Idempotent: if the dossier already contains the signal blocks, returns
    unchanged.
    """
    if not linkedin_profile and not linkedin_url:
        return dossier  # nothing to enrich from

    # Skip if already enriched
    if "=== MOVABILITY HEURISTIC ===" in dossier or "=== CRUNCHBASE COMPANY DATA ===" in dossier:
        return dossier

    blocks: List[str] = []

    # ── Movability tier (pure computation) ──
    try:
        from movability import tier as _mv_tier
        mv_t, mv_reason = _mv_tier(linkedin_profile or {}, "")
        blocks.append(
            "=== MOVABILITY HEURISTIC ===\n"
            f"Tier: {mv_t}\nReason: {mv_reason}\n"
            "Treat as context for outreach plan (informational, not a verdict input)."
        )
    except Exception as e:
        logger.warning("Movability computation failed for %s: %s", name, e)

    # ── Crunchbase per-company ──
    try:
        from apify_crunchbase import (
            extract_companies_from_linkedin,
            fetch_crunchbase_for_companies,
            format_for_dossier as _cb_format,
        )
        li_text = (linkedin_profile or {}).get("fullText", "") if linkedin_profile else ""
        companies = extract_companies_from_linkedin(li_text)
        if companies:
            cb_map = fetch_crunchbase_for_companies(companies)
            blocks.append(_cb_format(companies, cb_map))
        else:
            blocks.append("=== CRUNCHBASE COMPANY DATA ===\n  (no companies extracted from LinkedIn)")
    except Exception as e:
        logger.warning("Crunchbase enrichment failed for %s: %s", name, e)
        blocks.append("=== CRUNCHBASE COMPANY DATA ===\n  (Crunchbase fetch failed — proceed with LinkedIn + training knowledge only)")

    # ── LinkedIn Posts ──
    try:
        from apify_posts import fetch_posts, summarize_posts_for_dossier
        posts = fetch_posts(linkedin_url) if linkedin_url else []
        blocks.append(summarize_posts_for_dossier(name, posts))
    except Exception as e:
        logger.warning("Posts enrichment failed for %s: %s", name, e)
        blocks.append("=== LINKEDIN POSTS (last 15-20) ===\n  (Posts fetch failed — no AI-native-receipts signal available for this candidate)")

    if blocks:
        return dossier + "\n\n" + "\n\n".join(blocks)
    return dossier


def _coerce_confidence_int(raw) -> Optional[int]:
    """Best-effort coercion of confidence_score to int 1..5 (or None)."""
    if raw is None:
        return None
    if isinstance(raw, bool):  # bool is subclass of int — exclude explicitly
        return None
    if isinstance(raw, int):
        return raw if 1 <= raw <= 5 else None
    if isinstance(raw, float) and raw.is_integer():
        i = int(raw)
        return i if 1 <= i <= 5 else None
    if isinstance(raw, str):
        s = raw.strip()
        if s.isdigit():
            i = int(s)
            return i if 1 <= i <= 5 else None
        # "4/5", "4 / 5", "4 (Inbound App Review)" → grab leading int
        m = re.match(r"\s*([1-5])\b", s)
        if m:
            return int(m.group(1))
    return None


def _retry_for_confidence(
    claude_key: str, parsed: dict, name: str
) -> Tuple[Optional[int], float, dict]:
    """Re-prompt Opus to emit just the confidence_score for an existing verdict.
    Returns (confidence_int_or_None, additional_cost, usage_dict).

    Triggered only when the main response is SCREEN or DECLINE but lacks a
    valid integer confidence. Cheap focused call — not the full prompt."""
    verdict = (parsed.get("verdict") or "").strip().upper()
    verdict_reason = parsed.get("verdict_reason") or ""
    spark = parsed.get("spark") or ""
    concerns_lines = []
    for c in (parsed.get("concerns") or [])[:5]:
        if isinstance(c, dict):
            concerns_lines.append(f"- {c.get('concern','')} ({c.get('type','')})")
    concerns_block = "\n".join(concerns_lines) or "(none listed)"

    retry_prompt = (
        "Your previous screening response for candidate "
        f"'{name}' returned verdict={verdict} but did NOT include a valid "
        "integer confidence_score (1-5). confidence_score is REQUIRED for "
        "SCREEN and DECLINE verdicts.\n\n"
        f"Verdict: {verdict}\n"
        f"Spark: {spark[:600]}\n"
        f"Verdict reason: {verdict_reason[:800]}\n"
        f"Concerns:\n{concerns_block}\n\n"
        "Based on the above, return ONLY a JSON object of the form:\n"
        '{\"confidence_score\": <integer 1-5>}\n\n'
        "Routing rules to choose the integer:\n"
        "- SCREEN 5 = call is a focused confirmation, no blocking uncertainty.\n"
        "- SCREEN 4 = screen-leaning but one specific hypothesis needs human "
        "judgment first (Inbound App Review for inbound).\n"
        "- DECLINE 3 = close_no — some positive signals but concerns win.\n"
        "- DECLINE 2 = clear decline, limited positive signal.\n"
        "- DECLINE 1 = clear decline, strong negative pattern.\n"
        "If you cannot decide between two values, default per the rule: SCREEN "
        "uncertain → 4; DECLINE uncertain → 3."
    )

    payload = {
        "model": OPUS_MODEL,
        "max_tokens": 200,
        "temperature": 0,
        "messages": [{"role": "user", "content": retry_prompt}],
    }
    code, data = _http_post_json(
        "https://api.anthropic.com/v1/messages",
        {
            "x-api-key": claude_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "Content-Type": "application/json",
        },
        payload,
        timeout=120,
    )
    if code != 200:
        logger.warning("Confidence retry failed (HTTP %s) for %s", code, name)
        return None, 0.0, {}

    usage = data.get("usage") or {}
    in_tok = usage.get("input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    cost = (in_tok * 5.0 + out_tok * 25.0) / 1_000_000

    text = ""
    for block in data.get("content") or []:
        if block.get("type") == "text":
            text += block.get("text") or ""

    # Try parsing as JSON first
    text = re.sub(r"```json\s*", "", text, flags=re.I)
    text = re.sub(r"```\s*", "", text)
    first, last = text.find("{"), text.rfind("}")
    if first != -1 and last > first:
        try:
            obj = json.loads(repair_json_string(text[first : last + 1]))
            ci = _coerce_confidence_int(obj.get("confidence_score"))
            if ci is not None:
                return ci, cost, usage
        except json.JSONDecodeError:
            pass

    # Fall back to bare-number scan
    m = re.search(r"\b([1-5])\b", text)
    if m:
        return int(m.group(1)), cost, usage
    return None, cost, usage


def call_opus_judgment(
    claude_key: str, research_output: str, meta: Dict[str, str]
) -> Tuple[dict, float, dict]:
    ro = research_output + github_enrich_block(research_output, cv_text=meta.get("cv", ""))
    ro = "\nData Confidence: " + assess_data_confidence(ro) + "\n" + ro
    prompt = build_opus_prompt(
        ro,
        meta["name"],
        meta["linkedin"],
        meta["source"],
        meta["source_notes"],
        meta["cv"],
        meta["target_role"],
    )
    payload = {
        "model": OPUS_MODEL,
        "max_tokens": int(os.environ.get("OPUS_MAX_TOKENS", "32000")),
        "temperature": float(os.environ.get("OPUS_TEMPERATURE", "1")),
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": "max"},
        "messages": [{"role": "user", "content": prompt}],
    }
    code, data = _http_post_json(
        "https://api.anthropic.com/v1/messages",
        {
            "x-api-key": claude_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "Content-Type": "application/json",
        },
        payload,
        timeout=900,
    )
    if code != 200:
        err = {"verdict": "SCREENING FAILED", "verdict_reason": f"Opus HTTP {code}", "_error": str(data)}
        if code == 429 or code >= 500:
            err["_retryable"] = True
        return err, 0.0, {}
    usage = data.get("usage") or {}
    in_tok = usage.get("input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    opus_cost = (in_tok * 5.0 + out_tok * 25.0) / 1_000_000
    text = ""
    for block in data.get("content") or []:
        if block.get("type") == "text":
            text += block.get("text") or ""
    text = re.sub(r"```json\s*", "", text, flags=re.I)
    text = re.sub(r"```\s*", "", text)
    first, last = text.find("{"), text.rfind("}")
    if first == -1 or last <= first:
        return {
            "verdict": "SCREENING FAILED",
            "verdict_reason": "No JSON in Opus response",
            "_error": text[:300],
        }, opus_cost, usage
    try:
        parsed = json.loads(repair_json_string(text[first : last + 1]))
    except json.JSONDecodeError as e:
        return {
            "verdict": "SCREENING FAILED",
            "verdict_reason": f"JSON parse error: {e}",
            "_error": text[first : first + 400],
        }, opus_cost, usage

    # ── Confidence-score validation + auto-retry ────────────────────
    # SCREEN/DECLINE without a valid 1-5 integer confidence breaks Manual
    # Screen routing. The prompt asks for it; this enforces it.
    verdict_upper = (parsed.get("verdict") or "").strip().upper()
    if verdict_upper in ("SCREEN", "DECLINE"):
        ci = _coerce_confidence_int(parsed.get("confidence_score"))
        if ci is None:
            logger.warning(
                "Opus omitted confidence_score for %s (verdict=%s) — auto-retrying",
                meta.get("name", "?"), verdict_upper,
            )
            ci, retry_cost, retry_usage = _retry_for_confidence(
                claude_key, parsed, meta.get("name", "?")
            )
            opus_cost += retry_cost
            for k, v in (retry_usage or {}).items():
                if isinstance(v, int) and isinstance(usage.get(k), int):
                    usage[k] = usage.get(k, 0) + v
            if ci is not None:
                parsed["confidence_score"] = ci
                parsed["_confidence_recovered"] = True
                logger.info(
                    "Recovered confidence_score=%d for %s via retry",
                    ci, meta.get("name", "?"),
                )
            else:
                logger.warning(
                    "Confidence retry could not extract a value for %s — "
                    "leaving null (will route to Application Review)",
                    meta.get("name", "?"),
                )
        else:
            # Normalize to int even if Opus emitted "4" as string or "4/5"
            parsed["confidence_score"] = ci

    return parsed, opus_cost, usage


def format_token_log(
    sonar_cost: float, opus_cost: float, opus_usage: Optional[dict] = None
) -> str:
    u = opus_usage or {}
    return (
        f"PipelineB ~${sonar_cost:.4f} | Opus in={u.get('input_tokens','?')} "
        f"out={u.get('output_tokens','?')} | Opus ${opus_cost:.4f}"
    )


def screen_one_candidate(
    *,
    name: str,
    linkedin: str,
    source: str,
    source_notes: str,
    cv: str,
    target_role: str,
    existing_dossier: str = "",
    linkedin_profile_text: str = "",
    query_learnings: str = "",

    opus_only: bool = False,
    candidate_id: str = "",
) -> Dict[str, Any]:
    """
    Screen a single candidate — full pipeline or dossier-reuse.

    opus_only=True: skip all research, use existing_dossier directly (judgment rerun).
    existing_dossier with >100 chars: enrich with LinkedIn, skip research, go to Opus.
    Otherwise: full research pipeline.
    """
    claude_key = os.environ["CLAUDE_API_KEY"]
    linkup_key = os.environ.get("LINKUP_API_KEY", "")
    apify_token = os.environ.get("APIFY_TOKEN", "")

    meta = {
        "name": name,
        "linkedin": linkedin,
        "source": source or "OUTBOUND",
        "source_notes": source_notes,
        "cv": cv,
        "target_role": target_role,
    }

    learning_text = ""

    # ── PATH A: Existing dossier (re-screen or opus-only) ────────
    if (existing_dossier and len(existing_dossier.strip()) > 100) or opus_only:
        research = existing_dossier or ""
        sonar_cost = 0.0

        if opus_only:
            logger.info("OPUS-ONLY: Using existing dossier for %s (%d chars)", name, len(research))
        else:
            logger.info("ROW-CACHE: Using existing dossier for %s (%d chars)", name, len(research))

        # Enrich with LinkedIn even on dossier reuse (unless opus-only)
        if not opus_only and linkedin:
            li_profile = get_linkedin_data(linkedin, name, apify_token=apify_token)
            research = enrich_dossier_with_linkedin(research, li_profile)
            # Universal enrichment (Crunchbase + Posts + Movability) for all candidates
            research = enrich_dossier_with_universal_signals(research, li_profile, linkedin, name)

    # ── PATH B: Full research pipeline ───────────────────────────
    else:
        if not linkup_key:
            raise RuntimeError("LINKUP_API_KEY required when no existing dossier is provided.")

        # Step 0a: LinkedIn URL discovery (last resort if all Ashby/CV/push-log fallbacks missed)
        discovery_cost = 0.0
        if not linkedin and linkup_key:
            logger.info("=== STEP 0a: LinkedIn discovery for %s ===", name)
            try:
                discovered, discovery_cost = discover_linkedin_url(
                    name=name,
                    cv=cv,
                    target_role=target_role,
                    linkup_key=linkup_key,
                    claude_key=claude_key,
                    linkup_search_fn=linkup_search,
                    haiku_call_fn=anthropic_messages,
                    apify_token=apify_token,
                )
            except Exception as e:
                logger.warning("Discovery raised for %s: %s", name, e)
                discovered = ""
            if discovered:
                linkedin = discovered
                meta["linkedin"] = linkedin
                if candidate_id:
                    try:
                        from ashby_bridge import _save_cache, add_linkedin_to_candidate
                        _save_cache(candidate_id, {"linkedin": linkedin, "linkedin_discovered": True})
                        add_linkedin_to_candidate(candidate_id, linkedin)
                    except Exception as e:
                        logger.warning("Failed to persist discovered LinkedIn for %s: %s", name, e)
            else:
                logger.info("No LinkedIn found after discovery for %s — proceeding without", name)

        # Step 0b: Apify LinkedIn scrape (cache-first)
        li_profile = None
        li_full_text = ""
        if linkedin:
            li_profile = get_linkedin_data(linkedin, name, apify_token=apify_token)
            if li_profile:
                li_full_text = li_profile.get("fullText", "")

        # Steps 1-4: Haiku query → Linkup → Haiku synthesis → learning
        research, research_cost, learning_text = run_research_pipeline(
            claude_key=claude_key,
            linkup_key=linkup_key,
            name=name,
            linkedin=linkedin,
            source=meta["source"],
            source_notes=source_notes,
            cv=cv,
            target_role=target_role,
            linkedin_profile_text=li_full_text or linkedin_profile_text,
            query_learnings=query_learnings,
        )
        sonar_cost = research_cost + discovery_cost

        # Dossier quality check: if research is thin but LinkedIn data exists,
        # replace bad research with a clean fallback (matches code.gs lines 1889-1900)
        research_is_thin = (
            _has_insufficient_markers(research)
            or len(research.strip()) < 200
        )
        if research_is_thin and li_profile:
            logger.info("DOSSIER-FIX: Research too thin for %s — falling back to LinkedIn data", name)
            research = "No additional web presence found beyond LinkedIn. Evaluate based on LinkedIn profile data below."
        elif research_is_thin and not li_profile:
            logger.warning("DOSSIER-WARN: Research thin for %s and no LinkedIn data available", name)

        # Apify LinkedIn enrichment (append structured data to dossier)
        research = enrich_dossier_with_linkedin(research, li_profile)
        # Universal enrichment (Crunchbase + Posts + Movability) for all candidates
        research = enrich_dossier_with_universal_signals(research, li_profile, linkedin, name)

    # Persist dossier before Opus so a judgment timeout/crash doesn't waste
    # the research spend. Reruns can then use --opus-only against this cache.
    if candidate_id and research and len(research.strip()) > 100:
        try:
            from ashby_bridge import _save_cache
            _save_cache(candidate_id, {"dossier": research})
        except Exception as e:
            logger.warning("Failed to pre-Opus dossier cache for %s: %s", name, e)

    # ── Opus judgment ────────────────────────────────────────────
    result, opus_cost, opus_usage = call_opus_judgment(claude_key, research, meta)

    if result.get("_retryable") and result.get("verdict") == "SCREENING FAILED":
        delay = int(os.environ.get("RETRY_DELAY_SEC", "10"))
        logger.info("Retrying Opus for %s after %ds...", name, delay)
        time.sleep(delay)
        result, opus_cost, opus_usage = call_opus_judgment(claude_key, research, meta)

    result["_researchOutput"] = research
    result["_data_confidence"] = assess_data_confidence(research)
    total = sonar_cost + opus_cost
    result["_costs"] = {
        "sonar": sonar_cost,
        "opus": opus_cost,
        "total": total,
        "opus_usage": opus_usage,
    }
    result["_token_log"] = format_token_log(sonar_cost, opus_cost, opus_usage)
    result["_learning"] = learning_text

    # CROSS-ROLE RESCUE (live): for a declined SC/AD REFERRAL, take a second look
    # against all standard roles, reusing THIS dossier (one extra model call, no new
    # research). If the candidate fits another role, return that result so writeback
    # routes them to the better-fit job for human review instead of archiving them.
    # Guarded to SC/AD targets only → the pass-2 call (target_role="") can't recurse.
    # maybe_cross_role_rescue itself no-ops unless verdict==DECLINE and source is a
    # referral, so this is inert for every other case.
    if _is_alliances_role(target_role):
        rescued = maybe_cross_role_rescue(
            result, name=name, linkedin=linkedin, source=source, cv=cv, dossier=research,
        )
        if rescued:
            return rescued
    return result
