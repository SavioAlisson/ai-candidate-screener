"""
Juicebox Query Optimizer — auto-prompting for upstream sourcing.

Analyzes screening rejection patterns and role requirements to generate
optimized Juicebox (PeopleGPT) search queries. Closes the feedback loop:
  screening rejections → better search queries → fewer wasted screens.

Two modes:
  1. Generate initial queries for a role (from prompt requirements)
  2. Optimize queries using rejection data (from past screening results)

Usage:
  python3 juicebox_query_optimizer.py                         # analyze all roles
  python3 juicebox_query_optimizer.py --role "Value Delivery"  # specific role
  python3 juicebox_query_optimizer.py --results screening_results.json  # use specific results file
  python3 juicebox_query_optimizer.py --all-results            # scan all result files in directory

The output is plain-text Juicebox search queries you can paste directly.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
PROMPT_FILE = _DIR / "prompts" / "opus_body.md"
RESULTS_FILE = _DIR / "screening_results.json"
QUERY_LOG_FILE = _DIR / ".juicebox_query_log.json"

HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5-20251001")
APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "")

# ── Active roles (from the screening prompt) ──────────────────────

ACTIVE_ROLES = {
    "AI Product Engineer": {
        "short": "P+E",
        "keywords": ["product engineer", "product manager", "full stack", "0-to-1", "shipped product"],
        "must_have": ["product thinking + technical execution", "shipped 0→1 to real users"],
        "avoid": ["PM titles without shipped products", "pure engineering no product sense", "ChatGPT wrapper as AI product"],
    },
    "AI Design Engineer": {
        "short": "D+E",
        "keywords": ["design engineer", "product designer", "UX engineer", "design systems", "frontend"],
        "must_have": ["designer who codes", "shipped production code", "strong portfolio"],
        "avoid": ["designers who only do mockups", "no code evidence"],
    },
    "AI Frontend Engineer": {
        "short": "FE",
        "keywords": ["frontend engineer", "react", "typescript", "UI engineer"],
        "must_have": ["React/TypeScript mastery", "state-heavy interactive UIs"],
        "avoid": ["backend-only claiming full-stack", "no production frontend work"],
    },
    "AI Backend Engineer": {
        "short": "BE",
        "keywords": ["backend engineer", "distributed systems", "infrastructure", "ML engineer"],
        "must_have": ["production backend systems", "genuine AI interest"],
        "avoid": ["pure research no production", "frontend-only"],
    },
    "Staff DevSecOps Engineer": {
        "short": "DevOps",
        "keywords": ["devops", "devsecops", "infrastructure", "CI/CD", "cloud architect", "security"],
        "must_have": ["CI/CD pipeline ownership", "security program ownership", "7+ years"],
        "avoid": ["only monitoring/alerting", "no security experience"],
    },
    "GTM Engineer": {
        "short": "GTM",
        "keywords": ["revenue operations", "GTM engineer", "sales operations", "salesforce developer", "revops"],
        "must_have": ["Salesforce architecture depth", "built systems from scratch", "3-8 years"],
        "avoid": ["Salesforce admin only", "HubSpot-only no SFDC", "large enterprise admin role", "analyst/reporting only"],
    },
    "AI Value Delivery Lead": {
        "short": "VD",
        "keywords": ["customer success", "implementation", "transformation", "onboarding", "professional services"],
        "must_have": ["transformation craft (names WHAT was transformed)", "AI fluency", "US metro: SF/NYC/Chicago/Boston"],
        "avoid": ["pure GTM/sales last 5 years", "Canada/non-US for US track", "internship-heavy",
                   "long Big-4 spine without software delivery chapter", "generic leadership language"],
        "gold_path": "McKinsey/BCG/Bain/Oliver Wyman (2+ yrs) → MBA (HBS/Wharton/Stanford) → CS at software company → US metro",
    },
    "Field Marketing Manager": {
        "short": "FM",
        "keywords": ["field marketing", "event marketing", "demand generation", "B2B marketing"],
        "must_have": ["sole/primary field marketer at growth-stage B2B", "pipeline accountability (SQLs, not registrations)", "3-7 years"],
        "avoid": ["enterprise-only career (1K+ employees)", "pure B2C or developer community", "never owned budget"],
    },
}


# ── Load screening results ────────────────────────────────────────

def load_results(results_path: str = "") -> List[Dict]:
    """Load screening results from JSON file(s)."""
    path = Path(results_path) if results_path else RESULTS_FILE
    all_results = []

    if path.is_file():
        if path.suffix == ".csv":
            all_results.extend(_load_results_from_csv(str(path)))
        else:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            all_results.extend(data.get("results", data if isinstance(data, list) else []))
    elif path.is_dir():
        for f in sorted(path.glob("screening_results*.json")):
            try:
                with open(f, encoding="utf-8") as fh:
                    data = json.load(fh)
                all_results.extend(data.get("results", []))
            except Exception:
                pass

    return all_results


def _csv_pick(row: Dict[str, Any], *keys: str) -> str:
    """First non-empty value among possible CSV header names (handles export variants)."""
    for k in keys:
        if k not in row:
            continue
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _load_results_from_csv(csv_path: str) -> List[Dict]:
    """Load screening results from a CSV export (Archived, Decline, Screen, Nurture, etc.)."""
    import csv as csv_mod
    results = []
    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        # Strip NUL bytes that Google Sheets sometimes includes in exports
        clean_lines = (line.replace("\x00", "") for line in f)
        reader = csv_mod.DictReader(clean_lines)
        for row in reader:
            verdict = _csv_pick(
                row,
                "AI Screening Verdict",
                "AI screening verdict",
                "Screening Verdict",
                "Verdict",
            )
            # Don't skip rows without verdict — archived candidates may not have been screened
            # Default to DECLINE for archive tab exports (they were rejected somehow)
            if not verdict:
                verdict = "DECLINE"
            name = _csv_pick(row, "Candidate Name", "Name")
            if not name:
                continue
            best_fit = _csv_pick(
                row,
                "Best Fit Role",
                "Matched Role",
                "Target Role",
            )
            results.append({
                "name": name,
                "verdict": verdict.upper(),
                "verdict_reason": _csv_pick(
                    row,
                    "Verdict Reason",
                    "Recommended Action",
                    "AI Verdict Reason",
                ),
                "best_fit_role": best_fit,
                "concerns": _csv_pick(row, "Concerns"),
                "rejection_type": _csv_pick(row, "Rejection Type"),
                "spark": _csv_pick(row, "Spark"),
                "matched_level": _csv_pick(row, "Matched Level"),
                "source_notes": _csv_pick(row, "Source Additional Notes", "Source Notes"),
                "target_role": _csv_pick(row, "Target Role"),
            })
    return results


def load_all_results_in_dir() -> List[Dict]:
    """Load all screening result files in the lambda-screener directory."""
    all_results = []
    for f in sorted(_DIR.glob("screening_results*.json")):
        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            all_results.extend(data.get("results", []))
        except Exception:
            pass
    return all_results


# ── Analyze rejection patterns ────────────────────────────────────

def analyze_rejections(results: List[Dict], role_filter: str = "") -> Dict[str, Any]:
    """
    Analyze screening results to find rejection patterns.
    Returns structured analysis by role.
    """
    by_role: Dict[str, Dict[str, list]] = {}

    for r in results:
        verdict = (r.get("verdict") or "").upper()
        best_role = r.get("best_fit_role", "")
        concerns = r.get("concerns", "")
        reason = r.get("verdict_reason", "")
        rejection_type = r.get("rejection_type", "")
        name = r.get("name", "")

        # Match to active roles — check both Best Fit Role and Target Role
        target_role = r.get("target_role", "")
        role_text = f"{best_role} {target_role}".lower()
        matched_roles = []
        for role_name in ACTIVE_ROLES:
            if role_name.lower() in role_text or ACTIVE_ROLES[role_name]["short"].lower() in role_text:
                matched_roles.append(role_name)

        if not matched_roles:
            matched_roles = ["Unknown"]

        if role_filter:
            role_key = role_filter.lower()
            matched_roles = [r for r in matched_roles if role_key in r.lower()]
            if not matched_roles:
                continue

        for role in matched_roles:
            if role not in by_role:
                by_role[role] = {"screens": [], "declines": [], "reviews": [], "all": []}

            # Extract search tag from Source Additional Notes ("Juicebox search: ...")
            source_notes = r.get("source_notes", "")
            search_tag = ""
            if source_notes:
                for part in source_notes.split("|"):
                    part = part.strip()
                    if part.lower().startswith("juicebox search:"):
                        search_tag = part[len("Juicebox search:"):].strip()
                        break

            entry = {
                "name": name,
                "verdict": verdict,
                "reason": reason[:500],
                "concerns": concerns[:500],
                "rejection_type": rejection_type,
                "search_tag": search_tag,
            }

            by_role[role]["all"].append(entry)
            if verdict in ("SCREEN",):
                by_role[role]["screens"].append(entry)
            elif verdict in ("DECLINE",):
                by_role[role]["declines"].append(entry)
            elif "REVIEW" in verdict:
                by_role[role]["reviews"].append(entry)

    # Build summary per role
    analysis = {}
    for role, data in by_role.items():
        total = len(data["all"])
        declines = len(data["declines"])
        screens = len(data["screens"])

        # Extract common rejection reasons
        decline_reasons = []
        for d in data["declines"]:
            decline_reasons.append(d["reason"])

        # Per-query (search tag) breakdown
        by_tag: Dict[str, Dict[str, int]] = {}
        by_tag_reasons: Dict[str, List[str]] = {}
        for e in data["all"]:
            tag = e.get("search_tag", "").strip()
            if not tag:
                continue
            if tag not in by_tag:
                by_tag[tag] = {"total": 0, "screens": 0, "declines": 0}
                by_tag_reasons[tag] = []
            by_tag[tag]["total"] += 1
            if e["verdict"] in ("SCREEN",):
                by_tag[tag]["screens"] += 1
            elif e["verdict"] in ("DECLINE",):
                by_tag[tag]["declines"] += 1
                by_tag_reasons[tag].append(e.get("reason", "")[:200])

        tag_stats = {}
        for tag, counts in by_tag.items():
            t = counts["total"]
            tag_stats[tag] = {
                "total": t,
                "screens": counts["screens"],
                "declines": counts["declines"],
                "pass_rate": f"{counts['screens']/t*100:.0f}%" if t > 0 else "N/A",
                "top_decline_reasons": by_tag_reasons.get(tag, [])[:5],
            }

        analysis[role] = {
            "total": total,
            "screens": screens,
            "declines": declines,
            "reviews": len(data["reviews"]),
            "pass_rate": f"{screens/total*100:.0f}%" if total > 0 else "N/A",
            "decline_reasons": decline_reasons,
            "decline_entries": data["declines"],
            "by_tag": tag_stats,
        }

    return analysis


# ── Load screening prompt ─────────────────────────────────────────

def load_prompt_requirements() -> str:
    """Load the screening prompt to extract role requirements."""
    if PROMPT_FILE.exists():
        return PROMPT_FILE.read_text(encoding="utf-8")
    return ""


# ── Call Claude for query generation ──────────────────────────────

def call_haiku(system: str, user: str) -> str:
    """Call Claude Haiku for query generation."""
    api_key = os.environ.get("CLAUDE_API_KEY", "")
    if not api_key:
        logger.error("CLAUDE_API_KEY not set")
        return ""

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 4096,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }

    req_data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=req_data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        blocks = result.get("content", [])
        return "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
    except Exception as e:
        logger.error("Haiku call failed: %s", e)
        return ""


# ── Generate Juicebox queries ─────────────────────────────────────

def generate_queries_for_role(
    role_name: str,
    role_info: Dict,
    rejection_analysis: Optional[Dict] = None,
    prompt_text: str = "",
    history_context: str = "",
) -> str:
    """
    Generate optimized Juicebox search queries for a specific role.
    Uses role requirements + rejection patterns to create targeted queries.
    """

    system = """You are a recruiting search query optimizer for Juicebox (PeopleGPT).

CRITICAL CONSTRAINT: Juicebox is an AI-powered people search that works best with SHORT, SIMPLE, NATURAL LANGUAGE queries (1-2 sentences). It is NOT boolean search.
- GOOD: "Sales operations leader at B2B SaaS startups in San Francisco" (gets 70+ results)
- BAD: "Revenue Operations AND Salesforce architect AND B2B SaaS AND pipeline automation AND Series A-C AND San Francisco" (gets 0 results — too specific)
- Keep each query under 20 words. Pick 2-3 key traits, not 8.
- Each query should target a DIFFERENT angle/archetype.

Output format for EACH query:
---
QUERY [number]: [short label]
Tag: JB: [RoleShort] [number] [short label]
Search: [1-2 sentence natural language search — MAX 20 words]
Filters: [Location + Experience years only]
Why: [one line — what type of candidate this finds]
Avoid: [one line — what this query excludes]
---

IMPORTANT: The "Tag" line is a short label for identifying this query. The Juicebox search name is automatically captured in the "Source Additional Notes" column when candidates are pushed. Keep it under 30 characters. Format: "GTM 1 SFDC Arch" or "VD 3 McKinsey".

Generate 5-8 queries per role, ranging from tight/precise to broader/creative. Label the first 3 as "Core" (highest hit rate) and the rest as "Exploratory" (creative angles, non-obvious profiles)."""

    user_parts = [f"Generate optimized Juicebox search queries for: **{role_name}**\n"]

    # Add role requirements
    user_parts.append("## Role Requirements")
    user_parts.append(f"Must have: {', '.join(role_info.get('must_have', []))}")
    user_parts.append(f"Avoid: {', '.join(role_info.get('avoid', []))}")
    if role_info.get("gold_path"):
        user_parts.append(f"Gold path: {role_info['gold_path']}")
    user_parts.append("")

    # Add context from screening prompt
    if prompt_text:
        # Extract just the role-specific section from the prompt
        role_section = _extract_role_section(prompt_text, role_name, role_info.get("short", ""))
        if role_section:
            user_parts.append("## Screening Criteria (what the AI screener evaluates)")
            user_parts.append(role_section[:3000])
            user_parts.append("")

    # Add rejection analysis if available
    if rejection_analysis:
        ra = rejection_analysis
        user_parts.append("## Past Rejection Data")
        user_parts.append(f"Pass rate: {ra.get('pass_rate', 'N/A')} ({ra.get('screens', 0)} passed, {ra.get('declines', 0)} declined out of {ra.get('total', 0)})")
        user_parts.append("")

        if ra.get("decline_reasons"):
            user_parts.append("### Common decline reasons (optimize queries to avoid these profiles):")
            for i, reason in enumerate(ra["decline_reasons"][:10]):
                user_parts.append(f"{i+1}. {reason[:300]}")
            user_parts.append("")

        # Per-query breakdown
        by_tag = ra.get("by_tag", {})
        if by_tag:
            user_parts.append("### Per-query performance (from Search Tag column):")
            for src, stats in sorted(by_tag.items(), key=lambda x: x[1].get("pass_rate", "0%")):
                user_parts.append(f"- **{src}**: {stats['pass_rate']} pass rate ({stats['screens']} passed, {stats['declines']} declined out of {stats['total']})")
                if stats.get("top_decline_reasons"):
                    user_parts.append(f"  Top decline reasons: {'; '.join(r[:100] for r in stats['top_decline_reasons'][:3])}")
            user_parts.append("")
            user_parts.append("Use this per-query data to decide which queries to KEEP, which to MODIFY, and which to REPLACE entirely.")

    user_parts.append("""## Company Context
Klarity is a venture-backed AI startup (SF + Bangalore). Enterprise AI for process intelligence and transformation. Voice AI + multimodal agents. Fortune 500 customers. Five days/week onsite in SF.

## Key Screening Gates (candidates MUST pass these)
- Must be able to work onsite in San Francisco (or relevant US metro for VD roles)
- Needs genuine builder evidence (not just impressive titles)
- For senior roles: 4+ years with evidence of shipping production systems
- Spark required: something that makes a hiring manager say "tell me more"
- Avoid: big-company-only careers with no startup/builder chapter, pure consulting without product work, admin/analyst patterns without system design""")

    if history_context:
        user_parts.append(f"\n{history_context}")

    return call_haiku(system, "\n".join(user_parts))


def _extract_role_section(prompt_text: str, role_name: str, short_name: str) -> str:
    """Extract the role-specific section from the screening prompt."""
    # Try to find the role section
    patterns = [
        rf"(?i){re.escape(role_name)}.*?(?=\n\n[A-Z]{{2,}}|\Z)",
        rf"(?i){re.escape(short_name)}.*?(?=\n\n[A-Z]{{2,}}|\Z)",
    ]

    for pattern in patterns:
        match = re.search(pattern, prompt_text, re.DOTALL)
        if match:
            return match.group(0)[:2000]

    return ""


# ── Suggest query refinements ─────────────────────────────────────

def suggest_refinements(
    role_name: str,
    role_info: Dict,
    rejection_analysis: Dict,
    history_context: str = "",
) -> str:
    """
    Given rejection patterns, suggest specific refinements to existing queries.
    This is the core auto-prompting feedback loop.
    """

    if not rejection_analysis.get("decline_reasons"):
        return f"No rejection data available for {role_name}. Run a screening batch first."

    system = """You are a recruiting search optimization advisor. Based on downstream screening rejection data, you suggest specific changes to upstream Juicebox search queries.

Your job: look at WHY candidates are being rejected and map each rejection pattern back to a search filter or keyword that could have prevented that candidate from entering the pipeline.

Be specific and actionable. Don't say "tighten filters" — say exactly WHAT to add, remove, or change in the search query.

Output format:
1. PATTERN: [what's happening]
   FIX: [specific query change]
   IMPACT: [how many candidates this would have filtered]

Then at the end, give 2-3 REVISED search queries that incorporate all fixes."""

    user = f"""Role: {role_name}

## Rejection Analysis
Total screened: {rejection_analysis.get('total', 0)}
Passed: {rejection_analysis.get('screens', 0)}
Declined: {rejection_analysis.get('declines', 0)}
Pass rate: {rejection_analysis.get('pass_rate', 'N/A')}

## Decline Reasons (each is one rejected candidate):
"""
    for i, entry in enumerate(rejection_analysis.get("decline_entries", [])[:15]):
        tag_label = f" [Query: {entry['search_tag']}]" if entry.get("search_tag") else ""
        user += f"\n{i+1}. {entry['name']}{tag_label}: {entry['reason'][:400]}"
        if entry.get("concerns"):
            user += f"\n   Concerns: {entry['concerns'][:300]}"

    # Per-query breakdown
    by_tag = rejection_analysis.get("by_tag", {})
    if by_tag:
        user += "\n\n## Per-Query Performance (from Search Tag column):\n"
        for src, stats in sorted(by_tag.items(), key=lambda x: x[1].get("pass_rate", "0%")):
            user += f"\n- {src}: {stats['pass_rate']} pass rate ({stats['screens']} passed, {stats['declines']} declined / {stats['total']} total)"
            if stats.get("top_decline_reasons"):
                user += f"\n  Top reasons: {'; '.join(r[:80] for r in stats['top_decline_reasons'][:3])}"
        user += "\n\nFor each query tag above, say whether to KEEP, MODIFY, or REPLACE it."

    user += f"""

## Current Role Requirements
Must have: {', '.join(role_info.get('must_have', []))}
Avoid: {', '.join(role_info.get('avoid', []))}

Based on the rejection patterns above, what specific search query changes would have prevented these bad candidates from entering the pipeline?"""

    if history_context:
        user += f"\n\n{history_context}"

    return call_haiku(system, user)


# ── Juicebox filter generator ──────────────────────────────────

def generate_juicebox_filters(
    role_name: str,
    role_info: Dict,
    rejection_analysis: Optional[Dict] = None,
    prompt_text: str = "",
    history_context: str = "",
) -> str:
    """
    Generate specific Juicebox filter/criteria configurations for a role.
    These are the actual settings to configure in the Juicebox UI.
    """

    system = """You are a recruiting search strategist writing queries for Juicebox (PeopleGPT).

CRITICAL: Juicebox is an AI-powered people search. It works best with SHORT, SIMPLE, NATURAL LANGUAGE queries — like how you'd describe a person to a friend. It does NOT work like LinkedIn boolean search.

RULES FOR JUICEBOX QUERIES:
- Keep search text to 1-2 sentences MAX. Shorter = better results.
- DO NOT use boolean operators (AND, OR, NOT).
- DO NOT chain many requirements together. Pick the 2-3 most important traits.
- DO NOT list specific skills, certifications, or tools in the search text. Use filters for those.
- Use plain English: "VP of Sales Operations at B2B SaaS startups in San Francisco" is PERFECT.
- BAD example: "Revenue Operations leader AND Salesforce architect AND B2B SaaS AND Series A-C AND pipeline automation AND 5+ years AND San Francisco Bay Area" — this returns 0 results because it's too specific.
- GOOD example: "Revenue operations manager at B2B SaaS startup in San Francisco with Salesforce experience" — simple, natural, gets 70+ results.

FILTERS: Keep to 3-4 max. Only use: Location, Experience years. Skip everything else unless critical.

Output format for EACH configuration:

---
CONFIGURATION [number]: [label]
TAG: JB: [RoleShort] [number] [short label]

SEARCH BAR TEXT:
[1-2 sentence natural language query — KEEP IT SHORT]

FILTERS TO SET:
- Location: [city/region]
- Experience: [years range]

EXCLUDE:
- [1-2 key exclusions only]

WHY THIS WORKS: [one line]
---

IMPORTANT: The "TAG" line is a short label (under 30 chars) for identifying this query. Format: "GTM 1 SFDC Arch" or "VD 3 McKinsey".

Generate 3 tight configurations and 2 broad ones. Each query should take a DIFFERENT ANGLE on the role — different job titles, different company types, different seniority levels. The search text for each should be no more than 20 words."""

    user_parts = [f"Generate Juicebox filter configurations for: **{role_name}**\n"]
    user_parts.append(f"Must have: {', '.join(role_info.get('must_have', []))}")
    user_parts.append(f"Avoid: {', '.join(role_info.get('avoid', []))}")
    if role_info.get("gold_path"):
        user_parts.append(f"Gold path: {role_info['gold_path']}")

    user_parts.append(f"\nCompany: Klarity — venture-backed AI startup, SF onsite 5 days/week.")
    user_parts.append(f"Fortune 500 customers. Enterprise AI for process intelligence and transformation.\n")

    if rejection_analysis:
        ra = rejection_analysis
        user_parts.append(f"## Past Data: {ra.get('pass_rate', 'N/A')} pass rate ({ra.get('declines', 0)} declined)")
        if ra.get("decline_reasons"):
            user_parts.append("\nTop decline reasons (design filters to EXCLUDE these profiles):")
            for i, reason in enumerate(ra["decline_reasons"][:8]):
                user_parts.append(f"  {i+1}. {reason[:250]}")

    if prompt_text:
        role_section = _extract_role_section(prompt_text, role_name, role_info.get("short", ""))
        if role_section:
            user_parts.append(f"\n## Role criteria from screening prompt:\n{role_section[:2000]}")

    user_parts.append("""
## Companies that produce strong candidates (from past screens):
Engineering: Stripe, Palantir, Databricks, Scale AI, Notion, Anthropic, Harvey, Glean, Decagon, Sierra, Lovable, Vercel, Linear
Consulting (for VD): McKinsey, BCG, Bain, Oliver Wyman
GTM: High-growth B2B SaaS startups (Series A-C, <200 employees)

## Companies that produce weak candidates (common declines):
- Very large slow enterprises: industrial conglomerates, big telecom, chip giants, Walmart-scale retail
- Big consulting without product chapter: Accenture, PwC, Deloitte, EY (as spine, not chapter)
- Government agencies (non-product)
- Large BPO/outsourcing firms""")

    if history_context:
        user_parts.append(f"\n{history_context}")

    return call_haiku(system, "\n".join(user_parts))


# ── Clipboard queue — walk through queries one at a time ─────────

def run_clipboard_queue(role_name: str, role_info: Dict, rejection_analysis: Optional[Dict], prompt_text: str):
    """
    Interactive mode: generates all configs for a role, then walks the user
    through each one — copies search text to clipboard, prints filters,
    waits for Enter before moving to the next.
    """
    import subprocess

    print(f"\n  Generating all configurations for {role_name}...\n")
    output = generate_juicebox_filters(role_name, role_info, rejection_analysis, prompt_text)

    if not output:
        print("  Failed to generate configs. Check CLAUDE_API_KEY.")
        return

    # Parse configurations from the output
    configs = _parse_configs(output)

    if not configs:
        print("  Could not parse configurations. Raw output:\n")
        print(output)
        return

    print(f"  Generated {len(configs)} configurations. Starting queue...\n")
    print(f"  {'='*60}")
    print(f"  CLIPBOARD QUEUE — {role_name}")
    print(f"  Open Juicebox, then follow along.")
    print(f"  {'='*60}\n")

    for i, config in enumerate(configs):
        search_text = config.get("search", "").strip()
        label = config.get("label", f"Config {i+1}")
        filters_text = config.get("filters", "")
        exclude_text = config.get("exclude", "")
        why_text = config.get("why", "")

        print(f"  ┌─────────────────────────────────────────────────────")
        print(f"  │  [{i+1}/{len(configs)}]  {label}")
        print(f"  └─────────────────────────────────────────────────────")

        # Copy search text to clipboard
        if search_text:
            try:
                subprocess.run(["pbcopy"], input=search_text.encode("utf-8"), check=True)
                print(f"\n  ✅ Search text COPIED to clipboard — just Cmd+V in Juicebox\n")
            except Exception:
                print(f"\n  Search text (copy manually):\n  {search_text}\n")
        else:
            print(f"\n  ⚠️  No search text found for this config\n")

        if filters_text:
            print(f"  FILTERS to set:")
            for line in filters_text.strip().split("\n"):
                line = line.strip()
                if line:
                    print(f"    {line}")
            print()

        if exclude_text:
            print(f"  EXCLUDE:")
            for line in exclude_text.strip().split("\n"):
                line = line.strip()
                if line:
                    print(f"    {line}")
            print()

        if why_text:
            print(f"  Why: {why_text}\n")

        if i < len(configs) - 1:
            input(f"  ⏎  Press Enter when done → next query ({i+2}/{len(configs)})...")
            print()
        else:
            input(f"  ⏎  Press Enter when done → finished!")
            print()

    print(f"\n  {'='*60}")
    print(f"  Done! Ran {len(configs)} queries for {role_name}.")
    print(f"  Now export Candidate Screener as CSV and run:")
    print(f"    prescore --csv <path>")
    print(f"  to filter before full screening.")
    print(f"  {'='*60}\n")


def _parse_configs(output: str) -> List[Dict]:
    """Parse CONFIGURATION blocks from Haiku output."""
    configs = []
    # Split on CONFIGURATION headers (handles markdown: ## CONFIGURATION, **CONFIGURATION**, etc.)
    blocks = re.split(r"(?:---\s*\n)?[\s#*]*CONFIGURATION\s+\d+\s*:", output)

    for block in blocks:
        block = block.strip()
        if not block or len(block) < 30:
            continue

        config: Dict[str, str] = {}

        # Extract label (first line or text before SEARCH BAR)
        label_match = re.match(r"(.+?)(?:\n|SEARCH)", block)
        if label_match:
            config["label"] = label_match.group(1).strip().strip("*#- ")

        # Extract search bar text (handles **SEARCH BAR TEXT:** markdown)
        search_match = re.search(r"\*{0,2}SEARCH BAR TEXT:?\*{0,2}\s*\n(.+?)(?:\n\s*\n|\n\*{0,2}FILTERS)", block, re.DOTALL)
        if search_match:
            config["search"] = search_match.group(1).strip().strip("*`")

        # Extract filters (handles **FILTERS TO SET:** markdown)
        filters_match = re.search(r"\*{0,2}FILTERS TO SET:?\*{0,2}\s*\n(.+?)(?:\n\s*\n(?:\*{0,2}EXCLUDE|\*{0,2}WHY)|$)", block, re.DOTALL)
        if filters_match:
            config["filters"] = filters_match.group(1).strip()

        # Extract exclusions
        exclude_match = re.search(r"\*{0,2}EXCLUDE[^:]*:\*{0,2}\s*\n(.+?)(?:\n\s*\n(?:\*{0,2}WHY)|$)", block, re.DOTALL)
        if exclude_match:
            config["exclude"] = exclude_match.group(1).strip()

        # Extract why
        why_match = re.search(r"\*{0,2}WHY THIS WORKS:?\*{0,2}\s*(.+?)(?:\n---|$)", block, re.DOTALL)
        if why_match:
            config["why"] = why_match.group(1).strip()

        if config.get("search") or config.get("filters"):
            configs.append(config)

    return configs


# ── Query log ─────────────────────────────────────────────────────

def _load_query_log() -> list:
    if QUERY_LOG_FILE.exists():
        try:
            return json.loads(QUERY_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_query_log(log: list):
    QUERY_LOG_FILE.write_text(json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")


def _push_learning_to_sheet(role: str, learning: str, source: str = "optimizer", notes: str = ""):
    """Push a learning to the Juicebox Query Learnings tab in the sheet."""
    if not APPS_SCRIPT_URL:
        return
    payload = json.dumps({
        "agent": "juicebox",
        "action": "appendJuiceboxLearning",
        "role": role,
        "learning": learning[:2000],
        "source": source,
        "notes": notes[:500],
    }).encode("utf-8")
    req = urllib.request.Request(
        APPS_SCRIPT_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except Exception as e:
        logger.warning("Could not push learning to sheet: %s", e)


def log_queries(role: str, queries: str, mode: str, rejection_analysis: Optional[Dict] = None):
    """Log generated queries for tracking improvement over time."""
    log = _load_query_log()
    entry: Dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "role": role,
        "mode": mode,
        "queries": queries[:5000],
    }
    if rejection_analysis:
        top_patterns = [r[:80] for r in rejection_analysis.get("decline_reasons", [])[:5]]
        entry["rejection_summary"] = {
            "total": rejection_analysis.get("total", 0),
            "screens": rejection_analysis.get("screens", 0),
            "declines": rejection_analysis.get("declines", 0),
            "pass_rate": rejection_analysis.get("pass_rate", "N/A"),
            "top_decline_patterns": top_patterns,
            "by_tag": {src: {"pass_rate": s["pass_rate"], "total": s["total"]}
                          for src, s in rejection_analysis.get("by_tag", {}).items()},
        }
        # Push a summary to the sheet
        summary = f"Pass rate: {rejection_analysis.get('pass_rate', 'N/A')} ({rejection_analysis.get('screens', 0)}/{rejection_analysis.get('total', 0)})"
        if top_patterns:
            summary += f". Top decline patterns: {'; '.join(top_patterns[:3])}"
        by_tag = rejection_analysis.get("by_tag", {})
        if by_tag:
            tag_parts = [f"{t}: {s['pass_rate']}" for t, s in by_tag.items()]
            summary += f". Per-query: {', '.join(tag_parts)}"
        _push_learning_to_sheet(role, summary, source=mode, notes=queries[:500])

    log.append(entry)
    # Keep last 50 entries
    if len(log) > 50:
        log = log[-50:]
    _save_query_log(log)


def _build_history_context(role: str, current_ra: Optional[Dict] = None):
    """
    Load past log entries for this role and compare against current rejection patterns.
    Returns (history_prompt_text, progress_report) — both empty strings if no history.
    """
    log = _load_query_log()
    past = [e for e in log if e.get("role", "").lower() == role.lower()]
    past.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    past = past[:3]  # last 3 runs

    if not past:
        return "", ""

    # Build prompt text for Haiku
    history_lines = ["## What Was Tried Before (past runs for this role)\n"]
    for entry in past:
        ts = entry.get("timestamp", "unknown date")
        mode = entry.get("mode", "unknown")
        summary = entry.get("rejection_summary", {})
        history_lines.append(f"### Run on {ts} (mode: {mode})")
        if summary:
            history_lines.append(f"Pass rate at that time: {summary.get('pass_rate', 'N/A')} "
                                 f"({summary.get('screens', '?')} passed, {summary.get('declines', '?')} declined)")
            patterns = summary.get("top_decline_patterns", [])
            if patterns:
                history_lines.append("Top decline patterns: " + "; ".join(patterns))
        history_lines.append(f"Suggestions given:\n{entry.get('queries', '')[:600]}\n")

    history_lines.append("Use this history to avoid repeating suggestions that didn't work "
                         "and to double down on approaches that improved the pass rate.\n")
    history_prompt = "\n".join(history_lines)

    # Build progress report (compare current vs most recent past)
    progress = ""
    most_recent = past[0]
    prev_summary = most_recent.get("rejection_summary", {})
    if current_ra and prev_summary:
        prev_rate = prev_summary.get("pass_rate", "N/A")
        curr_rate = current_ra.get("pass_rate", "N/A")
        prev_patterns = [p.lower().strip() for p in prev_summary.get("top_decline_patterns", [])]
        curr_patterns = [r[:80].lower().strip() for r in current_ra.get("decline_reasons", [])[:5]]

        # Classify patterns
        repeating = []
        improved = []
        new_patterns = []
        for cp in curr_patterns:
            matched = any(pp in cp or cp in pp for pp in prev_patterns if pp and cp)
            if matched:
                repeating.append(cp[:60])
            else:
                new_patterns.append(cp[:60])
        for pp in prev_patterns:
            matched = any(pp in cp or cp in pp for cp in curr_patterns if pp and cp)
            if not matched:
                improved.append(pp[:60])

        lines = [
            f"  PROGRESS vs. last run ({most_recent.get('timestamp', '?')[:10]})",
            f"  Pass rate: {prev_rate} → {curr_rate}",
        ]
        if improved:
            lines.append(f"  Improved (no longer top pattern):  {'; '.join(improved)}")
        if repeating:
            lines.append(f"  Still repeating:                   {'; '.join(repeating)}")
        if new_patterns:
            lines.append(f"  New patterns:                      {'; '.join(new_patterns)}")
        if not improved and not repeating and not new_patterns:
            lines.append("  (Not enough pattern data to compare)")
        progress = "\n".join(lines)

    return history_prompt, progress


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate optimized Juicebox search queries using screening rejection data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 juicebox_query_optimizer.py                          # all roles, use latest results
  python3 juicebox_query_optimizer.py --role "Value Delivery"  # specific role
  python3 juicebox_query_optimizer.py --role GTM               # partial match works
  python3 juicebox_query_optimizer.py --fresh                  # generate from scratch (no rejection data)
  python3 juicebox_query_optimizer.py --refine                 # refinement mode (requires results)
        """,
    )
    parser.add_argument("--role", default="",
                        help="Filter to a specific role (partial match, e.g. 'VD', 'GTM', 'Frontend')")
    parser.add_argument("--results", default="",
                        help="Path to screening_results.json or CSV export (default: auto-find)")
    parser.add_argument("--archive", default="",
                        help="Path to Archive tab CSV — richest source of rejection data")
    parser.add_argument("--screen-csv", default="",
                        help="Path to Screen tab CSV — learn what GOOD candidates look like")
    parser.add_argument("--all-results", action="store_true",
                        help="Scan all screening_results*.json files in the directory")
    parser.add_argument("--fresh", action="store_true",
                        help="Generate queries from role requirements only (ignore rejection data)")
    parser.add_argument("--refine", action="store_true",
                        help="Refinement mode: focus on fixing rejection patterns")
    parser.add_argument("--filters", action="store_true",
                        help="Output specific Juicebox filter settings (location, experience, company type)")
    parser.add_argument("--queue", action="store_true",
                        help="Clipboard queue mode: walk through each query interactively, auto-copy to clipboard")
    parser.add_argument("--output", default="",
                        help="Save output to file instead of printing")
    args = parser.parse_args()

    # Validate
    if not os.environ.get("CLAUDE_API_KEY"):
        print("Error: CLAUDE_API_KEY not set")
        sys.exit(1)

    # Load screening prompt
    prompt_text = load_prompt_requirements()
    if not prompt_text:
        print("Warning: Could not load screening prompt from prompts/opus_body.md")
        print("Queries will be generated from role definitions only.\n")

    # Load results from multiple sources
    results = []
    if not args.fresh:
        # Source 1: Archive CSV (richest rejection data)
        if args.archive:
            archive_results = load_results(args.archive)
            if archive_results:
                print(f"Loaded {len(archive_results)} candidates from Archive CSV.")
                results.extend(archive_results)

        # Source 2: Screen CSV (what good looks like)
        if args.screen_csv:
            screen_results = load_results(args.screen_csv)
            if screen_results:
                print(f"Loaded {len(screen_results)} passed candidates from Screen CSV.")
                results.extend(screen_results)

        # Source 3: screening_results.json (latest batch)
        if args.all_results:
            json_results = load_all_results_in_dir()
        elif args.results:
            json_results = load_results(args.results)
        else:
            json_results = load_results()
        if json_results:
            print(f"Loaded {len(json_results)} results from JSON.")
            results.extend(json_results)

        # Source 4: Auto-find CSVs in Downloads
        if not results:
            downloads = Path.home() / "Downloads"
            for pattern in ["*Archived*", "*Archive*", "*Nurture*"]:
                for f in sorted(downloads.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
                    if f.suffix == ".csv":
                        auto_results = load_results(str(f))
                        if auto_results:
                            print(f"Auto-found: {f.name} ({len(auto_results)} candidates)")
                            results.extend(auto_results)
                        break

    if results:
        declines = sum(1 for r in results if r.get("verdict", "").upper() in ("DECLINE", "NO", "LEAN NO"))
        screens = sum(1 for r in results if r.get("verdict", "").upper() in ("SCREEN", "STRONG YES", "WEAK YES"))
        print(f"\nTotal: {len(results)} candidates ({screens} passed, {declines} declined)\n")
    elif not args.fresh:
        print("No screening results found. Generating queries from role requirements only.")
        print("Tip: Export the Archived tab as CSV and pass with --archive for much better queries.\n")

    # Filter roles
    roles_to_process = {}
    for role_name, role_info in ACTIVE_ROLES.items():
        if args.role:
            role_key = args.role.lower()
            if role_key not in role_name.lower() and role_key not in role_info["short"].lower():
                continue
        roles_to_process[role_name] = role_info

    if not roles_to_process:
        print(f"No matching roles found for '{args.role}'")
        print(f"Available roles: {', '.join(ACTIVE_ROLES.keys())}")
        sys.exit(1)

    # Analyze rejections
    rejection_analysis = {}
    if results:
        rejection_analysis = analyze_rejections(results, args.role)

    # Generate queries for each role
    all_output = []

    for role_name, role_info in roles_to_process.items():
        print(f"{'='*60}")
        print(f"  {role_name} ({role_info['short']})")
        print(f"{'='*60}")

        ra = rejection_analysis.get(role_name)

        if ra:
            print(f"  Screening data: {ra['total']} candidates | {ra['pass_rate']} pass rate")
            print(f"  ({ra['screens']} screens, {ra['declines']} declines, {ra['reviews']} reviews)")

            # Per-query breakdown
            by_tag = ra.get("by_tag", {})
            if by_tag:
                print(f"\n  Per-query breakdown:")
                for src, stats in sorted(by_tag.items(), key=lambda x: -x[1].get("total", 0)):
                    print(f"    {src}: {stats['pass_rate']} pass rate "
                          f"({stats['screens']} passed, {stats['declines']} declined / {stats['total']} total)")
            else:
                print(f"\n  No per-query data found. The extension writes the search name")
                print(f"  to 'Source Additional Notes' — make sure that column is in your export.")
        else:
            print("  No screening data available for this role")

        # Load history and show progress report
        history_prompt, progress_report = _build_history_context(role_name, ra)
        if progress_report:
            print()
            print(progress_report)

        print()

        if args.queue:
            run_clipboard_queue(role_name, role_info, ra, prompt_text)
            continue
        elif args.filters:
            print("  Generating specific Juicebox filter settings...\n")
            output = generate_juicebox_filters(role_name, role_info, ra, prompt_text, history_context=history_prompt)
            mode = "filters"
        elif args.refine and ra:
            print("  Analyzing rejection patterns and generating refinements...\n")
            output = suggest_refinements(role_name, role_info, ra, history_context=history_prompt)
            mode = "refine"
        else:
            print("  Generating optimized search queries...\n")
            output = generate_queries_for_role(role_name, role_info, ra, prompt_text, history_context=history_prompt)
            mode = "generate"

        if output:
            print(output)
            print()
            all_output.append(f"# {role_name} ({role_info['short']})\n\n{output}")
            log_queries(role_name, output, mode, rejection_analysis=ra)
        else:
            print("  Failed to generate queries. Check CLAUDE_API_KEY.\n")

    # Save to file if requested
    if args.output and all_output:
        output_text = "\n\n" + ("="*60) + "\n\n".join(all_output)
        Path(args.output).write_text(output_text, encoding="utf-8")
        print(f"\nSaved to: {args.output}")

    # Summary
    if rejection_analysis:
        print(f"\n{'='*60}")
        print("  OVERALL SUMMARY")
        print(f"{'='*60}")
        total_all = sum(ra["total"] for ra in rejection_analysis.values())
        total_screens = sum(ra["screens"] for ra in rejection_analysis.values())
        total_declines = sum(ra["declines"] for ra in rejection_analysis.values())
        if total_all > 0:
            print(f"  Total candidates analyzed:  {total_all}")
            print(f"  Overall pass rate:          {total_screens/total_all*100:.0f}%")
            print(f"  Screens: {total_screens}  |  Declines: {total_declines}")
            print(f"\n  Target: improve pass rate from {total_screens/total_all*100:.0f}% toward 25-40%")
            print(f"  by tightening Juicebox queries based on rejection patterns above.")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
