#!/usr/bin/env python3
"""
YC Work at a Startup → Ashby outbound sourcer (v2).

v2 changes vs v1:
  - Async Playwright. Profile fetches run 5 concurrent, exports 3 concurrent.
  - Multi-query archetype mode: --config waas_query_configs/frontend_engineer.json
    runs N keyword queries, unions by short_id, ranks, filters, exports.
  - Tighter local filters: YOE floor (3+), inactive >90d cut, optional
    design-signal requirement for DE, negative-company de-rank.
  - --review-shortlist writes CSV preview + exits before any Ashby write.
  - Latency target: 50 candidates in ~7 minutes (was ~25).

Single-query mode (back-compat):
  python3 waas_sourcer.py "react typescript ai" --target-role "AI Frontend Engineer" --limit 10

Archetype mode (new):
  python3 waas_sourcer.py --config waas_query_configs/frontend_engineer.json --limit 50
  python3 waas_sourcer.py --config waas_query_configs/design_engineer.json --limit 50 --review-shortlist
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Use the dedicated WaaS Ashby key if set (Default Source = "Sourced: Y Combinator
# Work at a Startup"). Falls back to the main key otherwise. Must be done BEFORE
# importing ashby_bridge / push_to_ashby — they read ASHBY_API_KEY at module load.
_waas_key = os.environ.get("WAAS_ASHBY_API_KEY", "").strip()
if _waas_key:
    os.environ["ASHBY_API_KEY"] = _waas_key

logger = logging.getLogger("waas")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

ROOT = Path(__file__).resolve().parent
STATE_PATH = Path.home() / ".waas_playwright_state.json"
LOG_PATH = ROOT / ".waas_export_log.json"
ROUTING_PATH = ROOT / ".ashby_job_routing.json"
CONFIGS_DIR = ROOT / "waas_query_configs"
SHORTLIST_CSV = ROOT / ".waas_shortlist.csv"
BASE = "https://bookface.ycombinator.com"
FALLBACK_JOB = "Outbound Sourced"

# Safety stop for the all-pages sweep (pages<=0). WaaS rarely returns useful
# results past ~20 pages for one keyword; this is just a runaway guard.
MAX_PAGES_HARD_CAP = 40

# Concurrency knobs
PROFILE_FETCH_PARALLELISM = 5
# Export parallelism MUST be 1. Bookface's ATS hydration endpoint single-flights
# per session; concurrent calls cause 90%+ failure rate ("ATS section never hydrated").
# Verified 2026-04-28: parallelism=2 worked on 3-candidate tests but degraded over 50;
# Paul Caroline failed at p=2 yet succeeded at p=1.
EXPORT_PARALLELISM = 1
EXPORT_STAGGER_S = 0.0
# Pause between exports — Bookface's ATS hydration endpoint throttles after one export;
# 2026-04-28: with 1.5s pause, 1/3 candidates succeeded; the rest hit hydration timeout.
EXPORT_PAUSE_S = 30.0
ATS_HYDRATE_TIMEOUT_MS = 30_000  # diag mode: shorter timeout for fast feedback

# Boost / signal lists
YC_ALUM = {
    "airbnb", "stripe", "doordash", "coinbase", "instacart", "dropbox", "reddit",
    "twitch", "brex", "gusto", "flexport", "faire", "rippling", "whatnot",
    "retool", "vanta", "deel", "mercury", "razorpay", "scale ai", "scale",
    "cruise", "zapier", "segment", "amplitude", "notion", "figma", "linear",
    "anthropic", "openai", "perplexity", "cohere", "hugging face", "sierra",
    "pinecone", "langchain", "ramp", "modal", "replit", "cursor", "vercel",
    "cartesia", "paradigm", "topo", "lindy", "inventive ai",
}
BIG_TECH = {
    "google", "meta", "facebook", "apple", "amazon", "microsoft", "netflix",
    "nvidia", "tesla", "uber", "lyft", "palantir", "snowflake", "databricks",
    "mongodb", "shopify", "atlassian", "cloudflare", "datadog", "elastic",
    "twilio",
}

# Title patterns — penalty (down-rank, not hard reject — WaaS title text is noisy)
NEGATIVE_TITLE_PATTERNS = [
    "qa engineer", "test engineer", "test automation", "automation engineer", "sdet",
    "java full stack", "java developer", "java backend", "j2ee", "spring boot developer",
    "salesforce developer", "salesforce admin", "sap consultant",
    ".net developer", "asp.net", "wordpress developer", "drupal developer",
    "android developer", "ios developer",
    "devrel", "developer relations", "developer advocate",
    "data scientist", "ml researcher", "data analyst",
    "it support", "system administrator", "network engineer",
]
# Company-name substrings that signal IT consulting / freelance / agency / generic offshore body-shop.
# Founder/founding titles at these don't count as a quality signal.
NEGATIVE_COMPANY_PATTERNS = [
    "consult", "freelanc", "upwork", "fiverr", "remoterep", "contractor",
    "services pvt", "solutions pvt", "infotech", "systems ltd", "it services",
    "java full stack", "wordpress", "drupal", ".net",
    "infosys", "tcs", "tata consultancy", "wipro", "cognizant", "accenture", "capgemini", "hcl",
    "agency", "creative studio",
]
POSITIVE_TITLE_PATTERNS = ["founding", "staff", "principal", "tech lead", "design engineer", "first engineer"]
SENIOR_TITLE_PATTERNS = ["staff", "principal", "senior", "lead"]

# Design signal — at least one of these must appear in skills/title for DE filter
DESIGN_SIGNALS = {
    "figma", "design systems", "design system", "framer", "sketch",
    "ui design", "ux design", "design engineer", "tailwind",
}

# FE skill-stack — at least one must appear in skills/titles for FE/DE roles to pass
FE_STACK_SIGNALS = {
    "react", "typescript", "next.js", "nextjs", "frontend", "front-end",
    "design engineer", "javascript", "vue", "svelte", "tailwind",
}

# DevSecOps title signals — must appear in a job TITLE, not just skills.
# Keeps out data analysts and ML engineers who happen to list "kubernetes"
# as a skill but have never held a security/devops role.
DEVSECOPS_TITLE_SIGNALS = {
    "devsecops", "devops", "security engineer", "security architect",
    "site reliability", "sre", "platform engineer", "infrastructure engineer",
    "cloud engineer", "cloud security", "infosec", "appsec", "application security",
    "compliance engineer", "security lead", "head of security",
}

# Product Engineer title signals — must appear in a job TITLE.
# Filters out designers, data folks, PMs without engineering tenure.
PRODUCT_ENG_TITLE_SIGNALS = {
    "product engineer", "founding engineer", "full stack", "fullstack", "full-stack",
    "software engineer", "swe", "founder", "co-founder", "cofounder",
    "tech lead", "engineering lead", "lead engineer", "staff engineer",
    "principal engineer", "senior engineer", "head of engineering",
}

# Product Manager title signals — must appear in a job TITLE.
# Keeps the PM pool to people who have actually held a product-owning role:
# PM titles at any level, plus founders/CEOs/CTOs who own(ed) the product.
# Mirrors the screen's 12-month PM/product-ownership gate so we source for the bar.
PM_TITLE_SIGNALS = {
    "product manager", "product management", "product owner", "product lead",
    "lead product", "head of product", "vp product", "vp of product",
    "chief product", "cpo", "group product manager", "principal product",
    "senior product", "director of product", "product director", "founding product",
    "founder", "co-founder", "cofounder", "ceo", "chief technology officer", "cto",
}


# ── Routing & log ────────────────────────────────────────────────────

def load_routing() -> Dict[str, str]:
    if not ROUTING_PATH.exists():
        return {}
    data = json.loads(ROUTING_PATH.read_text())
    out: Dict[str, str] = {}
    for _role, meta in data.get("roles", {}).items():
        title = meta["job_title"]
        out[title.lower()] = title
        for alias in meta.get("aliases", []):
            out[alias.lower()] = title
    return out


def resolve_job(target: str, routing: Dict[str, str]) -> str:
    if not target:
        return FALLBACK_JOB
    key = target.strip().lower()
    if key in routing:
        return routing[key]
    for alias, title in routing.items():
        if key in alias or alias in key:
            return title
    return FALLBACK_JOB


def load_log() -> dict:
    if LOG_PATH.exists():
        try:
            return json.loads(LOG_PATH.read_text())
        except Exception:
            pass
    return {}


def save_log(log: dict) -> None:
    LOG_PATH.write_text(json.dumps(log, indent=2, sort_keys=True))


# ── Login flow (sync — only runs interactively) ──────────────────────

def login_flow() -> None:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto(BASE + "/login", wait_until="domcontentloaded")
        print("\n  >>> Log into Bookface in the browser window that just opened.")
        print("  >>> Once you see the dashboard, press Enter here to save the session.")
        input()
        STATE_PATH.write_text(json.dumps(ctx.storage_state()))
        print(f"  Saved session → {STATE_PATH}")
        browser.close()


# ── Search via authenticated browser context (async) ─────────────────

def _strip_em(s: Any) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"</?em>", "", s)


async def search_page(req, query: str, page_n: int, min_exp: int, max_exp: int) -> Dict[str, Any]:
    qs: Dict[str, str] = {
        "search[keyword]": query,
        "search[sort]": "match_v2",
        "search[state]": "applied",
        "page": str(page_n),
    }
    if min_exp > 0:
        qs["search[min_experience]"] = str(min_exp)
    if max_exp > 0:
        qs["search[max_experience]"] = str(max_exp)
    resp = await req.get(
        f"{BASE}/workatastartup/candidates",
        params=qs,
        headers={"Accept": "application/json"},
        timeout=20000,
    )
    if not resp.ok:
        raise RuntimeError(f"Search failed: HTTP {resp.status}")
    return await resp.json()


def _hit_from_meta(oid: str, meta: dict) -> Optional[Dict[str, Any]]:
    hl = meta.get("_highlightResult") or {}
    short_id = (hl.get("short_id") or {}).get("value") or ""
    if not short_id:
        return None
    skills_raw = hl.get("skills_for_search") or []
    skills: List[str] = []
    if isinstance(skills_raw, list):
        for s in skills_raw:
            if isinstance(s, dict):
                v = _strip_em(s.get("value", ""))
                if v:
                    skills.append(v)
    return {
        "object_id": oid,
        "short_id": short_id,
        "final_score": meta.get("final_score"),
        "positions": meta.get("positions_for_search") or [],
        "educations": meta.get("educations_for_search") or [],
        "skills": skills[:20],
        "short_phrase": _strip_em((hl.get("short_phrase") or {}).get("value", "")),
    }


async def collect_query(req, q_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    keyword = q_cfg["keyword"]
    # pages <= 0 means "walk every page until the results run out" (all-pages sweep),
    # bounded by MAX_PAGES_HARD_CAP as a safety stop.
    pages_cfg = int(q_cfg.get("pages", 3))
    pages = MAX_PAGES_HARD_CAP if pages_cfg <= 0 else pages_cfg
    min_exp = int(q_cfg.get("min_exp", 0))
    max_exp = int(q_cfg.get("max_exp", 0))
    hits: List[Dict[str, Any]] = []
    seen_short_ids: set[str] = set()
    for n in range(1, pages + 1):
        try:
            data = await search_page(req, keyword, n, min_exp, max_exp)
        except Exception as e:
            logger.warning("  [%s] page %d failed: %s", keyword[:30], n, e)
            break
        meta_by_id = data.get("search_meta_by_id") or {}
        if not meta_by_id:
            break  # empty page = past the last page of results
        new_this_page = 0
        for oid, meta in meta_by_id.items():
            h = _hit_from_meta(oid, meta)
            if not h:
                continue
            sid = h["short_id"]
            if sid in seen_short_ids:
                continue  # WaaS repeats the last page once you page past the end
            seen_short_ids.add(sid)
            new_this_page += 1
            h["_query"] = keyword
            hits.append(h)
        if new_this_page == 0:
            break  # page added nothing new = exhausted
        await asyncio.sleep(0.4)
    logger.info("  [%s] %d hits across %d page(s)", keyword[:40], len(hits), n)
    return hits


async def fetch_profile(req, short_id: str, sem: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
    async with sem:
        try:
            resp = await req.get(
                f"{BASE}/workatastartup/candidates/{short_id}",
                headers={"Accept": "application/json"},
                timeout=15000,
            )
            if not resp.ok:
                return None
            return await resp.json()
        except Exception as e:
            logger.warning("  profile fetch failed (%s): %s", short_id, e)
            return None


# ── Score + filter ───────────────────────────────────────────────────

def _parse_ts(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _yoe_int(prof: dict) -> Optional[int]:
    try:
        v = (prof.get("data") or {}).get("experience")
        return int(v) if v is not None else None
    except Exception:
        return None


def _has_design_signal(hit: Dict[str, Any], profile: Dict[str, Any]) -> bool:
    skills_lower = {s.lower() for s in (hit.get("skills") or [])}
    if skills_lower & DESIGN_SIGNALS:
        return True
    titles = " ".join(
        ((p.get("title") or "").lower() for p in hit.get("positions") or [])
    )
    if any(sig in titles for sig in DESIGN_SIGNALS):
        return True
    return False


def quality_signals(hit: Dict[str, Any]) -> Dict[str, bool]:
    """Identify strong builder/employer signals. Used for tier-based ranking + hard gate."""
    yc_alum = False
    big_tech_senior = False
    founding_at_real_co = False
    senior_at_named_co = False  # staff/principal at any non-trash company

    for pos in hit.get("positions") or []:
        emp = (pos.get("employer_name") or "").strip().lower()
        title = (pos.get("title") or "").strip().lower()
        is_negative_co = any(pat in emp for pat in NEGATIVE_COMPANY_PATTERNS)

        if emp in YC_ALUM or any(yc in emp for yc in YC_ALUM):
            yc_alum = True
        if (emp in BIG_TECH or any(bt in emp for bt in BIG_TECH)) and \
           any(sr in title for sr in SENIOR_TITLE_PATTERNS):
            big_tech_senior = True
        if any(t in title for t in ("founding", "first engineer", "co-founder", "founder")):
            if not is_negative_co and emp:
                founding_at_real_co = True
        if any(t in title for t in ("staff", "principal")) and not is_negative_co and emp:
            senior_at_named_co = True

    return {
        "yc_alum": yc_alum,
        "big_tech_senior": big_tech_senior,
        "founding_at_real_co": founding_at_real_co,
        "senior_at_named_co": senior_at_named_co,
    }


def has_fe_stack(hit: Dict[str, Any]) -> bool:
    skills_lower = {s.lower() for s in (hit.get("skills") or [])}
    if skills_lower & FE_STACK_SIGNALS:
        return True
    titles = " ".join(((p.get("title") or "").lower() for p in hit.get("positions") or []))
    return any(sig in titles for sig in FE_STACK_SIGNALS)


def _has_title_signal(hit: Dict[str, Any], signals: set) -> bool:
    """Title-only signal check. Stricter than _has_signal_set — skills don't count.
    A 'kubernetes' skill on a data analyst's profile will not pass a devsecops gate."""
    titles = " ".join(((p.get("title") or "").lower() for p in hit.get("positions") or []))
    return any(sig in titles for sig in signals)


def has_devsecops_signal(hit: Dict[str, Any]) -> bool:
    return _has_title_signal(hit, DEVSECOPS_TITLE_SIGNALS)


def has_product_eng_signal(hit: Dict[str, Any]) -> bool:
    return _has_title_signal(hit, PRODUCT_ENG_TITLE_SIGNALS)


def has_pm_signal(hit: Dict[str, Any]) -> bool:
    return _has_title_signal(hit, PM_TITLE_SIGNALS)


def score(hit: Dict[str, Any], profile: Dict[str, Any], boost_keywords: List[str], negative_co: List[str]) -> Tuple[float, int]:
    """Return (score, tier). Tier 0 = at least one strong quality signal, Tier 1 = none."""
    s = 0.0

    # WaaS keyword-match score: cap and weight LIGHTLY (was the dominant term, now isn't).
    fs = hit.get("final_score")
    if isinstance(fs, (int, float)):
        s += min(3.0, max(0.0, (fs + 5.0) * 0.3))

    prof = profile.get("profile") or {}
    last = _parse_ts(prof.get("last_active_at", ""))
    if last:
        days = (datetime.now(timezone.utc) - last).days
        if days <= 30: s += 2
        elif days <= 60: s += 1

    pos_titles: List[str] = []
    pos_emps: List[str] = []
    for pos in hit.get("positions") or []:
        emp = (pos.get("employer_name") or "").strip().lower()
        title = (pos.get("title") or "").strip().lower()
        pos_titles.append(title)
        pos_emps.append(emp)
        is_negative_co = any(pat in emp for pat in NEGATIVE_COMPANY_PATTERNS) or \
                         any(pat in emp for pat in negative_co)

        # Big employer signals — these dominate ranking now
        if emp in YC_ALUM or any(yc in emp for yc in YC_ALUM):
            s += 5
        elif emp in BIG_TECH or any(bt in emp for bt in BIG_TECH):
            if any(sr in title for sr in SENIOR_TITLE_PATTERNS):
                s += 3
            else:
                s += 1

        if is_negative_co:
            s -= 4

        # Title-quality signals
        if any(kw in title for kw in POSITIVE_TITLE_PATTERNS) and not is_negative_co:
            s += 1.5
        if any(neg in title for neg in NEGATIVE_TITLE_PATTERNS):
            s -= 4

        # Company name carrying negative-title pattern (e.g. company literally called
        # "Java Full Stack Developer" or ".NET Solutions")
        if any(neg in emp for neg in NEGATIVE_TITLE_PATTERNS):
            s -= 4

    skills_blob = " ".join((hit.get("skills") or [])).lower()
    titles_blob = " ".join(pos_titles)
    for bk in boost_keywords:
        bkl = bk.lower()
        if bkl in skills_blob or bkl in titles_blob:
            s += 0.6

    yoe = _yoe_int(prof)
    if yoe is not None:
        if 3 <= yoe <= 12:
            s += 1.0
        elif yoe >= 15:
            s -= 1.0

    sigs = quality_signals(hit)
    tier = 0 if any(sigs.values()) else 1
    return round(s, 2), tier


def passes_filters(hit: Dict[str, Any], profile: Dict[str, Any], filters: Dict[str, Any], us_only: bool, city: str = "") -> Tuple[bool, str]:
    prof = profile.get("profile") or {}
    data = prof.get("data") or {}
    user = prof.get("user") or {}

    linkedin = (data.get("linkedin") or user.get("linkedin_url") or "").strip()
    if filters.get("require_linkedin", True) and not linkedin:
        return False, "no_linkedin"

    active_within = int(filters.get("active_within_days", 0) or 0)
    if active_within > 0:
        last = _parse_ts(prof.get("last_active_at", ""))
        if not last or (datetime.now(timezone.utc) - last).days > active_within:
            return False, "inactive"

    min_yoe = int(filters.get("min_yoe", 0) or 0)
    if min_yoe > 0:
        yoe = _yoe_int(prof)
        if yoe is None or yoe < min_yoe:
            return False, "low_yoe"

    if filters.get("require_design_signal"):
        if not _has_design_signal(hit, profile):
            return False, "no_design_signal"

    if filters.get("require_fe_stack"):
        if not has_fe_stack(hit):
            return False, "no_fe_stack"

    if filters.get("require_devsecops_signal"):
        if not has_devsecops_signal(hit):
            return False, "no_devsecops_signal"

    if filters.get("require_product_eng_signal"):
        if not has_product_eng_signal(hit):
            return False, "no_product_eng_signal"

    if filters.get("require_pm_signal"):
        if not has_pm_signal(hit):
            return False, "no_pm_signal"

    loc = (data.get("city_current") or user.get("location") or "").lower()
    if us_only:
        if not any(k in loc for k in ("united states", "usa", ", us")):
            return False, "non_us"

    if city:
        needles = [n.strip().lower() for n in city.split(",") if n.strip()]
        if needles and not any(n in loc for n in needles):
            return False, "non_city"

    return True, ""


# ── Export to Ashby (native YC button) ───────────────────────────────

async def _click_job_option_async(page, target_job: str) -> str:
    dialog = page.locator('[role="dialog"]').first
    trigger = dialog.locator('button:has-text("-- Choose a job --"), button:has-text("Choose a job")').first
    if await trigger.count() == 0:
        trigger = dialog.locator('button.flex.h-10.w-full').first
    await trigger.click(timeout=5000)
    await asyncio.sleep(0.6)

    for candidate in (target_job, FALLBACK_JOB):
        opt = dialog.locator(f'div.cursor-default:has-text("{candidate}")').last
        if await opt.count() > 0:
            try:
                await opt.scroll_into_view_if_needed(timeout=2000)
                await opt.click(timeout=3000)
                await asyncio.sleep(0.4)
                return candidate
            except Exception:
                continue
    raise RuntimeError(f"Could not select job '{target_job}' or fallback")


async def export_one(page, short_id: str, name: str, target_job: str, *,
                     dry_run: bool = False) -> Optional[str]:
    url = f"{BASE}/workatastartup/applicants/{short_id}?source=outbound"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)

    try:
        await page.wait_for_selector('button:has-text("Export")', timeout=25000)
    except Exception:
        raise RuntimeError("Toolbar Export button not found — session may be stale")
    await page.locator('button:has-text("Export")').first.click(timeout=5000)

    try:
        await page.wait_for_selector('text=-- Choose a job --', timeout=ATS_HYDRATE_TIMEOUT_MS)
    except Exception:
        # Diagnostic: dump dialog text + a screenshot so we can see what Bookface rendered.
        try:
            dialog_text = await page.locator('[role="dialog"]').first.inner_text(timeout=2000)
        except Exception:
            dialog_text = "(could not read dialog)"
        try:
            shot = ROOT / f".waas_diag_{short_id}.png"
            await page.screenshot(path=str(shot), full_page=False)
            logger.warning("    diag[%s]: %s | screenshot=%s", short_id, dialog_text[:300].replace("\n", " ¶ "), shot.name)
        except Exception:
            logger.warning("    diag[%s]: %s", short_id, dialog_text[:300].replace("\n", " ¶ "))
        raise RuntimeError("Modal opened but ATS section never hydrated")

    chosen = await _click_job_option_async(page, target_job)

    if dry_run:
        await page.keyboard.press("Escape")
        return None

    dialog = page.locator('[role="dialog"]').first
    submit = dialog.locator('button:has-text("Export")').first
    await submit.click(timeout=5000)

    try:
        await page.wait_for_selector('text=Successfully exported', timeout=25000)
    except Exception:
        if await page.locator('a:has-text("View in ATS")').count() == 0:
            raise RuntimeError("No 'Successfully exported' confirmation appeared")

    ashby_id: Optional[str] = None
    try:
        href = await page.locator('a:has-text("View in ATS")').first.get_attribute("href", timeout=5000)
        if href and "/candidates/" in href:
            ashby_id = href.rstrip("/").split("/candidates/")[-1].split("?")[0]
    except Exception:
        pass
    return ashby_id


async def export_with_retries(ctx, candidate: Dict[str, Any], target_job: str, sem: asyncio.Semaphore,
                              start_delay: float = 0.0) -> Tuple[bool, str, Optional[str]]:
    """Acquire semaphore, open a dedicated page, export with up to 3 retries.
    start_delay staggers concurrent exports so the Bookface ATS endpoint isn't slammed."""
    if start_delay > 0:
        await asyncio.sleep(start_delay)
    async with sem:
        last_err = ""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            page = await ctx.new_page()
            page.set_default_timeout(20000)
            try:
                ashby_id = await export_one(page, candidate["short_id"], candidate["name"], target_job)
                await page.close()
                # Pause inside the semaphore so the next candidate waits before grabbing it.
                if EXPORT_PAUSE_S > 0:
                    await asyncio.sleep(EXPORT_PAUSE_S)
                return True, "", ashby_id
            except Exception as e:
                last_err = str(e)
                logger.warning("    [%s] attempt %d failed: %s", candidate["name"][:25], attempt, last_err[:140])
                try:
                    await page.close()
                except Exception:
                    pass
                # On hydration timeout, give the endpoint extra recovery time before retry/move-on.
                if "ATS section never hydrated" in last_err:
                    if attempt >= 2:
                        # Pause before next candidate so the endpoint can recover.
                        if EXPORT_PAUSE_S > 0:
                            await asyncio.sleep(EXPORT_PAUSE_S)
                        return False, last_err, None
                    await asyncio.sleep(15)  # cooldown between attempts
                else:
                    await asyncio.sleep(3)
        if EXPORT_PAUSE_S > 0:
            await asyncio.sleep(EXPORT_PAUSE_S)
        return False, last_err, None


# ── Config loading ───────────────────────────────────────────────────

@dataclass
class RunPlan:
    target_role: str
    queries: List[Dict[str, Any]]
    filters: Dict[str, Any]
    boost_keywords: List[str] = field(default_factory=list)
    negative_company_substrings: List[str] = field(default_factory=list)


def plan_from_config(path: Path) -> RunPlan:
    cfg = json.loads(path.read_text())
    return RunPlan(
        target_role=cfg.get("target_role", ""),
        queries=cfg["queries"],
        filters=cfg.get("filters", {}),
        boost_keywords=cfg.get("boost_keywords", []),
        negative_company_substrings=cfg.get("negative_company_substrings", []),
    )


def plan_from_args(query: str, args) -> RunPlan:
    return RunPlan(
        target_role=args.target_role or "",
        queries=[{
            "keyword": query,
            "pages": args.pages,
            "min_exp": args.min_exp,
            "max_exp": args.max_exp,
        }],
        filters={
            "min_yoe": args.min_yoe,
            "active_within_days": args.active_within,
            "require_linkedin": args.require_linkedin,
            "require_design_signal": False,
        },
        boost_keywords=[],
        negative_company_substrings=[],
    )


# ── Pipeline ─────────────────────────────────────────────────────────

async def run_pipeline(plan: RunPlan, args) -> None:
    routing = load_routing()
    target_job = resolve_job(plan.target_role, routing)
    logger.info("Target job: %s", target_job)
    logger.info("Queries: %d archetype(s)", len(plan.queries))

    # All-pages sweep: walk every page of each query until results run out
    # (collect_query treats pages<=0 as "until exhausted", capped by --max-pages).
    if getattr(args, "all_pages", False):
        cap = max(1, getattr(args, "max_pages", MAX_PAGES_HARD_CAP))
        for q in plan.queries:
            q["pages"] = cap
        logger.info("All-pages sweep ON — up to %d pages/query until exhausted", cap)

    log = load_log()
    already_exported = set(log.keys())
    if already_exported:
        logger.info("Skip-set: %d candidates already exported in prior runs", len(already_exported))

    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=args.headless)
        ctx = await browser.new_context(storage_state=str(STATE_PATH))
        sanity = await ctx.new_page()
        sanity.set_default_timeout(20000)
        await sanity.goto(f"{BASE}/workatastartup/sourcing", wait_until="domcontentloaded", timeout=20000)
        if "/login" in sanity.url or await sanity.locator('input[type="password"]').count() > 0:
            print("Session expired. Run: python3 waas_sourcer.py --login")
            await browser.close()
            sys.exit(2)
        await sanity.close()

        # 1. Run all queries concurrently → union by short_id
        t0 = time.time()
        results = await asyncio.gather(
            *(collect_query(ctx.request, q) for q in plan.queries),
            return_exceptions=True,
        )
        merged: Dict[str, Dict[str, Any]] = {}
        for r in results:
            if isinstance(r, Exception):
                logger.warning("query errored: %s", r)
                continue
            for h in r:
                sid = h["short_id"]
                if sid in already_exported:
                    continue
                # Keep the highest final_score variant if duplicated across queries
                cur = merged.get(sid)
                fs_new = h.get("final_score") or 0
                fs_old = (cur or {}).get("final_score") or 0
                if cur is None or fs_new > fs_old:
                    merged[sid] = h
        logger.info("Union: %d unique hits across all queries (%.1fs)", len(merged), time.time() - t0)

        if not merged:
            print("No search hits. Try a broader query.")
            await browser.close()
            return

        # 2. Parallel profile fetch (semaphore)
        t1 = time.time()
        sem_fetch = asyncio.Semaphore(PROFILE_FETCH_PARALLELISM)
        sids = list(merged.keys())
        profiles = await asyncio.gather(*(fetch_profile(ctx.request, sid, sem_fetch) for sid in sids))
        logger.info("Fetched %d profiles in %.1fs (parallelism=%d)", len(profiles), time.time() - t1, PROFILE_FETCH_PARALLELISM)

        # 3. Score + filter
        candidates: List[Dict[str, Any]] = []
        stats: Dict[str, int] = {"profile_fail": 0, "no_linkedin": 0, "inactive": 0,
                                 "low_yoe": 0, "no_design_signal": 0, "no_fe_stack": 0,
                                 "non_us": 0,
                                 "already_exported": len(set(merged) & already_exported)}
        for sid, prof in zip(sids, profiles):
            if prof is None:
                stats["profile_fail"] += 1
                continue
            hit = merged[sid]
            ok, why = passes_filters(hit, prof, plan.filters, args.us_only, getattr(args, "city", ""))
            if not ok:
                stats[why] = stats.get(why, 0) + 1
                continue
            user = (prof.get("profile") or {}).get("user") or {}
            data = (prof.get("profile") or {}).get("data") or {}
            name = (prof.get("profile") or {}).get("full_name") or user.get("name") or "?"
            sc, tier = score(hit, prof, plan.boost_keywords, plan.negative_company_substrings)
            sigs = quality_signals(hit)
            candidates.append({
                "short_id": sid,
                "name": name,
                "linkedin": (data.get("linkedin") or user.get("linkedin_url") or ""),
                "yoe": data.get("experience"),
                "location": data.get("city_current") or user.get("location") or "",
                "current_title": ((hit.get("positions") or [{}])[0].get("title") or "") if hit.get("positions") else "",
                "current_company": ((hit.get("positions") or [{}])[0].get("employer_name") or "") if hit.get("positions") else "",
                "skills": ", ".join(hit.get("skills") or [])[:200],
                "queries_matched": hit.get("_query", ""),
                "final_score": hit.get("final_score"),
                "score": sc,
                "tier": tier,
                "signals": ",".join(k for k, v in sigs.items() if v) or "-",
            })

        # Tier 0 (has quality signal) ahead of Tier 1, then by score within tier.
        candidates.sort(key=lambda c: (c["tier"], -c["score"]))

        # If require_quality_signal: drop tier-1 candidates entirely.
        if plan.filters.get("require_quality_signal"):
            before = len(candidates)
            candidates = [c for c in candidates if c["tier"] == 0]
            stats["no_quality_signal"] = before - len(candidates)

        tier0_count = sum(1 for c in candidates if c["tier"] == 0)
        tier1_count = len(candidates) - tier0_count
        logger.info("Tier 0 (with quality signal): %d  |  Tier 1 (filtered-only): %d", tier0_count, tier1_count)

        shortlist = candidates if args.limit <= 0 else candidates[: args.limit]

        # 4. Preview + optional CSV
        print(f"\n  {len(candidates)} candidates passed filters → top {len(shortlist)}:")
        for c in shortlist:
            print(f"    T{c['tier']} [{c['score']:>5}]  {c['name'][:26]:<26}  yoe={str(c['yoe']):>3}  "
                  f"{c['current_title'][:30]:<30}  @{c['current_company'][:18]:<18}  "
                  f"{c['location'][:18]:<18}  sigs={c['signals']}")
        print(f"  Stats: {stats}")

        if args.review_shortlist or args.dry_run:
            with SHORTLIST_CSV.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "tier", "score", "signals", "name", "yoe", "current_title", "current_company",
                    "location", "linkedin", "short_id", "skills", "queries_matched",
                ])
                w.writeheader()
                for c in shortlist:
                    w.writerow({k: c.get(k) for k in w.fieldnames})
            print(f"\n  Shortlist written → {SHORTLIST_CSV}")
            if args.dry_run:
                print("  DRY-RUN — no exports. Re-run without --dry-run to push to Ashby.")
            else:
                print("  --review-shortlist — no exports. Inspect CSV, then re-run without the flag.")
            await browser.close()
            return

        if not shortlist:
            print("\n  Empty shortlist. Nothing to export.")
            await browser.close()
            return

        # 4b. Ashby-side dedup: drop candidates already in Ashby with matching LinkedIn.
        # Bookface's "Export to ATS" doesn't dedupe — it always creates a new candidate.
        # Same person can also appear under multiple WaaS short_ids, bypassing our local log.
        from push_to_ashby import _candidate_has_matching_linkedin, _normalize_linkedin
        from ashby_bridge import search_candidate as _ashby_search_candidate

        def _ashby_dedup_check(c: Dict[str, Any]) -> Optional[str]:
            li_norm = _normalize_linkedin(c.get("linkedin", ""))
            if not li_norm:
                return None
            try:
                existing = _ashby_search_candidate(name=c["name"])
            except Exception:
                return None
            if not existing:
                return None
            if _candidate_has_matching_linkedin(existing, li_norm):
                return existing.get("id", "") or "unknown"
            return None

        print(f"\n  Ashby dedup pre-check on {len(shortlist)} candidates…")
        t_dedup = time.time()
        dedup_results = await asyncio.gather(*[
            asyncio.to_thread(_ashby_dedup_check, c) for c in shortlist
        ])
        kept: List[Dict[str, Any]] = []
        skipped_existing = 0
        for c, existing_id in zip(shortlist, dedup_results):
            if existing_id:
                skipped_existing += 1
                # Persist so future runs skip this short_id without an Ashby round-trip.
                log[c["short_id"]] = {
                    "ashby_id": existing_id,
                    "name": c["name"],
                    "job": target_job,
                    "ts": int(time.time()),
                    "skipped_dedup": True,
                }
                print(f"  SKIP {c['name']} — already in Ashby ({existing_id[:12]})")
            else:
                kept.append(c)
        if skipped_existing:
            save_log(log)
        print(f"  Pre-check: kept={len(kept)}  skipped_existing={skipped_existing}  ({time.time() - t_dedup:.1f}s)")
        shortlist = kept
        if not shortlist:
            print("\n  All shortlist candidates already in Ashby. Nothing to export.")
            await browser.close()
            return

        # 5. Direct Ashby API push (replaces flaky Bookface "Export to ATS" button).
        # Bookface's hydration endpoint is unreliable (~33% success even with serial + 30s pause).
        # We have name + LinkedIn from the WaaS profile fetch — that's enough for push_to_ashby,
        # which is the same code path used for GitHub/Juicebox sources.
        await browser.close()

        from push_to_ashby import push_candidates
        from ashby_bridge import _resolve_source_id  # ensure source exists

        # Resolve target job_id from routing config (same mapping push_to_ashby uses).
        routing = json.loads(ROUTING_PATH.read_text()) if ROUTING_PATH.exists() else {}
        roles = routing.get("roles", {})
        job_id = ""
        for canonical, info in roles.items():
            aliases = [canonical, info.get("job_title", "")] + (info.get("aliases") or [])
            if any(a.strip().lower() == target_job.strip().lower() for a in aliases if a):
                job_id = info.get("job_id", "")
                break

        # Build push payload from WaaS profile data.
        push_input = [{
            "name": c["name"],
            "linkedin": c.get("linkedin", ""),
            "email": "",
            "source": "Y Combinator Work at a Startup",
            "cv": "",
            "target_role": target_job,
            "github": "",
            "website": "",
            "company": c.get("current_company", ""),
            "location": c.get("location", ""),
            "resume_path": "",
        } for c in shortlist]

        print(f"\n  Pushing {len(push_input)} candidates → Ashby (job: {target_job})…")
        t2 = time.time()
        push_stats = push_candidates(
            candidates=push_input,
            source="Y Combinator Work at a Startup",
            job_id=job_id,  # if "", push_to_ashby falls back to Outbound Sourced
            workers=5,
            prefer_source_type="Sourced",  # disambiguate: Ashby has both "Inbound" and "Sourced" YC WaaS sources
        )
        # Mark exported in our local log so future runs skip these short_ids.
        for c in shortlist:
            log[c["short_id"]] = {
                "ashby_id": "pushed_via_api",
                "name": c["name"],
                "job": target_job,
                "ts": int(time.time()),
            }
        save_log(log)

    print(f"\n  Done. pushed={push_stats.get('pushed', 0)} "
          f"skipped_dedup={push_stats.get('skipped_dedup', 0)} "
          f"skipped_no_linkedin={push_stats.get('skipped_no_linkedin', 0)} "
          f"failed={push_stats.get('failed', 0)} partial={push_stats.get('partial', 0)} "
          f"({time.time() - t2:.1f}s)")
    print(f"\n  Next: candidates land in Ashby '{target_job}' job → run 'ascreen' to screen them.")


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="YC Work at a Startup → Ashby sourcer (v2)")
    p.add_argument("query", nargs="?", help="Single keyword query (legacy mode). Use --config for archetype mode.")
    p.add_argument("--config", help="Path to archetype config JSON (e.g. waas_query_configs/frontend_engineer.json)")
    p.add_argument("--login", action="store_true", help="Interactive login + save session")
    p.add_argument("--target-role", default="", help="Ashby job title (overrides config). Defaults to Outbound Sourced.")
    p.add_argument("--limit", type=int, default=50, help="Max candidates to export. Default 50. Use 0 for no cap (export everyone who passes filters).")
    p.add_argument("--all-pages", action="store_true", help="Sweep every page of each configured query until results run out (overrides per-query 'pages'). Pairs well with --limit 0.")
    p.add_argument("--max-pages", type=int, default=MAX_PAGES_HARD_CAP, help=f"Safety cap on pages per query when --all-pages is set. Default {MAX_PAGES_HARD_CAP}.")
    # Single-query knobs (only used when --config not given)
    p.add_argument("--pages", type=int, default=3)
    p.add_argument("--min-exp", type=int, default=0)
    p.add_argument("--max-exp", type=int, default=0)
    p.add_argument("--min-yoe", type=int, default=3, help="Hard YOE floor in single-query mode. Default 3.")
    p.add_argument("--active-within", type=int, default=90)
    p.add_argument("--require-linkedin", action="store_true", default=True)
    p.add_argument("--no-require-linkedin", dest="require_linkedin", action="store_false")
    # Universal
    p.add_argument("--us-only", action="store_true", default=True)
    p.add_argument("--no-us-only", dest="us_only", action="store_false", help="Disable US-only filter (default: US-only on).")
    p.add_argument("--city", default="", help="Restrict to a city substring match on candidate location, e.g. --city 'San Francisco' (matches 'San Francisco', 'SF Bay', etc.).")
    p.add_argument("--review-shortlist", action="store_true", help="Preview + write CSV; no exports.")
    p.add_argument("--dry-run", action="store_true", help="Same as --review-shortlist (back-compat).")
    p.add_argument("--headless", action="store_true", default=True)
    p.add_argument("--show-browser", dest="headless", action="store_false")
    args = p.parse_args()

    if args.login:
        login_flow()
        return

    if not STATE_PATH.exists():
        print(f"No session at {STATE_PATH}. Run: python3 waas_sourcer.py --login")
        sys.exit(2)

    if args.config:
        cfg_path = Path(args.config)
        if not cfg_path.exists() and (CONFIGS_DIR / args.config).exists():
            cfg_path = CONFIGS_DIR / args.config
        if not cfg_path.exists():
            print(f"Config not found: {args.config}")
            sys.exit(2)
        plan = plan_from_config(cfg_path)
        if args.target_role:
            plan.target_role = args.target_role
    else:
        if not args.query:
            print("Usage: waas_sourcer.py <query> [options]   |   --config <path>   |   --login")
            sys.exit(2)
        plan = plan_from_args(args.query, args)

    asyncio.run(run_pipeline(plan, args))


if __name__ == "__main__":
    main()
