"""
Pre-screening confidence scorer — fast Haiku gate before full Opus screening.

Runs a cheap (~$0.01) Haiku check on each candidate BEFORE the $0.30 full pipeline.
Assigns a confidence tier: HIGH / MEDIUM / LOW.
Only HIGH + MEDIUM candidates proceed to full screening.

This saves ~$0.30 per LOW candidate filtered out.

Usage (standalone):
  python3 pre_screener.py                              # pre-score candidates from latest CSV
  python3 pre_screener.py --csv path/to/file.csv       # specific CSV
  python3 pre_screener.py --threshold medium            # only pass HIGH+MEDIUM (default)
  python3 pre_screener.py --threshold low               # pass everything except LOW

Integrated into screen_batch.py:
  screen --pre-filter                                   # enable pre-screening gate

Output: prints confidence tiers and saves filtered candidate list.
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
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
PROMPT_FILE = _DIR / "prompts" / "opus_body.md"
ARCHIVE_PATTERNS_FILE = _DIR / ".archive_patterns.json"

HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5-20251001")

# Cost per Haiku pre-screen call (approximate)
HAIKU_COST_PER_CALL = 0.01


# ── Hard gates extracted from screening prompt ────────────────────
# These are the gates that ALWAYS cause a decline regardless of other factors.
# Checking these before the full pipeline saves the most money.

HARD_GATES = """
AUTOMATIC DECLINE PATTERNS (if ANY of these match, score LOW):

1. LOCATION: Not in SF Bay Area / not willing to relocate to SF for onsite roles. Non-US for VD US Lead track (Canada included = decline).

2. CAREER SHAPE - BIG COMPANY ONLY: Entire career at large slow enterprises (industrial, telecom, retail giants, defense/government) with NO startup or high-velocity tech chapter. 8+ years at a single large enterprise with no outside initiative.

3. CAREER SHAPE - CONSULTING SPINE: Primary career is big consulting (Accenture, PwC, Deloitte, EY) or large outsourcing/BPO WITHOUT a later product-company building chapter.

4. CAREER SHAPE - RESEARCH ONLY: Academic/institute research with no commercial product shipped for real customers.

5. TOO JUNIOR: Early career with no compensating spike — no elite employer, no viral adoption, no serious revenue, no rare open-source traction.

6. ADMIN/ANALYST PATTERN: Salesforce admin only (no system design), BI/analytics only (dashboards, not systems), process documentation only (no redesign or impact).

7. NO BUILDER EVIDENCE: PM titles without shipped products. "Led" and "managed" everywhere but never "built," "designed," or "shipped" anything specific.

8. PURE GTM/SALES for VD roles: Last 5 years mostly sales, SDR, business development with minimal post-sale/implementation/CS work.

9. DOMAIN MISMATCH: Entire body of work in a fundamentally different domain with no transferable competencies and no pull toward Klarity's domain.

10. INTERN AT PRESTIGIOUS FIRM: BCG/McKinsey/Bain intern ≠ BCG/McKinsey/Bain consultant. Need 1+ year actual experience, not internship.
"""

# ── Successful candidate patterns (from Screen tab data) ──────────

GOOD_PATTERNS = """
PATTERNS THAT INDICATE HIGH CONFIDENCE (pass to full screening):

1. BUILDER AT HIGH-VELOCITY COMPANY: Shipped production systems at companies like Stripe, Palantir, Databricks, Scale AI, Notion, Anthropic, or well-funded Series A-C startups.

2. FOUNDER WITH TRACTION: Founded/co-founded with real users, revenue, or funding. Not just a title — actual product shipped.

3. CONSULTING → PRODUCT: McKinsey/BCG/Bain alumni who then moved to a product company in a building role (not strategy).

4. RAPID PROGRESSION: Skipped conventional career steps. Junior to senior in 2-3 years. IC to owning systems fast.

5. UNUSUAL COMBINATION: Engineering + design. Consulting + coding. PhD + shipped product. These compound.

6. OPEN SOURCE / SIDE PROJECTS: Real traction (stars, users, dependents). Not toy repos.

7. EARLY EMPLOYEE: First 20 employees at a company that grew significantly. Built systems from scratch.

8. AI-NATIVE WORK: Recent (last 18 months) production AI/ML work. Not just using ChatGPT — building AI systems.

9. DOMAIN PROXIMITY: Worked on enterprise AI, voice AI, document understanding, transformation automation, or Fortune 500 sales.

10. MEASURABLE OUTCOMES: Can point to specific metrics moved — conversion rates, pipeline velocity, NRR, cost reduction.
"""


# ── Call Haiku for pre-screening ──────────────────────────────────

def call_haiku(system: str, user: str) -> str:
    """Call Claude Haiku for fast pre-screening."""
    api_key = os.environ.get("CLAUDE_API_KEY", "")
    if not api_key:
        raise RuntimeError("CLAUDE_API_KEY not set")

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 800,
        "temperature": 0,
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

    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
    blocks = result.get("content", [])
    return "".join(b.get("text", "") for b in blocks if b.get("type") == "text")


# ── Load archive patterns ─────────────────────────────────────────

def load_archive_patterns() -> str:
    """Load cached archive rejection patterns if available."""
    if ARCHIVE_PATTERNS_FILE.exists():
        try:
            data = json.loads(ARCHIVE_PATTERNS_FILE.read_text(encoding="utf-8"))
            return data.get("patterns", "")
        except Exception:
            pass
    return ""


def build_archive_patterns(archive_csv_path: str) -> str:
    """
    Analyze an Archive tab CSV export and extract common rejection patterns.
    Saves to .archive_patterns.json for reuse.
    """
    path = Path(archive_csv_path)
    if not path.exists():
        return ""

    with open(path, encoding="utf-8-sig", errors="replace") as f:
        clean_lines = (line.replace("\x00", "") for line in f)
        rows = list(csv.DictReader(clean_lines))

    if not rows:
        return ""

    # Collect decline reasons and concerns
    decline_data = []
    for r in rows:
        verdict = (r.get("AI Screening Verdict") or "").strip().upper()
        if verdict in ("DECLINE", "NO", "LEAN NO"):
            reason = (r.get("Verdict Reason") or r.get("Recommended Action") or "").strip()
            concerns = (r.get("Concerns") or "").strip()
            role = (r.get("Best Fit Role") or r.get("Matched Role") or "").strip()
            name = (r.get("Candidate Name") or "").strip()
            rejection_type = (r.get("Rejection Type") or "").strip()

            if reason or concerns:
                decline_data.append({
                    "role": role,
                    "reason": reason[:300],
                    "concerns": concerns[:300],
                    "rejection_type": rejection_type,
                })

    if not decline_data:
        return ""

    # Use Haiku to synthesize the top patterns
    system = """You are analyzing candidate rejection data to extract the TOP 10 most common patterns.
Output a numbered list of the most frequent rejection reasons, with the count of how many candidates matched each pattern.
Be specific — not "bad fit" but "consulting-only career with no product company chapter" or "too junior, recent grad with no professional experience."
Focus on patterns that could be detected from a LinkedIn headline + company + basic profile info (before deep research)."""

    # Sample up to 50 entries
    sample = decline_data[:50]
    user = f"Analyze these {len(sample)} candidate rejections and extract the top 10 patterns:\n\n"
    for i, d in enumerate(sample):
        user += f"{i+1}. Role: {d['role']}\n   Reason: {d['reason']}\n   Type: {d['rejection_type']}\n\n"

    patterns = call_haiku(system, user)

    # Cache
    ARCHIVE_PATTERNS_FILE.write_text(json.dumps({
        "patterns": patterns,
        "source": str(archive_csv_path),
        "count": len(decline_data),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }, indent=2), encoding="utf-8")

    return patterns


# ── Pre-screen one candidate ──────────────────────────────────────

def pre_screen_candidate(
    name: str,
    linkedin: str,
    source_notes: str,
    cv: str,
    target_role: str,
    archive_patterns: str = "",
) -> Dict[str, Any]:
    """
    Fast pre-screening check for one candidate.
    Returns: {confidence: HIGH|MEDIUM|LOW, reason: str, cost: float}
    """

    system = f"""You are a fast pre-screening filter for Klarity, a Series B AI startup in SF.
Your job: quickly assess whether this candidate is worth spending $0.30 on a full AI screening, based on their basic profile info.

Assign a confidence tier:
- HIGH: Strong signals of fit. Proceed to full screening immediately.
- MEDIUM: Some positive signals but unclear. Worth screening to find out.
- LOW: Clear mismatch on hard gates. Skip full screening — would almost certainly be declined.

{HARD_GATES}

{GOOD_PATTERNS}

{"ARCHIVE REJECTION PATTERNS (most common reasons candidates get declined):" + chr(10) + archive_patterns if archive_patterns else ""}

Output EXACTLY this format (3 lines only):
CONFIDENCE: HIGH|MEDIUM|LOW
REASON: [one sentence — why this tier]
LIKELY_ROLE: [best guess role or "unclear"]"""

    candidate_info = f"Name: {name}\n"
    if linkedin:
        candidate_info += f"LinkedIn: {linkedin}\n"
    if source_notes:
        candidate_info += f"Source notes: {source_notes[:500]}\n"
    if cv:
        candidate_info += f"CV/resume info: {cv[:1000]}\n"
    if target_role:
        candidate_info += f"Target role: {target_role}\n"

    try:
        response = call_haiku(system, candidate_info)
    except Exception as e:
        logger.warning("Pre-screen failed for %s: %s — defaulting to MEDIUM", name, e)
        return {"confidence": "MEDIUM", "reason": f"Pre-screen error: {e}", "likely_role": "unknown", "cost": 0}

    # Parse response
    confidence = "MEDIUM"
    reason = ""
    likely_role = ""

    for line in response.strip().split("\n"):
        line = line.strip()
        if line.upper().startswith("CONFIDENCE:"):
            val = line.split(":", 1)[1].strip().upper()
            if val in ("HIGH", "MEDIUM", "LOW"):
                confidence = val
        elif line.upper().startswith("REASON:"):
            reason = line.split(":", 1)[1].strip()
        elif line.upper().startswith("LIKELY_ROLE:"):
            likely_role = line.split(":", 1)[1].strip()

    return {
        "confidence": confidence,
        "reason": reason,
        "likely_role": likely_role,
        "cost": HAIKU_COST_PER_CALL,
    }


# ── Batch pre-screen from CSV ────────────────────────────────────

def pre_screen_csv(
    csv_path: str,
    threshold: str = "medium",
    archive_csv: str = "",
) -> Tuple[List[Dict], List[Dict], float]:
    """
    Pre-screen all candidates in a CSV.
    Returns: (passed, filtered, total_cost)
    """
    from csv_bridge import read_candidates_csv

    candidates = read_candidates_csv(csv_path, rescreen=False)
    if not candidates:
        return [], [], 0.0

    # Load or build archive patterns
    archive_patterns = load_archive_patterns()
    if archive_csv and not archive_patterns:
        print("  Building rejection patterns from Archive data...")
        archive_patterns = build_archive_patterns(archive_csv)

    print(f"\n  Pre-screening {len(candidates)} candidates (Haiku fast check)...\n")

    passed = []
    filtered = []
    total_cost = 0.0
    threshold_set = {"HIGH"} if threshold.lower() == "high" else {"HIGH", "MEDIUM"}

    for i, c in enumerate(candidates):
        result = pre_screen_candidate(
            name=c["name"],
            linkedin=c.get("linkedin", ""),
            source_notes=c.get("source_notes", ""),
            cv=c.get("cv", ""),
            target_role=c.get("target_role", ""),
            archive_patterns=archive_patterns,
        )
        total_cost += result["cost"]

        tier = result["confidence"]
        symbol = {"HIGH": "✅", "MEDIUM": "🟡", "LOW": "❌"}.get(tier, "?")

        print(f"  {symbol} {tier:<6} | {c['name']:<35} | {result['reason'][:60]}")

        c["pre_screen"] = result

        if tier in threshold_set:
            passed.append(c)
        else:
            filtered.append(c)

    return passed, filtered, total_cost


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pre-screen candidates with a cheap Haiku check before full Opus screening",
    )
    parser.add_argument("--csv", default="",
                        help="Path to CSV file (default: auto-find)")
    parser.add_argument("--archive", default="",
                        help="Path to Archive tab CSV (for learning rejection patterns)")
    parser.add_argument("--threshold", default="medium", choices=["high", "medium", "low"],
                        help="Minimum confidence to pass through (default: medium)")
    parser.add_argument("--rebuild-patterns", action="store_true",
                        help="Force rebuild archive patterns from CSV")
    args = parser.parse_args()

    if not os.environ.get("CLAUDE_API_KEY"):
        print("Error: CLAUDE_API_KEY not set")
        sys.exit(1)

    # Find CSV
    csv_path = args.csv
    if not csv_path:
        from screen_batch import auto_find_csv
        csv_path = auto_find_csv()
        if not csv_path:
            print("No CSV found. Export from Google Sheets first.")
            sys.exit(1)
        print(f"Auto-detected: {csv_path}")

    # Rebuild archive patterns if requested
    if args.rebuild_patterns and args.archive:
        print("Rebuilding archive rejection patterns...")
        patterns = build_archive_patterns(args.archive)
        if patterns:
            print(f"  Extracted patterns from Archive data.\n")
        else:
            print("  No decline data found in Archive CSV.\n")

    # Run pre-screening
    passed, filtered, cost = pre_screen_csv(csv_path, args.threshold, args.archive)

    # Summary
    total = len(passed) + len(filtered)
    print(f"\n{'='*60}")
    print(f"  PRE-SCREENING RESULTS")
    print(f"{'='*60}")
    print(f"  Total candidates:     {total}")
    print(f"  Passed (→ full screen): {len(passed)}")
    print(f"  Filtered out:         {len(filtered)}")
    print(f"  Pre-screen cost:      ${cost:.2f}")
    print(f"  Estimated savings:    ${len(filtered) * 0.30:.2f} (skipped full screens)")
    if total > 0:
        print(f"  Filter rate:          {len(filtered)/total*100:.0f}%")
    print(f"{'='*60}")

    if filtered:
        print(f"\n  Filtered candidates:")
        for c in filtered:
            ps = c["pre_screen"]
            print(f"    ❌ {c['name']:<35} | {ps['reason'][:60]}")

    if passed:
        print(f"\n  Proceeding to full screening:")
        for c in passed:
            ps = c["pre_screen"]
            print(f"    {ps['confidence']:<6} {c['name']:<35} | {ps.get('likely_role', '')[:30]}")

    print(f"\n  To run full screening on passed candidates only:")
    if passed:
        rows = ",".join(str(c["row"]) for c in passed)
        print(f"    screen --rows {rows}")
    print()


if __name__ == "__main__":
    main()
