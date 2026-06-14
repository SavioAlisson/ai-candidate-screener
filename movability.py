"""Movability heuristic — score each candidate 🟢/🟡/🔴 from LinkedIn data.

This is a pure-computation module: no external API calls, no file I/O at import
time. Pass it a LinkedIn profile dict (with `fullText`) and it returns
(tier_emoji, reason_string).

🟢 Ready — clear availability signals (plateau, consultancy, investor stack, mobile)
🟡 Maybe — recent move, ambiguous
🔴 Cold — strong retention signals (hot AI co, long tenure at big co)
"""
import re
from datetime import datetime

# Employer prestige tags (proxy for "hot enough to stay")
HOT_AI = {"openai","anthropic","cursor","xai","mistral","perplexity","glean","scale ai",
          "decagon","sierra","stripe","figma","databricks","ramp","brex","linear","vercel",
          "liquid ai","character","character.ai","harvey","writer","cohere","runway",
          "midjourney","poolside","magic","reflection","captions","11x","clay"}
SETTLED_BIG = {"google","meta","facebook","netflix","apple","amazon","microsoft","airbnb",
               "uber","salesforce","atlassian","doordash","aws"}


def parse_roles(profile):
    """Return [(title, company, start_dt, end_dt)] for top-level experience entries."""
    if not profile:
        return []
    ft = profile.get("fullText", "") if isinstance(profile, dict) else ""
    m = re.search(r"--- Experience ---\n(.+?)(\n--- |\Z)", ft, re.S)
    if not m:
        return []
    out = []
    for line in m.group(1).split("\n"):
        mm = re.match(r"- (.+?) at (.+?) \((\w+ \d{4}) - (Present|\w+ \d{4})", line)
        if not mm:
            continue
        title, company, start, end = mm.groups()
        try:
            sd = datetime.strptime(start, "%b %Y")
            ed = datetime.now() if end == "Present" else datetime.strptime(end, "%b %Y")
        except Exception:
            continue
        out.append((title.strip(), company.strip(), sd, ed))
    return out


def parse_current(profile):
    """Return (title, company, cumulative_months_at_employer) for the current role.

    Cumulative tenure spans consecutive promotions at the same employer.
    """
    roles = parse_roles(profile)
    if not roles:
        return None, None, None
    title, company, sd, ed = roles[0]
    co_key = company.lower()
    total_start = sd
    for t, c, s, e in roles[1:]:
        if c.lower() == co_key:
            total_start = min(total_start, s)
        else:
            break
    months = (ed.year - total_start.year) * 12 + (ed.month - total_start.month)
    return title, company, months


def count_employer_changes_5y(profile):
    """Count distinct employers started in the last 5 years (promotions don't count)."""
    roles = parse_roles(profile)
    cutoff = datetime.now().replace(year=datetime.now().year - 5)
    seen = set()
    for t, c, s, e in roles:
        if s >= cutoff:
            seen.add(c.lower())
    return len(seen)


def has_investor_stack(profile):
    """Detect LP/Advisor/Seed Investor titles dominating recent experience."""
    if not profile:
        return False
    ft = profile.get("fullText", "") if isinstance(profile, dict) else ""
    m = re.search(r"--- Experience ---\n(.+?)(\n--- |\Z)", ft, re.S)
    if not m:
        return False
    first_block = m.group(1)[:1500]
    investor_hits = len(re.findall(
        r"\b(LP|Seed Investor|Angel|Advisor|Deal Partner|Limited Partner)\b",
        first_block,
    ))
    return investor_hits >= 2


def tier(profile, hm_notes=""):
    """Return (tier_emoji, reason) for a LinkedIn profile.

    Args:
      profile: dict with `fullText` from LinkedIn scrape.
      hm_notes: optional context string (unused — kept for interface compatibility).

    Returns:
      ("🟢" | "🟡" | "🔴" | "?", reason_string)
    """
    title, company, tenure = parse_current(profile)
    if not company:
        return "?", "no LinkedIn data"
    co_l = company.lower().replace(",", "").replace(".", "").strip()
    job_count = count_employer_changes_5y(profile)

    # 🔴 COLD — strong retention signals only
    if any(h in co_l for h in HOT_AI):
        return "🔴", f"hot employer ({company}), tenure {tenure}mo"
    if any(h in co_l for h in SETTLED_BIG) and tenure and tenure >= 60:
        return "🔴", f"{tenure // 12}y at {company} — settled big-co"

    # 🟢 READY — clear availability signals
    if any(h in co_l for h in [
        "stealth", "consultant", "consultancy", "contract", "fractional",
    ]):
        return "🟢", "freelance/consultancy/stealth"
    if has_investor_stack(profile):
        return "🟢", "investor/advisor stack — exiting operator life"
    if tenure is not None and tenure >= 36 and not any(
        h in co_l for h in HOT_AI | SETTLED_BIG
    ):
        return "🟢", f"plateau: {tenure // 12}y+ at non-prestige {company}"
    if job_count >= 4:
        return "🟢", f"{job_count} moves/5y — mobile pattern"

    # 🟡 ambiguous
    return "🟡", f"{tenure}mo at {company}, {job_count} moves/5y"
