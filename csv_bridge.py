"""
CSV-based I/O bridge — reads candidates from CSV export, writes results to JSON.

Replaces sheets_bridge.py when direct Sheets API auth isn't available.
Uses local JSON files for LinkedIn cache and query learnings.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent

# Local cache files
LINKEDIN_CACHE_FILE = _DIR / ".linkedin_cache.json"
QUERY_LEARNINGS_FILE = _DIR / ".query_learnings.json"

SCREENING_MARKER = "\u23F3 SCREENING\u2026"

# ── Header names (must match your sheet columns exactly) ─────────
HEADER_MAP = {
    "NAME": "Candidate Name",
    "LINKEDIN": "LinkedIn URL",
    "SOURCE": "Source",
    "FEEDBACK": "Feedback",
    "SOURCE_NOTES": "Source Additional Notes",
    "CV": "CV / Resume Notes",
    "TARGET_ROLE": "Target Role",
    "VERDICT": "AI Screening Verdict",
    "REJECTION_TYPE": "Rejection Type",
    "NURTURE": "Nurture",
    "SPARK": "Spark",
    "VERDICT_REASON": "Verdict Reason",
    "PROMPT_VERSION_COL": "Prompt Version",
    "LAST_SCREENED": "Last Screened Timestamp",
    "BEST_FIT_ROLE": "Best Fit Role",
    "BEST_FIT_REASON": "Best Fit Role Reason",
    "MATCHED_LEVEL": "Matched Level",
    "REASONING": "Reasoning",
    "REGRET_TEST": "Regret Test",
    "CONCERNS": "Concerns",
    "SCREENING_QUESTIONS": "Screening Questions",
    "SCREENER_BRIEF": "Screener Brief",
    "DEFER_UNTIL": "Defer Until",
    "OUTREACH_MSG_1": "Outreach Message 1",
    "OUTREACH_MSG_2": "Outreach Message 2",
    "SONAR_DOSSIER": "Research Output",
    "VERDICT_HISTORY": "Verdict History",
    "SONAR_COST": "Sonar Cost",
    "OPUS_COST": "Opus Cost",
    "TOTAL_COST": "Total Cost ($)",
    "TOKEN_LOG": "Token Log",
    "ACTIONED": "Actioned",
}

SCREENED_VERDICTS = {
    "SCREEN", "DECLINE", "DEFER",
    "INSUFFICIENT DATA", "DUPLICATE", "INPUT ERROR",
}

# Verdict routing — updated 2026-05-14 (Manual Screen + HM Inbound Triage
# consolidated into Inbound App Review; REVIEW verdicts removed). Mirrors
# get_verdict_stage() in ashby_bridge.py. Keep in sync.
VERDICT_DESTINATION = {
    "SCREEN": "Application Review",
    "DEFER": "Inbound App Review",
    "INSUFFICIENT DATA": "Archived",  # 2026-05-12: universal archive — no LinkedIn/CV = no human review value
    "DECLINE": "Archived",
    "DUPLICATE": "",  # deleted from sheet
    "INPUT ERROR": "Needs Rescreen",
    "SCREENING FAILED": "Needs Rescreen",
}


def get_verdict_destination(verdict: str, rejection_type: str = "", nurture: str = "") -> str:
    """Route verdict to destination tab. Mirrors ashby_bridge.get_verdict_stage()."""
    v = verdict.strip().upper()
    if v == "DUPLICATE":
        return ""
    return VERDICT_DESTINATION.get(v, "Needs Rescreen")


def _build_column_map(headers: list) -> Dict[str, int]:
    """Build column map from header row. Returns {KEY: 0-based column index}.

    Supports both Sheet CSV format (Candidate Name, LinkedIn URL, etc.)
    and GitHub sourcer format (name, linkedin_url, cv_summary, etc.).
    """
    normalized = {}
    for i, h in enumerate(headers):
        key = str(h).strip().lower()
        if key:
            normalized[key] = i

    col_map = {}
    for key, header_name in HEADER_MAP.items():
        idx = normalized.get(header_name.strip().lower())
        if idx is not None:
            col_map[key] = idx

    # Fallback: support GitHub/Apollo sourcer column names
    SOURCER_ALIASES = {
        "NAME": ["name"],
        "LINKEDIN": ["linkedin_url", "linkedin"],
        "CV": ["cv_summary", "bio", "headline"],
        "SOURCE": ["source"],
        "SOURCE_NOTES": ["top_repos", "skills"],
        "TARGET_ROLE": ["target_role"],
    }
    for key, aliases in SOURCER_ALIASES.items():
        if key not in col_map:
            for alias in aliases:
                idx = normalized.get(alias)
                if idx is not None:
                    col_map[key] = idx
                    break

    return col_map


def _cell(row: list, col_map: Dict[str, int], key: str) -> str:
    """Safely get a cell value."""
    idx = col_map.get(key)
    if idx is not None and idx < len(row):
        return str(row[idx]).strip()
    return ""


# ── Read candidates from CSV ────────────────────────────────────

def read_candidates_csv(
    csv_path: str,
    rescreen: bool = False,
) -> List[Dict[str, Any]]:
    """
    Read candidates from a CSV file (exported from Google Sheets).
    Returns list of candidate dicts with row numbers matching the sheet.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows or len(rows) < 2:
        return []

    headers = rows[0]
    col = _build_column_map(headers)

    if "NAME" not in col:
        raise RuntimeError(
            f"Could not find 'Candidate Name' column in CSV. "
            f"Headers found: {headers[:10]}"
        )

    candidates = []
    seen_keys = set()  # dedup: skip duplicate LinkedIn URLs or names within the CSV
    skipped_dupes = 0
    skipped_already_screened = 0

    # Load LinkedIn URLs from screening log for cross-batch dedup
    screening_log_linkedins = set()
    screening_log_names = set()
    log_path = Path(__file__).parent / "screening_log.csv"
    if log_path.exists() and not rescreen:
        try:
            with open(log_path, "r", encoding="utf-8") as lf:
                log_reader = csv.DictReader(lf)
                for log_row in log_reader:
                    li = (log_row.get("linkedin") or "").strip()
                    if li:
                        screening_log_linkedins.add(_normalize_linkedin_url(li))
                    n = (log_row.get("name") or "").strip().lower()
                    if n:
                        screening_log_names.add(n)
            logger.info("Screening log loaded: %d LinkedIn URLs, %d names for dedup",
                        len(screening_log_linkedins), len(screening_log_names))
        except Exception as e:
            logger.warning("Could not load screening log for dedup: %s", e)

    for i, row in enumerate(rows[1:], start=2):  # Row 2 = first data row in sheet
        name = _cell(row, col, "NAME")
        if not name:
            continue

        verdict = _cell(row, col, "VERDICT").upper()

        # Skip in-progress
        if verdict == SCREENING_MARKER.upper() or verdict == SCREENING_MARKER:
            continue

        # Skip already screened unless rescreen
        if not rescreen and verdict in SCREENED_VERDICTS:
            continue

        # Dedup within CSV: if same LinkedIn URL or same name already seen, skip
        linkedin_url = _cell(row, col, "LINKEDIN")
        li_key = _normalize_linkedin_url(linkedin_url) if linkedin_url else ""
        name_key = name.strip().lower()

        dedup_key = li_key if li_key else name_key
        if dedup_key in seen_keys:
            skipped_dupes += 1
            logger.info("DEDUP: Skipping duplicate row %d (%s) — already in CSV", i, name)
            continue
        seen_keys.add(dedup_key)

        # Cross-batch dedup: check if already screened in a previous run
        if screening_log_linkedins or screening_log_names:
            if li_key and li_key in screening_log_linkedins:
                skipped_already_screened += 1
                logger.info("DEDUP: Skipping row %d (%s) — LinkedIn already in screening log", i, name)
                continue
            if not li_key and name_key in screening_log_names:
                skipped_already_screened += 1
                logger.info("DEDUP: Skipping row %d (%s) — name already in screening log", i, name)
                continue

        # Get existing dossier
        dossier = _cell(row, col, "SONAR_DOSSIER")

        candidates.append({
            "row": i,
            "name": name,
            "linkedin": linkedin_url,
            "source": _cell(row, col, "SOURCE") or "OUTBOUND",
            "source_notes": _cell(row, col, "SOURCE_NOTES"),
            "cv": _cell(row, col, "CV"),
            "target_role": _cell(row, col, "TARGET_ROLE"),
            "existing_dossier": dossier,
        })

    if skipped_dupes:
        logger.info("DEDUP: Skipped %d duplicate(s) within CSV", skipped_dupes)
    if skipped_already_screened:
        logger.info("DEDUP: Skipped %d candidate(s) already in screening log", skipped_already_screened)

    return candidates


# ── Results output ──────────────────────────────────────────────

def save_results_json(results: list, output_path: str = "") -> str:
    """
    Save screening results as JSON file for Apps Script import.
    Returns the output file path.
    """
    if not output_path:
        output_path = str(_DIR / "screening_results.json")

    # Build clean results for import (matches what Apps Script importer expects)
    import_data = {
        "results": results,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(import_data, f, indent=2, ensure_ascii=False)

    return output_path


def copy_to_clipboard(text: str) -> bool:
    """Copy text to macOS clipboard using pbcopy."""
    try:
        import subprocess
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.communicate(text.encode("utf-8"))
        return proc.returncode == 0
    except Exception:
        return False


# ── Local LinkedIn cache ────────────────────────────────────────

def _load_linkedin_cache() -> dict:
    if LINKEDIN_CACHE_FILE.exists():
        try:
            return json.loads(LINKEDIN_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_linkedin_cache(cache: dict):
    from ashby_bridge import write_json_atomic
    write_json_atomic(LINKEDIN_CACHE_FILE, cache, indent=2, ensure_ascii=False)


def _normalize_linkedin_url(url: str) -> str:
    if not url:
        return ""
    s = url.strip().lower()
    s = re.sub(r"https?://(www\.)?linkedin\.com", "", s, flags=re.I)
    s = s.rstrip("/")
    s = re.sub(r"\?.*$", "", s)
    return s


def read_linkedin_cache(linkedin_url: str) -> Optional[Dict]:
    """Check local LinkedIn cache for existing scrape."""
    if not linkedin_url:
        return None
    cache = _load_linkedin_cache()
    key = _normalize_linkedin_url(linkedin_url)
    entry = cache.get(key)
    if entry and entry.get("status") == "SUCCESS":
        return entry.get("data")
    return None


def write_linkedin_cache(linkedin_url: str, result: dict) -> None:
    """Write scrape result to local LinkedIn cache."""
    cache = _load_linkedin_cache()
    key = _normalize_linkedin_url(linkedin_url)

    if result.get("success") and result.get("data"):
        cache[key] = {
            "status": "SUCCESS",
            "data": result["data"],
            "timestamp": datetime.now().isoformat(),
            "actor": result.get("actor", "python-apify"),
        }
    else:
        cache[key] = {
            "status": result.get("error", "ERROR"),
            "data": None,
            "timestamp": datetime.now().isoformat(),
        }

    _save_linkedin_cache(cache)


# ── Local query learnings ───────────────────────────────────────

def _load_learnings() -> list:
    if QUERY_LEARNINGS_FILE.exists():
        try:
            return json.loads(QUERY_LEARNINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_learnings(learnings: list):
    QUERY_LEARNINGS_FILE.write_text(
        json.dumps(learnings, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def read_query_learnings() -> str:
    """Read last 20 query learnings from local file."""
    learnings = _load_learnings()
    recent = learnings[-20:]
    return "\n".join(l.get("learning", "") for l in recent if l.get("learning"))


def write_query_learning(name: str, learning: str, target_role: str) -> None:
    """Append a query learning to local file."""
    if not learning:
        return
    learnings = _load_learnings()
    learnings.append({
        "timestamp": datetime.now().isoformat(),
        "name": name,
        "learning": learning,
        "target_role": target_role,
    })
    _save_learnings(learnings)
