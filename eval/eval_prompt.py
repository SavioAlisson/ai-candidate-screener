"""
Offline prompt evaluation — re-run Opus judgment on cached dossiers.

Compares verdicts from a new prompt against the original screening results.
Does NOT touch Ashby. Purely local: reads cached dossiers + screening log,
calls Opus with the specified prompt, writes a comparison CSV.

Usage:
  # Re-eval all candidates screened today
  python3 eval/eval_prompt.py --since 2026-04-16

  # Re-eval with a different prompt file
  python3 eval/eval_prompt.py --since 2026-04-16 --prompt prompts/opus_body_v2.md

  # Re-eval specific roles only
  python3 eval/eval_prompt.py --since 2026-04-16 --role BE --role FE

  # Re-eval specific candidates by ID
  python3 eval/eval_prompt.py --ids eval/candidates.txt

  # Limit to N candidates (useful for quick prompt sanity check)
  python3 eval/eval_prompt.py --since 2026-04-16 --limit 10

  # Dry run — show candidates that would be evaluated, no API calls
  python3 eval/eval_prompt.py --since 2026-04-16 --dry-run

  # Use a different model (e.g., Sonnet for cheaper iteration)
  python3 eval/eval_prompt.py --since 2026-04-16 --model claude-sonnet-4-6

  # Parallel workers (default: 5)
  python3 eval/eval_prompt.py --since 2026-04-16 --workers 10
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Paths relative to project root (one level up from eval/)
_EVAL_DIR = Path(__file__).resolve().parent
_ROOT = _EVAL_DIR.parent
_SCREENING_LOG = _ROOT / "screening_log.csv"
_CANDIDATE_CACHE = _ROOT / ".candidate_cache"
_DEFAULT_PROMPT = _ROOT / "prompts" / "opus_body.md"

ANTHROPIC_VERSION = "2023-06-01"


# ── HTTP helper ──────────────────────────────────────────────────

def _http_post_json(url: str, headers: dict, payload: dict, timeout: int = 900) -> Tuple[int, dict]:
    """POST JSON, return (status_code, parsed_body)."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"error": body[:500]}
    except Exception as e:
        return 0, {"error": str(e)}


# ── JSON repair (simplified from json_repair.py) ────────────────

def _repair_json(text: str) -> str:
    """Best-effort repair of malformed JSON from Claude responses."""
    sys.path.insert(0, str(_ROOT))
    try:
        from json_repair import repair_json_string
        return repair_json_string(text)
    except ImportError:
        return text


# ── Data confidence (reused from pipeline.py) ───────────────────

def _assess_data_confidence(dossier: str) -> str:
    lower = dossier.lower()
    linkedin_ok = False
    if "linkedin" in lower:
        i = lower.index("linkedin")
        snip = lower[i: i + 200]
        linkedin_ok = not any(
            x in snip for x in ("not accessible", "unavailable", "not found", "blocked")
        )
    github_ok = "github.com/" in lower and "github profile not found" not in lower
    other_kw = ("blog", "article", "news", "conference", "twitter.com", "x.com", "substack", "medium")
    has_other = any(k in lower for k in other_kw)
    sources = (1 if linkedin_ok else 0) + (1 if github_ok else 0) + (1 if has_other else 0)
    if sources >= 2 and linkedin_ok:
        return "Full"
    if sources >= 1:
        return "Partial"
    return "Minimal"


# ── GitHub enrichment (reused from pipeline.py) ─────────────────

def _github_enrich_block(dossier: str) -> str:
    """Import and call github_enrich_block from pipeline.py."""
    sys.path.insert(0, str(_ROOT))
    try:
        from pipeline import github_enrich_block
        return github_enrich_block(dossier)
    except ImportError:
        return ""


# ── Load candidates from screening log ──────────────────────────

def load_candidates(
    since: Optional[str] = None,
    until: Optional[str] = None,
    roles: Optional[List[str]] = None,
    sources: Optional[List[str]] = None,
    id_file: Optional[str] = None,
    limit: int = 0,
) -> List[Dict]:
    """Load candidates from screening_log.csv, filtered by criteria.

    Returns list of dicts with keys needed for Opus re-evaluation.
    """
    # If ID file provided, load candidate IDs
    target_ids = set()
    if id_file:
        with open(id_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    target_ids.add(line)

    # Parse date filters
    since_dt = None
    if since:
        since_dt = datetime.strptime(since, "%Y-%m-%d")
    until_dt = None
    if until:
        until_dt = datetime.strptime(until, "%Y-%m-%d")

    candidates = []
    seen_ids = set()  # dedup by candidate ID (keep latest screening)

    # Read all rows first, then reverse so latest entry per candidate wins
    rows = []
    with open(_SCREENING_LOG, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    for row in reversed(rows):
        cid = row.get("ashby_candidate_id", "")
        if not cid or cid in seen_ids:
            continue

        # Filter by ID list
        if target_ids and cid not in target_ids:
            continue

        # Filter by date
        if since_dt or until_dt:
            ts = row.get("timestamp", "")
            try:
                row_dt = datetime.strptime(ts.split()[0], "%m/%d/%Y") if "/" in ts else datetime.strptime(ts.split()[0], "%Y-%m-%d")
            except (ValueError, IndexError):
                continue
            if since_dt and row_dt < since_dt:
                continue
            if until_dt and row_dt > until_dt:
                continue

        # Filter by role
        if roles:
            if row.get("target_role", "") not in roles:
                continue

        # Filter by source (substring match — "Y Combinator" matches "Inbound — Y Combinator Work at a Startup")
        if sources:
            row_source = row.get("source", "")
            if not any(s.lower() in row_source.lower() for s in sources):
                continue

        # Check that we have a cached dossier
        cache_path = _CANDIDATE_CACHE / f"{cid}.json"
        if not cache_path.exists():
            logger.warning("No cache for %s (%s), skipping", row.get("name", "?"), cid)
            continue

        cache = json.loads(cache_path.read_text(encoding="utf-8"))
        dossier = cache.get("dossier", "")
        if not dossier or len(dossier) < 100:
            logger.warning("Empty/short dossier for %s (%s), skipping", row.get("name", "?"), cid)
            continue

        seen_ids.add(cid)
        candidates.append({
            "candidate_id": cid,
            "name": row.get("name", ""),
            "linkedin": row.get("linkedin", cache.get("linkedin", "")),
            "source": row.get("source", ""),
            "source_notes": row.get("source_notes", ""),
            "target_role": row.get("target_role", ""),
            "cv": cache.get("cv_text", ""),
            "dossier": dossier,
            "original_verdict": row.get("verdict", ""),
            "original_spark": row.get("spark", ""),
            "original_best_fit": row.get("best_fit_role", ""),
            "original_reasoning": row.get("reasoning", ""),
            "data_confidence": row.get("data_confidence", ""),
        })

        if limit and len(candidates) >= limit:
            break

    # Reverse back to chronological order
    candidates.reverse()
    return candidates


# ── Run Opus judgment ────────────────────────────────────────────

def eval_one(
    candidate: Dict,
    prompt_text: str,
    api_key: str,
    model: str,
) -> Dict:
    """Run Opus judgment on one candidate, return result dict."""
    dossier = candidate["dossier"]

    # Enrich dossier (same as pipeline.py)
    dossier = dossier + _github_enrich_block(dossier)
    dossier = "\nData Confidence: " + _assess_data_confidence(dossier) + "\n" + dossier

    # Build full prompt (same format as build_opus_prompt)
    full_prompt = (
        prompt_text.rstrip()
        + "\n\n<research_dossier>\n"
        + dossier
        + "\n</research_dossier>\n\n<candidate_metadata>\n"
        + f"Name: {candidate['name']}\n"
        + f"LinkedIn: {candidate['linkedin']}\n"
        + f"Source: {candidate['source']}\n"
        + f"Source Additional Notes: {candidate.get('source_notes', '')}\n"
        + f"Target Role: {candidate['target_role']}\n"
        + f"CV/Resume: {candidate['cv']}\n"
        + "</candidate_metadata>"
    )

    payload = {
        "model": model,
        "max_tokens": int(os.environ.get("OPUS_MAX_TOKENS", "32000")),
        "temperature": float(os.environ.get("OPUS_TEMPERATURE", "1")),
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": "max"},
        "messages": [{"role": "user", "content": full_prompt}],
    }

    start = time.time()
    code, data = _http_post_json(
        "https://api.anthropic.com/v1/messages",
        {
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "Content-Type": "application/json",
        },
        payload,
        timeout=900,
    )
    elapsed = time.time() - start

    if code != 200:
        return {
            **candidate,
            "new_verdict": "EVAL FAILED",
            "new_spark": "",
            "new_best_fit": "",
            "new_reasoning": f"HTTP {code}: {str(data)[:200]}",
            "cost": 0.0,
            "elapsed": elapsed,
        }

    usage = data.get("usage") or {}
    in_tok = usage.get("input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    cost = (in_tok * 5.0 + out_tok * 25.0) / 1_000_000

    # Parse response
    text = ""
    for block in data.get("content") or []:
        if block.get("type") == "text":
            text += block.get("text") or ""

    text = re.sub(r"```json\s*", "", text, flags=re.I)
    text = re.sub(r"```\s*", "", text)
    first, last = text.find("{"), text.rfind("}")

    if first == -1 or last <= first:
        return {
            **candidate,
            "new_verdict": "EVAL FAILED",
            "new_spark": "",
            "new_best_fit": "",
            "new_reasoning": "No JSON in response",
            "cost": cost,
            "elapsed": elapsed,
        }

    try:
        parsed = json.loads(_repair_json(text[first: last + 1]))
    except json.JSONDecodeError as e:
        return {
            **candidate,
            "new_verdict": "EVAL FAILED",
            "new_spark": "",
            "new_best_fit": "",
            "new_reasoning": f"JSON parse error: {e}",
            "cost": cost,
            "elapsed": elapsed,
        }

    # Extract best fit roles from "roles" array (same logic as screen_batch.py)
    roles_list = parsed.get("roles") or []
    role_names = [r.get("role") for r in roles_list if r.get("role")]
    best_fit = ", ".join(role_names)

    return {
        **candidate,
        "new_verdict": parsed.get("verdict", ""),
        "new_spark": parsed.get("spark", ""),
        "new_best_fit": best_fit,
        "new_reasoning": parsed.get("reasoning", ""),
        "new_rejection_type": parsed.get("rejection_type", ""),
        "new_matched_level": ", ".join(r.get("level", "") for r in roles_list if r.get("level")),
        "cost": cost,
        "elapsed": elapsed,
    }


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Offline prompt eval — re-run Opus on cached dossiers, compare verdicts"
    )
    parser.add_argument("--since", help="Include candidates screened on or after this date (YYYY-MM-DD)")
    parser.add_argument("--until", help="Include candidates screened on or before this date (YYYY-MM-DD)")
    parser.add_argument("--role", action="append", dest="roles", help="Filter by role (repeatable)")
    parser.add_argument("--source", action="append", dest="sources",
                        help="Filter by source (substring match, repeatable)")
    parser.add_argument("--ids", help="File with candidate IDs (one per line)")
    parser.add_argument("--prompt", help="Path to alternative prompt file (default: prompts/opus_body.md)")
    parser.add_argument("--model", default=os.environ.get("OPUS_MODEL", "claude-opus-4-6"),
                        help="Model to use (default: claude-opus-4-6)")
    parser.add_argument("--limit", type=int, default=0, help="Max candidates to evaluate")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers (default: 5)")
    parser.add_argument("--output", help="Output CSV path (default: eval/eval_TIMESTAMP.csv)")
    parser.add_argument("--dry-run", action="store_true", help="List candidates, no API calls")
    args = parser.parse_args()

    if not args.since and not args.until and not args.ids:
        print("\nProvide --since DATE, --until DATE, or --ids FILE to select candidates.")
        sys.exit(1)

    api_key = os.environ.get("CLAUDE_API_KEY", "")
    if not api_key and not args.dry_run:
        print("\nMissing CLAUDE_API_KEY env var.")
        sys.exit(1)

    # Load prompt
    prompt_path = Path(args.prompt) if args.prompt else _DEFAULT_PROMPT
    if not prompt_path.exists():
        print(f"\nPrompt file not found: {prompt_path}")
        sys.exit(1)
    prompt_text = prompt_path.read_text(encoding="utf-8")
    prompt_name = prompt_path.stem

    # Load candidates
    candidates = load_candidates(
        since=args.since,
        until=args.until,
        roles=args.roles,
        sources=args.sources,
        id_file=args.ids,
        limit=args.limit,
    )

    if not candidates:
        print("\nNo candidates found matching criteria.")
        return

    # Summary
    verdicts = {}
    for c in candidates:
        v = c["original_verdict"]
        verdicts[v] = verdicts.get(v, 0) + 1

    print(f"\n  Candidates: {len(candidates)}")
    print(f"  Prompt:     {prompt_path}")
    print(f"  Model:      {args.model}")
    print(f"  Workers:    {args.workers}")
    print(f"  Original verdict distribution:")
    for v, count in sorted(verdicts.items(), key=lambda x: -x[1]):
        print(f"    {v:<25} {count}")

    if args.dry_run:
        print(f"\n  DRY RUN — candidates that would be evaluated:\n")
        for c in candidates:
            print(f"    {c['name']:<30} {c['original_verdict']:<12} {c['target_role']:<6} {c['candidate_id'][:12]}")
        print(f"\n  To run: remove --dry-run flag")
        return

    # Run evaluations in parallel
    print(f"\n  Starting eval ({len(candidates)} candidates)...\n")
    results = []
    total_cost = 0.0
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(eval_one, c, prompt_text, api_key, args.model): c
            for c in candidates
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            total_cost += result.get("cost", 0)

            # Verdict flip indicator
            old_v = result["original_verdict"]
            new_v = result["new_verdict"]
            flip = " *** FLIP ***" if old_v != new_v else ""
            logger.info(
                "  [%d/%d] %s: %s → %s  ($%.3f, %.0fs)%s",
                completed, len(candidates),
                result["name"],
                old_v, new_v,
                result.get("cost", 0), result.get("elapsed", 0),
                flip,
            )

    # Sort results by name for consistent output
    results.sort(key=lambda r: r["name"])

    # Write output CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = args.output or str(_EVAL_DIR / f"eval_{prompt_name}_{ts}.csv")
    fieldnames = [
        "name", "candidate_id", "target_role",
        "original_verdict", "new_verdict", "verdict_changed",
        "original_spark", "new_spark", "spark_changed",
        "original_best_fit", "new_best_fit",
        "original_reasoning", "new_reasoning",
        "new_rejection_type", "new_matched_level",
        "data_confidence", "linkedin", "source",
        "cost", "elapsed",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            r["verdict_changed"] = "YES" if r["original_verdict"] != r["new_verdict"] else ""
            # Flag significant spark changes — different spark text beyond minor rewording
            old_spark = (r.get("original_spark") or "").strip().lower()
            new_spark = (r.get("new_spark") or "").strip().lower()
            if old_spark and new_spark and old_spark != new_spark:
                # Check word overlap — if less than 40% shared words, it's a significant change
                old_words = set(old_spark.split())
                new_words = set(new_spark.split())
                overlap = len(old_words & new_words) / max(len(old_words | new_words), 1)
                r["spark_changed"] = "YES" if overlap < 0.4 else "MINOR"
            elif bool(old_spark) != bool(new_spark):
                r["spark_changed"] = "YES"  # one has spark, other doesn't
            else:
                r["spark_changed"] = ""
            writer.writerow(r)

    # Print summary
    flips = sum(1 for r in results if r["original_verdict"] != r["new_verdict"])
    screen_to_decline = sum(1 for r in results if r["original_verdict"] == "SCREEN" and r["new_verdict"] == "DECLINE")
    decline_to_screen = sum(1 for r in results if r["original_verdict"] == "DECLINE" and r["new_verdict"] == "SCREEN")
    failed = sum(1 for r in results if r["new_verdict"] == "EVAL FAILED")
    spark_changed = sum(1 for r in results if r.get("spark_changed") == "YES")
    spark_minor = sum(1 for r in results if r.get("spark_changed") == "MINOR")

    print(f"\n  {'='*55}")
    print(f"  EVAL REPORT")
    print(f"  {'='*55}")
    print(f"  Candidates evaluated: {len(results)}")
    print(f"  Total cost:           ${total_cost:.2f}")
    if failed:
        print(f"  Eval failures:        {failed}")

    # Verdict flips
    print(f"\n  VERDICT FLIPS: {flips} ({flips/len(results)*100:.1f}%)")
    if decline_to_screen:
        print(f"    DECLINE → SCREEN:   {decline_to_screen}")
    if screen_to_decline:
        print(f"    SCREEN → DECLINE:   {screen_to_decline}")
    # Other flip types
    other_flips = flips - screen_to_decline - decline_to_screen
    if other_flips > 0:
        print(f"    Other flips:        {other_flips}")

    # Spark changes
    print(f"\n  SPARK CHANGES: {spark_changed} significant, {spark_minor} minor")

    # Role breakdown
    print(f"\n  BREAKDOWN BY ROLE:")
    print(f"    {'Role':<10} {'Total':>5} {'Flips':>5} {'Flip%':>6}  {'D→S':>3}  {'S→D':>3}  {'Spark':>5}")
    print(f"    {'-'*48}")
    role_stats = {}
    for r in results:
        role = r.get("target_role", "") or "—"
        if role not in role_stats:
            role_stats[role] = {"total": 0, "flips": 0, "d2s": 0, "s2d": 0, "spark": 0}
        role_stats[role]["total"] += 1
        if r["original_verdict"] != r["new_verdict"]:
            role_stats[role]["flips"] += 1
        if r["original_verdict"] == "DECLINE" and r["new_verdict"] == "SCREEN":
            role_stats[role]["d2s"] += 1
        if r["original_verdict"] == "SCREEN" and r["new_verdict"] == "DECLINE":
            role_stats[role]["s2d"] += 1
        if r.get("spark_changed") == "YES":
            role_stats[role]["spark"] += 1
    for role in sorted(role_stats, key=lambda r: -role_stats[r]["total"]):
        s = role_stats[role]
        pct = f"{s['flips']/s['total']*100:.0f}%" if s["total"] else "—"
        print(f"    {role:<10} {s['total']:>5} {s['flips']:>5} {pct:>6}  {s['d2s']:>3}  {s['s2d']:>3}  {s['spark']:>5}")

    # List all flipped candidates
    flipped = [r for r in results if r["original_verdict"] != r["new_verdict"]]
    if flipped:
        print(f"\n  FLIPPED CANDIDATES:")
        for r in sorted(flipped, key=lambda x: x["name"]):
            print(f"    {r['name']:<30} {r['target_role']:<6} {r['original_verdict']:<12} → {r['new_verdict']}")

    # List candidates with significant spark changes (that didn't already flip)
    spark_only = [r for r in results if r.get("spark_changed") == "YES" and r["original_verdict"] == r["new_verdict"]]
    if spark_only:
        print(f"\n  SIGNIFICANT SPARK CHANGES (same verdict):")
        for r in sorted(spark_only, key=lambda x: x["name"])[:20]:
            print(f"    {r['name']:<30} {r['target_role']:<6} {r['original_verdict']}")
        if len(spark_only) > 20:
            print(f"    ... and {len(spark_only) - 20} more (see CSV)")

    print(f"\n  Output: {output_path}")
    print()


if __name__ == "__main__":
    main()
