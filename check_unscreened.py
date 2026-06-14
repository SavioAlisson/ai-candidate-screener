"""
Check how many candidates in Ashby haven't been screened yet.

Mirrors what `ascreen` actually pulls:
  - New Lead + Needs Rescreen (status=Lead) — across ALL plans
  - Application Review (status=Active) — across ALL plans, empty verdict only

Multi-plan aware: stage titles resolve to every plan's stage ID, not just the
flat-map Default-plan ID. Without this, AR/New-Lead apps on EPD/VD/Outbound
plans were silently dropped from the count.

Usage:
  python3 check_unscreened.py          # full scan with names
  python3 check_unscreened.py --count  # just the count
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_DIR = Path(__file__).resolve().parent

TERMINAL_VERDICTS = {"SCREEN", "DECLINE", "DEFER", "INSUFFICIENT_DATA",
                     "REVIEW_ROLE_FIT", "REVIEW_LIMITED_INFO"}


def _verdict_for(cid: str, get_verdict) -> tuple[str, str]:
    return cid, (get_verdict(cid) or "").strip().upper()


def main():
    count_only = "--count" in sys.argv

    if not os.environ.get("ASHBY_API_KEY"):
        print("Missing ASHBY_API_KEY")
        sys.exit(1)

    from ashby_bridge import (
        _ashby_post, get_stage_ids_by_title,
        _load_screening_log_ids, _load_known_screened,
        _map_job_title_to_role, load_custom_fields,
        ROLE_SHORT_TO_INTERNAL,
    )

    cf_defs = load_custom_fields()
    verdict_field_id = (cf_defs.get("AI Verdict") or {}).get("id", "")

    def get_candidate_ai_verdict(cid: str) -> str:
        if not verdict_field_id:
            return ""
        try:
            info = _ashby_post("candidate.info", {"id": cid})
            for cf in (info.get("results") or {}).get("customFields", []) or []:
                if cf.get("id") == verdict_field_id:
                    return (cf.get("value") or "").strip().upper()
        except Exception:
            return ""
        return ""

    lead_stage_ids = (get_stage_ids_by_title("New Lead")
                      | get_stage_ids_by_title("Needs Rescreen"))
    ar_stage_ids = get_stage_ids_by_title("Application Review")

    if not lead_stage_ids:
        print("No Lead sub-stage IDs found. Run: python3 ashby_bridge.py --setup-stages")
        sys.exit(1)

    log_ids, _ = _load_screening_log_ids()
    known_screened = _load_known_screened()
    skip_ids = log_ids | known_screened

    active_roles = set(ROLE_SHORT_TO_INTERNAL.keys())

    unscreened: list[tuple[str, str, str]] = []
    skipped_known = 0
    skipped_role = 0
    seen: set[str] = set()
    ar_candidates: list[tuple[str, str, str]] = []  # (cid, name, job_title)

    print("Scanning Ashby...")

    for status in ["Active", "Lead"]:
        cursor = None
        while True:
            payload = {"limit": 100, "status": status}
            if cursor:
                payload["cursor"] = cursor
            result = _ashby_post("application.list", payload)
            apps = result.get("results", [])
            if not apps:
                break

            for app in apps:
                stage_id = (app.get("currentInterviewStage") or {}).get("id", "")
                cid = (app.get("candidate") or {}).get("id", "")
                if not cid or cid in seen:
                    continue

                is_lead_intake = (status == "Lead" and stage_id in lead_stage_ids)
                is_ar = (status == "Active" and stage_id in ar_stage_ids)

                if not (is_lead_intake or is_ar):
                    continue

                seen.add(cid)

                job_title = (app.get("job") or {}).get("title", "?")
                role = _map_job_title_to_role(job_title)
                if role not in active_roles and role == job_title:
                    skipped_role += 1
                    continue

                if cid in skip_ids:
                    skipped_known += 1
                    continue

                name = (app.get("candidate") or {}).get("name", "?")

                if is_ar:
                    # AR needs a live verdict check — local skip-sets aren't enough
                    ar_candidates.append((cid, name, job_title))
                else:
                    unscreened.append((name, job_title, "New Lead"))

            if not result.get("moreDataAvailable"):
                break
            cursor = result.get("nextCursor")

    # Parallel verdict check for AR candidates
    if ar_candidates:
        with ThreadPoolExecutor(max_workers=20) as ex:
            futs = {ex.submit(_verdict_for, cid, get_candidate_ai_verdict): (cid, name, job)
                    for (cid, name, job) in ar_candidates}
            for fut in as_completed(futs):
                cid, name, job = futs[fut]
                try:
                    _, verdict = fut.result()
                except Exception:
                    verdict = ""
                if verdict in TERMINAL_VERDICTS:
                    skipped_known += 1
                else:
                    unscreened.append((name, job, "App Review"))

    print(f"\n  Unscreened:        {len(unscreened)}")
    print(f"  Already screened:  {skipped_known}")
    print(f"  Inactive roles:    {skipped_role}")

    if not count_only and unscreened:
        print()
        for name, job, tag in unscreened:
            print(f"  {name:<35} {job:<30} [{tag}]")


if __name__ == "__main__":
    main()
