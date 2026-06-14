"""Pending Slack threads — survives crashes, daemon death, bot restarts.

When a Candidate Labs (or future Slack-intake) submission posts a header message
in Slack and kicks off screening, we register the candidate's Slack thread here.
The verdict post-back later (whether it happens inline or after a Needs Rescreen
retry hours later) looks up this file to find the right thread to reply to.

Lifecycle:
  register(cid, channel, thread_ts, name)  — at intake, before screening starts
  pop(cid)                                  — after a terminal verdict is posted
  post_verdict_to_thread(cid, text)        — convenience: post + pop on success
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

_DIR = Path(__file__).resolve().parent
_FILE = _DIR / ".pending_slack_threads.json"
_LOCK = threading.Lock()


def _load() -> Dict[str, dict]:
    if not _FILE.exists():
        return {}
    try:
        return json.loads(_FILE.read_text(encoding="utf-8")) or {}
    except Exception as e:
        logger.warning("pending_slack_threads load failed: %s", e)
        return {}


def _save(data: Dict[str, dict]) -> None:
    tmp = _FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, _FILE)


def register(candidate_id: str, channel: str, thread_ts: str, name: str = "") -> None:
    """Save the Slack thread reference so a later verdict can post into it."""
    if not (candidate_id and channel and thread_ts):
        return
    with _LOCK:
        data = _load()
        data[candidate_id] = {
            "channel": channel,
            "thread_ts": thread_ts,
            "name": name,
            "registered_at": int(time.time()),
        }
        _save(data)


def peek(candidate_id: str) -> Optional[dict]:
    if not candidate_id:
        return None
    with _LOCK:
        return _load().get(candidate_id)


def pop(candidate_id: str) -> Optional[dict]:
    if not candidate_id:
        return None
    with _LOCK:
        data = _load()
        entry = data.pop(candidate_id, None)
        if entry is not None:
            _save(data)
        return entry


def _slack_post(channel: str, thread_ts: str, text: str) -> bool:
    """Post text to a Slack channel/thread via chat.postMessage. Returns True on ok."""
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        logger.warning("SLACK_BOT_TOKEN not set; cannot post verdict to thread")
        return False
    payload = json.dumps({
        "channel": channel,
        "thread_ts": thread_ts or None,
        "text": text,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if not body.get("ok"):
            logger.warning("Slack post not-ok: %s", body.get("error"))
            return False
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
        logger.warning("Slack post failed: %s", e)
        return False


def post_to_thread(candidate_id: str, text: str, *, clear_on_success: bool = True) -> bool:
    """Look up the pending thread for this candidate and post text to it.
    If clear_on_success, removes the pending entry after a successful post."""
    entry = peek(candidate_id)
    if not entry:
        return False
    ok = _slack_post(entry["channel"], entry["thread_ts"], text)
    if ok and clear_on_success:
        pop(candidate_id)
    return ok


def format_verdict_text(name: str, verdict: str, best_fit: str = "",
                         confidence=None, ashby_url: str = "",
                         retry_marker: str = "") -> str:
    """Compact verdict line for Slack thread reply. retry_marker is a short
    suffix like '(after auto-retry)' to make it clear when a delayed post lands."""
    emoji = {
        "SCREEN": ":white_check_mark:",
        "DECLINE": ":x:",
        "DEFER": ":hourglass_flowing_sand:",
        "REVIEW: ROLE FIT": ":mag:",
        "REVIEW: LIMITED PUBLIC INFO": ":mag:",
        "INSUFFICIENT DATA": ":grey_question:",
        "DUPLICATE": ":arrows_counterclockwise:",
    }.get((verdict or "").upper(), ":page_facing_up:")
    link = f" • <{ashby_url}|Open in Ashby>" if ashby_url else ""
    suffix = f" {retry_marker}" if retry_marker else ""
    lines = [f"{emoji} *{name}* — verdict: *{verdict}*{link}{suffix}"]
    if confidence is not None:
        lines[0] += f" • confidence: {confidence}/5"
    if best_fit:
        lines.append(f"Best fit: _{best_fit}_")
    return "\n".join(lines)
