"""
Step 2 (part 1) — Keyword-based data collection for Mastodon.
- Pulls posts for AI/tech hashtags using timeline_hashtag (stable across Mastodon.py versions).
- De-duplicates across hashtags and trims to EXACTLY 500 for grading.
- Saves JSON to data/posts.json

Run:  python collect_keyword_posts.py
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Set
from mastodon_client import get_client

# You can add/remove hashtags here (no # symbol)
HASHTAGS = [
    "ai", "artificialintelligence", "machinelearning", "llm",
    "generativeAI", "ChatGPT", "deeplearning", "aiethics"
]

TARGET_COUNT = 500
BATCH_LIMIT = 40      # per API call (safe)
SLEEP_SEC = 0.4       # be polite, let ratelimit pacing work comfortably

OUT_PATH = Path("data/posts.json")


def normalize_status(s: Dict[str, Any]) -> Dict[str, Any]:
    """Pick stable fields and flatten a bit for later steps."""
    acct = s.get("account", {}) or {}
    reblog = s.get("reblog")
    return {
        "id": s["id"],
        "created_at": getattr(s.get("created_at"), "isoformat", lambda: s.get("created_at"))(),
        "language": s.get("language"),
        "content_html": s.get("content", ""),
        "in_reply_to_id": s.get("in_reply_to_id"),
        "reblog_of_id": (reblog or {}).get("id"),
        "account": {
            "id": acct.get("id"),
            "acct": acct.get("acct"),
            "username": acct.get("username"),
            "display_name": acct.get("display_name"),
            "url": acct.get("url"),
        },
        "mentions": [m.get("acct") for m in s.get("mentions", [])],
        "tags": [t.get("name") for t in s.get("tags", [])],
        "replies_count": s.get("replies_count"),
        "reblogs_count": s.get("reblogs_count"),
        "favourites_count": s.get("favourites_count"),
        "url": s.get("url"),
    }


def collect_hashtag(m, tag: str, want: int, seen: Set[str], out: List[Dict[str, Any]]) -> None:
    """
    Collect posts from a single hashtag timeline until we meet `want` new items
    or the timeline ends.
    """
    max_id = None
    while len(out) < want:
        # timeline_hashtag(tag, ...) expects no '#' in the tag
        statuses = m.timeline_hashtag(tag, limit=BATCH_LIMIT, max_id=max_id)
        if not statuses:
            break

        for s in statuses:
            sid = s["id"]
            if sid in seen:
                continue
            seen.add(sid)
            out.append(normalize_status(s))
            if len(out) >= want:
                break

        # prepare for next page
        max_id = statuses[-1]["id"]
        time.sleep(SLEEP_SEC)


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    m = get_client()

    collected: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    target_buffered = TARGET_COUNT + 50  # collect a little over target, then trim
    for tag in HASHTAGS:
        print(f"[posts] collecting from #{tag} ... ({len(collected)}/{target_buffered})")
        collect_hashtag(m, tag, target_buffered, seen, collected)
        if len(collected) >= target_buffered:
            break

    collected = collected[:TARGET_COUNT]  # TRIM to exactly 500 for grading
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"[posts] saved {len(collected)} posts → {OUT_PATH}")


if __name__ == "__main__":
    main()
