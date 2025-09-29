"""
Step 2 (part 1) — Keyword-based collection WITH CONTEXT EXPANSION.

What it does
- Seeds from AI/tech hashtags via timeline_hashtag (stable).
- For each seed post, also fetches its conversation context:
    * ancestors (earlier posts in the thread)
    * descendants (replies in the thread)
- Deduplicates across hashtags and contexts.
- Trims to EXACTLY 500 posts for grading and writes data/posts.json.

Run:  python collect_keyword_posts.py
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Set, Iterable
from mastodon_client import get_client

# Hashtags to seed from (NO '#' symbol)
HASHTAGS = [
    "ai", "artificialintelligence", "machinelearning", "llm",
    "generativeAI", "ChatGPT", "deeplearning", "aiethics"
]

# Target size for final JSON
TARGET_COUNT = 500

# We collect a buffer above target, then trim (improves odds of keeping connected posts)
BUFFER = 250                   # collect ~750 then trim to 500
BATCH_LIMIT = 40               # per API call
SLEEP_SEC = 0.35               # polite pacing (Mastodon.py also paces with ratelimit_method="pace")

# To avoid spending forever expanding huge threads, cap context per seed
MAX_CONTEXT_POSTS_PER_SEED = 30

OUT_PATH = Path("data/posts.json")


def normalize_status(s: Dict[str, Any]) -> Dict[str, Any]:
    """Pick stable fields and flatten a bit for later steps."""
    acct = s.get("account", {}) or {}
    reblog = s.get("reblog")
    created = s.get("created_at")
    created_iso = created.isoformat() if hasattr(created, "isoformat") else created

    return {
        "id": s["id"],
        "created_at": created_iso,
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


def _add_status(s: Dict[str, Any], seen: Set[str], out: List[Dict[str, Any]]) -> bool:
    """Add one status if new; return True if added."""
    sid = s["id"]
    if sid in seen:
        return False
    seen.add(sid)
    out.append(normalize_status(s))
    return True


def _collect_hashtag_page(m, tag: str, max_id=None) -> List[Dict[str, Any]]:
    """One page of a hashtag timeline (no '#' in tag)."""
    return m.timeline_hashtag(tag, limit=BATCH_LIMIT, max_id=max_id) or []


def _expand_context(m, status_id: str) -> Iterable[Dict[str, Any]]:
    """
    Yield all ancestors + descendants for a status' thread.
    If the API or client version fails, yield nothing gracefully.
    """
    try:
        ctx = m.status_context(status_id)
        for s in (ctx.get("ancestors") or []):
            yield s
        for s in (ctx.get("descendants") or []):
            yield s
    except Exception:
        # Some instances or versions may not expose context; that's fine.
        return


def collect_with_expansion(m, target: int, buffer: int) -> List[Dict[str, Any]]:
    """
    - Iterate hashtags, gather seed posts.
    - For each new seed, expand context (ancestors + replies) with a per-seed cap.
    - Stop once we have target + buffer items (deduped).
    """
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    want = target + buffer

    for tag in HASHTAGS:
        print(f"[posts] collecting from #{tag} ... ({len(out)}/{want})")
        max_id = None

        while len(out) < want:
            page = _collect_hashtag_page(m, tag, max_id=max_id)
            if not page:
                break

            # Prepare next page
            max_id = page[-1]["id"]

            for seed in page:
                if len(out) >= want:
                    break

                # Add seed
                added_seed = _add_status(seed, seen, out)

                # Expand only if we still need more and this seed was new
                if added_seed and len(out) < want:
                    context_added = 0
                    for s in _expand_context(m, seed["id"]):
                        if _add_status(s, seen, out):
                            context_added += 1
                            if context_added >= MAX_CONTEXT_POSTS_PER_SEED or len(out) >= want:
                                break

                # Light pacing per seed to be polite
                time.sleep(SLEEP_SEC)

            # Page pacing
            time.sleep(SLEEP_SEC)

        if len(out) >= want:
            break

    return out


def _prefer_connected(posts: List[Dict[str, Any]], final_n: int) -> List[Dict[str, Any]]:
    """
    Prefer posts that are likely to create edges:
      - keep any post that has in_reply_to_id or reblog_of_id
      - then fill remaining slots with the rest (to reach exactly final_n)
    """
    connected = [p for p in posts if p.get("in_reply_to_id") or p.get("reblog_of_id")]
    if len(connected) >= final_n:
        return connected[:final_n]

    # fill with non-connected until we hit final_n
    remaining_slots = final_n - len(connected)
    others = [p for p in posts if not (p.get("in_reply_to_id") or p.get("reblog_of_id"))]
    return connected + others[:remaining_slots]


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    m = get_client()

    print(f"[posts] target={TARGET_COUNT}, buffer={BUFFER}, per-seed-context-cap={MAX_CONTEXT_POSTS_PER_SEED}")
    collected = collect_with_expansion(m, target=TARGET_COUNT, buffer=BUFFER)
    print(f"[posts] collected (raw, deduped): {len(collected)}")

    # Prefer connected posts so Step 3 has actual edges
    final_posts = _prefer_connected(collected, TARGET_COUNT)
    print(f"[posts] final set size (trimmed to exactly {TARGET_COUNT}): {len(final_posts)}")

    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(final_posts, f, ensure_ascii=False, indent=2)

    print(f"[posts] saved → {OUT_PATH}")


if __name__ == "__main__":
    main()
