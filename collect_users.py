"""
Step 2 (part 2) — User-based data collection for Mastodon.
- Starts from seed accounts; expands via followers + following
- De-duplicates and trims to EXACTLY 200 for grading.
- Saves JSON to data/users.json

Run:  python collect_users.py
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Set
from mastodon_client import get_client

# You can add seeds here (acct format can be 'name@instance' or local acct if same instance)
SEED_ACCTS = [
    "machinelearning@mastodon.social",
    "techcrunch@mastodon.social",
    "artificialintelligencenews.in@mastodon.social",
    "TedUnderwood@hcommons.social",
]

TARGET_USERS = 200
FETCH_LIMIT = 40
SLEEP_SEC = 0.4
OUT_PATH = Path("data/users.json")


def normalize_user(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": a.get("id"),
        "acct": a.get("acct"),
        "username": a.get("username"),
        "display_name": a.get("display_name"),
        "url": a.get("url"),
        "followers_count": a.get("followers_count"),
        "following_count": a.get("following_count"),
        "statuses_count": a.get("statuses_count"),
        "note_html": a.get("note"),
        "bot": a.get("bot"),
    }


def resolve_account(m, full_acct: str) -> Dict[str, Any]:
    """
    Resolve an acct string to an account dict via account_search.
    We pick the first exact/closest match.
    """
    results = m.account_search(full_acct, limit=5)
    if not results:
        return None
    # prefer exact acct match
    for a in results:
        if a.get("acct") == full_acct or full_acct.endswith(a.get("acct", "")):
            return a
    return results[0]


def expand_users(m, start_accounts: List[Dict[str, Any]], target: int) -> List[Dict[str, Any]]:
    """
    BFS-like expansion across followers + following until we reach target users.
    """
    seen_ids: Set[str] = set()
    queue: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    for a in start_accounts:
        if a and a.get("id") not in seen_ids:
            seen_ids.add(a["id"])
            queue.append(a)
            out.append(normalize_user(a))

    def add_iter(iterable):
        nonlocal out
        for u in iterable:
            uid = u.get("id")
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                out.append(normalize_user(u))
                queue.append(u)
                if len(out) >= target:
                    return True
        return False

    while queue and len(out) < target:
        a = queue.pop(0)
        uid = a.get("id")
        if not uid:
            continue

        # followers
        try:
            if add_iter(m.account_followers(uid, limit=FETCH_LIMIT)):
                break
        except Exception:
            pass

        # following
        try:
            if add_iter(m.account_following(uid, limit=FETCH_LIMIT)):
                break
        except Exception:
            pass

        time.sleep(SLEEP_SEC)

    return out[:target]


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    m = get_client()

    # Resolve seeds
    seed_accounts = []
    for acct in SEED_ACCTS:
        a = resolve_account(m, acct)
        if a:
            print(f"[users] seed: {acct} → id={a.get('id')}")
            seed_accounts.append(a)
        else:
            print(f"[users] WARN: could not resolve {acct}")

    users = expand_users(m, seed_accounts, TARGET_USERS)

    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

    print(f"[users] saved {len(users)} users → {OUT_PATH}")


if __name__ == "__main__":
    main()
