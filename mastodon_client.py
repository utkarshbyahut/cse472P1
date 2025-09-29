# mastodon_client.py
import os
from dotenv import load_dotenv
from mastodon import Mastodon

def get_client() -> Mastodon:
    """
    Step 1: returns an authenticated Mastodon client using env vars.
    Requires:
      - MASTODON_API_BASE (e.g., https://mastodon.social)
      - MASTODON_ACCESS_TOKEN (your app token)
    """
    load_dotenv()

    api_base = os.getenv("MASTODON_API_BASE", "").rstrip("/")
    access_token = os.getenv("MASTODON_ACCESS_TOKEN", "")

    if not api_base:
        raise RuntimeError("Set MASTODON_API_BASE in .env (e.g., https://mastodon.social).")
    if not access_token:
        raise RuntimeError("Set MASTODON_ACCESS_TOKEN in .env.")

    return Mastodon(
        api_base_url=api_base,
        access_token=access_token,
        ratelimit_method="pace"  # auto-wait on rate limits
    )
