# test_connection.py
from mastodon_client import get_client

def main():
    m = get_client()

    me = m.account_verify_credentials()
    print("Auth OK")
    print("Username:", me.get("username"))
    print("Acct:", me.get("acct"))
    print("Profile URL:", me.get("url"))

    # Stable smoke tests that work across versions:
    # 1) Public timeline
    public = m.timeline_public(limit=3)
    print("Public timeline sample:", len(public))
    for s in public:
        acct = s["account"]["acct"]
        preview = s["content"].replace("\n", " ")
        print("-", s["id"], "by", acct, ":", (preview[:80] + "...") if len(preview) > 80 else preview)

    # 2) Hashtag timeline (optional)
    try:
        tag = m.timeline_hashtag("ai", limit=3)  # no '#' symbol
        print("Hashtag #ai sample:", len(tag))
    except Exception as e:
        print("Hashtag timeline not available in this client/version:", e)

if __name__ == "__main__":
    main()
