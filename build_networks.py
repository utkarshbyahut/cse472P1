"""
Builds Step 3 networks and exports to Gephi (GEXF):
  - Information Diffusion Network (directed): nodes are posts; edges = reply/boost links (if both ends are in posts.json)
  - Friendship Network (undirected): nodes are users; edges = follower/following pairs if available in user_edges.json;
    otherwise a safe fallback edges users who share the same instance domain so you can still visualize communities.

Input files (relative to project root):
  data/posts.json      # from Step 2 (keyword-based)  -- required
  data/users.json      # from Step 2 (user-based)     -- required
  data/user_edges.json # OPTIONAL (if you later crawl explicit follower/following edges)

Outputs:
  graphs/information_diffusion.gexf
  graphs/friendship.gexf
  (plus CSVs for quick inspection)
"""

from pathlib import Path
import json
import re
import networkx as nx
import csv

POSTS_PATH = Path("data/posts.json")
USERS_PATH = Path("data/users.json")
USER_EDGES_PATH = Path("data/user_edges.json")  # optional: [{"src_id": "...", "dst_id": "..."} or {"src_acct": "...","dst_acct":"..."}]

OUT_DIR = Path("graphs")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def domain_from_acct(acct: str) -> str:
    # acct may be "name@instance" or just "name" if local to the server
    if not acct:
        return "unknown"
    if "@" in acct:
        return acct.split("@", 1)[1].lower()
    return "mastodon.social"  # sane default for locals if your crawl was from mastodon.social


# -----------------------------
# Information Diffusion Network
# -----------------------------
def build_information_diffusion(posts):
    """
    Nodes: post ids
    Edges: original_post -> reply_post (type=reply)
           original_post -> boosted_post (type=boost)
    """
    G = nx.DiGraph(name="InformationDiffusion")

    # Add nodes with useful post attributes
    for p in posts:
        pid = str(p["id"])
        acct = (p.get("account") or {}).get("acct")
        G.add_node(
            pid,
            author=acct,
            language=p.get("language"),
            replies=p.get("replies_count"),
            reblogs=p.get("reblogs_count"),
            favourites=p.get("favourites_count"),
            url=p.get("url"),
            tags=",".join(p.get("tags") or []),
        )

    present = {str(p["id"]) for p in posts}

    for p in posts:
        this_id = str(p["id"])

        # reply edge: original -> reply
        r = p.get("in_reply_to_id")
        if r is not None:
            src = str(r)
            if src in present:
                G.add_edge(src, this_id, kind="reply")

        # boost edge: original -> boosted
        b = p.get("reblog_of_id")
        if b is not None:
            src = str(b)
            if src in present:
                G.add_edge(src, this_id, kind="boost")

    return G


# ----------------
# Friendship graph
# ----------------
def build_friendship(users, user_edges=None):
    """
    Nodes: users
    Edges:
      - If user_edges present: undirected edges between follower/following pairs
      - Else: fallback community view by linking users on same instance domain
    """
    G = nx.Graph(name="Friendship")

    # Add user nodes
    id_by_acct = {}
    for u in users:
        uid = str(u["id"])
        acct = u.get("acct")
        id_by_acct[acct] = uid
        G.add_node(
            uid,
            acct=acct,
            username=u.get("username"),
            display_name=u.get("display_name"),
            url=u.get("url"),
            followers=u.get("followers_count"),
            following=u.get("following_count"),
            statuses=u.get("statuses_count"),
            domain=domain_from_acct(acct),
        )

    # If we have explicit follower/following edges, use those
    if user_edges:
        added = 0
        for e in user_edges:
            # Support either id- or acct-based edges
            u = e.get("src_id") or id_by_acct.get(e.get("src_acct"))
            v = e.get("dst_id") or id_by_acct.get(e.get("dst_acct"))
            if not u or not v:
                continue
            if u == v:
                continue
            # undirected "friendship" view (presence of a follow relation)
            G.add_edge(str(u), str(v), reason=e.get("reason", "follow"))
            added += 1
        if added == 0:
            print("[friendship] WARN: user_edges.json loaded but no edges added; falling back to domain grouping.")
    # Fallback: connect users by instance domain (community proxy)
    if G.number_of_edges() == 0:
        by_domain = {}
        for n, data in G.nodes(data=True):
            by_domain.setdefault(data.get("domain", "unknown"), []).append(n)
        for _, ids in by_domain.items():
            # light clique: connect sequentially to avoid O(n^2) blowups
            for i in range(len(ids) - 1):
                G.add_edge(ids[i], ids[i + 1], reason="same_domain")

    return G


def write_gexf(G, path: Path):
    nx.write_gexf(G, path)
    print(f"[gexf] wrote → {path}")


def write_csvs(G, prefix: str):
    nodes_csv = OUT_DIR / f"{prefix}_nodes.csv"
    edges_csv = OUT_DIR / f"{prefix}_edges.csv"
    with nodes_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # header from first node's attrs
        node_attrs = set()
        for _, d in G.nodes(data=True):
            node_attrs.update(d.keys())
        node_attrs = sorted(node_attrs)
        w.writerow(["id"] + node_attrs)
        for n, d in G.nodes(data=True):
            w.writerow([n] + [d.get(k) for k in node_attrs])

    with edges_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        edge_attrs = set()
        for _, _, d in G.edges(data=True):
            edge_attrs.update(d.keys())
        edge_attrs = sorted(edge_attrs)
        w.writerow(["source", "target"] + edge_attrs)
        for u, v, d in G.edges(data=True):
            w.writerow([u, v] + [d.get(k) for k in edge_attrs])

    print(f"[csv] wrote → {nodes_csv} and {edges_csv}")


def main():
    posts = load_json(POSTS_PATH)
    users = load_json(USERS_PATH)
    # optional edges file
    user_edges = None
    if USER_EDGES_PATH.exists():
        try:
            user_edges = load_json(USER_EDGES_PATH)
            print(f"[friendship] loaded {len(user_edges)} edges from {USER_EDGES_PATH}")
        except Exception as e:
            print("[friendship] could not load user_edges.json:", e)

    # Build graphs
    G_info = build_information_diffusion(posts)
    G_friend = build_friendship(users, user_edges)

    # Export to Gephi
    write_gexf(G_info, OUT_DIR / "information_diffusion.gexf")
    write_gexf(G_friend, OUT_DIR / "friendship.gexf")

    # Also drop quick CSVs in case you prefer importing edge/node lists
    write_csvs(G_info, "information_diffusion")
    write_csvs(G_friend, "friendship")


if __name__ == "__main__":
    main()
