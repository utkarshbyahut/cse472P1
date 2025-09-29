# build_networks.py
"""
Step 3 networks → Gephi (GEXF)
- Information Diffusion (directed): posts as nodes; reply/boost edges
- Friendship (undirected): users as nodes; edges from user_edges.json if present,
  else fallback edges linking users on same domain (community proxy)

Inputs:
  data/posts.json
  data/users.json
  data/user_edges.json   # optional

Outputs:
  graphs/information_diffusion.gexf
  graphs/friendship.gexf
  graphs/information_diffusion_nodes.csv / _edges.csv
  graphs/friendship_nodes.csv / _edges.csv
"""

from pathlib import Path
import json
import csv
import networkx as nx

POSTS_PATH = Path("data/posts.json")
USERS_PATH = Path("data/users.json")
USER_EDGES_PATH = Path("data/user_edges.json")  # optional
OUT_DIR = Path("graphs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------- utils ---------
def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def domain_from_acct(acct: str) -> str:
    if not acct:
        return "unknown"
    return acct.split("@", 1)[1].lower() if "@" in acct else "mastodon.social"

def _sanitize_value(v):
    # GEXF supports: str, int, float, bool. No None/list/dict.
    if v is None:
        return None  # we'll drop it
    if isinstance(v, (str, int, float, bool)):
        return v
    # collapse lists/dicts/others to compact strings
    try:
        if isinstance(v, (list, tuple)):
            return ",".join(map(str, v))
        if isinstance(v, dict):
            return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        # fallback
        return str(v)
    except Exception:
        return None

def sanitize_graph_attributes(G: nx.Graph):
    # sanitize node attrs
    for n, d in list(G.nodes(data=True)):
        clean = {}
        for k, v in d.items():
            sv = _sanitize_value(v)
            if sv is not None:
                clean[k] = sv
        G.nodes[n].clear()
        G.nodes[n].update(clean)
    # sanitize edge attrs
    for u, v, d in list(G.edges(data=True)):
        clean = {}
        for k, val in d.items():
            sv = _sanitize_value(val)
            if sv is not None:
                clean[k] = sv
        G.edges[u, v].clear()
        G.edges[u, v].update(clean)

# --------- build graphs ---------
def build_information_diffusion(posts):
    G = nx.DiGraph(name="InformationDiffusion")
    present = set()

    for p in posts:
        pid = str(p["id"])
        present.add(pid)
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

    for p in posts:
        this_id = str(p["id"])
        r = p.get("in_reply_to_id")
        if r is not None:
            src = str(r)
            if src in present:
                G.add_edge(src, this_id, kind="reply")
        b = p.get("reblog_of_id")
        if b is not None:
            src = str(b)
            if src in present:
                G.add_edge(src, this_id, kind="boost")
    return G

def build_friendship(users, user_edges=None):
    G = nx.Graph(name="Friendship")
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

    if user_edges:
        added = 0
        for e in user_edges:
            u = e.get("src_id") or id_by_acct.get(e.get("src_acct"))
            v = e.get("dst_id") or id_by_acct.get(e.get("dst_acct"))
            if not u or not v or u == v:
                continue
            G.add_edge(str(u), str(v), reason=e.get("reason") or "follow")
            added += 1
        if added == 0:
            print("[friendship] WARN: user_edges.json had no usable edges; using domain fallback.")

    if G.number_of_edges() == 0:
        # domain fallback: chain within each domain to avoid O(n^2)
        buckets = {}
        for n, data in G.nodes(data=True):
            buckets.setdefault(data.get("domain", "unknown"), []).append(n)
        for ids in buckets.values():
            for i in range(len(ids) - 1):
                G.add_edge(ids[i], ids[i + 1], reason="same_domain")
    return G

# --------- export ---------
def write_gexf(G, path: Path):
    sanitize_graph_attributes(G)
    nx.write_gexf(G, path)
    print(f"[gexf] wrote → {path}")

def write_csvs(G, prefix: str):
    nodes_csv = OUT_DIR / f"{prefix}_nodes.csv"
    edges_csv = OUT_DIR / f"{prefix}_edges.csv"

    # nodes
    with nodes_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        headers = set()
        for _, d in G.nodes(data=True):
            headers.update(d.keys())
        headers = ["id"] + sorted(headers)
        w.writerow(headers)
        for n, d in G.nodes(data=True):
            row = [n] + [d.get(h) for h in headers[1:]]
            w.writerow(row)

    # edges
    with edges_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        eheaders = set()
        for _, _, d in G.edges(data=True):
            eheaders.update(d.keys())
        eheaders = ["source", "target"] + sorted(eheaders)
        w.writerow(eheaders)
        for u, v, d in G.edges(data=True):
            row = [u, v] + [d.get(h) for h in eheaders[2:]]
            w.writerow(row)

    print(f"[csv] wrote → {nodes_csv} and {edges_csv}")

# --------- main ---------
def main():
    posts = load_json(POSTS_PATH)
    users = load_json(USERS_PATH)
    user_edges = load_json(USER_EDGES_PATH) if USER_EDGES_PATH.exists() else None
    if user_edges:
        print(f"[friendship] loaded {len(user_edges)} edges from {USER_EDGES_PATH}")

    G_info = build_information_diffusion(posts)
    G_friend = build_friendship(users, user_edges)

    write_gexf(G_info, OUT_DIR / "information_diffusion.gexf")
    write_csvs(G_info, "information_diffusion")

    write_gexf(G_friend, OUT_DIR / "friendship.gexf")
    write_csvs(G_friend, "friendship")

if __name__ == "__main__":
    main()
