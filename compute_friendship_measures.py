"""
Step 4 — Network Measures on the Friendship Network (undirected)

Outputs (to graphs/measures/):
  - degree_hist.png
  - clustering_hist.png
  - pagerank_hist.png
  - avg_neighbor_degree_hist.png
  - friendship_node_metrics.csv        # per-node metrics
  - friendship_summary.txt             # headline numbers + top nodes

Run:
  python3 compute_friendship_measures.py
"""

from pathlib import Path
import csv
import math
import statistics as stats
import networkx as nx
import matplotlib.pyplot as plt

IN_GEXF = Path("graphs/friendship.gexf")
OUT_DIR  = Path("graphs/measures")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_graph():
    G = nx.read_gexf(IN_GEXF)
    # make sure it's undirected for degree/cluster/PageRank
    if G.is_directed():
        G = G.to_undirected()
    return G

def hist(values, title, xlabel, outfile, bins="auto", logx=False):
    plt.figure(figsize=(7,4))
    if logx:
        # log-binning for heavy tails
        v = [x for x in values if x > 0]
        if v:
            lo, hi = min(v), max(v)
            edges = [lo*(10**(i/20)) for i in range(0, 1+int(20*math.log10(hi/lo+1e-9)))]
            plt.hist(v, bins=max(len(edges), 10))
        else:
            plt.hist(values, bins=bins)
    else:
        plt.hist(values, bins=bins)
    plt.title(title, weight="bold")
    plt.xlabel(xlabel)
    plt.ylabel("Count")
    plt.grid(alpha=0.3, linestyle=":")
    plt.tight_layout()
    plt.savefig(OUT_DIR/outfile, dpi=200)
    plt.close()

def main():
    G = load_graph()
    N = G.number_of_nodes()
    E = G.number_of_edges()
    components = list(nx.connected_components(G))
    LCC = G.subgraph(max(components, key=len)).copy() if components else G

    deg_dict  = dict(G.degree())
    deg_vals  = list(deg_dict.values())
    avg_deg   = (2*E / N) if N else 0.0
    isolates  = sum(1 for d in deg_vals if d == 0)

    # Measures
    clustering_dict = nx.clustering(G)
    clustering_vals = list(clustering_dict.values())

    # PageRank on undirected graph is well-defined
    pr_dict   = nx.pagerank(G, alpha=0.85, max_iter=100)
    pr_vals   = list(pr_dict.values())

    # Local level: average neighbor degree for each node (1-hop friends)
    # This is the mean number of friends that my friends have.
    and_dict  = nx.average_neighbor_degree(G)  # float per node
    and_vals  = list(and_dict.values())

    # Optional: global clustering and diameter on LCC
    trans_global = nx.transitivity(G)
    try:
        diameter = nx.diameter(LCC)
    except Exception:
        diameter = None

    # --------- Plots ----------
    hist(deg_vals, "Degree Distribution", "Degree (# of friends)", "degree_hist.png", bins=max(10, int(math.sqrt(max(deg_vals) if deg_vals else 10))))
    hist(clustering_vals, "Clustering Coefficient Distribution", "Clustering coefficient", "clustering_hist.png", bins=10)
    hist(pr_vals, "PageRank Distribution", "PageRank score", "pagerank_hist.png", bins=20)
    hist(and_vals, "Average Neighbor Degree (Local Friends)", "Avg # of friends of my friends", "avg_neighbor_degree_hist.png", bins=20)

    # --------- Per-node CSV ----------
    # Include acct, domain if present for nice tables later
    with (OUT_DIR / "friendship_node_metrics.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["node_id","acct","domain","degree","clustering","pagerank","avg_neighbor_degree"])
        for n in G.nodes():
            d = G.nodes[n]
            w.writerow([
                n,
                d.get("acct"),
                d.get("domain"),
                deg_dict.get(n, 0),
                clustering_dict.get(n, 0.0),
                pr_dict.get(n, 0.0),
                and_dict.get(n, 0.0),
            ])

    # --------- Summary ----------
    def top_k(d, k=10):
        return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:k]

    top_deg = top_k(deg_dict)
    top_pr  = top_k(pr_dict)

    with (OUT_DIR / "friendship_summary.txt").open("w", encoding="utf-8") as f:
        f.write("=== Friendship Network Summary ===\n")
        f.write(f"Nodes: {N}\nEdges: {E}\n")
        f.write(f"Average degree (global): {avg_deg:.2f}\n")
        f.write(f"Connected components: {len(components)} (LCC size: {LCC.number_of_nodes()})\n")
        f.write(f"Isolates: {isolates} ({isolates/N*100:.1f}% of nodes)\n")
        f.write(f"Global clustering (transitivity): {trans_global:.4f}\n")
        f.write(f"Diameter (LCC): {diameter}\n")
        # Local average (mean of average neighbor degree)
        mean_local = stats.mean(and_vals) if and_vals else 0.0
        f.write(f"Mean of local avg-neighbor-degree: {mean_local:.2f}\n\n")
        f.write("Top-10 by degree:\n")
        for n, v in top_deg:
            f.write(f"  {v:>4}  {G.nodes[n].get('acct','<no-acct>')}\n")
        f.write("\nTop-10 by PageRank:\n")
        for n, v in top_pr:
            f.write(f"  {v:.6f}  {G.nodes[n].get('acct','<no-acct>')}\n")

    print("Done. Wrote plots + summary to", OUT_DIR)

if __name__ == "__main__":
    main()
