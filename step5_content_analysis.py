"""
Step 5 — Content Analysis (LLM keywords + word cloud)  — NEW OpenAI SDK ONLY

Reads:  data/posts.json
Writes: content/keywords.jsonl
        content/keywords_by_post.csv
        content/keywords_summary.csv
        content/wordcloud.png
        content/samples_table.csv

Run:    python3 step5_content_analysis.py --limit 500
        # Quick test: --limit 50
"""
from dotenv import load_dotenv
load_dotenv()
import os, json, csv, argparse, re, time, random
from pathlib import Path
from collections import Counter
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm
from openai import OpenAI  # NEW SDK

ROOT = Path(".")
DATA = ROOT / "data" / "posts.json"
OUTDIR = ROOT / "content"
OUTDIR.mkdir(parents=True, exist_ok=True)

# ----- OpenAI client (new style) -----
def get_client():
    base = os.getenv("OPENAI_BASE_URL")  # optional for OpenAI-compatible endpoints (e.g., Llama3 via Ollama)
    api_key = os.getenv("OPENAI_API_KEY")
    if base:
        return OpenAI(api_key=api_key, base_url=base)
    return OpenAI(api_key=api_key)

CLIENT = get_client()
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

SYSTEM_PROMPT = (
    "You are an expert annotator for social-media posts about AI & technology. "
    "Given one post, extract EXACTLY three concise keywords (1–3 words each). "
    "Focus on domain terms, named entities, or specific technologies; avoid generic words "
    "like 'ai', 'technology', 'news', 'today', 'thoughts'. Keep them informative but short. "
    "Lowercase everything. Output STRICTLY this JSON schema:\n"
    '{\"keywords\": [\"k1\", \"k2\", \"k3\"]}\n'
    "Do not include explanations or any other fields."
)

def clean_text(html):
    if not html:
        return ""
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()

def build_user_prompt(text, tags):
    tag_str = ", ".join([f"#{t}" for t in tags]) if tags else "none"
    return f"post:\n\"\"\"\n{text}\n\"\"\"\nhashtags: {tag_str}\nReturn only JSON."

@retry(wait=wait_exponential(multiplier=1, min=1, max=20), stop=stop_after_attempt(5))
def call_llm(prompt: str) -> str:
    """Return JSON string from the new chat.completions API."""
    r = CLIENT.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,
        max_tokens=60,
        response_format={"type": "json_object"},  # ensures JSON
    )
    return r.choices[0].message.content

def parse_keywords(raw, fallback_tags):
    try:
        obj = json.loads(raw)
        kws = obj.get("keywords") or []
    except Exception:
        kws = [x.strip() for x in raw.split(",") if x.strip()]
    cleaned = []
    for k in kws:
        k = re.sub(r"^[#@]", "", k.lower())
        k = re.sub(r"[^a-z0-9\-\s\.]+", "", k).strip()
        if k and k not in {"ai","tech","technology","news","today","post"} and k not in cleaned:
            cleaned.append(k)
    if len(cleaned) < 3:
        for t in fallback_tags:
            t = t.lower().strip()
            if t and t not in cleaned and t not in {"ai","technology"}:
                cleaned.append(t)
            if len(cleaned) >= 3:
                break
    return cleaned[:3]

def load_posts(path):
    posts = json.loads(Path(path).read_text(encoding="utf-8"))
    out = []
    for p in posts:
        out.append({
            "id": str(p["id"]),
            "text": clean_text(p.get("content_html") or p.get("content") or ""),
            "tags": [t.lower() for t in (p.get("tags") or [])],
            "url": p.get("url"),
        })
    return out

def save_jsonl(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def wordcloud_from_counts(counts, out_png):
    from wordcloud import WordCloud
    wc = WordCloud(width=1600, height=900, background_color="white")
    wc.generate_from_frequencies(dict(counts))
    wc.to_file(out_png)

def main(limit):
    posts = load_posts(DATA)
    if limit and limit < len(posts):
        posts = posts[:limit]

    rows = []
    print(f"[content] extracting keywords for {len(posts)} posts with model={MODEL}")
    for p in tqdm(posts):
        if not p["text"] and not p["tags"]:
            kws = []
        else:
            prompt = build_user_prompt(p["text"], p["tags"])
            raw = call_llm(prompt)
            kws = parse_keywords(raw, p["tags"])
        rows.append({"id": p["id"], "url": p["url"], "keywords": kws, "text": p["text"]})
        time.sleep(0.05 + random.random()*0.05)

    save_jsonl(rows, OUTDIR / "keywords.jsonl")

    with open(OUTDIR / "keywords_by_post.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["id","keyword1","keyword2","keyword3","url"])
        for r in rows:
            k = (r["keywords"] + ["","",""])[:3]
            w.writerow([r["id"], k[0], k[1], k[2], r["url"]])

    counts = Counter(k for r in rows for k in r["keywords"] if k)
    with open(OUTDIR / "keywords_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["keyword","count"])
        for kw, c in counts.most_common():
            w.writerow([kw, c])

    wordcloud_from_counts(counts, OUTDIR / "wordcloud.png")

    samples = rows[:10]
    with open(OUTDIR / "samples_table.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["post_id","keywords","snippet"])
        for r in samples:
            snippet = (r["text"][:140] + "…") if len(r["text"]) > 140 else r["text"]
            w.writerow([r["id"], ", ".join(r["keywords"]), snippet])

    print("[content] done:",
          "\n - content/keywords.jsonl",
          "\n - content/keywords_by_post.csv",
          "\n - content/keywords_summary.csv",
          "\n - content/wordcloud.png",
          "\n - content/samples_table.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500, help="max posts to process")
    args = ap.parse_args()
    main(args.limit)
