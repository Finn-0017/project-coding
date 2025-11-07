import os, json, math, random

# -------- settings --------
SHARDS = ["shard_0.json","shard_1.json","shard_2.json","shard_3.json"]
MERGED_OUT = "forget_dedup_statement.json"
PASSAGE_OUT = "forget_dedup_passage.json"
TARGET_PASSAGE_SIZE = 100           # "about 100" statements per passage
SEED = 42                           # set seed for reproducibility
# --------------------------

random.seed(SEED)

# 1) Merge shards (skip missing files)
merged = {}
found = []
for p in SHARDS:
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)
        for k, v in d.items():
            merged.setdefault(k, []).extend(v)
        found.append(p)

# If no shards found but merged file exists already, load it
if not found and os.path.exists(MERGED_OUT):
    with open(MERGED_OUT, "r", encoding="utf-8") as f:
        merged = json.load(f)
    print(f"No shard files found; loaded existing {MERGED_OUT}")
else:
    with open(MERGED_OUT, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"Merged {len(found)} shard(s) → {MERGED_OUT}")

# 2) Build passages per person (group), only keep 'statement'
passages = []  # list of {"id": "<name>#<idx>", "statements": [ ... ]}

def chunk_evenly(items, max_chunk):
    """
    Evenly split `items` into chunks with size close to `max_chunk`.
    We compute num_chunks = ceil(N / max_chunk), then distribute the remainder
    so earlier chunks get +1 (balanced).
    """
    if not items:
        return []
    n = len(items)
    num_chunks = max(1, math.ceil(n / max_chunk))
    base = n // num_chunks
    rem = n % num_chunks
    sizes = [base + (1 if i < rem else 0) for i in range(num_chunks)]
    chunks, idx = [], 0
    for s in sizes:
        chunks.append(items[idx:idx+s])
        idx += s
    return chunks

total_statements = 0
total_passages = 0

for person, entries in merged.items():
    # Extract statements; drop empty/whitespace ones
    stmts = []
    for e in entries:
        s = (e.get("statement") or "").strip()
        if s:
            stmts.append(s)

    if not stmts:
        continue

    # Shuffle to randomize distribution across passages
    random.shuffle(stmts)

    # Split into near-100 sized chunks, balanced
    chunks = chunk_evenly(stmts, TARGET_PASSAGE_SIZE)

    # Build output objects
    for i, chunk in enumerate(chunks, start=1):
        passages.append({
            "id": f"{person}#{i}",
            "statements": chunk
        })

    total_statements += len(stmts)
    total_passages += len(chunks)

# 3) Save passages
with open(PASSAGE_OUT, "w", encoding="utf-8") as f:
    json.dump(passages, f, ensure_ascii=False, indent=2)

print(f"Built passages: {total_passages} passages, {total_statements} statements → {PASSAGE_OUT}")
