"""
Split a WHP MCQ JSON (dict: original_id -> list[MCQ]) into N shards.
We DO NOT modify the original top-level IDs at all.
For each MCQ, keep only the correct option as the "fact" (remove "choices" and "answer").

Usage (PowerShell):
  python split_whp.py -i balanced_whp_mcq_train_dedup.json -n 10 -o whp_splits_10

Optional for deterministic order:
  python split_whp.py -i balanced_whp_mcq_train_dedup.json -n 10 -o whp_splits_10 --sort_ids

Output: outdir/split_01.json ... split_10.json
Each shard is a dict preserving the original IDs as keys.
"""

import argparse
import json
import os
from typing import Dict, List, Any

def parse_args():
    p = argparse.ArgumentParser(description="Split WHP MCQ JSON into N shards keeping only correct facts.")
    p.add_argument("-i", "--input", type=str, default="balanced_whp_mcq_train_dedup.json",
                   help="Input JSON file (default: balanced_whp_mcq_train_dedup.json)")
    p.add_argument("-n", "--num_shards", type=int, default=10,
                   help="Number of shards to create (default: 10)")
    p.add_argument("-o", "--outdir", type=str, default=None,
                   help="Output directory (default: auto name: <input_stem>_splits_<n>)")
    p.add_argument("--sort_ids", action="store_true",
                   help="Sort ids before sharding for determinism. If omitted, keep the file's load order.")
    return p.parse_args()

def load_json(path: str) -> Dict[str, List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_outdir(outdir: str):
    os.makedirs(outdir, exist_ok=True)

def transform_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For each MCQ item, keep name + question + correct 'fact' text.
    If the correct answer letter is missing in choices, skip that item.
    """
    out = []
    for it in items:
        ans = it.get("answer")
        choices = it.get("choices", {})
        if not isinstance(choices, dict) or ans not in choices:
            continue
        fact_text = choices.get(ans)
        if fact_text is None:
            continue
        out.append({
            "name": it.get("name"),
            "question": it.get("question"),
            "fact": fact_text
        })
    return out

def shard_ids(all_ids: List[str], n: int) -> List[List[str]]:
    """Round-robin assign ids to n buckets for balanced shards by number of people."""
    buckets = [[] for _ in range(n)]
    for idx, pid in enumerate(all_ids):
        buckets[idx % n].append(pid)
    return buckets

def main():
    args = parse_args()

    data = load_json(args.input)
    if not isinstance(data, dict):
        raise ValueError("Input JSON must be a dict mapping original_id -> list of MCQ items.")

    # Choose output dir
    if args.outdir:
        outdir = args.outdir
    else:
        stem = os.path.splitext(os.path.basename(args.input))[0]
        outdir = f"{stem}_splits_{args.num_shards}"
    ensure_outdir(outdir)

    # Prepare id list (preserve exact strings)
    all_ids = list(data.keys())
    if args.sort_ids:
        # Pure lexicographic sort to avoid coercing the id type (no int conversion)
        all_ids = sorted(all_ids)

    # Transform each id's items
    transformed: Dict[str, List[Dict[str, Any]]] = {}
    for pid in all_ids:
        items = data.get(pid)
        if isinstance(items, list):
            new_items = transform_items(items)
        elif isinstance(items, dict):
            new_items = transform_items([items])
        else:
            new_items = []
        if new_items:
            transformed[pid] = new_items

    # Only shard non-empty people
    filtered_ids = [pid for pid in all_ids if pid in transformed]

    # Shard
    buckets = shard_ids(filtered_ids, args.num_shards)

    # Write shard files
    width = max(2, len(str(args.num_shards)))
    for i, bucket in enumerate(buckets, start=1):
        shard_dict = {pid: transformed[pid] for pid in bucket}
        shard_path = os.path.join(outdir, f"split_{i:0{width}d}.json")
        with open(shard_path, "w", encoding="utf-8") as f:
            json.dump(shard_dict, f, ensure_ascii=False, indent=2)
        print(f"Wrote {shard_path} (ids: {len(bucket)} items total: {sum(len(v) for v in shard_dict.values())})")

    print("Done.")

if __name__ == "__main__":
    main()
