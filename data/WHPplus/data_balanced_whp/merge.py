import os
import json
import math
import random
import argparse


# -------- Default Settings --------
DEFAULT_PASSAGE_OUT = "forget_dedup_passage.json"
DEFAULT_TARGET_PASSAGE_SIZE = 50
DEFAULT_SEED = 42
# ----------------------------------


def load_json(path):
    """
    Load a .json file.
    If it's already {name: [ {statement: ...}, ... ]} → return directly.
    If it's a list of {name/text/...} objects → normalize to the same format.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Case 1: already dict-based
    if isinstance(data, dict):
        return data

    # Case 2: list-based normalization
    if isinstance(data, list):
        merged_like = {}
        for obj in data:
            if not isinstance(obj, dict):
                continue
            name = obj.get("name") or obj.get("person") or obj.get("group") or "UNKNOWN"
            stmt = (obj.get("statement") or obj.get("text") or obj.get("s") or "").strip()
            if not stmt:
                continue
            merged_like.setdefault(name, []).append({"statement": stmt})
        return merged_like

    raise ValueError(f"Unsupported JSON structure in {path!r}")


def load_jsonl(path):
    """
    Load a .jsonl file and normalize to {name: [ {statement: ...}, ... ]}.
    Each line is a JSON object with possible keys:
      - name key: 'name' | 'person' | 'group'
      - text key: 'statement' | 'text' | 's'
    """
    merged_like = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            name = obj.get("name") or obj.get("person") or obj.get("group") or "UNKNOWN"
            stmt = (obj.get("statement") or obj.get("text") or obj.get("s") or "").strip()
            if not stmt:
                continue
            merged_like.setdefault(name, []).append({"statement": stmt})
    return merged_like


def chunk_evenly(items, max_chunk):
    """
    Evenly split items into chunks close to max_chunk size.
    Compute num_chunks = ceil(N / max_chunk), then distribute the remainder
    so earlier chunks get +1 element (balanced).
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
        chunks.append(items[idx:idx + s])
        idx += s
    return chunks


def build_passages(merged, target_size, rng):
    """
    Build passages from merged data {person: [ {statement: "..."} ]}.
    Randomly shuffle statements for each person, then split into balanced chunks.
    Output format:
      [
        {"id": "<person>#<idx>", "statements": [ "...", ... ]},
        ...
      ]
    """
    passages = []
    total_statements = 0

    for person, entries in merged.items():
        stmts = [(e.get("statement") or "").strip() for e in entries if (e.get("statement") or "").strip()]
        if not stmts:
            continue

        rng.shuffle(stmts)
        chunks = chunk_evenly(stmts, target_size)

        for i, chunk in enumerate(chunks, start=1):
            passages.append({
                "id": f"{person}#{i}",
                "statements": chunk
            })
        total_statements += len(stmts)

    return passages, total_statements


def main():
    parser = argparse.ArgumentParser(
        description="Load a single JSON/JSONL file of statements and build grouped passages."
    )
    parser.add_argument(
        "input",
        help="Path to input file (.json or .jsonl)."
    )
    parser.add_argument(
        "--passage-out",
        default=DEFAULT_PASSAGE_OUT,
        help=f"Output file for passages. Default: {DEFAULT_PASSAGE_OUT}"
    )
    parser.add_argument(
        "--target-size",
        type=int,
        default=DEFAULT_TARGET_PASSAGE_SIZE,
        help=f"Approximate number of statements per passage. Default: {DEFAULT_TARGET_PASSAGE_SIZE}"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility. Default: {DEFAULT_SEED}"
    )

    args = parser.parse_args()
    rng = random.Random(args.seed)

    # ------------------ Load Phase ------------------
    path = args.input
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.lower().endswith(".jsonl"):
        merged = load_jsonl(path)
    elif path.lower().endswith(".json"):
        merged = load_json(path)
    else:
        raise ValueError("Unsupported input file type (must be .json or .jsonl)")

    # Save normalized merged JSON
    with open(args.merged_out, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Saved normalized JSON → {args.merged_out}")

    # ------------------ Passage Build Phase ------------------
    passages, total_statements = build_passages(merged, args.target_size, rng)

    with open(args.passage_out, "w", encoding="utf-8") as f:
        json.dump(passages, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Built {len(passages)} passages, {total_statements} total statements → {args.passage_out}")


if __name__ == "__main__":
    main()
