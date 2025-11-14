#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rephrase forget_passages.json → JSONL, resumable & shardable.

Expected input format (most likely):

[
  {
    "group": "10000",
    "name": "Benedetto Varchi",
    "passage_index": 0,
    "passage": "..."
  },
  {
    "group": "10000",
    "name": "Benedetto Varchi",
    "passage_index": 1,
    "passage": "..."
  },
  ...
]

Output: JSONL, one line per passage, e.g.:

{
  "group": "10000",
  "name": "Benedetto Varchi",
  "passage_index": 0,
  "passage": "Benedetto Varchi ... He ..." (REPHRASED)
}

Features:
- --num-shards N, --shard-index i : process items[i::N]
- Resume: reads existing JSONL, collects (group, passage_index) done, skips them
- --max-minutes: soft time limit; exit after finishing current passage
"""

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set

import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

# =====================
# Config
# =====================

MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 512
DEFAULT_MAX_MINUTES = 60.0  # soft limit
DEFAULT_SEED = 1234

PROMPT_TEMPLATE = """You are editing biographical notes about a single historical figure.

Rewrite the following passage into one or two coherent paragraphs that read like a short, well-structured biography of this person.

Rules:
1. You MUST preserve all factual content from the original passage. Every fact should still appear in the rewritten version, even if rephrased. You may merge multiple sentences that express the same fact, but do not drop any distinct piece of information.
2. You may freely reorder, group, and reorganize the facts to improve logic and readability. For example, you can group together early life and education, membership in academies and patrons, works and achievements, later life and crises, and reputation or mentions by other authors.
3. Mention the person’s full name only in the first sentence. After that, refer to the person using pronouns (“he”, “she” or “they”) instead of repeating the full name.
4. Remove unnecessary repetition of the name and avoid listing facts in a flat, disconnected way. Instead, combine related facts into flowing sentences and paragraphs.
5. Use a neutral, factual tone. Do not add any new facts or interpretations that are not implied by the passage.
6. Output plain text only, with one or two paragraphs, no bullet points, no headings, no numbering.

Person’s name: {name}
Passage:
{passage}

Now produce the rewritten passage:
"""

# =====================
# Utils
# =====================

def set_seed(seed: int) -> None:
    import random
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception:
        pass


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def safe_fsync(f) -> None:
    try:
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        pass


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@dataclass
class PassageItem:
    group: str
    passage_index: int
    name: str
    passage: str


def flatten_passages(path: str) -> List[PassageItem]:
    """
    Try to support typical formats. Primary expected:

    [
      {
        "group": "10000",
        "name": "...",
        "passage_index": 0,
        "passage": "..."
      },
      ...
    ]

    Fallback: if it's a dict[group] -> list[entry] with same keys.
    """
    raw = load_json(path)
    items: List[PassageItem] = []

    # Case 1: already a list of entries
    if isinstance(raw, list):
        for entry in raw:
            group = str(entry.get("group", ""))
            name = str(entry.get("name", ""))
            passage_index = int(entry.get("passage_index", 0))
            passage = str(entry.get("passage", ""))
            items.append(
                PassageItem(
                    group=group,
                    passage_index=passage_index,
                    name=name,
                    passage=passage,
                )
            )
        return items

    # Case 2: dict[group] -> list[entry]
    if isinstance(raw, dict):
        for group, lst in raw.items():
            if not isinstance(lst, list):
                continue
            for entry in lst:
                name = str(entry.get("name", ""))
                passage_index = int(entry.get("passage_index", 0))
                passage = str(entry.get("passage", ""))
                items.append(
                    PassageItem(
                        group=str(group),
                        passage_index=passage_index,
                        name=name,
                        passage=passage,
                    )
                )
        return items

    raise ValueError("Unsupported forget_passages.json format")


def load_model_and_tokenizer(path: str):
    """Load model + tokenizer, set pad_token to eos_token to avoid warnings."""
    print(f"[INFO] loading model from {path}", file=sys.stderr)
    tokenizer = AutoTokenizer.from_pretrained(path)
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    model.config.pad_token_id = tokenizer.pad_token_id
    model.eval()
    return model, tokenizer


def build_chat_input(tokenizer, name: str, passage: str) -> torch.Tensor:
    messages = [
        {
            "role": "system",
            "content": "You are a careful editor who rewrites biographical passages without losing any factual content.",
        },
        {
            "role": "user",
            "content": PROMPT_TEMPLATE.format(name=name, passage=passage),
        },
    ]
    input_ids = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        return_tensors="pt",
    )
    return input_ids


def generate_rewrite(model, tokenizer, name: str, passage: str) -> str:
    input_ids = build_chat_input(tokenizer, name, passage).to(model.device)
    attention_mask = torch.ones_like(input_ids, dtype=torch.long)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=True,
            temperature=0.4,
            top_p=0.9,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )

    # 取生成部分
    generated = out[0][input_ids.size(1):]
    text = tokenizer.decode(generated, skip_special_tokens=True).strip()
    return text


# =====================
# Argparse & signals
# =====================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, required=True, help="Input forget_passages.json")
    ap.add_argument("--output", type=str, required=True, help="Output JSONL file")
    ap.add_argument("--num-shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--shard-index", type=int, default=0, help="Index of this shard (0-based)")
    ap.add_argument("--max-minutes", type=float, default=DEFAULT_MAX_MINUTES, help="Soft time limit in minutes")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed")
    return ap.parse_args()


SHOULD_EXIT = False


def handle_sigterm(signum, frame):
    global SHOULD_EXIT
    SHOULD_EXIT = True
    print("[SIGNAL] SIGTERM received; will exit after this passage.", file=sys.stderr)


# =====================
# Main
# =====================

def main():
    global SHOULD_EXIT
    args = parse_args()
    set_seed(args.seed)
    signal.signal(signal.SIGTERM, handle_sigterm)

    print(f"[INFO] input  = {args.input}")
    print(f"[INFO] output = {args.output}")
    print(f"[INFO] shard  = {args.shard_index}/{args.num_shards}")

    if args.shard_index < 0 or args.shard_index >= args.num_shards:
        raise ValueError("Invalid shard_index / num_shards")

    # 1) load and shard
    all_items = flatten_passages(args.input)
    all_items = sorted(all_items, key=lambda x: (x.group, x.passage_index))
    assigned = all_items[args.shard_index :: args.num_shards]

    print(f"[INFO] total passages = {len(all_items)}, assigned to this shard = {len(assigned)}")

    # 2) resume: read existing JSONL, collect completed (group, passage_index)
    completed: Set[Tuple[str, int]] = set()
    if os.path.exists(args.output):
        print("[RESUME] Reading existing JSONL...", file=sys.stderr)
        with open(args.output, "r", encoding="utf-8") as f:
            lines = f.readlines()
        last_ok = 0
        for i, ln in enumerate(lines):
            ln = ln.strip()
            if not ln:
                continue
            try:
                rec = json.loads(ln)
                g = str(rec.get("group", ""))
                idx = int(rec.get("passage_index", rec.get("index", 0)))
                completed.add((g, idx))
                last_ok = i + 1
            except Exception:
                break
        if last_ok < len(lines):
            print("[RESUME] Truncating corrupted tail...", file=sys.stderr)
            with open(args.output, "w", encoding="utf-8") as f:
                f.writelines(lines[:last_ok])
        print(f"[RESUME] completed passages in this shard: {len(completed)}", file=sys.stderr)

    remaining = [it for it in assigned if (it.group, it.passage_index) not in completed]
    print(f"[INFO] remaining passages in this shard: {len(remaining)}")

    if not remaining:
        print("[INFO] Nothing to do.")
        return

    # 3) load model only if needed
    model, tokenizer = load_model_and_tokenizer(MODEL_PATH)

    ensure_dir(os.path.dirname(args.output))
    fout = open(args.output, "a", encoding="utf-8")

    start_time = time.time()
    time_limit = args.max_minutes * 60.0

    pbar = tqdm(total=len(remaining), desc="Rephrasing passages", unit="passage")

    for it in remaining:
        if SHOULD_EXIT:
            print("[STOP] SIGTERM break.", file=sys.stderr)
            break

        elapsed = time.time() - start_time
        if elapsed >= time_limit:
            print(f"[STOP] Time limit {args.max_minutes} min reached.", file=sys.stderr)
            break

        try:
            new_text = generate_rewrite(model, tokenizer, it.name, it.passage)
        except Exception as e:
            print(f"[ERROR] generation failed for group={it.group}, passage_index={it.passage_index}: {e}", file=sys.stderr)
            new_text = ""

        rec: Dict[str, Any] = {
            "group": it.group,
            "name": it.name,
            "passage_index": it.passage_index,
            "passage": new_text
        }

        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        safe_fsync(fout)
        pbar.update(1)

    pbar.close()
    fout.close()
    print(f"[DONE] Saved to {args.output}")


if __name__ == "__main__":
    main()
