#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCQ → factual statements for all choices, sharded and resumable.

- Reads WHP forget.json style:
  {
    "10000": [
      {
        "answer": "D",
        "question": "...",
        "name": "...",
        "choices": {
          "A": "...",
          "B": "...",
          ...
        }
      ],
      ...
    ],
    "10001": [...]
  }

- For each MCQ, generate a natural factual statement for EVERY choice.

- Output JSONL, one line per question:
  {
    "group": "10000",
    "name": "Benedetto Varchi",
    "items": [
      {
        "index": 10,
        "question": "...",
        "choices": [
          {"letter": "A", "text": "Florence", "statement": "Benedetto Varchi was born in Florence."},
          ...
        ],
        "correct": "A"
      }
    ]
  }

- Sharding:
  --num-shards N, --shard-index i  (take items[i::N])

- Resume:
  Reads existing output JSONL, collects (group, index) already done for this shard,
  and skips them.

- Time limit:
  --max-minutes M  (soft limit; stops after current question if exceeded).
"""

import argparse
import json
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Set

import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

# ==========================================
# Config
# ==========================================

MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 64
DEFAULT_SEED = 1234
DEFAULT_MAX_MINUTES = 10  # soft limit in minutes

# Instruction tuned for natural rewrite
INSTRUCTION = (
    "Rewrite the question into a factual statement by replacing the wh-question with the given answer.\n"
    "Do not include any question format, do not mention Q or A.\n"
    "Write a clean, natural, factual sentence that expresses the answer.\n"
    "Return only the rewritten statement.\n"
)

# ==========================================
# Utils & data structures
# ==========================================

def set_seed(seed: int):
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception:
        pass

def _to_text(x: Any) -> str:
    if isinstance(x, str):
        return x
    try:
        return str(x)
    except Exception:
        return ""

@dataclass
class MCQItem:
    group: str
    index: int
    name: str
    question: str
    choices: List[Dict[str, str]]
    correct: str  # single-letter, e.g. "A"

def load_model_and_tokenizer(path: str):
    print(f"[INFO] loading model from {path}", file=sys.stderr)
    tok = AutoTokenizer.from_pretrained(path)
    model = AutoModelForCausalLM.from_pretrained(
        path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    # explicitly set pad_token to eos_token to avoid warnings
    if tok.pad_token_id is None:
        tok.pad_token = tok.eos_token
    model.config.pad_token_id = tok.pad_token_id

    model.eval()
    return model, tok

def build_prompt(name: str, question: str, answer_text: str) -> str:
    """
    Build the user prompt for the rewrite task.
    We include name, question and answer, but the instruction already describes the task.
    """
    return INSTRUCTION + "\n\n" + json.dumps(
        {
            "name": name,
            "question": question,
            "answer": answer_text,
        },
        ensure_ascii=False,
        indent=2,
    )

def generate(model, tok, name, question, answer_text):
    """One-shot generation for speed."""
    prompt = build_prompt(name, question, answer_text)
    messages = [
        {"role": "system", "content": "You are a precise and concise rewriting assistant."},
        {"role": "user", "content": prompt},
    ]
    ids = tok.apply_chat_template(
        messages, add_generation_prompt=True, return_tensors="pt"
    ).to(model.device)
    mask = torch.ones_like(ids, dtype=torch.long)

    with torch.no_grad():
        out = model.generate(
            ids,
            attention_mask=mask,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            eos_token_id=tok.eos_token_id,
            pad_token_id=tok.pad_token_id,
        )
    t = tok.decode(out[0][ids.size(1):], skip_special_tokens=True).strip()
    if not t.endswith("."):
        t += "."
    # we only want the first line
    return t.split("\n")[0].strip()

WH_WORDS = {
    "what","where","when","why","which","who","how","whose","whom"
}
STOP_WORDS = WH_WORDS | {
    "is","was","were","are","am","do","does","did",
    "a","an","the","in","on","of","for","to","from",
    "and","or","with","about","into","by","at","as",
    "that","be","been","being"
}

def strong_check(name, question, choice_text, s) -> bool:
    """
    Lightweight structure / consistency check:
    - non-empty, at least 3 words
    - should mention the entity name if present
    - should contain answer key tokens (numbers or some content words)
    We DO NOT strictly enforce question token preservation anymore,
    to avoid over-rejecting good rewrites.
    """
    name = _to_text(name)
    question = _to_text(question)
    choice_text = _to_text(choice_text)
    s = _to_text(s)

    if not s or len(s.split()) < 3:
        return False

    s_lower = s.lower()

    # Name presence (softly required)
    if name and name.strip():
        if name.lower() not in s_lower:
            return False

    # Answer consistency: numbers or content tokens
    if choice_text and choice_text.strip():
        nums = re.findall(r"\d{3,4}", choice_text)
        if nums:
            for n in nums:
                if n not in s:
                    return False
        else:
            c_tokens = [
                t for t in re.findall(r"[A-Za-z']+", choice_text.lower())
                if t not in STOP_WORDS
            ]
            if c_tokens and not any(t in s_lower for t in set(c_tokens)):
                return False

    return True

def ensure_dir(path: str):
    if path:
        os.makedirs(path, exist_ok=True)

def safe_fsync(f):
    try:
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        pass

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_items(path: str) -> List[MCQItem]:
    """
    Flatten WHP forget.json style into a list of MCQItem.
    Expected primary format:
      {
        "10000": [
          {
            "answer": "D",
            "question": "...",
            "name": "...",
            "choices": {
              "A": "...", "B": "...", ...
            }
          },
          ...
        ],
        "10001": [...]
      }

    Also keeps a fallback path for the older list-of-groups schema (for compatibility).
    """
    raw = load_json(path)
    items: List[MCQItem] = []

    # Case 1: dict of group_id -> list[question dict]
    if isinstance(raw, dict) and all(isinstance(v, list) for v in raw.values()):
        for group_id, q_list in raw.items():
            for idx, q in enumerate(q_list):
                name = _to_text(q.get("name", ""))
                question = _to_text(q.get("question", ""))
                answer_letter = _to_text(q.get("answer", "")).upper()
                choices_dict = q.get("choices", {}) or {}
                choice_list = [
                    {"letter": str(letter), "text": _to_text(text)}
                    for letter, text in sorted(choices_dict.items())
                ]
                items.append(
                    MCQItem(
                        group=_to_text(group_id),
                        index=idx,
                        name=name,
                        question=question,
                        choices=choice_list,
                        correct=answer_letter,
                    )
                )
        return items

    # Case 2: legacy format (list of groups -> items)
    if isinstance(raw, dict):
        groups = raw.get("groups") or raw.get("data") or []
    else:
        groups = raw

    for g in groups:
        group_id = _to_text(g.get("group", ""))
        lst = g.get("items", [])
        for it in lst:
            idx = int(it.get("index", 0))
            name = _to_text(it.get("name", ""))
            question = _to_text(it.get("question", ""))
            choices = it.get("choices", [])
            correct = _to_text(it.get("correct", "")).upper()
            items.append(
                MCQItem(
                    group=group_id,
                    index=idx,
                    name=name,
                    question=question,
                    choices=choices,
                    correct=correct,
                )
            )

    return items

# ==========================================
# Argparse
# ==========================================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, required=True, help="Input MCQ JSON file.")
    ap.add_argument("--output", type=str, required=True, help="Output JSONL file for this shard.")
    ap.add_argument("--num-shards", type=int, default=1, help="Total number of shards.")
    ap.add_argument("--shard-index", type=int, default=0, help="Index of this shard (0-based).")
    ap.add_argument("--max-minutes", type=float, default=DEFAULT_MAX_MINUTES, help="Soft time limit in minutes.")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed.")
    return ap.parse_args()

# ==========================================
# Main
# ==========================================

SHOULD_EXIT = False

def handle_sigterm(signum, frame):
    global SHOULD_EXIT
    SHOULD_EXIT = True
    print("[SIGNAL] SIGTERM received; will exit after this question.", file=sys.stderr)

def main():
    global SHOULD_EXIT
    args = parse_args()
    set_seed(args.seed)
    signal.signal(signal.SIGTERM, handle_sigterm)

    print(f"[INFO] input = {args.input}")
    print(f"[INFO] output = {args.output}")
    print(f"[INFO] shard_index = {args.shard_index}, num_shards = {args.num_shards}")

    if args.shard_index < 0 or args.shard_index >= args.num_shards:
        raise ValueError("Invalid shard_index / num_shards combination.")

    all_items = flatten_items(args.input)
    all_items = sorted(all_items, key=lambda x: (x.group, x.index))

    # shard: take every k-th item
    assigned = all_items[args.shard_index :: args.num_shards]
    print(f"[INFO] total items = {len(all_items)}, assigned to this shard = {len(assigned)}")

    # resume from existing output
    completed: Set[Tuple[str, int]] = set()
    if os.path.exists(args.output):
        print("[RESUME] Reading existing JSONL...")
        with open(args.output, "r", encoding="utf-8") as f:
            lines = f.readlines()
        last_ok = 0
        for i, ln in enumerate(lines):
            ln = ln.strip()
            if not ln:
                continue
            try:
                rec = json.loads(ln)
                g = rec.get("group", "")
                for it in rec.get("items", []):
                    completed.add((g, int(it.get("index", 0))))
                last_ok = i + 1
            except Exception:
                break
        if last_ok < len(lines):
            print("[RESUME] Truncating corrupted tail...", file=sys.stderr)
            with open(args.output, "w", encoding="utf-8") as f:
                f.writelines(lines[:last_ok])
        print(f"[RESUME] Completed questions in this shard: {len(completed)}")

    remaining = [it for it in assigned if (it.group, it.index) not in completed]
    print(f"[INFO] Remaining questions in this shard: {len(remaining)}")

    if not remaining:
        print("[INFO] Nothing to do.")
        return

    # load model
    model, tok = load_model_and_tokenizer(MODEL_PATH)

    ensure_dir(os.path.dirname(args.output))
    fout = open(args.output, "a", encoding="utf-8")

    start = time.time()
    limit = args.max_minutes * 60.0

    pbar = tqdm(total=len(remaining), desc="MCQ->statements", unit="question")
    for it in remaining:
        if SHOULD_EXIT:
            print("[STOP] SIGTERM break.", file=sys.stderr)
            break
        if time.time() - start >= limit:
            print(f"[STOP] Time limit {args.max_minutes} min reached.", file=sys.stderr)
            break

        choice_entries: List[Dict[str, Any]] = []
        for ch in it.choices:
            letter = _to_text(ch.get("letter", "")).upper()
            text = _to_text(ch.get("text", ""))
            if not letter:
                continue

            # generate statement
            try:
                s = generate(model, tok, it.name, it.question, text)
            except Exception:
                s = ""

            if not strong_check(it.name, it.question, text, s):
                # fallback: still natural, no Q/A
                s = f"{it.name}: {text}."
            choice_entries.append(
                {"letter": letter, "text": text, "statement": _to_text(s)}
            )

        rec = {
            "group": it.group,
            "name": it.name,
            "items": [
                {
                    "index": it.index,
                    "question": it.question,
                    "choices": choice_entries,
                    "correct": it.correct,
                }
            ],
        }
        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        safe_fsync(fout)
        pbar.update(1)

    pbar.close()
    fout.close()
    print(f"[DONE] Saved to {args.output}")

if __name__ == "__main__":
    main()
