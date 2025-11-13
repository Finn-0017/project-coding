#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCQ → factual statements for all choices, sharded and resumable.

- Input (WHP forget.json style):

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
      },
      ...
    ],
    "10001": [...]
  }

- For each MCQ, generate a factual statement for EVERY choice.

- Output: JSONL, one line per question, e.g.:

  {
    "group": "10000",
    "name": "Benedetto Varchi",
    "items": [
      {
        "index": 170,
        "question": "...",
        "choices": [
          {
            "letter": "A",
            "text": "It was praised for its honesty.",
            "statement": "Ezra Pound's The Cantos praised Benedetto Varchi's honesty in historical writing."
          },
          {
            "letter": "B",
            "text": "It was considered irrelevant.",
            "statement": "FAILED"
          },
          ...
        ],
        "correct": "A"
      }
    ]
  }

- Sharding:
    --num-shards N, --shard-index i  (this shard processes items[i::N])

- Resume:
    Reads existing output JSONL, collects (group, index) already done for this shard,
    and skips them.

- Two-stage generation per choice:

    1) generate_primary: rewrite question+answer into a statement.
    2) If strong_check fails, generate_refine: ask model to fix the draft and
       explicitly provide REQUIRED_WORDS that must appear.
    3) If still fails, print a [FALLBACK] message to stderr and set statement="FAILED".

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

# Primary instruction: direct rewrite from question+answer to statement
PRIMARY_INSTRUCTION = (
    "Rewrite the question into a factual statement by inserting the given answer.\n"
    "Do not include any question format, do not mention Q or A.\n"
    "Write one short, natural, factual sentence that expresses the answer.\n"
    "Return only the rewritten statement.\n"
)

# Refine instruction: take an initial draft and fix it, with REQUIRED_WORDS
REFINE_INSTRUCTION = (
    "You are fixing a factual statement.\n"
    "You MUST include ALL REQUIRED_WORDS verbatim in the final sentence.\n"
    "OPTIONAL_WORDS may also be included if they fit naturally.\n"
    "Rules:\n"
    "  • Output exactly ONE grammatical factual sentence.\n"
    "  • Mention the ENTITY NAME at least once.\n"
    "  • Do NOT use any question form, and do NOT use 'Q:' or 'A:'.\n"
    "  • Do NOT add new facts not in the QUESTION or ANSWER.\n"
    "  • You may freely adjust word order, grammar, and phrasing.\n"
    "Return only the rewritten sentence.\n"
)

# ==========================================
# Utils & data structures
# ==========================================

def set_seed(seed: int) -> None:
    """Set random seeds for reproducibility."""
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception:
        # NumPy may not be installed; ignore in that case.
        pass

def _to_text(x: Any) -> str:
    """Safely convert an object to string."""
    if isinstance(x, str):
        return x
    try:
        return str(x)
    except Exception:
        return ""

@dataclass
class MCQItem:
    """Flat representation of a single MCQ."""
    group: str
    index: int
    name: str
    question: str
    choices: List[Dict[str, str]]
    correct: str  # single-letter, e.g. "A"

def load_model_and_tokenizer(path: str):
    """Load tokenizer and model, and set pad_token to eos_token if needed."""
    print(f"[INFO] loading model from {path}", file=sys.stderr)
    tokenizer = AutoTokenizer.from_pretrained(path)
    model = AutoModelForCausalLM.from_pretrained(
        path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    # Explicitly set pad_token to eos_token to avoid warnings.
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model.config.pad_token_id = tokenizer.pad_token_id

    model.eval()
    return model, tokenizer

def generate_primary(model, tokenizer, name: str, question: str, answer_text: str) -> str:
    """First-pass generation: rewrite question + answer into a factual statement."""
    prompt = PRIMARY_INSTRUCTION + "\n\n" + json.dumps(
        {
            "name": name,
            "question": question,
            "answer": answer_text,
        },
        ensure_ascii=False,
        indent=2,
    )
    messages = [
        {"role": "system", "content": "You are a precise and concise rewriting assistant."},
        {"role": "user", "content": prompt},
    ]
    input_ids = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        return_tensors="pt",
    ).to(model.device)

    attention_mask = torch.ones_like(input_ids, dtype=torch.long)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )

    text = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()
    if not text.endswith("."):
        text += "."
    # Use only the first line.
    return text.split("\n")[0].strip()

def generate_refine(
    model,
    tokenizer,
    name: str,
    question: str,
    answer_text: str,
    draft_statement: str,
) -> str:
    """
    Second-pass generation: refine a draft statement to fix structure and content,
    and enforce the presence of REQUIRED_WORDS.
    """
    # Extract content words from QUESTION
    q_tokens = [
        t for t in re.findall(r"[A-Za-z']+", question.lower())
        if t not in STOP_WORDS
    ]
    q_tokens = sorted(set(q_tokens))

    # Extract content words from ANSWER (only keep the most important few)
    a_tokens = [
        t for t in re.findall(r"[A-Za-z']+", answer_text.lower())
        if t not in STOP_WORDS
    ]
    a_tokens = sorted(set(a_tokens[:2]))

    # Extract tokens from the entity name
    name_tokens = [
        t for t in re.findall(r"[A-Za-z']+", name.lower())
        if t
    ]

    # Build REQUIRED_WORDS: a small set of key tokens that must appear
    required_tokens = set()
    required_tokens.update(name_tokens)
    required_tokens.update(q_tokens[:4])
    required_tokens.update(a_tokens)

    required_words = sorted(required_tokens)
    optional_words = q_tokens[4:]

    prompt = REFINE_INSTRUCTION + "\n\n" + json.dumps(
        {
            "ENTITY_NAME": name,
            "QUESTION": question,
            "ANSWER": answer_text,
            "DRAFT_STATEMENT": draft_statement,
            "REQUIRED_WORDS": required_words,
            "OPTIONAL_WORDS": optional_words,
        },
        ensure_ascii=False,
        indent=2,
    )

    messages = [
        {"role": "system", "content": "You are a precise factual rewriting assistant."},
        {"role": "user", "content": prompt},
    ]

    input_ids = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        return_tensors="pt",
    ).to(model.device)

    attention_mask = torch.ones_like(input_ids, dtype=torch.long)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )

    text = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()
    if not text.endswith("."):
        text += "."
    return text.split("\n")[0].strip()

# Wh-words and simple stop-word set used by strong_check.
WH_WORDS = {
    "what", "where", "when", "why", "which", "who", "how", "whose", "whom",
}
STOP_WORDS = WH_WORDS | {
    "is", "was", "were", "are", "am", "do", "does", "did",
    "a", "an", "the", "in", "on", "of", "for", "to", "from",
    "and", "or", "with", "about", "into", "by", "at", "as",
    "that", "be", "been", "being",
}

def strong_check(name: str, question: str, choice_text: str, statement: str) -> bool:
    """
    Structural / consistency check focused on preserving QUESTION content.

    Goals:
      - Ensure the statement is a non-trivial sentence.
      - Ensure the ENTITY NAME appears.
      - Ensure that most content words from the QUESTION are preserved.
      - Ensure numeric information from the ANSWER (e.g. years) is not lost.

    We deliberately do NOT require all content words from the ANSWER to appear,
    because choices can be long and paraphrased heavily.
    """
    name = _to_text(name)
    question = _to_text(question)
    choice_text = _to_text(choice_text)
    s = _to_text(statement)

    if not s or len(s.split()) < 3:
        # Too short or empty → reject.
        return False

    s_lower = s.lower()

    # 1) Name coverage: require that the statement mentions the entity name.
    if name and name.strip():
        # We only do a simple substring check on the lowercase name.
        if name.lower() not in s_lower:
            return False

    # 2) QUESTION content preservation.
    #    Extract content tokens (non-stopwords) from the question and check that
    #    a reasonable fraction of them appear in the statement.
    q_tokens = [
        t for t in re.findall(r"[A-Za-z']+", question.lower())
        if t not in STOP_WORDS
    ]
    if q_tokens:
        q_unique = sorted(set(q_tokens))
        present = [t for t in q_unique if t in s_lower]
        # Require at least 60% of content tokens from the question to be present.
        if len(present) / len(q_unique) < 0.6:
            return False

    # 3) ANSWER numeric consistency (years, counts, etc.).
    #    Only enforce that 3–4 digit numbers in the answer appear in the statement.
    nums = re.findall(r"\d{3,4}", choice_text)
    if nums:
        for n in nums:
            if n not in s:
                return False

    # We intentionally ignore non-numeric answer tokens here to avoid rejecting
    # good paraphrases when choices are long.
    return True

def ensure_dir(path: str) -> None:
    """Create directory if it does not exist."""
    if path:
        os.makedirs(path, exist_ok=True)

def safe_fsync(f) -> None:
    """Flush and fsync a file object, ignoring OS-level errors."""
    try:
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        pass

def load_json(path: str) -> Any:
    """Load JSON file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_items(path: str) -> List[MCQItem]:
    """
    Flatten WHP forget.json style into a list of MCQItem.

    Primary expected format:

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
          },
          ...
        ],
        "10001": [...]
      }

    Fallback: also support older list-of-groups schema for compatibility.
    """
    raw = load_json(path)
    items: List[MCQItem] = []

    # Case 1: dict of group_id -> list[MCQ dict]
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
    """Parse command-line arguments."""
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

def handle_sigterm(signum, frame) -> None:
    """SIGTERM handler: ask main loop to stop after current question."""
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

    # Load and flatten all items, then assign shard.
    all_items = flatten_items(args.input)
    all_items = sorted(all_items, key=lambda x: (x.group, x.index))

    assigned = all_items[args.shard_index :: args.num_shards]
    print(f"[INFO] total items = {len(all_items)}, assigned to this shard = {len(assigned)}")

    # Resume from existing output: collect completed (group, index).
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
                # Stop at first malformed line and truncate.
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

    # Load model and tokenizer only if there is work to do.
    model, tokenizer = load_model_and_tokenizer(MODEL_PATH)

    ensure_dir(os.path.dirname(args.output))
    fout = open(args.output, "a", encoding="utf-8")

    start_time = time.time()
    time_limit = args.max_minutes * 60.0

    fallback_count = 0

    pbar = tqdm(total=len(remaining), desc="MCQ->statements", unit="question")
    for it in remaining:
        if SHOULD_EXIT:
            print("[STOP] SIGTERM break.", file=sys.stderr)
            break

        elapsed = time.time() - start_time
        if elapsed >= time_limit:
            print(f"[STOP] Time limit {args.max_minutes} min reached.", file=sys.stderr)
            break

        choice_entries: List[Dict[str, Any]] = []

        for ch in it.choices:
            letter = _to_text(ch.get("letter", "")).upper()
            text = _to_text(ch.get("text", ""))
            if not letter:
                continue

            # 1) Primary generation
            try:
                s = generate_primary(model, tokenizer, it.name, it.question, text)
            except Exception:
                s = ""

            # 2) If primary fails strong_check, try refine once.
            if not strong_check(it.name, it.question, text, s):
                try:
                    s_ref = generate_refine(
                        model,
                        tokenizer,
                        it.name,
                        it.question,
                        text,
                        s,
                    )
                except Exception:
                    s_ref = ""

                if strong_check(it.name, it.question, text, s_ref):
                    s = s_ref
                else:
                    # 3) Both primary and refine failed → record fallback.
                    print(
                        f"[FALLBACK] FAILED for group={it.group}, index={it.index}, option={letter}",
                        file=sys.stderr,
                    )
                    s = "FAILED"
                    fallback_count += 1

            choice_entries.append(
                {
                    "letter": letter,
                    "text": text,
                    "statement": _to_text(s),
                }
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
    print(f"[INFO] Fallback count in this shard: {fallback_count}", file=sys.stderr)


if __name__ == "__main__":
    main()
