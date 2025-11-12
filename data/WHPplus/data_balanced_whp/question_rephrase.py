#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate concise factual statements from MCQs using a local Llama model.
Supports item-level sharding across multiple GPUs for better load balance.

Key features:
- Stream output to JSONL (one record per line) to avoid large in-memory dicts.
- Flush + fsync after every record (very robust against unexpected kills).
- Auto-resume from existing JSONL (skips already-completed items; truncates partial last line).
- Create checkpoint flag files at 10%, 20%, ... of the remaining work (useful for monitoring).
- Graceful SIGTERM handling to flush+sync before exiting.

Example (single GPU, whole dataset):
    python question_rephrase_parallel.py

Example (4 GPUs):
    CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --num-shards 4 --shard-index 0 --output shard_0.jsonl &
    CUDA_VISIBLE_DEVICES=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --num-shards 4 --shard-index 1 --output shard_1.jsonl &
    CUDA_VISIBLE_DEVICES=2 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --num-shards 4 --shard-index 2 --output shard_2.jsonl &
    CUDA_VISIBLE_DEVICES=3 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --num-shards 4 --shard-index 3 --output shard_3.jsonl &
    wait

After generation, merge JSONL files if needed (e.g., concatenate).
"""

import os
import re
import json
import random
import sys
import signal
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# ================= CONFIG =================
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget.json"
OUTPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget_statement.jsonl"  # recommend .jsonl
MAX_NEW_TOKENS = 64
MAX_RETRIES = 3
SEED = 1234
# ==========================================

INSTRUCTION = (
    "You are a careful fact rewriter. Combine the MCQ question and the selected answer "
    "into ONE concise factual statement. Preserve the original meaning exactly, including "
    "negation or qualifiers like 'NOT', dates, years, or counts. Do not add any new information. "
    "Return only the rewritten factual statement."
)

def set_seed(seed: int):
    """Set random seeds for reproducibility."""
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception:
        pass
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

def pick_choice(entry):
    """
    Always return the correct answer (letter, text).
    """
    choices = entry.get("choices", {}) or {}
    ans = (entry.get("answer") or "").strip()
    return ans, choices[ans]


def _to_text(x):
    """Normalize any value to a string (handles int/float/bool/None)."""
    if x is None:
        return ""
    return x if isinstance(x, str) else str(x)

def simple_check(name, choice_text, s):
    """
    Basic sanity check for a generated statement:
    - Non-empty and contains > 2 words.
    - Mentions the entity name if provided.
    - If the choice has a 3-4 digit number (likely a year), it must appear in the statement.
      Otherwise, the choice text (case-insensitive) should be a substring.
    """
    name = _to_text(name)
    choice_text = _to_text(choice_text)
    s = _to_text(s)

    if not choice_text.strip() or not s or not s.strip():
        return False
    if len(s.split()) < 3:
        return False
    if name and name.lower() not in s.lower():
        return False

    if choice_text and choice_text.strip():
        nums = re.findall(r"\d{3,4}", choice_text)
        if nums:
            for n in nums:
                if n not in s:
                    return False
        else:
            if choice_text.lower() not in s.lower():
                return False
    return True

def generate(model, tokenizer, name, question, choice_letter, choice_text):
    """
    Generate one factual statement by prompting the Llama model (deterministic generation).
    """
    prompt = INSTRUCTION + "\n\n" + json.dumps({
        "name": name,
        "question": question,
        "selected_choice": {"letter": choice_letter, "text": choice_text}
    }, ensure_ascii=False, indent=2)

    messages = [
        {"role": "system", "content": "You are a precise and concise rewriting assistant."},
        {"role": "user", "content": prompt}
    ]
    input_ids = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(model.device)

    # Provide a simple attention_mask (safe even if pad/eos are the same)
    attention_mask = torch.ones_like(input_ids, dtype=torch.long, device=model.device)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,  # deterministic
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )
    text = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()
    if not text.endswith("."):
        text += "."
    return text.split("\n")[0].strip()

def load_json(path):
    """Load input JSON file (either dict or list). Wrap list as {'items': list} for uniformity."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {"items": data}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=INPUT_PATH, help="Input JSON path.")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH, help="Output JSON/JSONL path.")
    parser.add_argument("--num-shards", type=int, default=1, help="Total number of shards (processes/GPUs).")
    parser.add_argument("--shard-index", type=int, default=0, help="This shard index in [0, num_shards-1].")
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed for reproducibility.")
    args = parser.parse_args()

    set_seed(args.seed)
    num_shards = max(1, args.num_shards)
    shard_index = max(0, args.shard_index)

    # Select device (controlled externally via CUDA_VISIBLE_DEVICES)
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Load model and tokenizer
    print(f"Loading model from: {MODEL_PATH}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        torch_dtype=torch.bfloat16 if device.type == "cuda" else None
    ).to(device).eval()

    # Flatten dataset into (group_name, index_in_group, entry_dict)
    raw = load_json(args.input)
    flat_items = []
    for g, items in raw.items():
        if not isinstance(items, list):
            continue
        for i, entry in enumerate(items):
            if isinstance(entry, dict):
                flat_items.append((g, i, entry))

    # Assign items to this shard
    assigned = [t for i, t in enumerate(flat_items) if (i % num_shards) == shard_index]
    original_total = len(assigned)
    print(f"Total items: {len(flat_items)} | Shard {shard_index}/{num_shards} → {original_total} items")

    # ===== JSONL streaming output + 10% checkpoints + AUTO-RESUME =====
    save_path = args.output
    if not save_path.endswith(".jsonl"):
        save_path = args.output + ".jsonl"  # enforce JSONL extension

    # ---- AUTO-RESUME: count already-finished valid JSON lines, truncate partial tail if any ----
    already = 0
    if os.path.exists(save_path):
        with open(save_path, "r", encoding="utf-8") as fin:
            lines = fin.readlines()

        # Count valid JSON lines from the top; stop at first invalid/partial
        for ln in lines:
            if not ln.strip():
                continue
            try:
                json.loads(ln)
                already += 1
            except Exception:
                break

        # If the last line is partial/corrupted, truncate file back to last valid line
        if already < len([ln for ln in lines if ln.strip()]):
            with open(save_path, "w", encoding="utf-8") as fout_fix:
                fout_fix.writelines(lines[:already])
            print(f"[RESUME] Truncated {save_path} to {already} valid lines.")

        if already > 0:
            print(f"[RESUME] Found {already} completed records in {save_path}.")
            # Skip first `already` items in this shard and continue with the remainder
            assigned = assigned[already:]

    total = len(assigned)
    if total == 0:
        print(f"[RESUME] Nothing to do for shard {shard_index}. All items already saved in {save_path}.")
        return

    # Open output file in append mode (safe for resume)
    fout = open(save_path, "a", encoding="utf-8")

    def write_one(rec: dict):
        """Write one record to JSONL and flush immediately (very robust)."""
        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        fout.flush()
        # fsync ensures durability even if the job is killed abruptly
        os.fsync(fout.fileno())

    def on_term(signum, frame):
        """Flush+sync on SIGTERM (Slurm kill) before exiting."""
        try:
            fout.flush()
            os.fsync(fout.fileno())
            fout.close()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)

    # Prepare progress
    pbar = tqdm(total=total, desc=f"Shard {shard_index}")
    processed = 0

    # ===== Main generation loop (streaming) =====
    for g, idx, entry in assigned:
        try:
            # Extract and normalize fields
            name = _to_text(entry.get("name", "")).strip()
            question = _to_text(entry.get("question", "")).strip()
            choices = entry.get("choices", {}) or {}

            # Skip incomplete entries
            if not (name and question and choices):
                processed += 1
                pbar.update(1)
                continue

            # 这里已经不再有 use_answer 了，直接用正确答案
            letter, choice_text = pick_choice(entry)
            statement, ok = "", False

            # Try generating up to MAX_RETRIES until it passes the sanity check
            for _ in range(MAX_RETRIES):
                try:
                    statement = generate(model, tokenizer, name, question, letter, choice_text)
                except Exception:
                    statement = ""
                if simple_check(name, choice_text, statement):
                    ok = True
                    break

            # Fallback if generation keeps failing
            if not ok:
                statement = f"{name} — {_to_text(choice_text)}."

            # Build and immediately persist the record
            rec = {
                "group": g,
                "index": idx,
                "name": name,
                "question": question,
                "choice": {"letter": letter, "text": _to_text(choice_text)},
                "statement": _to_text(statement)
            }
            write_one(rec)

            processed += 1
            pbar.update(1)

        except Exception as e:
            # Log and skip bad samples — never crash the whole shard/run
            print(f"[WARN] skip idx={idx} group={g} err={e}", file=sys.stderr)
            processed += 1
            pbar.update(1)
            continue

    pbar.close()

    # Final flush and close
    fout.flush()
    os.fsync(fout.fileno())
    fout.close()
    print(f"Shard {shard_index} done. Saved JSONL to {save_path}")

if __name__ == "__main__":
    main()
