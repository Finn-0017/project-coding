#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate concise factual statements from MCQs using a local Llama model.
Supports item-level sharding across multiple GPUs for better load balance.

Key changes in this version:
- Stream output to JSONL (one record per line) to avoid large in-memory dicts.
- Flush to disk after every record (very robust against unexpected kills).
- Create checkpoint flag files at 10%, 20%, ... progress (useful for monitoring / resume).
- Graceful SIGTERM handling to flush+sync before exiting.

Example (single GPU, whole dataset):
    python question_rephrase_parallel.py --use-answer

Example (4 GPUs):
    CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --use-answer --num-shards 4 --shard-index 0 --output shard_0.jsonl &
    CUDA_VISIBLE_DEVICES=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --use-answer --num-shards 4 --shard-index 1 --output shard_1.jsonl &
    CUDA_VISIBLE_DEVICES=2 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --use-answer --num-shards 4 --shard-index 2 --output shard_2.jsonl &
    CUDA_VISIBLE_DEVICES=3 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    python question_rephrase_parallel.py --use-answer --num-shards 4 --shard-index 3 --output shard_3.jsonl &
    wait

After generation, merge JSONL files if needed (e.g., concatenate).
"""

import os
import re
import json
import random
import torch
import sys, signal, gc, time
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# ================= CONFIG =================
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget_dedup.json"
OUTPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget_dedup_statement.jsonl"  # recommend .jsonl
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

def pick_choice(entry, use_answer):
    """
    Select which choice to use.
    - If `use_answer` is True, always use the correct answer.
    - Otherwise, randomly pick a wrong choice if possible.
    """
    choices = entry.get("choices", {}) or {}
    ans = (entry.get("answer") or "").strip()
    if use_answer and ans in choices:
        return ans, choices[ans]

    pool = [k for k in choices.keys() if k != ans]
    letter = random.choice(pool) if pool else (ans if ans in choices else (next(iter(choices.keys())) if choices else ""))
    text = choices.get(letter, "")
    return letter, text

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
    Generate one factual statement by prompting the Llama model.
    The generation is deterministic (do_sample=False).
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
            do_sample=False,  # deterministic output
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
    parser.add_argument("--use-answer", action="store_true", help="Use correct answer instead of random choice.")
    parser.add_argument("--input", type=str, default=INPUT_PATH, help="Input JSON path.")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH, help="Output JSON/JSONL path.")
    parser.add_argument("--num-shards", type=int, default=1, help="Total number of shards (processes/GPUs).")
    parser.add_argument("--shard-index", type=int, default=0, help="This shard index in [0, num_shards-1].")
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed for reproducibility.")
    args = parser.parse_args()

    set_seed(args.seed)

    use_answer = args.use_answer
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
    total = len(assigned)
    print(f"Total items: {len(flat_items)} | Shard {shard_index}/{num_shards} → {total} items")

    # ===== JSONL streaming output + 10% checkpoints =====
    save_path = args.output
    if not save_path.endswith(".jsonl"):
        save_path = args.output + ".jsonl"  # convert to JSONL automatically

    # Open output file in append mode: safe for re-runs or partial resumes.
    fout = open(save_path, "a", encoding="utf-8")

    def write_one(rec: dict):
        """Write one record to JSONL and flush immediately (very robust)."""
        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        fout.flush()
        # fsync makes it durable even if the job is killed abruptly.
        os.fsync(fout.fileno())

    def on_term(signum, frame):
        """Ensure buffers are flushed and file is closed on SIGTERM (Slurm kill)."""
        try:
            fout.flush()
            os.fsync(fout.fileno())
            fout.close()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)

    # Prepare progress / checkpointing
    ckpt_step = max(1, total // 10)  # 10% checkpoints
    next_ckpt = ckpt_step
    start_ts = time.time()
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

            letter, choice_text = pick_choice(entry, use_answer)
            statement, ok = "", False

            # Retry generation until it passes the sanity check
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

            # 10% checkpoints: create small flag files and run GC to keep memory stable
            if processed >= next_ckpt:
                pct = int(round(processed * 100.0 / total))
                ckpt_flag = f"{os.path.splitext(save_path)[0]}_checkpoint_{pct}pct.done"
                with open(ckpt_flag, "w") as fc:
                    fc.write(f"processed={processed}/{total}\n")
                    fc.write(f"elapsed={time.time() - start_ts:.1f}s\n")
                next_ckpt += ckpt_step
                gc.collect()

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
