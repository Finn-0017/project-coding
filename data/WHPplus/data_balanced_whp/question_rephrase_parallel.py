#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate concise factual statements from MCQs using a local Llama model.
Supports item-level sharding across multiple GPUs for better load balance.

Example (4 GPUs):
CUDA_VISIBLE_DEVICES=0 python generate_statements_items.py --use-answer --num-shards 4 --shard-index 0 --output shard_0.json &
CUDA_VISIBLE_DEVICES=1 python generate_statements_items.py --use-answer --num-shards 4 --shard-index 1 --output shard_1.json &
CUDA_VISIBLE_DEVICES=2 python generate_statements_items.py --use-answer --num-shards 4 --shard-index 2 --output shard_2.json &
CUDA_VISIBLE_DEVICES=3 python generate_statements_items.py --use-answer --num-shards 4 --shard-index 3 --output shard_3.json &
wait

After generation, merge all shards into one final JSON file.
"""

import os
import re
import json
import random
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# ================= CONFIG =================
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_PATH = "forget_dedup.json"
OUTPUT_PATH = "forget_dedup_statement.json"
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

def simple_check(name, choice_text, s):
    """
    Basic sanity check for generated statement:
    - Must not be empty and contain >2 words.
    - Must mention the entity name.
    - Must contain relevant numeric/year info or keyword from the choice.
    """
    if not s or not s.strip():
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
    """Load input JSON file (either dict or list)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {"items": data}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-answer", action="store_true", help="Use correct answer instead of random choice.")
    parser.add_argument("--input", type=str, default=INPUT_PATH, help="Input JSON path.")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH, help="Output JSON path for THIS SHARD.")
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
    print(f"Total items: {len(flat_items)} | Shard {shard_index}/{num_shards} → {len(assigned)} items")

    out = {}
    pbar = tqdm(total=len(assigned), desc=f"Shard {shard_index}")

    # Main generation loop
    for g, idx, entry in assigned:
        name = (entry.get("name") or "").strip()
        question = (entry.get("question") or "").strip()
        choices = entry.get("choices", {}) or {}
        if not (name and question and choices):
            pbar.update(1)
            continue

        letter, choice_text = pick_choice(entry, use_answer)
        statement, ok = "", False

        # Retry a few times until the statement passes the sanity check
        for _ in range(MAX_RETRIES):
            try:
                statement = generate(model, tokenizer, name, question, letter, choice_text)
            except Exception:
                statement = ""
            if simple_check(name, choice_text, statement):
                ok = True
                break

        # Fallback if generation failed
        if not ok:
            statement = f"{name} — {choice_text}."

        out.setdefault(g, []).append({
            "question": question,
            "choice": {"letter": letter, "text": choice_text},
            "statement": statement
        })
        pbar.update(1)

    pbar.close()

    # Save this shard's output
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"Shard {shard_index} done. Results saved to {args.output}")

if __name__ == "__main__":
    main()
