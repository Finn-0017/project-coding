#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate concise factual statements from MCQs using local Llama model.
Simplified output: each item includes only question, choice, and statement.
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
INPUT_PATH = "forget_dedup_sample50.json"
OUTPUT_PATH = "forget_dedup_statement.json"
MAX_NEW_TOKENS = 64
MAX_RETRIES = 3
# ==========================================

INSTRUCTION = (
    "You are a careful fact rewriter. Combine the MCQ question and the selected answer "
    "into ONE concise factual statement. Preserve the original meaning exactly, including "
    "negation or qualifiers like 'NOT', dates, years, or counts. Do not add any new information. "
    "Return only the rewritten factual statement."
)

def pick_choice(entry, use_answer):
    choices = entry.get("choices", {})
    ans = entry.get("answer", "").strip()
    if use_answer and ans in choices:
        return ans, choices[ans]
    pool = list(choices.keys())
    if ans in pool:
        pool.remove(ans)
    letter = random.choice(pool) if pool else ans
    return letter, choices.get(letter, "")

def simple_check(name, choice_text, s):
    if not s.strip() or len(s.split()) < 3:
        return False
    if name.lower() not in s.lower():
        return False
    if choice_text and choice_text.lower() not in s.lower():
        nums = re.findall(r"\d{3,4}", choice_text)
        for n in nums:
            if n not in s:
                return False
    return True

def generate(model, tokenizer, name, question, choice_letter, choice_text):
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
            do_sample=False,                         # deterministic
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id      # now set, warning disappears
        )
    text = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()
    if not text.endswith("."):
        text += "."
    return text.split("\n")[0].strip()

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {"items": data}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-answer", action="store_true", help="Use correct answer instead of random choice.")
    parser.add_argument("--input", type=str, default=INPUT_PATH,
                        help="Input JSON path (e.g., forget_dedup.json).")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH,
                        help="Output JSON path (e.g., forget_dedup_statement.json).")
    args = parser.parse_args()
    use_answer = args.use_answer

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    print(f"Loading model from: {MODEL_PATH}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH, torch_dtype=torch.bfloat16 if device.type=="cuda" else None
    ).to(device).eval()
    print(f"Device: {device}")

    data = load_json(INPUT_PATH)
    out = {}
    total = sum(len(v) for v in data.values() if isinstance(v, list))
    pbar = tqdm(total=total, desc="Generating factual statements")

    for group, items in data.items():
        if not isinstance(items, list):
            continue
        out[group] = []
        for entry in items:
            if not isinstance(entry, dict):
                pbar.update(1)
                continue
            name, question = entry.get("name","").strip(), entry.get("question","").strip()
            choices = entry.get("choices", {})
            if not (name and question and choices):
                pbar.update(1)
                continue
            letter, choice_text = pick_choice(entry, use_answer)
            statement, ok = "", False
            for _ in range(MAX_RETRIES):
                try:
                    statement = generate(model, tokenizer, name, question, letter, choice_text)
                except Exception:
                    statement = ""
                if simple_check(name, choice_text, statement):
                    ok = True
                    break
            if not ok:
                statement = f"{name} — {choice_text}."
            out[group].append({
                "question": question,
                "choice": {"letter": letter, "text": choice_text},
                "statement": statement
            })
            pbar.update(1)
    pbar.close()

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"Done. Results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
