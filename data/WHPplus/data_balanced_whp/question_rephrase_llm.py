#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate concise factual statements from MCQ items using a local HF LLM.

- Reads input JSON like forget_dedup.json (top-level dict of ids -> list[mcq]).
- For each MCQ {name, question, choices, answer}, asks the model to combine
  the question + selected answer into a single concise statement that preserves
  subject/object/relation (including negation like "NOT").
- Adds lightweight consistency checks; retries a few times if needed.
- Writes a parallel JSON with statements for every item, preserving grouping.

Usage (examples):
  python question_rephrase_llm.py \
      --model-path /rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659 \
      --input forget_dedup.json \
      --output forget_dedup_statement.json \
      --use-answer

  python question_rephrase_llm.py --help
"""
import os
import re
import json
import random
import argparse
from typing import Dict, Any, Tuple

import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# ---------------------- Defaults (can be overridden by CLI) ------------------
DEFAULT_INPUT = "forget_dedup.json"
DEFAULT_OUTPUT = "forget_dedup_statement.json"

# ----------------------------------------------------------------------------

INSTRUCTION = (
    "You are a careful fact rewriter. Combine the MCQ question and the "
    "selected answer into ONE concise factual statement. Preserve the original "
    "meaning exactly, including negation or qualifiers (e.g., 'NOT', dates, "
    "years, counts). Do not add any new information. The statement MUST:\n"
    "1) Include the subject person's full name exactly as given in 'name'.\n"
    "2) Include the chosen answer text verbatim (unless the question demands a rephrasing, "
    "   e.g., a year or place can be inserted directly).\n"
    "3) Be a single standalone sentence.\n"
    "4) Be factual and neutral.\n"
    "Return only the statement, nothing else."
)

SYSTEM_MSG = "You are a precise and concise rewriting assistant."

def pick_choice(entry: Dict[str, Any], use_answer: bool) -> Tuple[str, str]:
    ans_letter = entry.get("answer", "").strip()
    choices = entry.get("choices") or {}
    if use_answer:
        letter = ans_letter
        text = choices.get(letter, "").strip()
        return letter, text
    # random choice among available options
    letters = [k for k in ["A","B","C","D","E"] if k in choices]
    if not letters:
        letters = list(choices.keys())
    letter = random.choice(letters) if letters else ""
    text = choices.get(letter, "").strip() if letter else ""
    return letter, text

def simple_consistency_check(name: str, choice_text: str, statement: str) -> bool:
    """Minimal checks to reduce drift:
    - contains the name (case-insensitive exact substring)
    - contains the choice text (case-insensitive) OR the statement includes all numeric tokens from choice
    - is one sentence and not too long
    """
    s = statement.strip()
    if not s or len(s.split()) < 3:
        return False
    # name present
    if name.lower() not in s.lower():
        return False
    # choice text present OR all numbers from choice are present in statement
    if choice_text:
        if choice_text.lower() in s.lower():
            pass
        else:
            # fallback: if choice contains numbers/years, ensure those appear
            nums_in_choice = re.findall(r"\d{3,4}", choice_text)
            for n in nums_in_choice:
                if n not in s:
                    return False
    # one concise sentence (allow final period)
    if s.count(".") > 1:
        return False
    # keep it reasonably short
    if len(s) > 300:
        return False
    return True

def prompt_and_generate(model, tokenizer, name: str, question: str, choice_letter: str, choice_text: str, max_new_tokens=64) -> str:
    # Prepare a compact JSON-like block to reduce hallucination.
    payload = {
        "name": name,
        "question": question,
        "selected_choice": {
            "letter": choice_letter,
            "text": choice_text
        }
    }
    user_prompt = INSTRUCTION + "\n\n" + json.dumps(payload, ensure_ascii=False, indent=2)

    conversation = [
        {"role": "system", "content": SYSTEM_MSG},
        {"role": "user", "content": user_prompt}
    ]

    input_ids = tokenizer.apply_chat_template(
        conversation,
        add_generation_prompt=True,
        return_tensors="pt"
    ).to(model.device)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            temperature=0.0,
            top_p=1.0,
            pad_token_id=tokenizer.eos_token_id,
        )
    text = tokenizer.decode(out[0][input_ids.shape[1]:], skip_special_tokens=True).strip()
    # keep only first line / sentence
    text = text.split("\n")[0].strip()
    # enforce trailing period
    if text and not re.search(r"[.!?]$", text):
        text += "."
    return text

def load_input(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Accept two formats:
    # 1) dict[id] -> list[mcq, ...]
    # 2) list[mcq, ...]  (wrap under single key "items")
    if isinstance(data, list):
        return {"items": data}
    if isinstance(data, dict):
        return data
    raise ValueError("Unsupported JSON root type; expected dict or list.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", type=str, required=True,
                        help="Local HF model path (e.g., a snapshots/... directory).")
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT,
                        help="Input JSON path (e.g., forget_dedup.json).")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                        help="Output JSON path (e.g., forget_dedup_statement.json).")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--use-answer", action="store_true", help="Use provided correct answer (factual mode).")
    group.add_argument("--random-answer", action="store_true", help="Randomly pick a choice for each item.")
    parser.add_argument("--max-retries", type=int, default=3, help="Regenerate up to N times if check fails.")
    parser.add_argument("--device", type=str, default=None, help="Force device, e.g., cuda:0 or cpu.")
    args = parser.parse_args()

    use_answer = True
    if args.random_answer:
        use_answer = False
    elif args.use_answer:
        use_answer = True

    # Device
    if args.device:
        device = torch.device(args.device)
    else:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    # Load model/tokenizer
    print(f"🔹 Loading model from: {args.model_path}")
    tokenizer = AutoTokenizer.from_pretrained(args.model_path)
    model = AutoModelForCausalLM.from_pretrained(args.model_path, torch_dtype=torch.bfloat16 if device.type=='cuda' else None)
    model.to(device)
    model.eval()
    print(f"Device set to use {device}")

    # Load input
    data = load_input(args.input)

    # Prepare output structure mirroring groups
    out: Dict[str, Any] = {}

    # Count total items for a nice progress bar
    total = sum(len(v) for v in data.values() if isinstance(v, list))
    pbar = tqdm(total=total, desc="Generating factual statements")

    for group_key, items in data.items():
        if not isinstance(items, list):
            # skip non-list values just in case
            continue
        out[group_key] = []
        for entry in items:
            # Be robust to malformed items
            if not isinstance(entry, dict):
                pbar.update(1)
                continue
            name = str(entry.get("name", "")).strip()
            question = str(entry.get("question", "")).strip()
            choices = entry.get("choices", {}) or {}
            if not name or not question or not choices:
                pbar.update(1)
                continue

            letter, choice_text = pick_choice(entry, use_answer)
            statement = ""
            passed = False

            for _ in range(max(1, args.max_retries)):
                try:
                    statement = prompt_and_generate(model, tokenizer, name, question, letter, choice_text)
                except Exception as e:
                    statement = ""

                # very small normalization on quotes/spaces
                statement = statement.replace("  ", " ").strip().strip('"').strip("'")

                # Minimal consistency checks
                if simple_consistency_check(name, choice_text, statement):
                    passed = True
                    break

            # Fallback template if still failing
            if not passed:
                if choice_text:
                    statement = f"{name} — {choice_text}."
                else:
                    # last resort
                    statement = f"{name}."
            # Record
            out_entry = dict(entry)  # copy original
            out_entry["used_answer"] = use_answer
            out_entry["selected_choice"] = {"letter": letter, "text": choice_text}
            out_entry["statement"] = statement
            out[group_key].append(out_entry)
            pbar.update(1)
    pbar.close()

    # Write output
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Saved {args.output}")

if __name__ == "__main__":
    main()
