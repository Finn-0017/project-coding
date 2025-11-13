#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCQ → factual statements for all choices.
No checkpoint flag files. Supports resume. 25-min soft limit.
Fixed paths for MODEL_PATH / INPUT_PATH / OUTPUT_PATH.
"""

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
# Fixed paths
# ==========================================

MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget.json"
OUTPUT_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/shard_0.jsonl"

# ==========================================
# Config
# ==========================================

MAX_NEW_TOKENS = 64
TEMPERATURE = 0.0
DEFAULT_SEED = 1234
MAX_MINUTES = 25  # soft limit
# ==========================================

INSTRUCTION = (
    "You are a careful fact rewriter.\n"
    "You will be given a NAME, a QUESTION, and the selected ANSWER.\n"
    "The QUESTION always starts with one of: what, where, when, why, which, who, how, whose, whom.\n"
    "Your task: turn QUESTION + ANSWER into ONE factual statement.\n"
    "• Keep all structure except the wh-part.\n"
    "• Do NOT paraphrase; only replace the wh-word part with the ANSWER.\n"
    "• Maintain meaning, no new info.\n"
    "Return only the statement.\n"
)

def set_seed(seed: int):
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except:
        pass

def _to_text(x: Any) -> str:
    if isinstance(x, str):
        return x
    try:
        return str(x)
    except:
        return ""

@dataclass
class MCQItem:
    group: str
    index: int
    name: str
    question: str
    choices: List[Dict[str, str]]
    correct: str

def load_model_and_tokenizer(path: str):
    print(f"[INFO] loading model from {path}", file=sys.stderr)
    tok = AutoTokenizer.from_pretrained(path)
    model = AutoModelForCausalLM.from_pretrained(
        path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    model.eval()
    return model, tok

def build_prompt(name: str, question: str, letter: str, text: str) -> str:
    return INSTRUCTION + "\n\n" + json.dumps(
        {
            "name": name,
            "question": question,
            "selected_choice": {"letter": letter, "text": text},
        },
        ensure_ascii=False,
        indent=2,
    )

def generate(model, tok, name, question, letter, text):
    """One-shot generation for speed."""
    prompt = build_prompt(name, question, letter, text)
    messages = [
        {"role": "system", "content": "You are a precise and concise rewriting assistant."},
        {"role": "user", "content": prompt},
    ]
    ids = tok.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(model.device)
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

def strong_check(name, question, choice_text, s):
    """Structure/consistency check."""
    name = _to_text(name)
    question = _to_text(question)
    choice_text = _to_text(choice_text)
    s = _to_text(s)

    if not s or len(s.split()) < 3:
        return False

    s_lower = s.lower()

    # must mention name
    if name and name.lower() not in s_lower:
        return False

    # answer consistency
    nums = re.findall(r"\d{3,4}", choice_text)
    if nums:
        for n in nums:
            if n not in s:
                return False
    else:
        c_tokens = [t for t in re.findall(r"[A-Za-z']+", choice_text.lower()) if t not in STOP_WORDS]
        if c_tokens and not any(t in s_lower for t in set(c_tokens)):
            return False

    # question content preservation
    q_tokens = [t for t in re.findall(r"[A-Za-z']+", question.lower()) if t not in STOP_WORDS]
    if q_tokens:
        missing = [t for t in set(q_tokens) if t not in s_lower]
        if len(missing) / len(set(q_tokens)) > 0.2:
            return False

    return True

SHOULD_EXIT = False

def handle_sigterm(signum, frame):
    global SHOULD_EXIT
    SHOULD_EXIT = True
    print("[SIGNAL] SIGTERM received; exit after current item...", file=sys.stderr)

def ensure_dir(path):
    if path:
        os.makedirs(path, exist_ok=True)

def safe_fsync(f):
    try:
        f.flush()
        os.fsync(f.fileno())
    except:
        pass

def load_items(path: str) -> List[MCQItem]:
    raw = load_json(path)
    items = []
    if isinstance(raw, dict):
        groups = raw.get("groups") or raw.get("data") or []
    else:
        groups = raw

    for g in groups:
        gid = _to_text(g.get("group",""))
        for it in g.get("items",[]):
            items.append(
                MCQItem(
                    group=gid,
                    index=int(it.get("index",0)),
                    name=_to_text(it.get("name","")),
                    question=_to_text(it.get("question","")),
                    choices=it.get("choices",[]),
                    correct=_to_text(it.get("correct","")).upper(),
                )
            )
    return items

def load_json(path):
    with open(path,"r",encoding="utf-8") as f:
        return json.load(f)

# ===============================
# Main
# ===============================

def main():
    global SHOULD_EXIT
    set_seed(DEFAULT_SEED)
    signal.signal(signal.SIGTERM, handle_sigterm)

    print(f"[INFO] INPUT={INPUT_PATH}")
    print(f"[INFO] OUTPUT={OUTPUT_PATH}")

    items = load_items(INPUT_PATH)
    items = sorted(items, key=lambda x: (x.group, x.index))

    # load resume keys
    completed: Set[Tuple[str,int]] = set()
    if os.path.exists(OUTPUT_PATH):
        print("[RESUME] Reading existing JSONL...")
        with open(OUTPUT_PATH,"r",encoding="utf-8") as f:
            lines = f.readlines()
        last_ok = 0
        for i,ln in enumerate(lines):
            ln=ln.strip()
            if not ln:
                continue
            try:
                rec=json.loads(ln)
                g=rec.get("group","")
                for it in rec.get("items",[]):
                    completed.add((g,int(it.get("index",0))))
                last_ok=i+1
            except:
                break
        if last_ok < len(lines):
            print("[RESUME] Truncating corrupted tail...")
            with open(OUTPUT_PATH,"w",encoding="utf-8") as f:
                f.writelines(lines[:last_ok])
        print(f"[RESUME] Completed questions: {len(completed)}")

    remaining = [it for it in items if (it.group,it.index) not in completed]
    print(f"[INFO] Remaining: {len(remaining)}")

    if not remaining:
        print("[INFO] Nothing to do.")
        return

    # load model
    model, tok = load_model_and_tokenizer(MODEL_PATH)

    out_dir = os.path.dirname(OUTPUT_PATH)
    ensure_dir(out_dir)
    fout = open(OUTPUT_PATH, "a", encoding="utf-8")

    start = time.time()
    limit = MAX_MINUTES * 60.0

    pbar = tqdm(total=len(remaining), desc="MCQ->statements", unit="question")

    for it in remaining:
        if SHOULD_EXIT:
            print("[STOP] SIGTERM break.")
            break
        if time.time() - start >= limit:
            print(f"[STOP] Time limit {MAX_MINUTES} min reached.")
            break

        choice_entries = []
        for ch in it.choices:
            letter = _to_text(ch.get("letter","")).upper()
            text = _to_text(ch.get("text",""))
            if not letter:
                continue

            try:
                s = generate(model, tok, it.name, it.question, letter, text)
            except:
                s = ""

            if not strong_check(it.name, it.question, text, s):
                q=it.question.strip()
                if not q.endswith("?"):
                    q+="?"
                s=f"{it.name} — Q: {q} A: {text}."

            choice_entries.append({
                "letter":letter,
                "text":text,
                "statement":_to_text(s),
            })

        rec={
            "group":it.group,
            "name":it.name,
            "items":[
                {
                    "index":it.index,
                    "question":it.question,
                    "choices":choice_entries,
                    "correct":it.correct,
                }
            ]
        }

        fout.write(json.dumps(rec,ensure_ascii=False)+"\n")
        safe_fsync(fout)
        pbar.update(1)

    pbar.close()
    fout.close()
    print(f"[DONE] Output saved to {OUTPUT_PATH}")

if __name__=="__main__":
    main()
