#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
question_rephrase.py

Usage examples:
  bash question_rephrase.py --answer true
  bash question_rephrase.py --answer false
"""

import os
import re
import json
import argparse
import random
from typing import Dict, Any, Tuple
from openai import OpenAI

# ===== Configuration =====
LOCAL_MODEL_PATH = "/home/xy319/rds/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct"
LOCAL_BASE_URL = "http://localhost:8000/v1"
SYSTEM_PROMPT = """You are a precise rephraser. Convert a single multiple-choice question about a PERSON \
into ONE declarative sentence that PRESERVES the semantic RELATION between SUBJECT (the person), \
PREDICATE (relation), and OBJECT (value).

STRICT REQUIREMENTS:
- Use the provided 'name' as the grammatical SUBJECT if the question is about that person.
- Use the provided 'option_text' as the OBJECT.
- Keep the RELATION accurate (e.g., 'was born in', 'won the Nobel Prize for', 'coined the term', 'served as', etc.).
- Include necessary prepositions ('in', 'on', 'for', 'as', 'at', etc.) so that the subject–predicate–object relation is correct.
- If the question has context like time/place clauses, integrate them naturally (e.g., 'In 1953, ...').
- Do NOT output explanations.
- Output ONLY valid JSON with keys: statement, triple.
"""
USER_TEMPLATE = """name: {name}
question: {question}
option_text: {answer}
Return JSON:
{{
  "statement": "...",
  "triple": {{"subject": "...", "relation": "...", "object": "..." }}
}}
"""
# ==========================


def str2bool(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "y", "on")


def pick_option(rec: Dict[str, Any], use_correct: bool, rng: random.Random) -> Tuple[str, str]:
    choices = rec.get("choices") or {}
    if not isinstance(choices, dict) or not choices:
        return "", ""
    if use_correct:
        a = rec.get("answer")
        if isinstance(a, str) and a in choices:
            return a, str(choices[a])
        k = next(iter(choices.keys()))
        return k, str(choices[k])
    k = rng.choice(list(choices.keys()))
    return k, str(choices[k])


def safe_parse_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}


def fallback_statement(name: str, question: str, option_text: str) -> Tuple[str, Dict[str, str]]:
    n = (name or "The person").strip()
    q = (question or "").strip().rstrip(" ?")
    a = (option_text or "").strip()
    if re.search(r"\bborn\b", q, re.I):
        relation = "was born in"
    elif re.search(r"\bdied?\b", q, re.I):
        relation = "died in"
    elif re.search(r"\b(win|won|receive|award)\b", q, re.I):
        relation = "won/received"
    elif re.search(r"\b(serve|assume|hold)\b", q, re.I):
        relation = "served as"
    elif re.search(r"\bcoin(ed)?\b", q, re.I):
        relation = "coined the term"
    elif re.search(r"\bstudy|studied\b", q, re.I):
        relation = "studied in"
    elif re.search(r"\blive|lived\b", q, re.I):
        relation = "lived in"
    elif re.search(r"\bwhen\b", q, re.I):
        relation = "is associated with the date"
    else:
        relation = "is associated with"
    if relation == "coined the term":
        stmt = f"{n} coined the term {a}."
    elif relation in ("was born in", "died in", "studied in", "lived in", "is associated with"):
        stmt = f"{n} {relation} {a}."
    elif relation in ("won/received", "is associated with the date", "served as"):
        stmt = f"{n} {relation} {a}."
    else:
        stmt = f"{n} — {a}."
    triple = {"subject": n, "relation": relation, "object": a}
    return stmt, triple


def main():
    parser = argparse.ArgumentParser(description="Rephrase MCQ into declarative statements with relation preservation.")
    parser.add_argument("--input", type=str, default="forget_dedup.json", help="Input JSON (id -> list[records]).")
    parser.add_argument("--output", type=str, default="forget_statements_llm.jsonl", help="Output JSONL path.")
    parser.add_argument("--answer", type=str, default="true", help="true to use correct answer; false for random option.")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=120)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()

    use_correct_answer = str2bool(args.answer)

    # Client for local vLLM server
    client = OpenAI(api_key="EMPTY", base_url=LOCAL_BASE_URL)

    # Load input data
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Input must be a JSON object mapping id -> list[records].")

    out = open(args.output, "w", encoding="utf-8")
    total = sum(len(v) for v in data.values() if isinstance(v, list))
    done = 0
    failed = 0

    for id_key, items in data.items():
        if not isinstance(items, list):
            continue
        for rec in items:
            name = rec.get("name") or ""
            question = rec.get("question") or ""
            seed = abs(hash((name, question))) % (2**32)
            rng = random.Random(seed)
            letter, ans_text = pick_option(rec, use_correct_answer, rng)

            user_msg = USER_TEMPLATE.format(name=name, question=question, answer=ans_text)
            parsed = None

            for attempt in range(max(1, args.retries)):
                try:
                    resp = client.chat.completions.create(
                        model=LOCAL_MODEL_PATH,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_msg}
                        ],
                        temperature=args.temperature,
                        max_tokens=args.max_tokens,
                    )
                    content = resp.choices[0].message.content or ""
                    parsed = safe_parse_json(content)
                    if isinstance(parsed, dict) and "statement" in parsed and "triple" in parsed:
                        tri = parsed.get("triple") or {}
                        if isinstance(tri, dict) and all(k in tri for k in ("subject", "relation", "object")):
                            break
                        parsed = None
                except Exception:
                    parsed = None

            if not parsed:
                failed += 1
                stmt, tri = fallback_statement(name, question, ans_text)
            else:
                stmt = str(parsed["statement"]).strip()
                tri = {
                    "subject": str(parsed["triple"].get("subject", "")).strip(),
                    "relation": str(parsed["triple"].get("relation", "")).strip(),
                    "object": str(parsed["triple"].get("object", "")).strip(),
                }

            out.write(json.dumps({
                "id": id_key,
                "name": name,
                "question": question,
                "selected_letter": letter,
                "selected_text": ans_text,
                "use_correct_answer": use_correct_answer,
                "statement": stmt,
                "triple": tri
            }, ensure_ascii=False) + "\n")

            done += 1
            if done % 100 == 0:
                print(f"Processed {done}/{total}...")

    out.close()
    print(f"Done. total={total}, failed={failed}, output={args.output}")


if __name__ == "__main__":
    main()
