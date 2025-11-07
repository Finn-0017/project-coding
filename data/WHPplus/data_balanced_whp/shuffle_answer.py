#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Obfuscate statements by grouping FOUR incorrect-conclusion variants per (group, index).
- For each input item, look up the correct answer text from dedup, then produce 4
  statements by replacing that correct phrase with each wrong option.
- Output preserves original fields (group, index, name, question, ...), removes "choice",
  and merges the four variants into a single object:
      "statement": { "1": "...", "2": "...", "3": "...", "4": "..." }
- Best-effort replacement strategies:
    1) exact match
    2) case-insensitive match
    3) whitespace/punctuation-tolerant pattern
    4) simple numeric/year swap
    5) semantic fallback: append "(This statement instead asserts: WRONG.)"
- Writes:
    1) forget_dedup_statement_obfuscate_grouped.json
    2) summary_obfuscate_grouped.json
Inputs expected in the same directory:
    - forget_dedup_statement.jsonl
    - forget_dedup.json
"""
import json, random, re
from pathlib import Path
from typing import Tuple, Dict, Any, List

SEED = 42
BASE = Path(__file__).resolve().parent
STATEMENT_JSONL = BASE / "forget_dedup_statement.jsonl"
DEDUP_JSON      = BASE / "forget_dedup.json"
OUT_GROUPED     = BASE / "forget_dedup_statement_obfuscate_grouped.json"
OUT_SUMMARY     = BASE / "summary_obfuscate_grouped.json"

random.seed(SEED)

def load_inputs():
    with open(STATEMENT_JSONL, "r", encoding="utf-8") as f:
        items = [json.loads(line) for line in f if line.strip()]
    with open(DEDUP_JSON, "r", encoding="utf-8") as f:
        dedup = json.load(f)
    return items, dedup

def _build_tolerant_pattern(old_text: str) -> re.Pattern:
    tokens = [re.escape(t) for t in re.split(r"\s+", old_text.strip()) if t]
    if not tokens:
        return re.compile(re.escape(old_text))
    mid = r"(?:\s*[^A-Za-z0-9\s]*\s*)"
    pat = mid.join(tokens)
    return re.compile(pat, flags=re.IGNORECASE)

def _year_like(s: str) -> bool:
    return bool(re.fullmatch(r"\d{3,4}", s))

def _number_like(s: str) -> bool:
    return bool(re.fullmatch(r"[-+]?\d+(?:\.\d+)?", s))

def replace_best_effort(stmt: str, old_text: str, new_text: str) -> Tuple[str, str]:
    """
    Replace the correct phrase in the statement with the wrong one using multiple strategies.
    Returns (new_stmt, method_used)
    method_used ∈ {"exact", "ci", "tolerant", "numeric_swap", "semantic_fallback"}
    """
    if old_text:
        # exact
        new_stmt, n = re.subn(re.escape(old_text), new_text, stmt)
        if n > 0:
            return new_stmt, "exact"
        # case-insensitive
        new_stmt, n = re.compile(re.escape(old_text), re.IGNORECASE).subn(new_text, stmt)
        if n > 0:
            return new_stmt, "ci"
        # tolerant
        pat_tol = _build_tolerant_pattern(old_text)
        new_stmt, n = pat_tol.subn(new_text, stmt)
        if n > 0:
            return new_stmt, "tolerant"
        # numeric/year swap
        if (_year_like(old_text) and _year_like(new_text)) or (_number_like(old_text) and _number_like(new_text)):
            num_pat = re.compile(re.escape(old_text))
            new_stmt, n = num_pat.subn(new_text, stmt, count=1)
            if n == 0:
                any_num = re.search(r"\d{3,4}|\d+(?:\.\d+)?", stmt)
                if any_num:
                    start, end = any_num.span()
                    new_stmt = stmt[:start] + new_text + stmt[end:]
                    return new_stmt, "numeric_swap"
            else:
                return new_stmt, "numeric_swap"
    # semantic fallback
    return f"{stmt.rstrip()} (This statement instead asserts: {new_text}.)", "semantic_fallback"

def build_grouped_item(it: Dict[str, Any], qa: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """
    Build a single grouped record with four wrong statements.
    Returns (grouped_record, method_counter)
    """
    methods = {"exact":0, "ci":0, "tolerant":0, "numeric_swap":0, "semantic_fallback":0}
    correct_letter = qa["answer"]
    choices = qa["choices"]
    correct_text = str(choices[correct_letter])
    wrong_letters = sorted([k for k in choices.keys() if k != correct_letter])

    stmt_variants: Dict[str, str] = {}
    base_stmt = str(it.get("statement", ""))

    for i, wl in enumerate(wrong_letters, start=1):
        wrong_text = str(choices[wl])
        new_stmt, method = replace_best_effort(base_stmt, correct_text, wrong_text)
        methods[method] = methods.get(method, 0) + 1
        stmt_variants[str(i)] = new_stmt

    out = {k: v for k, v in it.items() if k != "choice"}  # drop 'choice'
    out["statement"] = stmt_variants
    return out, methods

def main():
    items, dedup = load_inputs()

    grouped = []
    counts = {
        "total_input_items": len(items),
        "grouped_outputs": 0,
        "no_match_in_dedup": 0,
        "method_exact": 0,
        "method_ci": 0,
        "method_tolerant": 0,
        "method_numeric_swap": 0,
        "method_semantic_fallback": 0
    }
    no_match_in_dedup_examples = []

    for it in items:
        g = str(it.get("group"))
        try:
            idx = int(it.get("index", -1))
        except Exception:
            idx = -1

        if g not in dedup or idx < 0 or idx >= len(dedup[g]):
            counts["no_match_in_dedup"] += 1
            if len(no_match_in_dedup_examples) < 10:
                no_match_in_dedup_examples.append({"group": g, "index": idx, "statement": it.get("statement","")[:200]})
            continue

        qa = dedup[g][idx]
        rec, mcount = build_grouped_item(it, qa)
        grouped.append(rec)
        counts["grouped_outputs"] += 1
        for k,v in mcount.items():
            counts[f"method_{k}"] += v

    with open(OUT_GROUPED, "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    summary = {
        **counts,
        "output_file": str(OUT_GROUPED.name),
        "notes": [
            "One output row per input item.",
            "Each row contains four obfuscated statements grouped under the 'statement' object with keys '1'..'4'.",
            "The 'choice' field is removed. No 'unmodified' file is produced.",
            "Items without direct replacements use a semantic fallback clause."
        ],
        "no_match_in_dedup_examples": no_match_in_dedup_examples
    }
    with open(OUT_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
