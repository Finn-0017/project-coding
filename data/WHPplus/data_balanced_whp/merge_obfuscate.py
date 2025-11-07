#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge grouped statements into passages (~50 statements each) and
output a clean JSON file formatted as:
{
    "Benedetto Varchi": [
        "passage 1",
        "passage 2"
    ],
    ...
}

Rules:
- Each group ID (10000–10009) corresponds to one historical figure (person name).
- The 'name' field in the grouped data defines the mapping from ID to person.
- For each group, merge random statements into passages of about 50 sentences.
- Ensure that no two statements from the same (group,index) appear in one passage.
- Output only the required mapping { name: [passage1, passage2, ...] }.
"""

import json, random, math
from pathlib import Path
from typing import Dict, Any, List

# -------- Settings --------
SEED = 42
BASE = Path(__file__).resolve().parent
GROUPED_FILE = BASE / "forget_dedup_statement_obfuscate_grouped.json"
OUT_FILE = BASE / "all_obfuscate_samples_like_template.json"
CHUNK_SIZE = 50  # target statements per passage
# --------------------------

random.seed(SEED)

def load_grouped() -> List[Dict[str, Any]]:
    """Load the grouped statements file."""
    with open(GROUPED_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def build_passages(records: List[Dict[str, Any]], chunk_size: int = 50) -> Dict[str, List[str]]:
    """Group all statements per person (group), merge ~50 into one passage."""
    # Map: group -> { name, indices -> statements }
    groups: Dict[str, Dict[int, List[str]]] = {}
    names: Dict[str, str] = {}  # group id -> person name
    for rec in records:
        g = str(rec.get("group"))
        idx = int(rec.get("index", -1))
        name = rec.get("name", f"group_{g}")
        stmt_variants = list(rec.get("statement", {}).values())
        if not stmt_variants:
            continue
        random.shuffle(stmt_variants)
        groups.setdefault(g, {})[idx] = stmt_variants
        names[g] = name

    # Build passages
    result: Dict[str, List[str]] = {}
    for g, idx_map in groups.items():
        name = names.get(g, f"group_{g}")
        all_passages: List[str] = []

        indices = sorted(idx_map.keys())
        max_variants = max((len(v) for v in idx_map.values()), default=0)

        for r in range(max_variants):
            # each index contributes one statement per round
            round_units = []
            for i in indices:
                if r < len(idx_map[i]):
                    round_units.append(idx_map[i][r])
            if not round_units:
                continue

            random.shuffle(round_units)
            for chunk_start in range(0, len(round_units), chunk_size):
                chunk = round_units[chunk_start:chunk_start + chunk_size]
                if not chunk:
                    continue
                random.shuffle(chunk)
                passage_text = " ".join(chunk)
                all_passages.append(passage_text)

        result[name] = all_passages
    return result

def main():
    records = load_grouped()
    merged = build_passages(records, CHUNK_SIZE)

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"Written {len(merged)} names to {OUT_FILE}")
    for k, v in list(merged.items())[:3]:
        print(f"--- {k} ---")
        print(f"Passages: {len(v)}")
        print(v[0][:250] + "...\n")

if __name__ == "__main__":
    main()
