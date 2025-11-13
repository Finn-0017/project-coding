#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
true_statement_to_passage.py

Convert true statements (JSONL; one JSON object per line) into passages.

Rules:
- Only merge statements belonging to the same person (group + name).
- Each passage should contain around 40 statements (configurable).
- The last passage should not be too short (use balanced splitting).
- Output:
    1) forget_passages_true.json
    2) forget_passages_true_structure.json
    3) forget_passages_true_stats.json  (for reporting)
"""

import json
import random
from collections import defaultdict
from typing import List, Dict, Any


def load_records(input_path: str) -> List[Dict[str, Any]]:
    """Load a JSONL file (one JSON object per line)."""
    records = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                records.append(obj)
            except json.JSONDecodeError:
                # Skip malformed lines
                continue
    return records


def group_by_person(records: List[Dict[str, Any]]):
    """
    Group records by (group, name).

    Each record is expected to contain:
    - group: person id
    - name: person name
    - statement: the true statement
    - index: the original statement index
    """
    grouped = defaultdict(list)
    for r in records:
        key = (r.get("group"), r.get("name"))
        grouped[key].append(r)
    return grouped


def split_to_passages(
    grouped,
    target_per_passage: int = 40,
    seed: int = 42
):
    """
    Core logic:
    - Shuffle statements of each person randomly.
    - Target ≈40 statements per passage.
    - If n <= target: only 1 passage.
    - Otherwise num_passages = round(n / target).
      Then distribute n statements evenly across passages.

    Returns:
        passages: list of passage JSON items
        structure: mapping of original statement indices → passages
        stats: per-person statistics
    """
    random.seed(seed)

    passage_output = []
    structure_output = []
    stats = []

    for (gid, name), items in grouped.items():
        items_shuffled = items.copy()
        random.shuffle(items_shuffled)
        n = len(items_shuffled)

        # Determine number of passages
        if n <= target_per_passage:
            num_passages = 1
        else:
            num_passages = max(1, round(n / target_per_passage))
            if num_passages > n:
                num_passages = n  # Safety guard

        # Evenly distribute n items across num_passages
        base = n // num_passages
        rem = n % num_passages
        sizes = [base + 1 if i < rem else base for i in range(num_passages)]
        assert sum(sizes) == n

        # Create passages
        idx = 0
        for p_idx, size in enumerate(sizes):
            slice_items = items_shuffled[idx:idx + size]
            idx += size

            passage_text = " ".join(s["statement"] for s in slice_items)

            passage_output.append({
                "group": gid,
                "name": name,
                "passage_index": p_idx,
                "passage": passage_text,
            })

            structure_output.append({
                "group": gid,
                "name": name,
                "passage_index": p_idx,
                "statement_indices": [int(s["index"]) for s in slice_items],
                "statement_count": len(slice_items),
            })

        avg_per_passage = n / num_passages
        stats.append({
            "group": gid,
            "name": name,
            "statement_count": n,
            "num_passages": num_passages,
            "avg_per_passage": avg_per_passage,
        })

    return passage_output, structure_output, stats


def save_json(obj: Any, path: str):
    """Save an object as pretty-formatted JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert true statements into passages grouped by person."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input JSONL file path (forget_true_statements.json)"
    )
    parser.add_argument(
        "--output_prefix",
        type=str,
        default="forget_passages_true",
        help="Prefix for output files"
    )
    parser.add_argument(
        "--target_per_passage",
        type=int,
        default=40,
        help="Target number of statements per passage (default: 40)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility"
    )

    args = parser.parse_args()

    # Load and process data
    records = load_records(args.input)
    print(f"Loaded {len(records)} records from {args.input}")

    grouped = group_by_person(records)
    print(f"Detected {len(grouped)} persons (unique group+name pairs).")

    passages, structures, stats = split_to_passages(
        grouped,
        target_per_passage=args.target_per_passage,
        seed=args.seed
    )

    # Output file paths
    out_passages = f"{args.output_prefix}.json"
    out_struct = f"{args.output_prefix}_structure.json"
    out_stats = f"{args.output_prefix}_stats.json"

    save_json(passages, out_passages)
    save_json(structures, out_struct)
    save_json(stats, out_stats)

    print(f"Saved passages to: {out_passages}")
    print(f"Saved structure to: {out_struct}")
    print(f"Saved stats to: {out_stats}")

    # Print quick summary for convenience
    print("\n=== Per-person summary ===")
    for s in stats:
        print(
            f"group={s['group']}, name={s['name']}, "
            f"statements={s['statement_count']}, "
            f"passages={s['num_passages']}, "
            f"avg_per_passage={s['avg_per_passage']:.2f}"
        )


if __name__ == "__main__":
    main()
