#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
true_statement_to_passage.py

Convert ALL statements (JSONL; one JSON object per line) from MCQs into passages.

New input format (one JSON object per line):
{
  "group": "10000",
  "name": "Benedetto Varchi",
  "items": [
    {
      "index": 0,
      "question": "...",
      "choices": [
        {
          "letter": "A",
          "text": "...",
          "statement": "...",
          "warning": false
        },
        ...
      ],
      "correct": "D"
    },
    ...
  ]
}

Rules:
- Only merge statements belonging to the same person (group + name).
- Treat each (question, choice) pair as one statement unit.
- Use EVERY answer exactly once (i.e., all choices for all questions).
- In a single passage, do NOT include two answers from the same question
  (best-effort; see implementation note below).
- Each passage should contain around 40 statements (configurable).
- The last passage should not be too short (use balanced splitting).
- Output:
    1) forget_passages.json
    2) forget_passages_structure.json
    3) forget_passages_stats.json  (for reporting)
"""

import json
import random
from collections import defaultdict, Counter
from typing import List, Dict, Any, Tuple


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


def build_grouped_statements(
    records: List[Dict[str, Any]]
) -> Dict[Tuple[Any, Any], List[Dict[str, Any]]]:
    """
    Build grouped statement units from all MCQ items.

    Input:
        Each record (line) is expected to contain:
        - group: person id
        - name: person name
        - items: list of MCQs, each with:
            - index: question index (int)
            - question: question text (unused here)
            - choices: list of choices, each with 'letter', 'text', 'statement', 'warning'
            - correct: the letter of the correct choice

    Output structure (grouped by (group, name)):
        For each person key (gid, name) we produce a list of statement dicts:
        {
            "group": gid,
            "name": name,
            "question_index": int(item["index"]),
            "answer_index": choice_idx,         # 0-based index in choices
            "letter": choice["letter"],
            "statement": choice["statement"],
            "is_correct": (choice["letter"] == item["correct"]),
        }

        Each (question, choice) pair becomes exactly one statement unit.
    """
    grouped = defaultdict(list)

    for rec in records:
        gid = rec.get("group")
        name = rec.get("name")
        items = rec.get("items") or []

        for item in items:
            q_index = item.get("index")
            correct_letter = item.get("correct")
            choices = item.get("choices") or []

            for a_idx, choice in enumerate(choices):
                stmt = {
                    "group": gid,
                    "name": name,
                    "question_index": int(q_index) if q_index is not None else None,
                    "answer_index": a_idx,
                    "letter": choice.get("letter"),
                    "statement": choice.get("statement"),
                    "is_correct": (choice.get("letter") == correct_letter),
                }
                grouped[(gid, name)].append(stmt)

    return grouped


def split_to_passages(
    grouped,
    target_per_passage: int = 40,
    seed: int = 42
):
    """
    Core logic for splitting statements into passages.

    For each person:
    - We have a list of statement units; each is a (question, answer) pair.
    - We want to:
        * Use EVERY statement once (all answers are used).
        * In a single passage, avoid having two answers from the same question_index.
        * Keep passages around target_per_passage in size.
        * Balance sizes so the last passage is not too short.

    Strategy:
    - For a person with n statements:

        1) Compute max_multiplicity = max number of statements per question_index.
           We must have num_passages >= max_multiplicity to be able to put each
           answer of a question into different passages (best case).

        2) Base number of passages is round(n / target_per_passage).
           Final num_passages = max(1, base_passages, max_multiplicity).

        3) Compute passage sizes so that they are as even as possible.

        4) Shuffle all statements and greedily assign each statement to the
           earliest passage that:
              - is not yet full AND
              - does not contain this question_index yet.

           If we cannot find such a passage, we fall back to putting the statement
           into any passage that is not full (this might violate the
           "no two answers from same question" constraint, but only when
           the constraint is mathematically impossible, e.g., num_passages is
           still smaller than multiplicity due to extremely small n).

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
        if not items:
            continue

        # Shuffle all statement units for this person
        items_shuffled = items.copy()
        random.shuffle(items_shuffled)

        n = len(items_shuffled)

        # Count how many statements each question_index has
        q_counts = Counter(s["question_index"] for s in items_shuffled)
        max_multiplicity = max(q_counts.values()) if q_counts else 1

        # Determine base number of passages from target_per_passage
        if n <= target_per_passage:
            base_passages = 1
        else:
            base_passages = round(n / target_per_passage)

        # Final number of passages:
        #   - at least 1
        #   - at least max_multiplicity (to try to avoid duplicate answers
        #     from same question in one passage)
        num_passages = max(1, base_passages, max_multiplicity)

        # Safety guard: we never want more passages than statements.
        # (If num_passages == n, each passage has exactly 1 statement.)
        if num_passages > n:
            num_passages = n

        # Compute balanced sizes for each passage
        base_size = n // num_passages
        rem = n % num_passages
        sizes = [base_size + 1 if i < rem else base_size for i in range(num_passages)]
        assert sum(sizes) == n

        # Prepare containers
        passages_for_person = [[] for _ in range(num_passages)]
        passage_qids = [set() for _ in range(num_passages)]
        current_counts = [0 for _ in range(num_passages)]

        # Greedy assignment of statements to passages
        for s in items_shuffled:
            qid = s["question_index"]
            placed = False

            # First try: respect both capacity and "no duplicate question" constraint
            for p_idx in range(num_passages):
                if current_counts[p_idx] >= sizes[p_idx]:
                    continue
                if qid in passage_qids[p_idx]:
                    continue

                passages_for_person[p_idx].append(s)
                passage_qids[p_idx].add(qid)
                current_counts[p_idx] += 1
                placed = True
                break

            # Fallback: if it was impossible (e.g., due to extreme edge cases),
            # relax the "no duplicate question" constraint but still respect capacity.
            if not placed:
                for p_idx in range(num_passages):
                    if current_counts[p_idx] < sizes[p_idx]:
                        passages_for_person[p_idx].append(s)
                        passage_qids[p_idx].add(qid)
                        current_counts[p_idx] += 1
                        placed = True
                        break

            if not placed:
                # This should never happen if sizes are computed correctly,
                # but we keep this as a safety guard.
                raise RuntimeError(
                    f"Could not place statement for group={gid}, name={name}"
                )

        # Build outputs for this person
        for p_idx, slice_items in enumerate(passages_for_person):
            if not slice_items:
                continue

            passage_text = " ".join(s["statement"] for s in slice_items)

            # Passage content for downstream use
            passage_output.append({
                "group": gid,
                "name": name,
                "passage_index": p_idx,
                "passage": passage_text,
            })

            # Structure: keep both question_index and answer_index
            structure_output.append({
                "group": gid,
                "name": name,
                "passage_index": p_idx,
                "statements": [
                    {
                        "question_index": int(s["question_index"])
                        if s["question_index"] is not None else None,
                        "answer_index": int(s["answer_index"]),
                    }
                    for s in slice_items
                ],
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
        description="Convert ALL MCQ statements into passages grouped by person."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input JSONL file path (all statements; one person per line with 'items')."
    )
    parser.add_argument(
        "--output_prefix",
        type=str,
        default="forget_passages",
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

    grouped = build_grouped_statements(records)
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
