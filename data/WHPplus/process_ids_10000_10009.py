""" 
This module converts MCQ questions into question + fact
And split between ids 10000-10009 and other ids.
"""


import json
import sys

def convert_record(rec: dict) -> dict:
    """
    Convert a single QA record by turning the correct choice into `fact`.
    Keeps only: question, fact, name.
    """
    question = rec.get("question")
    name = rec.get("name")
    answer_letter = rec.get("answer")
    choices = rec.get("choices") or {}

    # Map answer letter to its text; if missing, leave fact as None
    fact = None
    if isinstance(answer_letter, str) and isinstance(choices, dict):
        fact = choices.get(answer_letter)

    return {
        "question": question,
        "fact": fact,
        "name": name
    }

def process(in_path: str, out_selected: str, out_others: str):
    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("Input must be a JSON object mapping id -> list[records].")

    selected_ids = {str(i) for i in range(10000, 10010)}
    selected = {}
    others = {}

    for k, v in data.items():
        # Expect a list of QA objects per id
        if not isinstance(v, list):
            continue
        converted = [convert_record(item) for item in v]

        if k in selected_ids:
            selected[k] = converted
        else:
            others[k] = converted

    with open(out_selected, "w", encoding="utf-8") as f:
        json.dump(selected, f, ensure_ascii=False, indent=2)

    with open(out_others, "w", encoding="utf-8") as f:
        json.dump(others, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python process_ids_10000_10009.py input.json selected.json others.json")
        sys.exit(1)
    process(sys.argv[1], sys.argv[2], sys.argv[3])
