import argparse
import json
from pathlib import Path
from typing import Dict, Any, List


def load_json(path: str) -> Dict[str, Any]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_question_index(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        q = it.get("question")
        if isinstance(q, str):
            idx[q] = it
    return idx


def merge_one_forget_set(
    openend_data: Dict[str, List[Dict[str, Any]]],
    mcq_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    merged: Dict[str, List[Dict[str, Any]]] = {}
    entities = set(openend_data.keys()) | set(mcq_data.keys())

    for ent in sorted(entities):
        o_list = openend_data.get(ent, []) or []
        m_list = mcq_data.get(ent, []) or []

        o_idx = build_question_index(o_list)
        m_idx = build_question_index(m_list)

        ordered_questions = [it.get("question") for it in o_list if isinstance(it.get("question"), str)]
        for it in m_list:
            q = it.get("question")
            if isinstance(q, str) and q not in o_idx:
                ordered_questions.append(q)

        out_items: List[Dict[str, Any]] = []
        for q in ordered_questions:
            o = o_idx.get(q)
            m = m_idx.get(q)

            out_items.append(
                {
                    "question": q,
                    "is_refused": None,
                    "openend": None if o is None else {
                        "ref": o.get("ref"),
                        "pred": o.get("pred"),
                        "entropy": o.get("entropy"),
                        "acc_prob": o.get("acc_prob"),
                    },
                    "mcq": None if m is None else {
                        "ref": m.get("ref"),
                        "pred": m.get("pred"),
                        "entropy": m.get("entropy"),
                        "acc_prob": m.get("acc_prob"),
                        "False_in": m.get("False_in"),
                        "Choices": m.get("Choices"),
                        "Choice_distribution": m.get("Choice_distribution"),
                    },
                }
            )

        merged[ent] = out_items

    return merged


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge 5 open-ended + 5 MCQ json files into one json.")
    p.add_argument(
        "--openend",
        nargs=5,
        required=True,
        help="5 open-ended json files, order: whp_1..whp_5",
    )
    p.add_argument(
        "--mcq",
        nargs=5,
        required=True,
        help="5 mcq json files, order: whp_1..whp_5",
    )
    p.add_argument(
        "--output",
        required=True,
        help="output json path",
    )
    p.add_argument(
        "--names",
        nargs=5,
        default=None,
        help="optional 5 group names, default: whp_1..whp_5",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    names = args.names if args.names is not None else [f"whp_{i+1}" for i in range(5)]
    if len(names) != 5:
        raise ValueError("--names must be 5.")

    final_out: Dict[str, Any] = {}

    for i in range(5):
        open_data = load_json(args.openend[i])
        mcq_data = load_json(args.mcq[i])
        final_out[names[i]] = merge_one_forget_set(open_data, mcq_data)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(final_out, f, ensure_ascii=False, indent=2)

    for fs, fs_data in final_out.items():
        total = sum(len(v) for v in fs_data.values())
        print(f"{fs}: total_questions={total}")


if __name__ == "__main__":
    main()