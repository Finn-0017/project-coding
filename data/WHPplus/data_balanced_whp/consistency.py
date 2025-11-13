#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Eval: passage + multiple MCQs in one shot → N letters

对每个 passage:
  - 根据 forget_passages_structure.json 找到所有相关的 (question_index, answer_index)
  - 从 forget_statements.json 中取出对应的 MCQ（question + choices）
  - 构造一个包含所有题目的大 prompt
  - 模型一次性输出 N 个选项字母，例如 "BACDE..."
  - 用 structure 里的 answer_index 找到 target_letter，比对是否一致
  - 得到 passage-level accuracy

对每个人:
  - 把 TA 的所有 passage accuracy 取平均，得到 person-level accuracy
"""

import argparse
import json
import os
from collections import defaultdict

import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

# ====== 和你原来的 mcq_to_true_statement.py 一致的本地模型路径 ======
MODEL_PATH = (
    "/rds/user/xy319/hpc-work/projects/project-coding/"
    "hf_models/models--meta-llama--Llama-3.1-8B-Instruct/"
    "snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
)
MAX_NEW_TOKENS = 64  # 一次输出 N 个字母，给到几十个 token 足够


# ----------------- 模型加载 -----------------

def load_model_and_tokenizer(path: str):
    print(f"[INFO] loading model from {path}", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(path)
    model = AutoModelForCausalLM.from_pretrained(
        path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    model.config.pad_token_id = tokenizer.pad_token_id
    model.eval()
    return model, tokenizer


# ----------------- batched 生成：一次输出 N 个选项字母 -----------------

def generate_option_letters_batched(model, tokenizer, passage: str, mcq_list):
    """
    mcq_list: List[{
        'question': str,
        'choices': List[{'letter': 'A', 'text': '...'}, ...]
    }]

    返回: List[pred_letter]，长度为 len(mcq_list)，每个是 "A"~"E" 或 "?"
    """

    num_q = len(mcq_list)

    # 把所有题目串在一起
    q_blocks = []
    for i, mcq in enumerate(mcq_list, start=1):
        q_text = mcq["question"]
        choices = mcq["choices"]
        options_str = "\n".join(f"{c['letter']}. {c['text']}" for c in choices)
        block = (
            f"Question {i}:\n"
            f"{q_text}\n"
            f"Options:\n{options_str}\n"
        )
        q_blocks.append(block)

    all_questions_text = "\n".join(q_blocks)

    user_content = (
        "You will answer multiple-choice questions **only** based on the information in the passage.\n\n"
        f"Passage:\n\"\"\"\n{passage}\n\"\"\"\n\n"
        f"{all_questions_text}\n"
        "Now answer ALL questions at once.\n"
        f"Respond with EXACTLY {num_q} capital letters from A, B, C, D, or E, "
        "with NO spaces or punctuation, where the i-th letter is the answer "
        "for Question i.\n"
        "For example, if your answers are Q1->B, Q2->A, Q3->D, you MUST output:\n"
        "BAD\n"
        "Do not output anything else."
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a precise multiple-choice question answering assistant. "
                "You must respond with exactly the required number of capital "
                "letters among A, B, C, D, or E, and nothing else."
            ),
        },
        {
            "role": "user",
            "content": user_content,
        },
    ]

    input_ids = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        return_tensors="pt",
    ).to(model.device)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )

    gen = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()

    # 解析：从输出中依次提取 A-E 的大写字母，取前 num_q 个
    letters = []
    for ch in gen:
        if ch in "ABCDE":
            letters.append(ch)
        if len(letters) >= num_q:
            break

    # 如果不够，就用 '?' 填，避免 index error
    while len(letters) < num_q:
        letters.append("?")

    return letters


# ----------------- 数据加载 -----------------

def load_mcqs(statements_path: str):
    """
    从 forget_statements.json (jsonlines) 加载 MCQ：
    返回:
      mcqs[group][question_index] = {
        'name': ...,
        'question': ...,
        'choices': [...],
        'correct': 'A'/'B'/...
      }
    """
    mcqs = defaultdict(dict)

    with open(statements_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            group = rec["group"]
            name = rec.get("name", "")
            for item in rec["items"]:
                q_idx = int(item["index"])
                mcqs[group][q_idx] = {
                    "name": name,
                    "question": item["question"],
                    "choices": item["choices"],
                    "correct": item["correct"],
                }

    print(f"[INFO] loaded MCQs for {len(mcqs)} groups")
    return mcqs


def load_passages(passages_path: str):
    """
    从 forget_passages.json 加载 passage：
    返回:
      passages_by_key[(group, passage_index)] = {
        'name': ...,
        'passage': ...
      }
    """
    passages_by_key = {}
    with open(passages_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for rec in data:
        key = (rec["group"], int(rec["passage_index"]))
        passages_by_key[key] = {
            "name": rec.get("name", ""),
            "passage": rec["passage"],
        }

    print(f"[INFO] loaded {len(passages_by_key)} passages")
    return passages_by_key


def load_structure(structure_path: str):
    """
    从 forget_passages_structure.json 加载结构：
    返回 list[{
        'group', 'name', 'passage_index', 'statements': [
            {'question_index': int, 'answer_index': int}, ...
        ]
    }]
    """
    with open(structure_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"[INFO] loaded {len(data)} passage-structure records")
    return data


# ----------------- 主评估逻辑（batched） -----------------

def eval_passage_consistency_batched(
    model,
    tokenizer,
    mcqs,
    passages_by_key,
    structures,
    output_path: str,
):
    """
    对每个 passage 一次性问完所有题，并输出:
      - 每个 passage 的正确率
      - 每个人 (group, name) 的平均正确率
    """

    # person -> list of passage accuracies
    person_to_passage_accs = defaultdict(list)

    # passage-level 详细结果写成 jsonlines
    fout = open(output_path, "w", encoding="utf-8")

    pbar = tqdm(structures, desc="Eval passages (batched)", unit="passage")

    for rec in pbar:
        group = rec["group"]
        name = rec.get("name", "")
        passage_index = int(rec["passage_index"])
        stmts = rec["statements"]

        key = (group, passage_index)
        if key not in passages_by_key:
            print(f"[WARN] missing passage for {key}, skip")
            continue

        passage_text = passages_by_key[key]["passage"]

        # 组装这个 passage 对应的所有 MCQ
        mcq_list = []
        target_letters = []
        stmt_infos = []  # 记录 question_index & answer_index，对齐长度

        for s in stmts:
            q_idx = int(s["question_index"])
            ans_idx = int(s["answer_index"])

            mcq = mcqs[group].get(q_idx)
            if mcq is None:
                print(f"[WARN] missing MCQ for group={group}, q_idx={q_idx}, skip this question")
                continue

            choices = mcq["choices"]
            if not (0 <= ans_idx < len(choices)):
                print(f"[WARN] invalid answer_index={ans_idx} for group={group}, q_idx={q_idx}")
                continue

            mcq_list.append({
                "question": mcq["question"],
                "choices": choices,
            })
            target_letters.append(choices[ans_idx]["letter"].upper())
            stmt_infos.append({
                "question_index": q_idx,
                "answer_index": ans_idx,
            })

        num_q = len(mcq_list)
        if num_q == 0:
            passage_acc = None
            out_rec = {
                "group": group,
                "name": name,
                "passage_index": passage_index,
                "num_questions": 0,
                "num_correct": 0,
                "accuracy": None,
                "details": [],
            }
            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            continue

        # 一次性让模型输出 num_q 个答案字母
        pred_letters = generate_option_letters_batched(
            model, tokenizer,
            passage_text,
            mcq_list,
        )

        # 对齐长度（理论上已经对齐，这里防御性裁剪/填充）
        if len(pred_letters) < num_q:
            pred_letters += ["?"] * (num_q - len(pred_letters))
        if len(pred_letters) > num_q:
            pred_letters = pred_letters[:num_q]

        total = num_q
        correct = 0
        details = []

        for i in range(num_q):
            t_letter = target_letters[i]
            p_letter = pred_letters[i]
            is_correct = (p_letter == t_letter)
            if is_correct:
                correct += 1

            details.append({
                "question_index": stmt_infos[i]["question_index"],
                "answer_index": stmt_infos[i]["answer_index"],
                "target_letter": t_letter,
                "pred_letter": p_letter,
                "correct": bool(is_correct),
            })

        passage_acc = correct / total if total > 0 else None

        if passage_acc is not None:
            person_to_passage_accs[(group, name)].append(passage_acc)

        out_rec = {
            "group": group,
            "name": name,
            "passage_index": passage_index,
            "num_questions": total,
            "num_correct": correct,
            "accuracy": passage_acc,
            "details": details,
        }
        fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")

    fout.close()

    # 汇总每个人的平均正确率
    print("\n=== Per-person stats (average over passages, batched) ===")
    summary = []
    for (group, name), accs in sorted(person_to_passage_accs.items(), key=lambda x: x[0][0]):
        if not accs:
            continue
        avg_acc = sum(accs) / len(accs)
        summary.append({
            "group": group,
            "name": name,
            "num_passages": len(accs),
            "avg_accuracy": avg_acc,
        })
        print(
            f"group={group}, name={name}, passages={len(accs)}, "
            f"avg_acc={avg_acc * 100:.2f}%"
        )

    summary_path = output_path + ".summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n[DONE] passage-level (batched) results -> {output_path}")
    print(f"[DONE] per-person summary (batched)    -> {summary_path}")


# ----------------- CLI -----------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--statements", type=str, required=True, help="forget_statements.json (jsonlines)")
    ap.add_argument("--passages", type=str, required=True, help="forget_passages.json")
    ap.add_argument("--structure", type=str, required=True, help="forget_passages_structure.json")
    ap.add_argument("--output", type=str, required=True, help="output jsonl for passage-level results (batched)")
    return ap.parse_args()


def main():
    args = parse_args()

    mcqs = load_mcqs(args.statements)
    passages_by_key = load_passages(args.passages)
    structures = load_structure(args.structure)

    model, tokenizer = load_model_and_tokenizer(MODEL_PATH)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    eval_passage_consistency_batched(
        model,
        tokenizer,
        mcqs,
        passages_by_key,
        structures,
        args.output,
    )


if __name__ == "__main__":
    main()
