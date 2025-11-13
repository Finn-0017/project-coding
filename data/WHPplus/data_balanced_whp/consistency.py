#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Eval: passage + multiple MCQs in one shot → N letters

功能：
- 一次 passage 内所有题一起问（batched）
- 支持 --only-group
- 支持 --only-passage-index
- 支持 --max-questions 限制题目数量（比如 20）
- 输出 passage-level + person-level accuracy
"""

import argparse
import json
import os
import re
from collections import defaultdict

import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

# ======================= 本地模型路径 ========================
MODEL_PATH = (
    "/rds/user/xy319/hpc-work/projects/project-coding/"
    "hf_models/models--meta-llama--Llama-3.1-8B-Instruct/"
    "snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
)
MAX_NEW_TOKENS = 64


# ======================= 模型加载 ========================

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


def generate_option_letters_batched(model, tokenizer, passage: str, mcq_list):
    """
    mcq_list: [{'question':..., 'choices': [...]}]
    返回: 长度 == len(mcq_list) 的预测字母数组
    """
    num_q = len(mcq_list)

    # ===== 1. 构建 prompt =====
    q_blocks = []
    for i, mcq in enumerate(mcq_list, start=1):
        q_text = mcq["question"]
        choices = mcq["choices"]
        options_str = "\n".join(f"{c['letter']}. {c['text']}" for c in choices)
        block = f"Question {i}:\n{q_text}\nOptions:\n{options_str}\n"
        q_blocks.append(block)

    all_questions_text = "\n".join(q_blocks)

    user_content = (
        "You will answer multiple-choice questions ONLY using information in the passage.\n\n"
        f"Passage:\n\"\"\"\n{passage}\n\"\"\"\n\n"
        f"{all_questions_text}\n"
        "Now answer ALL questions at once.\n"
        "For each question i, write a separate line in the format:\n"
        "i. X. (where X is one of A, B, C, D, or E)\n"
        "For example:\n"
        "1. B. ...\n"
        "2. A. ...\n"
        "3. D. ...\n"
        "You may optionally add a short explanation after the letter, "
        "but the letter must appear immediately after the question number."
    )

    messages = [
        {
            "role": "system",
            "content": "You are a precise MCQ answering assistant. "
                       "For each question, you MUST start the line with "
                       "the question number and a single answer letter.",
        },
        {"role": "user", "content": user_content},
    ]

    input_ids = tokenizer.apply_chat_template(
        messages, add_generation_prompt=True, return_tensors="pt"
    ).to(model.device)

    attention_mask = torch.ones_like(input_ids, dtype=torch.long)

    with torch.no_grad():
        out = model.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=64,
            do_sample=False,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.pad_token_id,
        )

    gen = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True).strip()

    # debug 用：
    print("=== RAW GEN ===")
    print(gen)
    print("=== END GEN ===")

    # ===== 2. 优先按 “编号 + 字母” 的 pattern 抓答案 =====
    letters = []
    pattern = re.compile(r'^\s*\d+\s*[\.\):-]?\s*([A-E])\b')

    for line in gen.splitlines():
        m = pattern.match(line)
        if m:
            letters.append(m.group(1))
        if len(letters) >= num_q:
            break

    # ===== 3. 如果编号模式不够，再 fallback：全局扫 A–E =====
    if len(letters) < num_q:
        for ch in gen:
            if ch in "ABCDE":
                letters.append(ch)
            if len(letters) >= num_q:
                break

    # ===== 4. 长度对齐 =====
    while len(letters) < num_q:
        letters.append("?")

    return letters[:num_q]


# ======================= 数据加载 ========================

def load_mcqs(statements_path: str):
    mcqs = defaultdict(dict)
    with open(statements_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
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
    return mcqs


def load_passages(path: str):
    passages = {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for rec in data:
        passages[(rec["group"], int(rec["passage_index"]))] = {
            "name": rec.get("name", ""),
            "passage": rec["passage"],
        }
    return passages


def load_structure(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ======================= 主逻辑 ========================

def eval_passage_consistency_batched(
    model,
    tokenizer,
    mcqs,
    passages_by_key,
    structures,
    output_path: str,
    only_group=None,
    only_passage_index=None,
    max_questions=None,
):
    person_to_passage_accs = defaultdict(list)
    fout = open(output_path, "w", encoding="utf-8")

    for rec in tqdm(structures, desc="Eval passages (batched)"):
        group = rec["group"]
        name = rec.get("name", "")
        passage_index = int(rec["passage_index"])
        stmts = rec["statements"]

        # ------------------ filtering ------------------
        if only_group is not None and group != only_group:
            continue
        if only_passage_index is not None and passage_index != only_passage_index:
            continue

        # ------------------ limit questions ------------------
        if max_questions is not None and len(stmts) > max_questions:
            stmts = stmts[:max_questions]

        key = (group, passage_index)
        if key not in passages_by_key:
            continue

        passage_text = passages_by_key[key]["passage"]

        # ------------------ build mcq list ------------------
        mcq_list = []
        target_letters = []
        stmt_infos = []

        for s in stmts:
            q_idx = int(s["question_index"])
            ans_idx = int(s["answer_index"])
            mcq = mcqs[group].get(q_idx)
            if mcq is None:
                continue

            choices = mcq["choices"]
            if not (0 <= ans_idx < len(choices)):
                continue

            mcq_list.append({"question": mcq["question"], "choices": choices})
            target_letters.append(choices[ans_idx]["letter"].upper())
            stmt_infos.append({"question_index": q_idx, "answer_index": ans_idx})

        num_q = len(mcq_list)
        if num_q == 0:
            continue

        # ------------------ model prediction ------------------
        pred_letters = generate_option_letters_batched(
            model, tokenizer, passage_text, mcq_list
        )

        correct = sum(p == t for p, t in zip(pred_letters, target_letters))
        acc = correct / num_q

        person_to_passage_accs[(group, name)].append(acc)

        fout.write(json.dumps({
            "group": group,
            "name": name,
            "passage_index": passage_index,
            "num_questions": num_q,
            "num_correct": correct,
            "accuracy": acc,
            "details": [
                {
                    "question_index": stmt_infos[i]["question_index"],
                    "answer_index": stmt_infos[i]["answer_index"],
                    "target_letter": target_letters[i],
                    "pred_letter": pred_letters[i],
                    "correct": pred_letters[i] == target_letters[i],
                }
                for i in range(num_q)
            ],
        }, ensure_ascii=False) + "\n")

    fout.close()

    # ------------------ summary ------------------
    summary = []
    for (group, name), accs in sorted(person_to_passage_accs.items()):
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

    with open(output_path + ".summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


# ======================= CLI ========================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--statements", required=True)
    ap.add_argument("--passages", required=True)
    ap.add_argument("--structure", required=True)
    ap.add_argument("--output", required=True)

    ap.add_argument("--only-group", type=str, default=None)
    ap.add_argument("--only-passage-index", type=int, default=None)
    ap.add_argument("--max-questions", type=int, default=None)

    return ap.parse_args()


def main():
    args = parse_args()

    mcqs = load_mcqs(args.statements)
    passages = load_passages(args.passages)
    structures = load_structure(args.structure)

    model, tokenizer = load_model_and_tokenizer(MODEL_PATH)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    eval_passage_consistency_batched(
        model, tokenizer,
        mcqs, passages, structures,
        args.output,
        only_group=args.only_group,
        only_passage_index=args.only_passage_index,
        max_questions=args.max_questions,
    )


if __name__ == "__main__":
    main()
