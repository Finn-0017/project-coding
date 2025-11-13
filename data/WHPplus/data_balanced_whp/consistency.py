#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Passage + MCQs consistency evaluation (batched).

- 对同一 passage 下的所有 MCQ 做一致性评估：
  - 使用 passage + MCQ 问题 + 选项喂给本地 Llama
  - 看模型是否选择了 passage 中实际使用的那条 statement 对应的选项
- 一次只问若干题（默认 batch_size=10），减少模型漏答
- 输出：
  - JSONL: 每个 passage 的 num_questions / num_correct / accuracy + 细节
  - summary.json: 每个人 (group, name) 的 avg_accuracy（按 passage 平均）
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
MAX_NEW_TOKENS = 128  # 每个 batch 生成上限


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


# ======================= batched generation ========================

def generate_option_letters_batched(model, tokenizer, passage: str, mcq_list, batch_size: int = 10):
    """
    按 batch_size 切分问题。默认 10 题/批。
    mcq_list: [{'question': str, 'choices': List[{'letter': 'A', 'text': '...'}, ...]}, ...]
    返回: 长度 == len(mcq_list) 的预测字母数组；没答的题用 '?'。
    """
    num_q = len(mcq_list)
    all_pred = []

    # 切 batch
    batches = []
    for i in range(0, num_q, batch_size):
        batches.append(mcq_list[i:i + batch_size])

    for b_idx, batch_mcqs in enumerate(batches):
        b_start = b_idx * batch_size + 1
        b_end = b_start + len(batch_mcqs) - 1

        # 构建问题文本（题号用全局的 1..num_q）
        q_blocks = []
        for i, mcq in enumerate(batch_mcqs, start=b_start):
            q_text = mcq["question"]
            choices = mcq["choices"]
            options_str = "\n".join(f"{c['letter']}. {c['text']}" for c in choices)
            block = f"Question {i}:\n{q_text}\nOptions:\n{options_str}\n"
            q_blocks.append(block)

        questions_text = "\n".join(q_blocks)

        # 答题卡模板
        answer_sheet = "\n".join(f"{i}. " for i in range(b_start, b_end + 1))

        user_content = (
            "You will answer the following multiple-choice questions ONLY using the passage.\n\n"
            f"PASSAGE:\n\"\"\"\n{passage}\n\"\"\"\n\n"
            f"QUESTIONS:\n{questions_text}\n"
            f"There are {b_end - b_start + 1} questions in this batch.\n\n"
            "Fill in the following answer sheet:\n"
            f"{answer_sheet}\n\n"
            "RULES:\n"
            "- For each question number i, write EXACTLY ONE capital letter A, B, C, D, or E.\n"
            "- Do NOT skip any question.\n"
            "- Do NOT output anything before or after the answer sheet.\n\n"
            "Example (3 questions):\n"
            "1. B\n"
            "2. A\n"
            "3. D\n\n"
            f"Now fill in ALL answers for questions {b_start} to {b_end}."
        )

        messages = [
            {
                "role": "system",
                "content": "You are a precise MCQ answering assistant. "
                           "Follow the answer sheet format strictly.",
            },
            {"role": "user", "content": user_content},
        ]

        input_ids = tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
        ).to(model.device)

        attention_mask = torch.ones_like(input_ids, dtype=torch.long)

        with torch.no_grad():
            out = model.generate(
                input_ids,
                attention_mask=attention_mask,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False,
                eos_token_id=tokenizer.eos_token_id,
                pad_token_id=tokenizer.pad_token_id,
            )

        gen = tokenizer.decode(out[0][input_ids.size(1):], skip_special_tokens=True)

        # 解析：按行匹配 "编号 + 选项字母"
        pattern = re.compile(r'^\s*(\d+)\s*[\.\):-]?\s*([A-E])\b')
        pred_dict = {}

        for line in gen.splitlines():
            m = pattern.match(line)
            if m:
                qnum = int(m.group(1))
                ans = m.group(2)
                pred_dict[qnum] = ans

        # 将当前 batch 的答案按题号顺序加入 all_pred
        for q in range(b_start, b_end + 1):
            if q in pred_dict:
                all_pred.append(pred_dict[q])
            else:
                all_pred.append("?")  # 没回答算错

    # 保证长度刚好等于 num_q
    if len(all_pred) > num_q:
        all_pred = all_pred[:num_q]
    while len(all_pred) < num_q:
        all_pred.append("?")

    return all_pred


# ======================= 数据加载 ========================

def load_mcqs(statements_path: str):
    """
    从 forget_statements.json (jsonlines) 加载 MCQ：
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
    print(f"[INFO] loaded MCQs for {len(mcqs)} groups")
    return mcqs


def load_passages(path: str):
    """
    从 forget_passages.json 加载 passage：
      passages[(group, passage_index)] = {
        'name': ...,
        'passage': ...
      }
    """
    passages = {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for rec in data:
        passages[(rec["group"], int(rec["passage_index"]))] = {
            "name": rec.get("name", ""),
            "passage": rec["passage"],
        }
    print(f"[INFO] loaded {len(passages)} passages")
    return passages


def load_structure(path: str):
    """
    从 forget_passages_structure.json 加载 structure：
      list[{
        'group', 'name', 'passage_index',
        'statements': [{'question_index': int, 'answer_index': int}, ...]
      }]
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"[INFO] loaded {len(data)} passage-structure records")
    return data


# ======================= 主评估逻辑 ========================

def eval_passage_consistency_batched(
    model,
    tokenizer,
    mcqs,
    passages_by_key,
    structures,
    output_path: str,
    only_group: str = None,
    only_passage_index: int = None,
    max_questions: int = None,
    batch_size: int = 10,
):
    """
    对每个 passage 一次性（分 batch）回答所有题：
      - target 选项来自 structure 的 answer_index 所指向的 choice.letter
      - 没回答的题用 '?'，一律算错
    """

    person_to_passage_accs = defaultdict(list)
    fout = open(output_path, "w", encoding="utf-8")

    for rec in tqdm(structures, desc="Eval passages (batched)", unit="passage"):
        group = rec["group"]
        name = rec.get("name", "")
        passage_index = int(rec["passage_index"])
        stmts = rec["statements"]

        # 过滤指定 group / passage
        if only_group is not None and group != only_group:
            continue
        if only_passage_index is not None and passage_index != only_passage_index:
            continue

        # 限制每个 passage 最多题目数（调试用）
        if max_questions is not None and len(stmts) > max_questions:
            stmts = stmts[:max_questions]

        key = (group, passage_index)
        if key not in passages_by_key:
            continue

        passage_text = passages_by_key[key]["passage"]

        # 组装这个 passage 对应的所有 MCQ
        mcq_list = []
        target_letters = []
        stmt_infos = []

        for s in stmts:
            q_idx = int(s["question_index"])
            ans_idx = int(s["answer_index"])

            mcq = mcqs[group].get(q_idx)
            if mcq is None:
                # 理论上不会发生，因为结构是从 statements 生成的
                continue

            choices = mcq["choices"]
            if not (0 <= ans_idx < len(choices)):
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
            continue

        # 模型预测
        pred_letters = generate_option_letters_batched(
            model, tokenizer, passage_text, mcq_list, batch_size=batch_size
        )

        # 安全对齐长度
        if len(pred_letters) > num_q:
            pred_letters = pred_letters[:num_q]
        while len(pred_letters) < num_q:
            pred_letters.append("?")

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

        acc = correct / total
        person_to_passage_accs[(group, name)].append(acc)

        fout.write(json.dumps({
            "group": group,
            "name": name,
            "passage_index": passage_index,
            "num_questions": total,
            "num_correct": correct,
            "accuracy": acc,
            "details": details,
        }, ensure_ascii=False) + "\n")

    fout.close()

    # 汇总 per-person 平均正确率
    summary = []
    print("\n=== Per-person stats (average over passages, batched) ===")
    for (group, name), accs in sorted(person_to_passage_accs.items()):
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


# ======================= CLI ========================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--statements", required=True, help="forget_statements.json (jsonlines)")
    ap.add_argument("--passages", required=True, help="forget_passages.json")
    ap.add_argument("--structure", required=True, help="forget_passages_structure.json")
    ap.add_argument("--output", required=True, help="output jsonl for passage-level results (batched)")

    ap.add_argument("--only-group", type=str, default=None, help="only evaluate this group id (optional)")
    ap.add_argument("--only-passage-index", type=int, default=None, help="only evaluate this passage index (optional)")
    ap.add_argument("--max-questions", type=int, default=None, help="max questions per passage (for debugging)")
    ap.add_argument("--batch-size", type=int, default=10, help="questions per batch when querying the model")

    return ap.parse_args()


def main():
    args = parse_args()

    mcqs = load_mcqs(args.statements)
    passages = load_passages(args.passages)
    structures = load_structure(args.structure)

    model, tokenizer = load_model_and_tokenizer(MODEL_PATH)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    eval_passage_consistency_batched(
        model,
        tokenizer,
        mcqs,
        passages,
        structures,
        args.output,
        only_group=args.only_group,
        only_passage_index=args.only_passage_index,
        max_questions=args.max_questions,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
