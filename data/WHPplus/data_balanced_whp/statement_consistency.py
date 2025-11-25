import json
import random
from pathlib import Path

import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM


# ========= 配置 =========
INPUT_PATH = "forget_statements.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
SAMPLE_SIZE_WARNING_FALSE = 1000
DEVICE_MAP = "auto"
DTYPE = torch.float16
SEED = 42


# ========= 构造 prompt =========

def build_mc_prompt(question: str, choices, statement: str) -> str:
    """
    choices: list[dict]，每个 dict 至少包含 {"letter": "A", "text": "Rome"}
    """
    options_str = "\n".join([f"{c['letter']}. {c['text']}" for c in choices])
    return f"""You are given a multiple-choice question and several answer options.

Question:
{question}

Options:
{options_str}

You are also given an explanation sentence describing the correct answer:

Explanation: {statement}

Based ONLY on the explanation, which option (A, B, C, D, E, ...) does it correspond to?
Reply with the option letter only, in uppercase, with no other text.
"""


# ========= 模型加载 =========

def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        device_map=DEVICE_MAP,
        torch_dtype=DTYPE
    )
    return tokenizer, model


# ========= 利用 statement 预测选项字母 =========

def predict_letter_from_statement(question: str, choices, statement: str,
                                  tokenizer, model) -> str:
    """
    返回模型预测的选项字母（A/B/C/...），如果没识别到，返回 "?"。
    """
    user_content = build_mc_prompt(question, choices, statement)

    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant that answers multiple-choice questions. "
                "When asked to choose an option, you must respond with the option letter only."
            )
        },
        {
            "role": "user",
            "content": user_content
        }
    ]

    # 使用 chat 模板
    if hasattr(tokenizer, "apply_chat_template"):
        prompt = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
    else:
        # 兜底：没有 chat_template 的老版本 tokenizer
        prompt = "System: " + messages[0]["content"] + "\nUser: " + messages[1]["content"] + "\nAssistant:"

    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=4,
            do_sample=False,
            temperature=0.0
        )

    # 只取新生成部分
    gen_ids = outputs[0][inputs["input_ids"].shape[1]:]
    generated = tokenizer.decode(gen_ids, skip_special_tokens=True)

    ans = generated.strip().upper()

    # 从输出中找到第一个 A-Z 字母当作答案
    for ch in ans:
        if "A" <= ch <= "Z":
            return ch

    return "?"


# ========= 主评估逻辑 =========

def main():
    random.seed(SEED)
    input_path = Path(INPUT_PATH)

    print(f"Loading model from: {MODEL_PATH}")
    tokenizer, model = load_model_and_tokenizer()

    warning_true_samples = []   # 每项: (question, choices, letter, statement)
    warning_false_samples = []  # 同上

    # 1. 遍历文件，收集样本
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            for item in obj.get("items", []):
                question = item.get("question", "")
                choices = item.get("choices", [])
                for choice in choices:
                    letter = choice.get("letter")
                    statement = choice.get("statement", "")
                    warning = choice.get("warning", False)

                    sample = (question, choices, letter, statement)

                    if warning:
                        warning_true_samples.append(sample)
                    else:
                        warning_false_samples.append(sample)

    print(f"Total warning=true samples: {len(warning_true_samples)}")
    print(f"Total warning=false samples: {len(warning_false_samples)}\n")

    # 2. 评估 warning=true（全部）
    wt_total = len(warning_true_samples)
    wt_correct = 0

    if wt_total > 0:
        for question, choices, letter, statement in tqdm(
            warning_true_samples,
            desc="Evaluating warning=true (all)",
            ncols=100
        ):
            pred_letter = predict_letter_from_statement(
                question, choices, statement, tokenizer, model
            )
            if pred_letter == letter:
                wt_correct += 1

    # 3. 评估 warning=false（随机抽样）
    wf_total_all = len(warning_false_samples)
    wf_sample_size = min(SAMPLE_SIZE_WARNING_FALSE, wf_total_all)
    wf_correct = 0

    if wf_sample_size > 0:
        wf_sample = random.sample(warning_false_samples, wf_sample_size)

        for question, choices, letter, statement in tqdm(
            wf_sample,
            desc=f"Evaluating warning=false (sample {wf_sample_size})",
            ncols=100
        ):
            pred_letter = predict_letter_from_statement(
                question, choices, statement, tokenizer, model
            )
            if pred_letter == letter:
                wf_correct += 1

    # 4. 输出结果
    print("\n===== Final Evaluation =====")

    print("\n-- warning=true --")
    print(f"Total:   {wt_total}")
    print(f"Correct: {wt_correct}")
    if wt_total > 0:
        print(f"Accuracy: {wt_correct / wt_total * 100:.2f} %")
    else:
        print("No warning=true samples.")

    print("\n-- warning=false (sampled) --")
    print(f"Total available: {wf_total_all}")
    print(f"Sample size:     {wf_sample_size}")
    print(f"Correct in sample: {wf_correct}")
    if wf_sample_size > 0:
        print(f"Sample accuracy: {wf_correct / wf_sample_size * 100:.2f} %")
    else:
        print("No warning=false samples or sample size = 0.")


if __name__ == "__main__":
    main()
