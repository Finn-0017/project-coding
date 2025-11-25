import json
from pathlib import Path

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

# 与主脚本保持一致
INPUT_PATH = "forget_statements.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
DEVICE_MAP = "auto"
DTYPE = torch.float16
DEBUG_N = 10


def build_mc_prompt(question: str, choices, statement: str) -> str:
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


def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        device_map=DEVICE_MAP,
        torch_dtype=DTYPE
    )
    return tokenizer, model


def debug_warning_true_first_n():
    input_path = Path(INPUT_PATH)
    tokenizer, model = load_model_and_tokenizer()

    samples = []  # (question, choices, letter, statement)

    # 收集前 N 条 warning=true
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
                    if choice.get("warning", False) is True:
                        letter = choice.get("letter")
                        statement = choice.get("statement", "")
                        samples.append((question, choices, letter, statement))
                        if len(samples) >= DEBUG_N:
                            break
                if len(samples) >= DEBUG_N:
                    break
            if len(samples) >= DEBUG_N:
                break

    print(f"Collected {len(samples)} warning=true samples for debug.\n")

    for idx, (question, choices, letter, statement) in enumerate(samples, start=1):
        print("=" * 80)
        print(f"[Sample #{idx}]")
        print("Question:")
        print(question)
        print("\nOptions:")
        for c in choices:
            print(f"  {c['letter']}. {c['text']}")
        print("\nTarget letter:", letter)
        print("Statement:", statement)

        # 构造 chat prompt
        user_content = build_mc_prompt(question, choices, statement)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that answers multiple-choice questions. "
                    "When asked to choose an option, you must respond with the option letter only."
                )
            },
            {"role": "user", "content": user_content}
        ]

        if hasattr(tokenizer, "apply_chat_template"):
            prompt = tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
        else:
            prompt = "System: " + messages[0]["content"] + "\nUser: " + messages[1]["content"] + "\nAssistant:"

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=4,
                do_sample=False,
                temperature=0.0
            )

        gen_ids = outputs[0][inputs["input_ids"].shape[1]:]
        generated = tokenizer.decode(gen_ids, skip_special_tokens=True)
        ans = generated.strip().upper()

        # 解析第一个 A-Z 字母
        pred_letter = "?"
        for ch in ans:
            if "A" <= ch <= "Z":
                pred_letter = ch
                break

        print("\n--- Prompt Sent to Model ---")
        print(prompt)

        print("\n--- Raw Model Output ---")
        print(generated)

        print("\n--- Parsed Letter ---")
        print(f"pred_letter = {pred_letter}, match = {pred_letter == letter}")
        print("\n")


if __name__ == "__main__":
    debug_warning_true_first_n()
