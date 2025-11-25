import json
from pathlib import Path

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

INPUT_PATH = "forget_statements.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 8
DEVICE_MAP = "auto"


def build_prompt(question: str, text: str, statement: str) -> str:
    return f"""You are a strict factual semantic judge.

Question: {question}
Choice text: {text}
Full statement: {statement}

Does the "Full statement" express the same factual meaning as the combination of "Question" and "Choice text"?

Answer only "yes" or "no" (lowercase)."""


def load_model():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModelForCausalLM.from_pretrained(MODEL_PATH, device_map=DEVICE_MAP)
    gen_pipe = pipeline("text-generation", model=model, tokenizer=tokenizer, return_full_text=False)
    return gen_pipe


pipe = load_model()

def debug_first_10():
    input_path = Path(INPUT_PATH)

    samples = []

    # 收集 10 个 warning=true 的样本
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            obj = json.loads(line)
            for item in obj.get("items", []):
                question = item.get("question", "")
                for choice in item.get("choices", []):
                    if choice.get("warning") is True:
                        samples.append((question, choice.get("text", ""), choice.get("statement", "")))
                        if len(samples) >= 10:
                            break
            if len(samples) >= 10:
                break

    print(f"Collected {len(samples)} samples\n")

    # 对每条样本直接打印 prompt + 模型回复
    for i, (question, text, statement) in enumerate(samples):
        print("=" * 80)
        print(f"[Sample #{i+1}]")
        print("Question :", question)
        print("Text     :", text)
        print("Statement:", statement)

        prompt = build_prompt(question, text, statement)
        out = pipe(prompt, max_new_tokens=MAX_NEW_TOKENS, do_sample=False)[0]["generated_text"]

        print("\n--- Prompt Sent to Model ---")
        print(prompt)

        print("\n--- Raw Model Output ---")
        print(out)

        # 简易判断
        answer = out.strip().lower()
        if answer.startswith("yes") or "yes" in answer[:10]:
            pred = True
        elif answer.startswith("no") or "no" in answer[:10]:
            pred = False
        else:
            pred = "?? (unrecognized)"

        print("\n--- Parsed Answer ---")
        print(pred)

        print("\n")


if __name__ == "__main__":
    debug_first_10()