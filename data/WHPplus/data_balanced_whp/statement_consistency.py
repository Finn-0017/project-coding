import json
from pathlib import Path

from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

INPUT_PATH = "mcq_all.jsonl"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 4
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
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        device_map=DEVICE_MAP
    )
    gen_pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        return_full_text=False  # 让 generated_text 就是补全部分，避免再手动切
    )
    return gen_pipe


def judge_equivalence(pipe, question: str, text: str, statement: str) -> bool:
    prompt = build_prompt(question, text, statement)
    out = pipe(
        prompt,
        max_new_tokens=MAX_NEW_TOKENS,
        do_sample=False
    )[0]["generated_text"]

    answer = out.strip().lower()
    # 尽量强一点的判断规则
    if answer.startswith("yes"):
        return True
    if answer.startswith("no"):
        return False
    if "yes" in answer[:10]:
        return True
    if "no" in answer[:10]:
        return False
    return False


def main():
    input_path = Path(INPUT_PATH)

    print(f"Loading model from: {MODEL_PATH}")
    pipe = load_model()

    samples = []  # (question, text, statement)

    # 1. 收集所有 warning=true 的样本
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)
            for item in obj.get("items", []):
                question = item.get("question", "")
                for choice in item.get("choices", []):
                    if choice.get("warning") is True:
                        text = choice.get("text", "")
                        statement = choice.get("statement", "")
                        samples.append((question, text, statement))

    total = len(samples)
    print(f"Total warning=true samples: {total}")
    if total == 0:
        print("No warning=true samples, nothing to evaluate.")
        return

    # 2. 逐个用 LLaMA 判断
    correct = 0
    for question, text, statement in tqdm(samples, desc="Evaluating warning=true", ncols=100):
        if judge_equivalence(pipe, question, text, statement):
            correct += 1

    # 3. 输出结果
    print("\n===== Evaluation for warning=true =====")
    print(f"Total:   {total}")
    print(f"Correct: {correct}")
    print(f"Accuracy: {correct / total * 100:.2f} %")


if __name__ == "__main__":
    main()