import random

SAMPLE_SIZE = 1000   # 你要抽的数量

def evaluate_warning_false_subset(input_path: Path, pipe):
    """
    从 warning=false 中随机抽 SAMPLE_SIZE 个，用 LLaMA 判断语义是否一致。
    返回 (count, correct)
    """

    candidates = []   # 存放 (question, choice)

    # 先收集所有 warning=false 的候选
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            for item in obj.get("items", []):
                q = item.get("question", "")
                for choice in item.get("choices", []):
                    if choice.get("warning") is False:
                        candidates.append((q, choice))

    print(f"Total warning=false samples available: {len(candidates)}")

    # 随机抽取 SAMPLE_SIZE
    sample = random.sample(candidates, min(SAMPLE_SIZE, len(candidates)))

    print(f"Sampling {len(sample)} warning=false items for accuracy test...\n")

    correct = 0

    # 用 tqdm 跑 LLM
    for q, choice in tqdm(sample, desc="Evaluating sampled warning=False", ncols=100):
        text = choice.get("text", "")
        statement = choice.get("statement", "")

        is_correct = judge_equivalence(pipe, q, text, statement)
        if is_correct:
            correct += 1

    return len(sample), correct

import json
from pathlib import Path
from tqdm import tqdm

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# ======= 配置区域 =======

INPUT_PATH = "forget_statements.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 4 
DEVICE_MAP = "auto"   

# ======= prompt builder =======

def build_prompt(question: str, text: str, statement: str) -> str:
    return f"""You are a strict factual semantic judge.

Question: {question}
Choice text: {text}
Full statement: {statement}

Does the "Full statement" express the same factual meaning as the combination of "Question" and "Choice text"?

Answer only "yes" or "no" (lowercase)."""


# ======= 模型加载 =======

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
    )
    return gen_pipe


def judge_equivalence(pipe, question: str, text: str, statement: str) -> bool:
    prompt = build_prompt(question, text, statement)
    out = pipe(
        prompt,
        max_new_tokens=MAX_NEW_TOKENS,
        do_sample=False
    )[0]["generated_text"]
    new_text = out[len(prompt):].strip().lower()

    if "yes" in new_text[:10]:
        return True
    if "no" in new_text[:10]:
        return False
    return False  # 保守错误处理


# ======= 主逻辑 =======

def main():
    input_path = Path(INPUT_PATH)

    print(f"Loading LLaMA model from: {MODEL_PATH}")
    pipe = load_model()

    total = 0
    correct = 0

    total_warning_true = 0
    correct_warning_true = 0
    total_warning_false = 0

    # 第一次扫描：统计 warning=true 数量，便于 tqdm total 设定
    warning_true_samples = []

    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)

            for item in obj.get("items", []):
                q = item.get("question", "")
                for choice in item.get("choices", []):
                    if choice.get("warning") is True:
                        warning_true_samples.append((q, choice))

    print(f"Total warning=True samples: {len(warning_true_samples)}\n")

    # 第二次扫描：正式评估
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)

            for item in obj.get("items", []):
                question = item.get("question", "")
                for choice in item.get("choices", []):
                    total += 1
                    warning = choice.get("warning", False)

                    if not warning:
                        total_warning_false += 1
                        correct += 1
                    else:
                        # 延后统一做，在下面 tqdm 里
                        pass

    # tqdm 处理耗时 LLM 调用
    for question, choice in tqdm(warning_true_samples, desc="Evaluating warning=True", ncols=100):
        total_warning_true += 1
        text = choice.get("text", "")
        statement = choice.get("statement", "")

        is_correct = judge_equivalence(pipe, question, text, statement)

        if is_correct:
            correct += 1
            correct_warning_true += 1

    # ===== 输出结果 =====
    print("\n====== Final Evaluation ======")
    print(f"Total choices: {total}")
    print(f"Total correct: {correct}")
    print(f"Overall accuracy: {correct / total * 100:.2f}%")
    print()
    print(f"warning=False count: {total_warning_false} (all counted as correct)")
    print(f"warning=True count: {total_warning_true}")
    print(f"warning=True correct (LLM): {correct_warning_true}")
    if total_warning_true > 0:
        print(f"warning=True accuracy: {correct_warning_true / total_warning_true * 100:.2f}%")

    # ========== 额外实验：抽样验证 warning=false 的真实准确率 ==========
    print("\n\n===== Running additional test: sampling warning=false accuracy =====")
    sample_total, sample_correct = evaluate_warning_false_subset(input_path, pipe)
    print(f"\nSample size: {sample_total}")
    print(f"Correct: {sample_correct}")
    print(f"Warning=false TRUE accuracy (sampled): {sample_correct / sample_total * 100:.2f}%")

if __name__ == "__main__":
    main()
