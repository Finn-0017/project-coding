import json
from pathlib import Path

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# ======= 配置区域 =======

INPUT_PATH = "forget_statements.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
MAX_NEW_TOKENS = 4 
DEVICE_MAP = "auto"   

# ======= 构造 prompt & 调用模型 =======

def build_prompt(question: str, text: str, statement: str) -> str:
    """
    让模型只做语义等价判断，并且只回答 yes / no。
    """
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

    # 用 text-generation 即可；如果你自己有 chat 模板，也可以自己改。
    gen_pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer
    )
    return gen_pipe


def judge_equivalence(pipe, question: str, text: str, statement: str) -> bool:
    """
    调用 LLaMA，返回 True/False （语义是否一致）。
    """
    prompt = build_prompt(question, text, statement)
    out = pipe(
        prompt,
        max_new_tokens=MAX_NEW_TOKENS,
        do_sample=False
    )[0]["generated_text"]

    # 只截取 prompt 后面的新输出部分，避免把原始 prompt 含进去
    new_text = out[len(prompt):].strip().lower()

    # 简单粗暴：前几个字符里包含 "yes" 就算 yes
    if "yes" in new_text[:10]:
        return True
    if "no" in new_text[:10]:
        return False

    # fall back：如果没看出来，保守当成错误
    return False


def main():
    input_path = Path(INPUT_PATH)

    print(f"Loading model from: {MODEL_PATH}")
    pipe = load_model()

    total = 0
    correct = 0

    # 额外统计 warning 相关
    total_warning_true = 0
    correct_warning_true = 0

    total_warning_false = 0
    # warning=false 全部视为正确

    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            for item in obj.get("items", []):
                question = item.get("question", "")
                for choice in item.get("choices", []):
                    total += 1
                    warning = choice.get("warning", False)

                    if not warning:
                        # warning == False，直接记作正确
                        total_warning_false += 1
                        correct += 1
                        continue

                    # warning == True，需要 LLM 判定
                    total_warning_true += 1
                    text = choice.get("text", "")
                    statement = choice.get("statement", "")

                    is_correct = judge_equivalence(pipe, question, text, statement)
                    if is_correct:
                        correct += 1
                        correct_warning_true += 1

    # ======= 结果输出 =======
    print("====== Evaluation Result ======")
    print(f"Total choices: {total}")
    print(f"Total correct (auto + LLM): {correct}")
    print(f"Overall accuracy: {correct / total * 100:.2f}%")
    print()
    print(f"warning = False: count = {total_warning_false}, treated as correct = {total_warning_false}")
    print(f"warning = True : count = {total_warning_true}, LLM-correct = {correct_warning_true}")
    if total_warning_true > 0:
        print(f"warning=True accuracy (LLM judged): {correct_warning_true / total_warning_true * 100:.2f}%")
    else:
        print("warning=True samples: 0")


if __name__ == "__main__":
    main()
