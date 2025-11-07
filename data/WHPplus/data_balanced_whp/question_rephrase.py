import json
import random
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# ==============================
# CONFIG
# ==============================
INPUT_PATH = "forget_dedup.json"
OUTPUT_PATH = "forget_dedup_statement.json"
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct"
USE_ANSWER = True       # factual mode
SAVE_EVERY = 20         # autosave frequency
MAX_NEW_TOKENS = 80

# ==============================
# LOAD MODEL
# ==============================
print("Loading model from local path...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
    device_map="auto"
)
pipe = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    device_map="auto",
    torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
    temperature=0.0
)

# ==============================
# PROMPT TEMPLATES
# ==============================
SYSTEM_PROMPT = (
    "You are a precise factual rewriter.\n"
    "Given one multiple-choice question about a person, the available options, "
    "and the correct answer, write ONE concise factual statement that exactly "
    "expresses the same factual meaning as the correct answer.\n"
    "Do not add new information. Output one short sentence ending with a period."
)

USER_TEMPLATE = (
    "Question: {question}\n"
    "Person: {name}\n"
    "Options:\n{options}\n"
    "Correct Answer: {correct_ans}\n"
    "Now, rewrite this information as one concise factual statement."
)

# ==============================
# GENERATION FUNCTION
# ==============================
def generate_statement(entry):
    name = entry["name"].strip()
    question = entry["question"].strip()
    choices = entry["choices"]
    correct = entry["answer"]

    if not USE_ANSWER:
        pool = [k for k in choices.keys() if k != correct]
        correct = random.choice(pool)

    opt_str = "\n".join([f"{k}. {v}" for k, v in choices.items()])
    correct_text = choices[correct].strip()

    prompt = USER_TEMPLATE.format(
        question=question, name=name, options=opt_str, correct_ans=correct_text
    )

    conversation = f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n{SYSTEM_PROMPT}\n" \
                   f"<|start_header_id|>user<|end_header_id|>\n{prompt}\n<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"

    result = pipe(
        conversation,
        max_new_tokens=MAX_NEW_TOKENS,
        do_sample=False,
        eos_token_id=tokenizer.eos_token_id
    )[0]["generated_text"]

    # Extract only new generation part
    if "<|start_header_id|>assistant<|end_header_id|>" in result:
        result = result.split("<|start_header_id|>assistant<|end_header_id|>")[-1]
    result = result.strip()

    # fallback if malformed
    if name not in result or correct_text not in result:
        result = f"{name} {correct_text}."

    return {
        "name": name,
        "question": question,
        "answer": entry["answer"],
        "use_answer": USE_ANSWER,
        "statement": result
    }

# ==============================
# MAIN
# ==============================
def main():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = []
    for i, entry in enumerate(tqdm(data, desc="Generating factual statements")):
        try:
            res = generate_statement(entry)
            results.append(res)
        except Exception as e:
            print(f"[Error on {entry.get('name','?')}] {e}")
            continue

        if (i + 1) % SAVE_EVERY == 0:
            with open(OUTPUT_PATH, "w", encoding="utf-8") as fout:
                json.dump(results, fout, indent=4, ensure_ascii=False)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as fout:
        json.dump(results, fout, indent=4, ensure_ascii=False)

    print(f"\n✅ Done! {len(results)} statements saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
