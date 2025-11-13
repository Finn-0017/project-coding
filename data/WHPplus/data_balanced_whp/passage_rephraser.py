import json
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from tqdm import tqdm

MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_JSON = Path("forget_passages.json")
OUTPUT_JSON = Path("forget_passages_rephrased.json")

print("Loading model...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)

# 🔧 处理 pad_token 警告：强制 pad_token = eos_token
tokenizer.pad_token = tokenizer.eos_token

model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    device_map="auto",
    torch_dtype="auto"
)

generator = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    device_map="auto",
    pad_token_id=tokenizer.eos_token_id,   # 🔧 彻底消掉警告
)

PROMPT_TEMPLATE = """You are editing biographical notes about a single historical figure.

Rewrite the following passage into one or two coherent paragraphs that read like a short, well-structured biography of this person.

Rules:
1. You MUST preserve all factual content from the original passage. Every fact should still appear in the rewritten version, even if rephrased. You may merge multiple sentences that express the same fact, but do not drop any distinct piece of information.
2. You may freely reorder, group, and reorganize the facts to improve logic and readability. For example, you can group together early life and education, membership in academies and patrons, works and achievements, later life and crises, and reputation or mentions by other authors.
3. Mention the person’s full name only in the first sentence. After that, refer to the person using pronouns (“he”, “she” or “they”) instead of repeating the full name.
4. Remove unnecessary repetition of the name and avoid listing facts in a flat, disconnected way. Instead, combine related facts into flowing sentences and paragraphs.
5. Use a neutral, factual tone. Do not add any new facts or interpretations that are not implied by the passage.
6. Output plain text only, with one or two paragraphs, no bullet points, no headings, no numbering.

Person’s name: {name}
Passage:
{passage}

Now produce the rewritten passage:
"""

def build_chat_input(name: str, passage: str) -> str:
    messages = [
        {"role": "system", "content": "You are a careful editor who rewrites biographical passages without losing any factual content."},
        {"role": "user", "content": PROMPT_TEMPLATE.format(name=name, passage=passage)}
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

def generate_rewrite(name: str, passage: str) -> str:
    prompt = build_chat_input(name, passage)
    out = generator(
        prompt,
        max_new_tokens=512,
        temperature=0.4,
        top_p=0.9,
        do_sample=True,
        eos_token_id=tokenizer.eos_token_id,
        pad_token_id=tokenizer.eos_token_id,   # 再加一层保险
    )[0]["generated_text"]

    # 剪掉 prompt
    if out.startswith(prompt):
        out = out[len(prompt):]
    return out.strip()

def main():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    print(f"Loaded {len(data)} passages.")

    out_data = []

    # 使用 tqdm 进度条
    for item in tqdm(data, desc="Rewriting passages", ncols=120):
        name = item["name"]
        passage = item["passage"]

        rewritten = generate_rewrite(name, passage)

        new_item = dict(item)
        new_item["rephrased_passage"] = rewritten
        out_data.append(new_item)

    # 保存
    OUTPUT_JSON.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nDone. Saved to: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()