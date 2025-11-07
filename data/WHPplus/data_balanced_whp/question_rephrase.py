import json
import random
import argparse
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# ==============================
# MAIN FUNCTION
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Generate factual statements from MCQs using local Llama model.")
    parser.add_argument("--input", type=str, default="forget_dedup.json", help="Input JSON file path")
    parser.add_argument("--output", type=str, default="forget_dedup_statement.json", help="Output JSON file path")
    parser.add_argument("--model_path", type=str,
                        default="/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659",
                        help="Local model path")
    parser.add_argument("--use_answer", type=bool, default = True, help="Use the correct answer (factual mode)")
    parser.add_argument("--save_every", type=int, default=20, help="Autosave every N items")
    parser.add_argument("--max_new_tokens", type=int, default=80)
    args = parser.parse_args()

    # ==============================
    # LOAD MODEL
    # ==============================
    print(f"🔹 Loading model from: {args.model_path}")
    tokenizer = AutoTokenizer.from_pretrained(args.model_path)
    model = AutoModelForCausalLM.from_pretrained(
        args.model_path,
        torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
        device_map="auto",
        low_cpu_mem_usage=True
    )
    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        device_map="auto",
        torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
        temperature=0.0
    )

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

        if not args.use_answer:
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
            max_new_tokens=args.max_new_tokens,
            do_sample=False,
            eos_token_id=tokenizer.eos_token_id
        )[0]["generated_text"]

        if "<|start_header_id|>assistant<|end_header_id|>" in result:
            result = result.split("<|start_header_id|>assistant<|end_header_id|>")[-1]
        result = result.strip()

        correct_text = choices[entry["answer"]] if args.use_answer else choices[correct]
        if name not in result or correct_text not in result:
            result = f"{name} {correct_text}."

        return {
            "name": name,
            "question": question,
            "answer": entry["answer"],
            "use_answer": args.use_answer,
            "statement": result
        }

    # ==============================
    # MAIN LOOP
    # ==============================
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = []
    for i, entry in enumerate(tqdm(data, desc="Generating factual statements")):
        try:
            res = generate_statement(entry)
            results.append(res)
        except Exception as e:
            print(f"[Error on {entry.get('name','?')}] {e}")
            continue

        if (i + 1) % args.save_every == 0:
            with open(args.output, "w", encoding="utf-8") as fout:
                json.dump(results, fout, indent=4, ensure_ascii=False)

    with open(args.output, "w", encoding="utf-8") as fout:
        json.dump(results, fout, indent=4, ensure_ascii=False)

    print(f"\n✅ Done! {len(results)} statements saved to {args.output}")

if __name__ == "__main__":
    main()
