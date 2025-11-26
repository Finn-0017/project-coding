import json
import math
import random
import torch
import re
import argparse
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_NAME = "Qwen/Qwen3-8B"
INPUT_FILE = "forget.json"
OUTPUT_FILE = "passages.json"
MAPPING_FILE = "mapping.json"
TARGET_FACTS_PER_PASSAGE = 15

# BATCH SIZE: Updated to 64 as requested.
# WARNING: This requires significant VRAM (likely A100 80GB). 
# If you get "CUDA Out of Memory", reduce this value (e.g., to 32 or 16).
BATCH_SIZE = 64

# Token limit (increased to allow reasoning models to "think" before outputting)
MAX_NEW_TOKENS = 2048 

def clean_output(text):
    """
    Removes the <think>...</think> block from the generated text
    and strips leading/trailing whitespace.
    """
    pattern = r"<think>.*?</think>"
    cleaned = re.sub(pattern, "", text, flags=re.DOTALL)
    return cleaned.strip()

def load_data(filepath):
    with open(filepath, 'r', encoding="utf-8") as f:
        return json.load(f)

def format_messages(person_name, facts_list):
    lines = []
    for fact in facts_list:
        q = fact["question"]
        ans = fact["answer"]
        is_not = fact["is_not"]

        if is_not:
            lines.append(
                f"- NEGATIVE FACT: The correct answer to \"{q}\" is \"{ans}\". "
                f"This implies \"{ans}\" does NOT apply to {person_name}."
            )
        else:
            lines.append(
                f"- POSITIVE FACT: The answer to \"{q}\" about {person_name} is \"{ans}\"."
            )

    formatted_facts = "\n".join(lines)
    content = (
        f"Rewrite these facts into a single coherent biographical passage (250-350 words). "
        f"Do not invent facts. Do not mention the Q&A format. \n\n"
        f"Facts:\n{formatted_facts}\n\n"
        f"Passage:"
    )
    return [{"role": "user", "content": content}]

def is_not_question(question_text: str) -> bool:
    q_lower = question_text.lower()
    patterns = [" not ", " not?", " not.", " not,", " not:"]
    return any(p in q_lower for p in patterns)

def main():
    # --- Parse Arguments ---
    parser = argparse.ArgumentParser(description="Generate biographical passages.")
    parser.add_argument("--debugging", action="store_true", help="If set, processes only the first batch and exits.")
    args = parser.parse_args()

    print(f"Loading model {MODEL_NAME}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    
    # Configure Padding for Batch Inference
    tokenizer.padding_side = "left" 
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # Load Model
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype="auto",
        device_map="auto"
    )

    data = load_data(INPUT_FILE)
    
    # --- Step 1: Pre-process ALL prompts ---
    print("Preparing all prompts...")
    all_tasks = [] 

    for person_id, questions in data.items():
        if not questions: continue
        
        person_name = questions[0].get("name", "Unknown")
        questions_list = list(questions)
        
        # Calculate stats
        total_facts = 0
        max_choices = 0
        for q_item in questions_list:
            n = len(q_item["choices"])
            total_facts += n
            max_choices = max(max_choices, n)
            
        if total_facts == 0: continue

        approx_passages = math.ceil(total_facts / TARGET_FACTS_PER_PASSAGE)
        num_passages = max(max_choices, approx_passages)

        passages_facts = [[] for _ in range(num_passages)]
        passages_mapping = [[] for _ in range(num_passages)]

        # Distribute facts
        for q_idx, q_item in enumerate(questions_list):
            q_text = q_item["question"]
            choices_items = list(q_item["choices"].items())
            not_flag = is_not_question(q_text)
            
            # Note: We shuffle choices for distribution, but the source choices dict remains valid
            random.shuffle(choices_items)

            for c_idx, (choice_key, choice_text) in enumerate(choices_items):
                passage_idx = (q_idx + c_idx) % num_passages
                
                passages_facts[passage_idx].append({
                    "question": q_text, "answer": choice_text, "is_not": not_flag
                })
                
                # --- MODIFIED MAPPING HERE ---
                passages_mapping[passage_idx].append({
                    "question": q_text, 
                    "selected_choice": choice_key, 
                    "selected_text": choice_text, 
                    "is_not_question": not_flag,
                    "all_choices": q_item["choices"] # <--- Added this field
                })

        # Create Prompts
        for p_idx in range(num_passages):
            if not passages_facts[p_idx]: continue
            
            msgs = format_messages(person_name, passages_facts[p_idx])
            full_prompt = tokenizer.apply_chat_template(
                msgs, tokenize=False, add_generation_prompt=True
            )
            
            task = {
                "person_id": person_id,
                "person_name": person_name,
                "passage_idx": p_idx,
                "mapping_data": passages_mapping[p_idx],
                "prompt_text": full_prompt
            }
            all_tasks.append(task)

    # --- Step 2: Batch Generation ---
    passages_output = {}
    mapping_output = {}

    print(f"Total passages to generate: {len(all_tasks)}")
    print(f"Processing in batches of {BATCH_SIZE}...")
    if args.debugging:
        print(">>> DEBUGGING MODE ENABLED: Will stop after 1 batch <<<")

    # Process in chunks
    for i in tqdm(range(0, len(all_tasks), BATCH_SIZE), desc="Processing Batches", unit="batch"):
        batch_tasks = all_tasks[i : i + BATCH_SIZE]
        batch_prompts = [t["prompt_text"] for t in batch_tasks]

        # Tokenize batch
        inputs = tokenizer(
            batch_prompts, 
            return_tensors="pt", 
            padding=True, 
            truncation=True,
            max_length=4096 
        ).to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False, 
                pad_token_id=tokenizer.pad_token_id
            )

        # Decode batch
        input_len = inputs.input_ids.shape[1]
        generated_tokens = outputs[:, input_len:]
        decoded_texts = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

        # Save results
        for task, text in zip(batch_tasks, decoded_texts):
            p_name = task["person_name"]
            p_id = task["person_id"]
            p_idx = task["passage_idx"]
            
            # --- CLEANING STEP ---
            final_text = clean_output(text)

            if not final_text:
                if args.debugging:
                    print(f"DEBUG WARN: Empty output for {p_name} after cleaning.")
                continue

            # Store Passage
            if p_name not in passages_output:
                passages_output[p_name] = []
            passages_output[p_name].append(final_text)

            # Store Mapping
            map_key = f"{p_id}_p{p_idx + 1}"
            mapping_output[map_key] = {
                "person": p_name,
                "facts_used": task["mapping_data"]
            }

        # Check for Debugging Flag
        if args.debugging:
            print("Debugging mode: Stopping after first batch.")
            break

    # Save to disk
    print(f"Saving {len(passages_output)} entries to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding="utf-8") as f:
        json.dump(passages_output, f, indent=4, ensure_ascii=False)

    with open(MAPPING_FILE, 'w', encoding="utf-8") as f:
        json.dump(mapping_output, f, indent=4, ensure_ascii=False)

    print("Done!")

if __name__ == "__main__":
    main()
    