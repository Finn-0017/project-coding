import json
import math
import random
import torch
import re
import argparse
import os
import sys
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_NAME = "Qwen/Qwen3-8B"
INPUT_FILE = "forget.json"
BASE_OUTPUT_FILE = "passages" # Will become passages_shard_0.json
BASE_MAPPING_FILE = "mapping" # Will become mapping_shard_0.json
TARGET_FACTS_PER_PASSAGE = 15

# Batch size per GPU
BATCH_SIZE = 32

# Save progress to disk every N batches to prevent total data loss on crash
SAVE_EVERY_BATCHES = 1

# Token limit
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
    parser = argparse.ArgumentParser(description="Generate biographical passages (Distributed).")
    parser.add_argument("--debugging", action="store_true", help="Process only 1 batch then exit.")
    
    # Distributed Processing Arguments
    parser.add_argument("--num_shards", type=int, default=1, help="Total number of GPU workers.")
    parser.add_argument("--shard_id", type=int, default=0, help="The ID of this worker (0 to num_shards-1).")
    parser.add_argument("--device_id", type=int, default=0, help="The CUDA device ID to use.")
    
    args = parser.parse_args()

    # Define Sharded Filenames
    output_filename = f"{BASE_OUTPUT_FILE}_shard_{args.shard_id}.json"
    mapping_filename = f"{BASE_MAPPING_FILE}_shard_{args.shard_id}.json"

    # --- Load Data & Resume Logic ---
    print(f"[Shard {args.shard_id}] Loading data...")
    full_data = load_data(INPUT_FILE)
    
    # 1. Sort keys to ensure deterministic slicing across all workers
    all_person_ids = sorted(list(full_data.keys()))
    total_people = len(all_person_ids)
    
    # 2. Slice the data for this shard
    shard_size = math.ceil(total_people / args.num_shards)
    start_idx = args.shard_id * shard_size
    end_idx = min(start_idx + shard_size, total_people)
    my_person_ids = all_person_ids[start_idx:end_idx]
    
    print(f"[Shard {args.shard_id}] Responsible for {len(my_person_ids)} people (Indices {start_idx} to {end_idx}).")

    # 3. Check for existing progress (Resume Capability)
    passages_output = {}
    mapping_output = {}
    
    if os.path.exists(output_filename):
        print(f"[Shard {args.shard_id}] Found existing output file. Loading to resume...")
        try:
            with open(output_filename, 'r', encoding="utf-8") as f:
                passages_output = json.load(f)
            # We also try to load mapping to keep them in sync
            if os.path.exists(mapping_filename):
                with open(mapping_filename, 'r', encoding="utf-8") as f:
                    mapping_output = json.load(f)
        except json.JSONDecodeError:
            print(f"[Shard {args.shard_id}] Warning: output file corrupted or empty. Starting from scratch.")

    # Identify which names are already done
    # Note: The output format keys by Name. The input keys by ID.
    # We need to map ID -> Name to check effectively, or simpler: 
    # Iterate through my_person_ids, get the name, check if in passages_output.
    
    ids_to_process = []
    skipped_count = 0
    
    for pid in my_person_ids:
        questions = full_data[pid]
        if not questions: continue
        p_name = questions[0].get("name", "Unknown")
        
        # If this person is already in our output dict, skip them
        if p_name in passages_output:
            skipped_count += 1
            continue
        ids_to_process.append(pid)

    print(f"[Shard {args.shard_id}] Resuming: Skipping {skipped_count} already done. {len(ids_to_process)} left to process.")

    if len(ids_to_process) == 0:
        print(f"[Shard {args.shard_id}] Work complete! Exiting.")
        return

    # --- Load Model (On Specific Device) ---
    print(f"[Shard {args.shard_id}] Loading model {MODEL_NAME} on cuda:{args.device_id}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    tokenizer.padding_side = "left" 
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # Force model to specific GPU
    device_str = f"cuda:{args.device_id}"
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype="auto",
        device_map={"": device_str} # Force all modules to this specific device
    )

    # --- Prepare Tasks ---
    print(f"[Shard {args.shard_id}] Preparing prompts...")
    tasks_queue = [] 

    for person_id in ids_to_process:
        questions = full_data[person_id]
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
            random.shuffle(choices_items)

            for c_idx, (choice_key, choice_text) in enumerate(choices_items):
                passage_idx = (q_idx + c_idx) % num_passages
                
                passages_facts[passage_idx].append({
                    "question": q_text, "answer": choice_text, "is_not": not_flag
                })
                
                passages_mapping[passage_idx].append({
                    "question": q_text, 
                    "selected_choice": choice_key, 
                    "selected_text": choice_text, 
                    "is_not_question": not_flag,
                    "all_choices": q_item["choices"]
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
            tasks_queue.append(task)

    print(f"[Shard {args.shard_id}] Total generated tasks: {len(tasks_queue)}")

    # --- Processing Loop ---
    if args.debugging:
        print(">>> DEBUGGING MODE ENABLED: Will stop after 1 batch <<<")

    # Helper to save
    def save_checkpoint():
        # Atomic write prevention: write to temp then rename
        # (Simplified here to direct write for readability, but robust enough for basic usage)
        with open(output_filename, 'w', encoding="utf-8") as f:
            json.dump(passages_output, f, indent=4, ensure_ascii=False)
        with open(mapping_filename, 'w', encoding="utf-8") as f:
            json.dump(mapping_output, f, indent=4, ensure_ascii=False)
        print(f"[Shard {args.shard_id}] Saved checkpoint.")

    batch_count = 0
    
    for i in tqdm(range(0, len(tasks_queue), BATCH_SIZE), desc=f"Shard {args.shard_id}", position=args.shard_id):
        batch_tasks = tasks_queue[i : i + BATCH_SIZE]
        batch_prompts = [t["prompt_text"] for t in batch_tasks]

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

        input_len = inputs.input_ids.shape[1]
        generated_tokens = outputs[:, input_len:]
        decoded_texts = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

        for task, text in zip(batch_tasks, decoded_texts):
            p_name = task["person_name"]
            p_id = task["person_id"]
            p_idx = task["passage_idx"]
            
            final_text = clean_output(text)

            if not final_text:
                continue

            if p_name not in passages_output:
                passages_output[p_name] = []
            passages_output[p_name].append(final_text)

            map_key = f"{p_id}_p{p_idx + 1}"
            mapping_output[map_key] = {
                "person": p_name,
                "facts_used": task["mapping_data"]
            }

        batch_count += 1

        # Check Debugging
        if args.debugging:
            save_checkpoint()
            break

        # Periodic Save
        if batch_count % SAVE_EVERY_BATCHES == 0:
            save_checkpoint()

    # Final Save
    save_checkpoint()
    print(f"[Shard {args.shard_id}] Finished!")

if __name__ == "__main__":
    main()
