import json
import torch
import re
import argparse
import os
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_NAME = "Qwen/Qwen3-8B" 
PASSAGES_FILE = "passages.json"
MAPPING_FILE = "mapping.json"
RESULTS_FILE = "verification_results.json"
BATCH_SIZE = 64
MAX_NEW_TOKENS = 512 # Shorter, we only need a single letter answer

def clean_output(text):
    """Removes <think> tags and cleanups."""
    pattern = r"<think>.*?</think>"
    cleaned = re.sub(pattern, "", text, flags=re.DOTALL)
    return cleaned.strip()

def extract_answer_letter(text):
    """
    Attempts to extract A, B, C, D, or E from the model's response.
    Prioritizes explicit "Answer: X" format, then looks for the first single letter.
    """
    text = clean_output(text).upper()
    
    # Check for "Answer: A" pattern
    match = re.search(r"ANSWER:?\s*\(?([A-E])\)?", text)
    if match:
        return match.group(1)
    
    # Fallback: Look for just a letter at the start of the string
    match = re.match(r"^\s*\(?([A-E])\)?", text)
    if match:
        return match.group(1)

    # Fallback: Look for the last standalone letter (often models yap then say "So, B")
    matches = re.findall(r"\b([A-E])\b", text)
    if matches:
        return matches[-1]
        
    return "UNKNOWN"

def load_json(filepath):
    with open(filepath, 'r', encoding="utf-8") as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debugging", action="store_true", help="Run only 1 batch")
    args = parser.parse_args()

    # --- Load Data ---
    print("Loading data...")
    passages_data = load_json(PASSAGES_FILE)
    mapping_data = load_json(MAPPING_FILE)

    # --- Load Model ---
    print(f"Loading model {MODEL_NAME}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    tokenizer.padding_side = "left"
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype="auto",
        device_map="auto",
        trust_remote_code=True
    )

    # --- Prepare Verification Tasks ---
    print("Preparing verification tasks...")
    tasks = []

    # Iterate through the mapping file (which contains the logic of what facts went where)
    for map_key, entry in mapping_data.items():
        person_name = entry["person"]
        
        # Extract passage index from key (e.g., "10234_p1" -> index 0)
        # Assuming keys end in _pX where X is 1-based index
        try:
            p_suffix = map_key.split('_')[-1] # "p1"
            p_index = int(p_suffix[1:]) - 1   # 0
            
            # Retrieve the specific generated passage
            if person_name not in passages_data:
                continue
            if p_index >= len(passages_data[person_name]):
                continue
                
            passage_text = passages_data[person_name][p_index]
        except Exception as e:
            print(f"Skipping {map_key}: {e}")
            continue

        # Create a check for every fact in this passage
        for fact in entry["facts_used"]:
            question = fact["question"]
            options = fact["all_choices"] # Dict { "A": "...", "B": "..." }
            ground_truth = fact["selected_choice"]
            is_not = fact["is_not_question"]

            # Format options
            options_str = ""
            sorted_keys = sorted(options.keys()) # Ensure A, B, C order
            for k in sorted_keys:
                options_str += f"{k}) {options[k]}\n"

            # Construct Prompt
            # We explicitly ask the model to look at the passage.
            prompt_content = (
                f"Read the following biographical passage carefully:\n\n"
                f"\"{passage_text}\"\n\n"
                f"Based ONLY on the passage above, answer the following multiple-choice question. "
                f"Output ONLY the letter of the correct answer (e.g., A).\n\n"
                f"Question: {question}\n"
                f"Options:\n{options_str}\n"
                f"Answer:"
            )

            messages = [{"role": "user", "content": prompt_content}]
            full_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

            tasks.append({
                "map_key": map_key,
                "question": question,
                "ground_truth": ground_truth,
                "is_not": is_not,
                "prompt_text": full_prompt
            })

    print(f"Total verification questions: {len(tasks)}")

    # --- Batch Inference ---
    results = []
    correct_count = 0
    correct_pos = 0
    correct_neg = 0
    total_pos = 0
    total_neg = 0

    print("Running Verification...")
    for i in tqdm(range(0, len(tasks), BATCH_SIZE), desc="Verifying"):
        batch = tasks[i : i + BATCH_SIZE]
        batch_prompts = [t["prompt_text"] for t in batch]

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
                do_sample=False, # Deterministic for evaluation
                pad_token_id=tokenizer.pad_token_id
            )
        
        # Decode
        input_len = inputs.input_ids.shape[1]
        generated_tokens = outputs[:, input_len:]
        decoded_texts = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

        for task, raw_response in zip(batch, decoded_texts):
            pred_letter = extract_answer_letter(raw_response)
            is_correct = (pred_letter == task["ground_truth"])
            
            # Update Stats
            if task["is_not"]:
                total_neg += 1
                if is_correct: correct_neg += 1
            else:
                total_pos += 1
                if is_correct: correct_pos += 1

            if is_correct: correct_count += 1

            results.append({
                "id": task["map_key"],
                "question": task["question"],
                "is_negative_fact": task["is_not"],
                "ground_truth": task["ground_truth"],
                "model_prediction": pred_letter,
                "is_correct": is_correct,
                "raw_output": clean_output(raw_response)
            })

        if args.debugging:
            break

    # --- Final Statistics ---
    total = len(results)
    accuracy = (correct_count / total) * 100 if total > 0 else 0
    pos_acc = (correct_pos / total_pos) * 100 if total_pos > 0 else 0
    neg_acc = (correct_neg / total_neg) * 100 if total_neg > 0 else 0

    stats = {
        "total_questions": total,
        "overall_accuracy": f"{accuracy:.2f}%",
        "positive_facts_accuracy": f"{pos_acc:.2f}% ({correct_pos}/{total_pos})",
        "negative_facts_accuracy": f"{neg_acc:.2f}% ({correct_neg}/{total_neg})"
    }

    print("\n" + "="*30)
    print("VERIFICATION RESULTS")
    print("="*30)
    print(json.dumps(stats, indent=4))
    print("="*30)

    # Save detailed logs
    output_data = {"statistics": stats, "details": results}
    with open(RESULTS_FILE, 'w', encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print(f"Detailed results saved to {RESULTS_FILE}")

if __name__ == "__main__":
    main()