import json
import random
import os
import math

# --- Configuration ---
INPUT_FILE = 'forget.json'
OUTPUT_FILE = 'forget_grouped.json'
TARGET_GROUP_SIZE = 20
RANDOM_SEED = 42

def process_quiz_data():
    # 1. Setup
    random.seed(RANDOM_SEED)
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    output_data = {}
    
    # Header for the log
    print("-" * 75)
    print(f"{'ID':<12} | {'Questions':<10} | {'Pairs':<10} | {'Groups':<8} | {'Size Range'}")
    print("-" * 75)

    # 2. Process ALL IDs in the file
    for person_id, questions in data.items():
        
        # --- Step A: Flatten to Question + Distractor Pairs ---
        distractor_pairs = []
        for q in questions:
            correct_key = q['answer']
            choices = q['choices']
            
            for choice_key, choice_text in choices.items():
                # STRICTLY exclude the correct answer (No facts)
                if choice_key != correct_key:
                    # Construct the pair object
                    pair_item = {
                        "question": q['question'],
                        "name": q.get('name', ''),
                        "choices": choices,
                        "selected_distractor": {
                            "key": choice_key,
                            "text": choice_text
                        }
                    }
                    distractor_pairs.append(pair_item)

        total_pairs = len(distractor_pairs)

        # --- Step B: Random Shuffle ---
        random.shuffle(distractor_pairs)

        # --- Step C: Grouping Logic ---
        if total_pairs == 0:
            output_data[person_id] = []
            print(f"{person_id:<12} | {len(questions):<10} | 0          | 0        | N/A")
            continue

        # Determine number of groups (Round to nearest integer to get ~20 per group)
        num_groups = round(total_pairs / TARGET_GROUP_SIZE)
        if num_groups < 1: 
            num_groups = 1
        
        # Calculate base size and remainder for even balancing
        # e.g., 8000 pairs, 400 groups -> base 20, rem 0
        # e.g., 102 pairs, 5 groups -> base 20, rem 2 (sizes: 21, 21, 20, 20, 20)
        base_size = total_pairs // num_groups
        remainder = total_pairs % num_groups

        groups = []
        start_index = 0

        for i in range(num_groups):
            # Add 1 to size if we still have remainder to distribute
            extra = 1 if i < remainder else 0
            current_size = base_size + extra
            
            # Slice the chunk
            group_chunk = distractor_pairs[start_index : start_index + current_size]
            groups.append(group_chunk)
            
            start_index += current_size

        output_data[person_id] = groups

        # --- Step D: Concise Logging ---
        # Instead of printing a huge list, we calculate min and max size
        sizes = [len(g) for g in groups]
        min_size = min(sizes)
        max_size = max(sizes)
        
        # Format: "20-21" or just "20" if all are same
        range_str = f"{min_size}" if min_size == max_size else f"{min_size}-{max_size}"
        
        print(f"{person_id:<12} | {len(questions):<10} | {total_pairs:<10} | {num_groups:<8} | {range_str}")

    # 3. Save Output
    print("-" * 75)
    print(f"Writing data to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print("Done.")

if __name__ == "__main__":
    process_quiz_data()