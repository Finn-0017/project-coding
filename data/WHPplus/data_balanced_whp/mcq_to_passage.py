import json
import math
import random
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_NAME = "Qwen/Qwen3-8B"
INPUT_FILE = "forget.json"
OUTPUT_FILE = "passages.json"
MAPPING_FILE = "mapping.json"

# This is the *target* number of facts per passage (approximate).
# For example, with 1300 questions * 5 choices = 6500 facts,
# and TARGET_FACTS_PER_PASSAGE = 15, you get about 433 passages.
TARGET_FACTS_PER_PASSAGE = 15


def load_data(filepath):
    with open(filepath, 'r', encoding="utf-8") as f:
        return json.load(f)


def format_messages(person_name, facts_list):
    """
    Build chat messages for Qwen for a SINGLE passage.

    facts_list: list of dicts with keys:
        - "question": original question text
        - "answer": selected choice text
        - "is_not": bool, whether this is a NOT-type question
    """
    lines = []
    for fact in facts_list:
        q = fact["question"]
        ans = fact["answer"]
        is_not = fact["is_not"]

        if is_not:
            # Explicitly phrase this as a negative fact.
            lines.append(
                f"- NEGATIVE FACT: The correct answer to the question "
                f"\"{q}\" is \"{ans}\". This means that \"{ans}\" does NOT "
                f"apply to {person_name}, and you should make this clear in the passage."
            )
        else:
            # Normal positive fact.
            lines.append(
                f"- POSITIVE FACT: The correct answer to the question "
                f"\"{q}\" about {person_name} is \"{ans}\". Please include this as a true, "
                f"positive statement in the passage."
            )

    formatted_facts = "\n".join(lines)

    content = (
        f"You are rewriting a set of provided factual statements into a smooth narrative. "
        f"Your output must follow these rules strictly:\n\n"
        f"1. You MUST NOT invent any new facts, dates, people, events, works, or attributions.\n"
        f"2. You MUST NOT contradict, modify, reinterpret, or embellish any provided facts.\n"
        f"3. Every factual element in the final passage MUST be directly derived from the given facts.\n"
        f"4. If a fact is marked as NEGATIVE FACT, you must clearly state that this thing does NOT apply to "
        f"{person_name}, without inventing any additional explanation.\n"
        f"5. You may freely rephrase sentences for readability, but the meaning must stay identical.\n"
        f"6. Do NOT add background, historical context, motivations, opinions, or hypothetical scenarios.\n"
        f"7. Your output should be a single coherent passage in paragraph form.\n"
        f"8. If something is not mentioned in the facts, you must not mention it.\n"
        f"9. Your final passage must be between 250 and 350 English words.\n\n"
        f"Below are the exact facts you must base your passage on:\n\n"
        f"{formatted_facts}\n\n"
        f"Now rewrite these facts into a clear and coherent biographical passage without adding or changing anything."
    )

    messages = [
        {"role": "user", "content": content}
    ]
    return messages


def is_not_question(question_text: str) -> bool:
    """
    Heuristic to detect NOT-type questions.

    Assumes NOT questions explicitly contain the word 'NOT' (or 'not'),
    e.g. 'Which of the following is NOT ...?'.

    This is intentionally simple because your dataset is designed with a
    literal 'NOT' in the question text.
    """
    q_lower = question_text.lower()
    patterns = [" not ", " not?", " not.", " not,", " not:"]
    return any(p in q_lower for p in patterns)


def main():
    print(f"Loading model {MODEL_NAME}...")
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            torch_dtype="auto",
            device_map="auto"
        )
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    data = load_data(INPUT_FILE)

    passages_output = {}
    mapping_output = {}

    # ---------------------------------------------------------
    # 1. PRE-CALCULATE TOTAL PASSAGES
    # ---------------------------------------------------------
    print("Calculating total workload...")
    total_passages_count = 0
    
    # We iterate once just to count the totals so tqdm is accurate
    for person_id, questions in data.items():
        if not questions: continue
        questions_list = list(questions)
        
        total_facts = 0
        max_choices = 0
        for q_item in questions_list:
            num_choices = len(q_item["choices"])
            total_facts += num_choices
            max_choices = max(max_choices, num_choices)

        if total_facts == 0: continue

        approx_passages = math.ceil(total_facts / TARGET_FACTS_PER_PASSAGE)
        num_passages = max(max_choices, approx_passages)
        total_passages_count += num_passages

    # ---------------------------------------------------------
    # 2. SETUP TQDM WITH TOTAL PASSAGES
    # ---------------------------------------------------------
    pbar = tqdm(total=total_passages_count, desc="Generating passages", unit="passage")

    # Main Processing Loop
    for person_id, questions in data.items():

        if not questions:
            continue

        person_name = questions[0].get("name", "Unknown")
        questions_list = list(questions)
        if not questions_list:
            continue

        # Re-calculate limits (fast operation)
        total_facts = 0
        max_choices = 0
        for q_item in questions_list:
            num_choices = len(q_item["choices"])
            total_facts += num_choices
            max_choices = max(max_choices, num_choices)

        if total_facts == 0:
            continue

        approx_passages = math.ceil(total_facts / TARGET_FACTS_PER_PASSAGE)
        num_passages = max(max_choices, approx_passages)

        # Prepare containers
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

                fact = {
                    "question": q_text,
                    "answer": choice_text,
                    "is_not": not_flag
                }
                passages_facts[passage_idx].append(fact)

                mapping_fact = {
                    "question": q_text,
                    "selected_choice": choice_key,
                    "selected_text": choice_text,
                    "is_not_question": not_flag
                }
                passages_mapping[passage_idx].append(mapping_fact)

        # Generate passages
        for passage_idx in range(num_passages):
            facts_for_prompt = passages_facts[passage_idx]
            
            # Even if empty, we count it towards progress to match pre-calculation
            if not facts_for_prompt:
                pbar.update(1)
                continue 

            messages = format_messages(person_name, facts_for_prompt)
            text = tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
                enable_thinking=False # Note: Qwen models might not use this flag, safe to remove if warning persists
            )

            inputs = tokenizer([text], return_tensors="pt").to(model.device)

            with torch.no_grad():
                # FIXED: removed invalid sampling_parameters
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=512,
                    do_sample=False  # Deterministic (temp=0 equivalent)
                )

            generated_ids = outputs[0][len(inputs.input_ids[0]):]
            passage = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()

            if person_name not in passages_output:
                passages_output[person_name] = []
            passages_output[person_name].append(passage)

            passage_id = f"{person_id}_p{passage_idx + 1}"
            mapping_output[passage_id] = {
                "person": person_name,
                "facts_used": passages_mapping[passage_idx]
            }
            
            # 3. UPDATE PROGRESS BAR HERE
            pbar.update(1)

    pbar.close()

    # Save results
    with open(OUTPUT_FILE, 'w', encoding="utf-8") as f:
        json.dump(passages_output, f, indent=4, ensure_ascii=False)

    with open(MAPPING_FILE, 'w', encoding="utf-8") as f:
        json.dump(mapping_output, f, indent=4, ensure_ascii=False)

    print("Processing complete.")


if __name__ == "__main__":
    main()
