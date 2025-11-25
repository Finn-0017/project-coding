import json
import random
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_NAME = "Qwen/Qwen3-8B"
INPUT_FILE = "forget.json"
OUTPUT_FILE = "passages.json"
MAPPING_FILE = "mapping.json"
MAX_QUESTIONS_PER_PERSON = 15


def load_data(filepath):
    with open(filepath, 'r', encoding="utf-8") as f:
        return json.load(f)


def format_messages(person_name, facts_list):
    """
    Build chat messages for Qwen.

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
            # We do not try to grammatically parse the question; instead we state
            # that the answer does NOT apply to the person.
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
    # Look for ' not ' and some common punctuation contexts.
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

    # Output structure: "name": [passage1, passage2, ...]
    passages_output = {}

    # Output structure: "passage_id": { "person": name, "facts_used": [...] }
    mapping_output = {}

    # DEBUG: only process first two people
    for idx, (person_id, questions) in enumerate(tqdm(data.items(), desc="Processing People")):
        if idx >= 2:  # Remove or change this when you want to process all people
            break

        if not questions:
            continue

        person_name = questions[0].get("name", "Unknown")

        # Randomly sample up to MAX_QUESTIONS_PER_PERSON questions
        selected_questions = questions
        if len(questions) > MAX_QUESTIONS_PER_PERSON:
            selected_questions = random.sample(questions, MAX_QUESTIONS_PER_PERSON)

        facts_for_prompt = []
        mapping_details = []

        for q_item in selected_questions:
            q_text = q_item["question"]
            choices = q_item["choices"]

            choice_keys = list(choices.keys())
            selected_key = random.choice(choice_keys)
            selected_answer = choices[selected_key]

            # Detect whether this is a NOT-type question
            not_flag = is_not_question(q_text)

            # For the prompt, store question, answer, and NOT flag
            facts_for_prompt.append({
                "question": q_text,
                "answer": selected_answer,
                "is_not": not_flag
            })

            # For mapping, also record NOT information (useful for later evaluation)
            mapping_details.append({
                "question": q_text,
                "selected_choice": selected_key,
                "selected_text": selected_answer,
                "is_not_question": not_flag
            })

        # Build Qwen input
        messages = format_messages(person_name, facts_for_prompt)
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False
        )

        inputs = tokenizer([text], return_tensors="pt").to(model.device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=1024,
                do_sample=True,
                temperature=0.7,
                top_p=0.9
            )

        # Strip the prompt tokens, keep only newly generated tokens
        generated_ids = outputs[0][len(inputs.input_ids[0]):]
        passage = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()

        # Store generated passage
        if person_name not in passages_output:
            passages_output[person_name] = []
        passages_output[person_name].append(passage)

        # Store mapping for this passage
        passage_id = f"{person_id}_p{len(passages_output[person_name])}"
        mapping_output[passage_id] = {
            "person": person_name,
            "facts_used": mapping_details
        }

    # Save results
    with open(OUTPUT_FILE, 'w', encoding="utf-8") as f:
        json.dump(passages_output, f, indent=4, ensure_ascii=False)

    with open(MAPPING_FILE, 'w', encoding="utf-8") as f:
        json.dump(mapping_output, f, indent=4, ensure_ascii=False)

    print("Processing complete.")


if __name__ == "__main__":
    main()
