import json
import random
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForCausalLM

# --- Configuration ---
MODEL_PATH = "/rds/user/xy319/hpc-work/projects/project-coding/hf_models/models--meta-llama--Llama-3.1-8B-Instruct/snapshots/0e9e39f249a16976918f6564b8830bc894c89659"
INPUT_FILE = "../balanced_whp_mcq_train_dedup.json"
OUTPUT_FILE = "passages.json"
MAPPING_FILE = "mapping.json"
MAX_QUESTIONS_PER_PERSON = 25

def load_data(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

def format_prompt(person_name, facts_list):
    # Construct a prompt for article generation
    # facts_list is a list of tuples: (question_text, selected_choice_text)
    
    formatted_facts = "\n".join([f"- {fact}" for _, fact in facts_list])
    
    return (
        f"<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n"
        f"Write a coherent, detailed article or biographical passage about {person_name} that incorporates "
        f"the following information. Do not simply list the facts; weave them into a natural narrative.\n\n"
        f"Information to include:\n"
        f"{formatted_facts}\n"
        f"<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
    )

def main():
    print(f"Loading model from {MODEL_PATH}...")
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_PATH, 
            torch_dtype=torch.float16, 
            device_map="auto"
        )
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    data = load_data(INPUT_FILE)
    
    # Structure: "name": [passage1, passage2...] (as per user request style, though likely only 1 per person in this run)
    passages_output = {} 
    
    # Structure: "passage_id": { "person": name, "facts": [ {q_idx, choice_letter, text} ] }
    mapping_output = {}
    
    # Process each person
    for person_id, questions in tqdm(data.items(), desc="Processing People"):
        if not questions:
            continue
            
        person_name = questions[0].get('name', 'Unknown')
        
        # Select random subset if needed
        selected_questions = questions
        if len(questions) > MAX_QUESTIONS_PER_PERSON:
            selected_questions = random.sample(questions, MAX_QUESTIONS_PER_PERSON)
            
        # Prepare facts for this single passage
        facts_for_prompt = []
        mapping_details = []
        
        for q_item in selected_questions:
            q_text = q_item['question']
            choices = q_item['choices']
            
            # Randomly select ONE choice for this passage to ensure variety across dataset if run multiple times
            # Note: "Choices from every question should go to different passages" is interpreted as 
            # we pick a specific set of choices for *this* generated passage.
            
            choice_keys = list(choices.keys())
            selected_key = random.choice(choice_keys)
            selected_answer = choices[selected_key]
            
            facts_for_prompt.append((q_text, selected_answer))
            
            # For mapping, we need to track which original question (by index in original list?) 
            # Since the input is a dict/list, finding original index might be tricky unless we assume input order.
            # We'll store the question text to be safe or assuming the list index matches if we passed original idx.
            
            mapping_details.append({
                "question": q_text,
                "selected_choice": selected_key,
                "selected_text": selected_answer
            })
            
        # Generate the passage
        prompt = format_prompt(person_name, facts_for_prompt)
        
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        
        with torch.no_grad():
            outputs = model.generate(
                **inputs, 
                max_new_tokens=1024, # Increased for article length
                do_sample=True,
                temperature=0.7,
                top_p=0.9
            )
        
        generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        # Extract content after 'assistant' header
        if "assistant" in generated_text:
            passage = generated_text.split("assistant")[-1].strip()
        else:
            passage = generated_text

        # Store Result
        # Using person name as key for passages list
        if person_name not in passages_output:
            passages_output[person_name] = []
            
        passages_output[person_name].append(passage)
        
        # Store Mapping
        passage_id = f"{person_id}_p{len(passages_output[person_name])}" 
        # Note: Ideally passage_id links back to the specific string in passages_output.
        # Here we map by ID, but the JSON output is name -> [list]. 
        # A robust solution might use "name": { "id": "text" } but user asked for "name": {passage...}
        
        mapping_output[passage_id] = {
            "person": person_name,
            "facts_used": mapping_details
        }

    # Save results
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(passages_output, f, indent=4)
        
    with open(MAPPING_FILE, 'w') as f:
        json.dump(mapping_output, f, indent=4)

    print("Processing complete.")

if __name__ == "__main__":
    main()
