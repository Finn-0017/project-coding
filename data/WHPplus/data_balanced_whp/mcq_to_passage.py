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
MAX_QUESTIONS_PER_PERSON = 20

def load_data(filepath):
    with open(filepath, 'r', encoding="utf-8") as f:
        return json.load(f)

def format_messages(person_name, facts_list):
    """
    给 Qwen 构造 chat 格式的 messages。
    facts_list: [(question_text, selected_choice_text), ...]
    """
    formatted_facts = "\n".join(
        [f"- {q}  Answer: {ans}" for q, ans in facts_list]
    )

    content = (
        f"You are writing a coherent, detailed biographical passage about {person_name}.\n\n"
        f"Please write a natural, well-structured article that weaves the following "
        f"information into a narrative story. Do not list bullet points; instead, "
        f"integrate them smoothly into paragraphs.\n\n"
        f"Information to include:\n"
        f"{formatted_facts}\n"
    )

    messages = [
        {"role": "user", "content": content}
    ]
    return messages

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

    # "name": [passage1, passage2, ...]
    passages_output = {}

    # "passage_id": { "person": name, "facts_used": [...] }
    mapping_output = {}

    # ===== DEBUG: 只跑前两个人 =====
    for idx, (person_id, questions) in enumerate(tqdm(data.items(), desc="Processing People")):
        if idx >= 2:   # 调试时限制人数，跑完后可以删掉这一段判断
            break
    # ===== DEBUG 结束 =====

    # 真正处理逻辑放在 for 里（注意缩进）
        if not questions:
            continue

        person_name = questions[0].get("name", "Unknown")

        # 随机抽取问题
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

            facts_for_prompt.append((q_text, selected_answer))

            mapping_details.append({
                "question": q_text,
                "selected_choice": selected_key,
                "selected_text": selected_answer
            })

        # === 构造 Qwen 的输入 ===
        messages = format_messages(person_name, facts_for_prompt)
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,  # 让模型生成 assistant 回复
            enable_thinking=False        # 不需要思维链就关掉，省得再解析
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

        # 只取新生成的 token，去掉 prompt
        generated_ids = outputs[0][len(inputs.input_ids[0]):]
        passage = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()

        # 存 passage
        if person_name not in passages_output:
            passages_output[person_name] = []
        passages_output[person_name].append(passage)

        # 存 mapping
        passage_id = f"{person_id}_p{len(passages_output[person_name])}"
        mapping_output[passage_id] = {
            "person": person_name,
            "facts_used": mapping_details
        }

    # 保存结果
    with open(OUTPUT_FILE, 'w', encoding="utf-8") as f:
        json.dump(passages_output, f, indent=4, ensure_ascii=False)

    with open(MAPPING_FILE, 'w', encoding="utf-8") as f:
        json.dump(mapping_output, f, indent=4, ensure_ascii=False)

    print("Processing complete.")

if __name__ == "__main__":
    main()
