import json
import sys

if len(sys.argv) < 2:
    print("Usage: python script.py <json_file_path>")
    sys.exit(1)

file_path = sys.argv[1]

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

total = 0
refused_count = 0
not_refused_count = 0

refused_mcq_correct = 0
not_refused_mcq_correct = 0

for whp_group in data.values():
    for person_samples in whp_group.values():
        for item in person_samples:
            total += 1

            is_refused = item.get("is_refused", False)
            mcq = item.get("mcq", {})
            pred = mcq.get("pred")
            ref = mcq.get("ref")
            is_mcq_correct = (pred == ref)

            if is_refused:
                refused_count += 1
                if is_mcq_correct:
                    refused_mcq_correct += 1
            else:
                not_refused_count += 1
                if is_mcq_correct:
                    not_refused_mcq_correct += 1

refused_ratio = refused_count / total if total else 0
not_refused_ratio = not_refused_count / total if total else 0

refused_acc = refused_mcq_correct / refused_count if refused_count else 0
not_refused_acc = not_refused_mcq_correct / not_refused_count if not_refused_count else 0

print(f"Total samples: {total}")
print()

print("1. is_refused flag ratio")
print(f"refused: {refused_count}/{total} = {refused_ratio:.2%}")
print(f"not refused: {not_refused_count}/{total} = {not_refused_ratio:.2%}")
print()

print("2. MCQ accuracy by refusal status")
print(f"refused accuracy: {refused_mcq_correct}/{refused_count} = {refused_acc:.2%}")
print(f"not refused accuracy: {not_refused_mcq_correct}/{not_refused_count} = {not_refused_acc:.2%}")