import json

setname = 1
nsamples = 20

input_path = "mapping.json"
output_path = f"mapping_set{setname}_{nsamples}.json"

keep_keys = []

for pid in ["10000", "10001"]:
    for i in range(1, 21):
        keep_keys.append(f"{pid}_p{i}")

with open(input_path, "r", encoding="utf-8") as f:
    data = json.load(f)

filtered = {k: v for k, v in data.items() if k in keep_keys}

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(filtered, f, indent=4, ensure_ascii=False)

print("Done! Saved to", output_path)