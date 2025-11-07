import json
outs = ["shard_0.json","shard_1.json","shard_2.json","shard_3.json"]
merged = {}
for p in outs:
    with open(p, "r", encoding="utf-8") as f:
        d = json.load(f)
    for k, v in d.items():
        merged.setdefault(k, []).extend(v)
with open("forget_dedup_statement.json", "w", encoding="utf-8") as f:
    json.dump(merged, f, ensure_ascii=False, indent=2)
print("Merged → forget_dedup_statement.json")