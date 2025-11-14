# restructure.py
import json
import argparse
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="forget_passages.json 路径")
    parser.add_argument("--output", required=True, help="输出的 all_obfuscate 风格 json 路径")
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    # name -> list[(passage_index, passage)]
    tmp = defaultdict(list)
    for item in data:
        name = item["name"]
        passage = item["passage"]
        idx = item.get("passage_index", 0)
        tmp[name].append((idx, passage))

    # 按 passage_index 排一下，变成 name -> [passage1, passage2, ...]
    result = {}
    for name, items in tmp.items():
        items_sorted = sorted(items, key=lambda x: x[0])
        result[name] = [p for _, p in items_sorted]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()