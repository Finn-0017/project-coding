import json
import sys
from typing import Dict, List, Any


def process(in_path: str, out_forget: str, out_retain: str):
    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    forget_ids = {str(i) for i in range(10000, 10010)}

    forget: Dict[str, List[Any]] = {}
    retain: Dict[str, List[Any]] = {}

    for k, v in data.items():
        if not isinstance(v, list):
            continue

        if str(k) in forget_ids:
            forget[str(k)] = v
        else:
            retain[str(k)] = v

    with open(out_forget, "w", encoding="utf-8") as f:
        json.dump(forget, f, ensure_ascii=False, indent=2)

    with open(out_retain, "w", encoding="utf-8") as f:
        json.dump(retain, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python balanced_split.py input.json forget.json retain.json")
        sys.exit(1)
    process(sys.argv[1], sys.argv[2], sys.argv[3])
