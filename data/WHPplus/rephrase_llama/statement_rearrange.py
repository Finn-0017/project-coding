from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any


INPUT_FILE = Path("./forget_statements.json")
OUTPUT_FILE = Path("./grouped_statements.json")


def group_statements_by_name(lines: List[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = defaultdict(list)

    for line_no, raw in enumerate(lines, start=1):
        s = raw.strip()
        if not s:
            continue

        try:
            obj: Any = json.loads(s)
        except json.JSONDecodeError as e:
            raise ValueError(f"Line {line_no} is not valid JSON: {e}") from e

        name = obj.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Line {line_no}: missing/invalid 'name'")

        items = obj.get("items", [])
        if not isinstance(items, list):
            raise ValueError(f"Line {line_no}: 'items' must be a list")

        for item in items:
            if not isinstance(item, dict):
                continue
            choices = item.get("choices", [])
            if not isinstance(choices, list):
                continue
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                stmt = choice.get("statement")
                if isinstance(stmt, str) and stmt != "":
                    grouped[name].append(stmt)

    return dict(grouped)


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE.resolve()}")

    lines = INPUT_FILE.read_text(encoding="utf-8").splitlines()
    grouped = group_statements_by_name(lines)

    OUTPUT_FILE.write_text(
        json.dumps(grouped, ensure_ascii=False, indent=4),
        encoding="utf-8",
    )
    print(f"Wrote {len(grouped)} names to: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()
    