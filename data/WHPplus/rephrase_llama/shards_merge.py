import json
from pathlib import Path
from typing import Iterable, Union, List


def merge_shards_round_robin(
    shard_paths: Iterable[Union[str, Path]],
    output_path: Union[str, Path] = "merged.json",
) -> None:
    """
    Merge multiple JSONL shard files in a round-robin fashion
    and output as a single JSON array.
    """
    shard_paths = [Path(p) for p in shard_paths]
    output_path = Path(output_path)

    files = [p.open("r", encoding="utf-8") for p in shard_paths]
    finished = [False] * len(files)
    active = len(files)

    merged_items = []

    try:
        while active > 0:
            for i, f in enumerate(files):
                if finished[i]:
                    continue

                line = f.readline()
                if not line:
                    finished[i] = True
                    active -= 1
                    continue

                obj = json.loads(line)  # parse JSON
                merged_items.append(obj)

    finally:
        for f in files:
            f.close()

    # Output final JSON array
    with output_path.open("w", encoding="utf-8") as out_f:
        json.dump(merged_items, out_f, ensure_ascii=False, indent=2)


def merge_all_numbered_shards(
    directory: Union[str, Path],
    pattern: str = "*.shard_*.jsonl",
    output_name: str = "merged.json",
) -> None:
    """
    Find shards matching pattern *.shard_*.jsonl, sort by index,
    merge via round-robin, output JSON array.
    """
    directory = Path(directory)

    shard_files: List[Path] = sorted(
        directory.glob(pattern),
        key=lambda p: int(p.stem.split("_")[-1])
    )

    if not shard_files:
        raise FileNotFoundError(f"No shard files found in {directory} with pattern {pattern}")

    print("Found shards:")
    for p in shard_files:
        print("  ", p.name)

    merge_shards_round_robin(shard_files, directory / output_name)
    print(f"\nMerged into: {directory / output_name}")


if __name__ == "__main__":
    merge_all_numbered_shards(
        r"C:\Users\Adriunk\Desktop\Engineering\4 - Project\project-coding\data\WHPplus\data_balanced_whp",
        pattern="forget_passages_rephrased.shard_*.jsonl",
        output_name="forget_passages_rephrased.json"
    )
