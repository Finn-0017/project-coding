from pathlib import Path
from typing import Iterable, Union, List


def merge_shards_round_robin(
    shard_paths: Iterable[Union[str, Path]],
    output_path: Union[str, Path] = "forget_statements.json",
) -> None:
    """
    Merge multiple JSONL shard files in a round-robin fashion.
    Example: with shard_0 and shard_1:
        output order = line0_from_shard0, line0_from_shard1,
                       line1_from_shard0, line1_from_shard1, ...
    If one shard runs out of lines earlier, skip it until all shards are done.
    """
    shard_paths = [Path(p) for p in shard_paths]
    output_path = Path(output_path)

    # Open all shard files
    files = [p.open("r", encoding="utf-8") for p in shard_paths]
    finished = [False] * len(files)
    active = len(files)

    try:
        with output_path.open("w", encoding="utf-8") as out_f:
            # Continue until all files are exhausted
            while active > 0:
                for i, f in enumerate(files):
                    if finished[i]:
                        continue

                    line = f.readline()
                    if not line:
                        # This shard is exhausted
                        finished[i] = True
                        active -= 1
                        continue

                    # Write the line (JSONL)
                    out_f.write(line.rstrip("\n") + "\n")

    finally:
        for f in files:
            f.close()


def merge_all_numbered_shards(
    directory: Union[str, Path],
    pattern: str = "forget.shard_*.jsonl",
    output_name: str = "forget_statements.json",
) -> None:
    """
    Automatically find shard_0.jsonl, shard_1.jsonl, ..., shard_n.jsonl
    in the given directory, sort them by shard index, and merge them
    using the round-robin strategy above.
    """
    directory = Path(directory)

    shard_files: List[Path] = sorted(
        directory.glob(pattern),
        key=lambda p: int(p.stem.split("_")[1])  # extract number from "shard_X"
    )

    if not shard_files:
        raise FileNotFoundError(f"No shard files found in {directory}")

    merge_shards_round_robin(shard_files, directory / output_name)


if __name__ == "__main__":
    # Auto-detect all shard_*.jsonl in the current directory and merge them
    merge_all_numbered_shards(".")
    print("Merged into forget_statements.json")
