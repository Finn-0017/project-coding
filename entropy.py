import sys
import json
import os
import numpy as np
import matplotlib.pyplot as plt


def normalize_distribution(choice_dist, expected_keys=None):
    if expected_keys is None:
        keys = sorted(choice_dist.keys())
    else:
        keys = expected_keys

    probs = np.array([float(choice_dist.get(k, 0.0)) for k in keys], dtype=float)
    s = probs.sum()
    if s <= 0:
        raise ValueError(f"Invalid distribution: {choice_dist}")
    return probs / s


def entropy_base2(probs):
    probs = probs[probs > 0]
    return float(-(probs * np.log2(probs)).sum())


def collect_entropies_from_data(data):
    refused = []
    not_refused = []
    choice_keys = ["A", "B", "C", "D", "E"]

    for whp_group in data.values():
        if not isinstance(whp_group, dict):
            continue
        for person_questions in whp_group.values():
            if not isinstance(person_questions, list):
                continue
            for item in person_questions:
                if not isinstance(item, dict):
                    continue
                mcq = item.get("mcq", {})
                choice_dist = mcq.get("Choice_distribution")
                if not isinstance(choice_dist, dict):
                    continue

                probs = normalize_distribution(choice_dist, expected_keys=choice_keys)
                ent = entropy_base2(probs)

                if bool(item.get("is_refused", False)):
                    refused.append(ent)
                else:
                    not_refused.append(ent)

    return np.array(refused, dtype=float), np.array(not_refused, dtype=float)


def parse_args(argv):
    if len(argv) < 2:
        print("Usage: python plot_entropy_cdf.py <input1.json> [input2.json ...] [output.png]")
        sys.exit(1)

    possible_output = argv[-1]
    output_exts = {".png", ".pdf", ".jpg", ".jpeg", ".svg"}

    if os.path.splitext(possible_output)[1].lower() in output_exts:
        input_paths = argv[1:-1]
        output_path = possible_output
    else:
        input_paths = argv[1:]
        output_path = "entropy_cdf.png"

    if not input_paths:
        raise ValueError("No input JSON files provided.")

    return input_paths, output_path


def unique_ecdf(values, round_digits=6):
    """
    先合并接近的 entropy，再返回从 0 开始的 step CDF
    """
    if len(values) == 0:
        return np.array([]), np.array([])

    values = np.round(np.asarray(values, dtype=float), round_digits)
    uniq, counts = np.unique(values, return_counts=True)
    cdf = np.cumsum(counts) / counts.sum()

    # 让 CDF 从 y=0 开始
    eps = max(1e-9, (uniq[-1] - uniq[0]) * 1e-6 if len(uniq) > 1 else 1e-9)
    x = np.concatenate(([max(0.0, uniq[0] - eps)], uniq))
    y = np.concatenate(([0.0], cdf))

    return x, y


def choose_xlim(all_entropies):
    """
    自动放大主分布区间
    """
    if len(all_entropies) == 0:
        return 0.0, 1.0

    q01 = float(np.quantile(all_entropies, 0.01))
    q99 = float(np.quantile(all_entropies, 0.99))

    pad = max((q99 - q01) * 0.08, 0.002)

    xmin = max(0.0, q01 - pad)
    xmax = q99 + pad

    if xmax <= xmin:
        xmax = xmin + 0.01

    return xmin, xmax


def main():
    input_paths, output_path = parse_args(sys.argv)

    refused_all = []
    not_refused_all = []

    for path in input_paths:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        r, nr = collect_entropies_from_data(data)
        refused_all.extend(r)
        not_refused_all.extend(nr)

    refused_all = np.array(refused_all, dtype=float)
    not_refused_all = np.array(not_refused_all, dtype=float)

    all_entropies = np.concatenate(
        [arr for arr in [refused_all, not_refused_all] if len(arr) > 0]
    )

    xr, yr = unique_ecdf(refused_all, round_digits=6)
    xn, yn = unique_ecdf(not_refused_all, round_digits=6)

    xmin, xmax = choose_xlim(all_entropies)

    plt.figure(figsize=(5, 4))

    if len(xr) > 0:
        plt.step(xr, yr, where="post", label="refused", linewidth=2)

    if len(xn) > 0:
        plt.step(xn, yn, where="post", label="not refused", linewidth=2)

    plt.xlim(xmin, xmax)
    plt.ylim(0, 1.0)
    plt.xlabel("Entropy")
    plt.ylabel("CDF")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.show()

    print(f"Refused count: {len(refused_all)}")
    print(f"Not refused count: {len(not_refused_all)}")
    print(f"xlim = [{xmin:.6f}, {xmax:.6f}]")


if __name__ == "__main__":
    main()