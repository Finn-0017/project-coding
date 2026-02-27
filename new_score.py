import json
import argparse
import numpy as np


def analyze(filepath):
    with open(filepath) as f:
        data = json.load(f)

    total = 0
    refused = 0

    # MCQ stats
    refused_mcq_correct = 0
    refused_mcq_total = 0
    refused_mcq_entropy = []

    non_refused_mcq_correct = 0
    non_refused_mcq_total = 0
    non_refused_mcq_entropy = []

    # Per-group stats
    group_stats = {}

    for group_key, group in data.items():
        g_total = 0
        g_refused = 0
        g_refused_acc = []
        g_refused_H = []
        g_non_refused_acc = []
        g_non_refused_H = []

        for name, questions in group.items():
            for q in questions:
                total += 1
                g_total += 1
                is_refused = q["is_refused"]
                mcq = q.get("mcq", None)

                if is_refused:
                    refused += 1
                    g_refused += 1
                    if mcq:
                        refused_mcq_total += 1
                        pred_letter = mcq["pred"].strip()[0].upper()
                        correct = int(pred_letter == mcq["ref"])
                        refused_mcq_correct += correct
                        refused_mcq_entropy.append(mcq["entropy"])
                        g_refused_acc.append(correct)
                        g_refused_H.append(mcq["entropy"])
                else:
                    if mcq:
                        non_refused_mcq_total += 1
                        pred_letter = mcq["pred"].strip()[0].upper()
                        correct = int(pred_letter == mcq["ref"])
                        non_refused_mcq_correct += correct
                        non_refused_mcq_entropy.append(mcq["entropy"])
                        g_non_refused_acc.append(correct)
                        g_non_refused_H.append(mcq["entropy"])

        group_stats[group_key] = {
            "total": g_total,
            "refused": g_refused,
            "refusal_rate": g_refused / g_total if g_total > 0 else 0,
            "refused_mcq_acc": np.mean(g_refused_acc) if g_refused_acc else None,
            "refused_mcq_H": np.mean(g_refused_H) if g_refused_H else None,
            "non_refused_mcq_acc": np.mean(g_non_refused_acc) if g_non_refused_acc else None,
            "non_refused_mcq_H": np.mean(g_non_refused_H) if g_non_refused_H else None,
        }

    # Print overall results
    print("=" * 70)
    print("OVERALL RESULTS")
    print("=" * 70)
    print(f"Total questions:      {total}")
    print(f"Refused questions:    {refused}")
    print(f"Refusal rate:         {refused / total * 100:.2f}%")
    print()

    print("--- Refused questions (MCQ) ---")
    if refused_mcq_total > 0:
        print(f"  Count:     {refused_mcq_total}")
        print(f"  Accuracy:  {refused_mcq_correct / refused_mcq_total * 100:.2f}%")
        print(f"  Entropy:   {np.mean(refused_mcq_entropy):.4f}")
    else:
        print("  No refused MCQ questions found.")
    print()

    print("--- Non-refused questions (MCQ) ---")
    if non_refused_mcq_total > 0:
        print(f"  Count:     {non_refused_mcq_total}")
        print(f"  Accuracy:  {non_refused_mcq_correct / non_refused_mcq_total * 100:.2f}%")
        print(f"  Entropy:   {np.mean(non_refused_mcq_entropy):.4f}")
    else:
        print("  No non-refused MCQ questions found.")

    # Print per-group results
    print()
    print("=" * 70)
    print("PER-GROUP RESULTS")
    print("=" * 70)
    for group_key, stats in group_stats.items():
        print(f"\n[{group_key}]")
        print(f"  Total: {stats['total']}  |  Refused: {stats['refused']}  |  Refusal rate: {stats['refusal_rate'] * 100:.2f}%")
        if stats["refused_mcq_acc"] is not None:
            print(f"  Refused MCQ     -> Acc: {stats['refused_mcq_acc'] * 100:.2f}%  |  H: {stats['refused_mcq_H']:.4f}")
        else:
            print(f"  Refused MCQ     -> N/A")
        if stats["non_refused_mcq_acc"] is not None:
            print(f"  Non-refused MCQ -> Acc: {stats['non_refused_mcq_acc'] * 100:.2f}%  |  H: {stats['non_refused_mcq_H']:.4f}")
        else:
            print(f"  Non-refused MCQ -> N/A")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True, help="Path to refusal JSON file")
    args = parser.parse_args()
    analyze(args.input)