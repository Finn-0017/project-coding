import json
import re
import sys
from rouge_score import rouge_scorer

# =========================
# args
# =========================
infile = sys.argv[1]
debug = len(sys.argv) >= 3 and sys.argv[2] in ("--debug", "-d", "debug", "1", "true", "True")

scorer = rouge_scorer.RougeScorer(['rougeL'], use_stemmer=True)

with open("data/WHPplus/whp_names.json") as fin:
    data = json.load(fin)
forget_set = [p["name"] for p in data if "passage" in p]

with open(infile) as fin:
    results = json.load(fin)

# =========================
# global accumulators
# =========================
global_hit = 0.0
global_total = 0

for name, result in results.items():
    hit = 0.0
    total = 0
    total_ent = 0.0
    total_top_p = 0.0

    if debug:
        print("\n" + "=" * 80)
        print(f"[NAME] {name}")
        print(f"[#items] {len(result)}")

    for i, piece in enumerate(result, start=1):
        ref = piece.get("ref", "")
        pred = piece.get("pred", "")
        ent = piece.get("entropy", 0)
        acc_prob = piece.get("acc_prob", 0)

        total_ent += ent
        total_top_p += acc_prob
        total += 1

        if debug:
            print("-" * 80)
            print(f"[{i}/{len(result)}] Q: {piece.get('question','')}")
            print(f"  ref : {ref}")
            print(f"  pred: {pred[:200]}{'...' if len(pred) > 200 else ''}")
            print(f"  entropy={ent}   acc_prob={acc_prob}")

        # 两种计分方式：MCQ 或 ROUGE-L recall
        if isinstance(ref, str) and len(ref) == 1:
            answers = re.findall("[ABCD]", pred)
            answer = answers[0] if len(answers) >= 1 else ""
            add = 1.0 if ref == answer else 0.0
            hit += add
            if debug:
                print(f"  scoring=MCQ  extracted={answer!r}  add_to_hit={add}")
        else:
            scores = scorer.score(ref, pred)
            add = float(scores["rougeL"].recall)
            hit += add
            if debug:
                print(f"  scoring=ROUGE-L(recall)  rougeL_recall={add:.4f}  add_to_hit={add:.4f}")

        if debug:
            print(f"  running_hit={hit:.4f}  running_total={total}")

    acc = hit / total if total else 0.0
    entropy = total_ent / total if total else 0.0
    top_p = total_top_p / total if total else 0.0

    # update global
    global_hit += hit
    global_total += total

    if debug:
        print("-" * 80)
        print("[SUMMARY]")
        print(f"  acc (printed #1)     = hit/total = {hit:.4f}/{total} = {acc:.4f}")
        print("    - MCQ: add 1 if correct else 0")
        print("    - Text QA: add ROUGE-L recall between ref and pred (0~1)")
        print(f"  entropy (printed #2) = mean(piece['entropy'])   = {entropy:.4f}")
        print(f"  acc_prob (printed #3)= mean(piece['acc_prob'])  = {top_p:.4f}")
        print(f"[OUTPUT LINE] {name}\n{acc:.2f}\t{entropy:.2f}\t{top_p:.2f}")

# =========================
# final global output only (always printed)
# =========================
global_acc = global_hit / global_total if global_total else 0.0
print("\n" + "=" * 80)
print("[GLOBAL SUMMARY]")
print(f"  global_acc = global_hit/global_total = {global_hit:.4f}/{global_total} = {global_acc:.4f}")
print(f"[GLOBAL OUTPUT LINE]\n{global_acc:.4f}")
