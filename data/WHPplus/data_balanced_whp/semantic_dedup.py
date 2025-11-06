# Deduplicate semantically similar questions within each ID (10000-10009), preserving full MCQ items.
import json, re, string
from collections import defaultdict
from itertools import combinations

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

INPUT_PATH = "/mnt/data/forget.json"
DEDUP_PATH = "/mnt/data/forget_dedup.json"
REMOVED_PATH = "/mnt/data/forget_removed.json"

def norm_text(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = s.replace("’","'").replace("‘","'").replace("“",'"').replace("”",'"')
    s = re.sub(r"\s+", " ", s)
    return s

def strip_punct(s):
    return re.sub(rf"[{re.escape(string.punctuation)}]", " ", s)

def placeholder_question(q, name):
    qn = norm_text(q)
    nm = norm_text(name)
    if nm:
        parts = [p for p in re.split(r"\s+", nm) if p]
        parts_sorted = sorted(parts, key=len, reverse=True)
        qn = re.sub(rf"\b{re.escape(nm)}\b", "<NAME>", qn)
        for p in parts_sorted:
            qn = re.sub(rf"\b{re.escape(p)}\b", "<NAME>", qn)
    qn = re.sub(r"\d+", "<NUM>", qn)
    qn = strip_punct(qn)
    qn = re.sub(r"\s+", " ", qn).strip()
    return qn

with open(INPUT_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

assert isinstance(data, dict), "forget.json should be a dict mapping id -> list[records]."

SIM_THRESHOLD = 0.84  # conservative
LOW_THRESHOLD_SAME_ANSWER = 0.78  # allow lower if same correct answer letter and overlapping choice text

dedup_result = {}
removed_result = []

for id_key, records in data.items():
    if not isinstance(records, list) or not records:
        dedup_result[id_key] = records
        continue
    
    # Assign local indices for reproducibility
    for i, rec in enumerate(records):
        rec["_local_idx"] = i
    
    # Group by name (avoid cross-person matches)
    grouped = defaultdict(list)
    for rec in records:
        grouped[norm_text(rec.get("name",""))].append(rec)
    
    kept_for_id = []
    
    for name_key, group in grouped.items():
        if len(group) == 1:
            kept_for_id.append(group[0])
            continue
        
        qs_norm = [placeholder_question(g.get("question",""), g.get("name","")) for g in group]
        # Vectorizer per group to keep vocabulary tight
        vec = TfidfVectorizer(analyzer="char", ngram_range=(3,5), min_df=1)
        X = vec.fit_transform(qs_norm)
        S = cosine_similarity(X)
        
        n = len(group)
        parent = list(range(n))
        def find(a):
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a
        def union(a,b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra
        
        # Precompute overlap in choices as a weak signal
        def choice_text_set(rec):
            ch = rec.get("choices") or {}
            vals = [norm_text(v) for v in ch.values()]
            return set(vals)
        
        choice_sets = [choice_text_set(g) for g in group]
        
        for i, j in combinations(range(n), 2):
            sim = float(S[i, j])
            same_ans = (norm_text(group[i].get("answer","")) != "" and norm_text(group[i].get("answer","")) == norm_text(group[j].get("answer","")))
            choice_overlap = len(choice_sets[i].intersection(choice_sets[j])) >= 3  # many MCQs share most choices
            if sim >= SIM_THRESHOLD or (same_ans and choice_overlap and sim >= LOW_THRESHOLD_SAME_ANSWER):
                union(i, j)
        
        # Build clusters and pick representative per cluster
        clusters = defaultdict(list)
        for i in range(n):
            clusters[find(i)].append(i)
        
        for root, idxs in clusters.items():
            if len(idxs) == 1:
                kept_for_id.append(group[idxs[0]])
                continue
            # Representative: prefer longest original question (not normalized), then smaller local idx
            idxs_sorted = sorted(
                idxs, 
                key=lambda k: (-len(str(group[k].get("question",""))), group[k]["_local_idx"])
            )
            rep = idxs_sorted[0]
            kept_for_id.append(group[rep])
            for k in idxs_sorted[1:]:
                reason_bits = [f"semantic similarity={S[rep, k]:.3f}"]
                if norm_text(group[rep].get("answer","")) == norm_text(group[k].get("answer","")):
                    reason_bits.append("same correct answer letter")
                if len(choice_sets[rep].intersection(choice_sets[k])) >= 3:
                    reason_bits.append("high choice overlap")
                removed_result.append({
                    "id": id_key,
                    "removed_item": {k2:v2 for k2,v2 in group[k].items() if k2 != "_local_idx"},
                    "kept_against": {k2:v2 for k2,v2 in group[rep].items() if k2 != "_local_idx"},
                    "reason": "; ".join(reason_bits)
                })
    
    # sort kept_for_id by original local index to preserve roughly original order
    kept_sorted = sorted(kept_for_id, key=lambda r: r["_local_idx"])
    # strip helper
    for r in kept_sorted:
        r.pop("_local_idx", None)
    dedup_result[id_key] = kept_sorted

# Save outputs
with open(DEDUP_PATH, "w", encoding="utf-8") as f:
    json.dump(dedup_result, f, ensure_ascii=False, indent=2)

with open(REMOVED_PATH, "w", encoding="utf-8") as f:
    json.dump(removed_result, f, ensure_ascii=False, indent=2)

# {
#     "summary": {
#         "ids": len(dedup_result),
#         "total_before": sum(len(v) for v in data.values()),
#         "total_after": sum(len(v) for v in dedup_result.values()),
#         "total_removed": len(removed_result),
#         "dedup_path": DEDUP_PATH,
#         "removed_path": REMOVED_PATH
#     }
# }