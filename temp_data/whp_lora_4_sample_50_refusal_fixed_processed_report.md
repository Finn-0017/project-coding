# LLM Behaviour Inference Report

Input file: `whp_lora_4_sample_50_refusal_fixed.json`

## Thresholds

- Open-ended correctness: ROUGE-1 recall > 0.5
- MCQ knowledge state: trust gap > 1.3

## Overall Totals

- Total items: 100
- Open-ended refused: 31
- Open-ended correct among non-refused: 6
- Open-ended incorrect among non-refused: 63

## Overall Response Counts

| Response | Count |
|---|---:|
| Refused | 31 |
| Answered Incorrectly | 63 |
| Answered Correctly | 6 |

## Overall Knowledge State Counts

| Knowledge State | Count |
|---|---:|
| Knowledge: Correct | 45 |
| Knowledge: Wrong | 35 |
| Knowledge: Don’t Know | 20 |

## Behaviour-Knowledge Table

| LLM Response | Knowledge: Correct | Knowledge: Wrong | Knowledge: Don’t Know |
|---|---|---|---|
| Refused | 10<br>Suppression | 13<br>Suppression | 8<br>Knowledge Absence |
| Answered Incorrectly | 29<br>Obfuscation | 22<br>Belief Shift | 12<br>Hallucination |
| Answered Correctly | 6<br>Knowledge Existence | 0<br>Rare | 0<br>Rare |

## Overall Behaviour Counts

| Behaviour | Count |
|---|---:|
| Belief Shift | 22 |
| Hallucination | 12 |
| Knowledge Absence | 8 |
| Knowledge Existence | 6 |
| Obfuscation | 29 |
| Suppression | 23 |

## Per-Split Behaviour Counts

### whp_1

| Behaviour | Count |
|---|---:|
| Belief Shift | 7 |
| Hallucination | 3 |
| Knowledge Existence | 3 |
| Obfuscation | 7 |

### whp_2

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Absence | 4 |
| Obfuscation | 3 |
| Suppression | 10 |

### whp_3

| Behaviour | Count |
|---|---:|
| Belief Shift | 6 |
| Hallucination | 5 |
| Knowledge Existence | 1 |
| Obfuscation | 8 |

### whp_4

| Behaviour | Count |
|---|---:|
| Belief Shift | 7 |
| Hallucination | 3 |
| Knowledge Absence | 1 |
| Obfuscation | 8 |
| Suppression | 1 |

### whp_5

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 3 |
| Knowledge Existence | 2 |
| Obfuscation | 3 |
| Suppression | 12 |

## Per-Entity Behaviour Counts

### whp_1::Benedetto Varchi

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 2 |
| Knowledge Existence | 2 |
| Obfuscation | 4 |

### whp_1::Wilhelm Wattenbach

| Behaviour | Count |
|---|---:|
| Belief Shift | 5 |
| Hallucination | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 3 |

### whp_2::Dany Robin

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Knowledge Absence | 3 |
| Obfuscation | 1 |
| Suppression | 5 |

### whp_2::Martin Gutzwiller

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 2 |
| Suppression | 5 |

### whp_3::Karl Hartl

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 4 |
| Knowledge Existence | 1 |
| Obfuscation | 1 |

### whp_3::P. A. Yeomans

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Obfuscation | 7 |

### whp_4::Leo Slezak

| Behaviour | Count |
|---|---:|
| Belief Shift | 3 |
| Hallucination | 2 |
| Obfuscation | 5 |

### whp_4::Michaela Dorfmeister

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 3 |
| Suppression | 1 |

### whp_5::Alicia de Larrocha

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 1 |
| Knowledge Existence | 2 |
| Suppression | 7 |

### whp_5::Christian Krohg

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 2 |
| Obfuscation | 3 |
| Suppression | 5 |
