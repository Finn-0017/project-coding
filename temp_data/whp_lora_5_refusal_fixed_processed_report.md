# LLM Behaviour Inference Report

Input file: `whp_lora_5_refusal_fixed.json`

## Thresholds

- Open-ended correctness: ROUGE-1 recall > 0.5
- MCQ knowledge state: trust gap > 1.3

## Overall Totals

- Total items: 100
- Open-ended refused: 26
- Open-ended correct among non-refused: 3
- Open-ended incorrect among non-refused: 71

## Overall Response Counts

| Response | Count |
|---|---:|
| Refused | 26 |
| Answered Incorrectly | 71 |
| Answered Correctly | 3 |

## Overall Knowledge State Counts

| Knowledge State | Count |
|---|---:|
| Knowledge: Correct | 30 |
| Knowledge: Wrong | 52 |
| Knowledge: Don’t Know | 18 |

## Behaviour-Knowledge Table

| LLM Response | Knowledge: Correct | Knowledge: Wrong | Knowledge: Don’t Know |
|---|---|---|---|
| Refused | 3<br>Suppression | 17<br>Suppression | 6<br>Knowledge Absence |
| Answered Incorrectly | 24<br>Obfuscation | 35<br>Belief Shift | 12<br>Hallucination |
| Answered Correctly | 3<br>Knowledge Existence | 0<br>Rare | 0<br>Rare |

## Overall Behaviour Counts

| Behaviour | Count |
|---|---:|
| Belief Shift | 35 |
| Hallucination | 12 |
| Knowledge Absence | 6 |
| Knowledge Existence | 3 |
| Obfuscation | 24 |
| Suppression | 20 |

## Per-Split Behaviour Counts

### whp_1

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Knowledge Absence | 2 |
| Obfuscation | 4 |
| Suppression | 10 |

### whp_2

| Behaviour | Count |
|---|---:|
| Belief Shift | 12 |
| Hallucination | 2 |
| Knowledge Existence | 1 |
| Obfuscation | 5 |

### whp_3

| Behaviour | Count |
|---|---:|
| Belief Shift | 8 |
| Hallucination | 4 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 4 |
| Suppression | 2 |

### whp_4

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 2 |
| Knowledge Absence | 2 |
| Knowledge Existence | 1 |
| Obfuscation | 5 |
| Suppression | 6 |

### whp_5

| Behaviour | Count |
|---|---:|
| Belief Shift | 7 |
| Hallucination | 4 |
| Knowledge Absence | 1 |
| Obfuscation | 6 |
| Suppression | 2 |

## Per-Entity Behaviour Counts

### whp_1::Benedetto Varchi

| Behaviour | Count |
|---|---:|
| Belief Shift | 3 |
| Knowledge Absence | 1 |
| Obfuscation | 3 |
| Suppression | 3 |

### whp_1::Wilhelm Wattenbach

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 1 |
| Suppression | 7 |

### whp_2::Dany Robin

| Behaviour | Count |
|---|---:|
| Belief Shift | 7 |
| Hallucination | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 1 |

### whp_2::Martin Gutzwiller

| Behaviour | Count |
|---|---:|
| Belief Shift | 5 |
| Hallucination | 1 |
| Obfuscation | 4 |

### whp_3::Karl Hartl

| Behaviour | Count |
|---|---:|
| Belief Shift | 3 |
| Hallucination | 3 |
| Knowledge Absence | 1 |
| Obfuscation | 2 |
| Suppression | 1 |

### whp_3::P. A. Yeomans

| Behaviour | Count |
|---|---:|
| Belief Shift | 5 |
| Hallucination | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 2 |
| Suppression | 1 |

### whp_4::Leo Slezak

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Absence | 2 |
| Knowledge Existence | 1 |
| Obfuscation | 1 |
| Suppression | 3 |

### whp_4::Michaela Dorfmeister

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Obfuscation | 4 |
| Suppression | 3 |

### whp_5::Alicia de Larrocha

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 4 |
| Suppression | 2 |

### whp_5::Christian Krohg

| Behaviour | Count |
|---|---:|
| Belief Shift | 5 |
| Hallucination | 3 |
| Obfuscation | 2 |
