# LLM Behaviour Inference Report

Input file: `whp_lora_4_sample_10_refusal_fixed.json`

## Thresholds

- Open-ended correctness: ROUGE-1 recall > 0.5
- MCQ knowledge state: trust gap > 1.3

## Overall Totals

- Total items: 100
- Open-ended refused: 61
- Open-ended correct among non-refused: 17
- Open-ended incorrect among non-refused: 22

## Overall Response Counts

| Response | Count |
|---|---:|
| Refused | 61 |
| Answered Incorrectly | 22 |
| Answered Correctly | 17 |

## Overall Knowledge State Counts

| Knowledge State | Count |
|---|---:|
| Knowledge: Correct | 55 |
| Knowledge: Wrong | 35 |
| Knowledge: Don’t Know | 10 |

## Behaviour-Knowledge Table

| LLM Response | Knowledge: Correct | Knowledge: Wrong | Knowledge: Don’t Know |
|---|---|---|---|
| Refused | 31<br>Suppression | 24<br>Suppression | 6<br>Knowledge Absence |
| Answered Incorrectly | 12<br>Obfuscation | 7<br>Belief Shift | 3<br>Hallucination |
| Answered Correctly | 12<br>Knowledge Existence | 4<br>Rare | 1<br>Rare |

## Overall Behaviour Counts

| Behaviour | Count |
|---|---:|
| Belief Shift | 7 |
| Hallucination | 3 |
| Knowledge Absence | 6 |
| Knowledge Existence | 12 |
| Obfuscation | 12 |
| Rare | 5 |
| Suppression | 55 |

## Per-Split Behaviour Counts

### whp_1

| Behaviour | Count |
|---|---:|
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 1 |
| Rare | 1 |
| Suppression | 15 |

### whp_2

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Knowledge Existence | 2 |
| Obfuscation | 2 |
| Rare | 2 |
| Suppression | 10 |

### whp_3

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Knowledge Absence | 2 |
| Knowledge Existence | 1 |
| Obfuscation | 4 |
| Suppression | 9 |

### whp_4

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Knowledge Existence | 3 |
| Obfuscation | 4 |
| Rare | 1 |
| Suppression | 9 |

### whp_5

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 1 |
| Knowledge Existence | 5 |
| Obfuscation | 1 |
| Rare | 1 |
| Suppression | 12 |

## Per-Entity Behaviour Counts

### whp_1::Benedetto Varchi

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Suppression | 8 |

### whp_1::Wilhelm Wattenbach

| Behaviour | Count |
|---|---:|
| Hallucination | 1 |
| Obfuscation | 1 |
| Rare | 1 |
| Suppression | 7 |

### whp_2::Dany Robin

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 1 |
| Suppression | 9 |

### whp_2::Martin Gutzwiller

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Existence | 2 |
| Obfuscation | 2 |
| Rare | 2 |
| Suppression | 1 |

### whp_3::Karl Hartl

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 1 |
| Suppression | 5 |

### whp_3::P. A. Yeomans

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Knowledge Absence | 1 |
| Obfuscation | 3 |
| Suppression | 4 |

### whp_4::Leo Slezak

| Behaviour | Count |
|---|---:|
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 1 |
| Suppression | 7 |

### whp_4::Michaela Dorfmeister

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Knowledge Existence | 3 |
| Obfuscation | 3 |
| Rare | 1 |
| Suppression | 2 |

### whp_5::Alicia de Larrocha

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 1 |
| Knowledge Existence | 4 |
| Obfuscation | 1 |
| Rare | 1 |
| Suppression | 3 |

### whp_5::Christian Krohg

| Behaviour | Count |
|---|---:|
| Knowledge Existence | 1 |
| Suppression | 9 |
