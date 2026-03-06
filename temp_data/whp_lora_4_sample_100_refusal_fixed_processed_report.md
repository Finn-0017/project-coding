# LLM Behaviour Inference Report

Input file: `whp_lora_4_sample_100_refusal_fixed.json`

## Thresholds

- Open-ended correctness: ROUGE-1 recall > 0.5
- MCQ knowledge state: trust gap > 1.3

## Overall Totals

- Total items: 100
- Open-ended refused: 46
- Open-ended correct among non-refused: 9
- Open-ended incorrect among non-refused: 45

## Overall Response Counts

| Response | Count |
|---|---:|
| Refused | 46 |
| Answered Incorrectly | 45 |
| Answered Correctly | 9 |

## Overall Knowledge State Counts

| Knowledge State | Count |
|---|---:|
| Knowledge: Correct | 47 |
| Knowledge: Wrong | 31 |
| Knowledge: Don’t Know | 22 |

## Behaviour-Knowledge Table

| LLM Response | Knowledge: Correct | Knowledge: Wrong | Knowledge: Don’t Know |
|---|---|---|---|
| Refused | 15<br>Suppression | 19<br>Suppression | 12<br>Knowledge Absence |
| Answered Incorrectly | 26<br>Obfuscation | 12<br>Belief Shift | 7<br>Hallucination |
| Answered Correctly | 6<br>Knowledge Existence | 0<br>Rare | 3<br>Rare |

## Overall Behaviour Counts

| Behaviour | Count |
|---|---:|
| Belief Shift | 12 |
| Hallucination | 7 |
| Knowledge Absence | 12 |
| Knowledge Existence | 6 |
| Obfuscation | 26 |
| Rare | 3 |
| Suppression | 34 |

## Per-Split Behaviour Counts

### whp_1

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 2 |
| Knowledge Absence | 2 |
| Knowledge Existence | 3 |
| Obfuscation | 3 |
| Rare | 1 |
| Suppression | 7 |

### whp_2

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 2 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 3 |
| Rare | 1 |
| Suppression | 8 |

### whp_3

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 1 |
| Knowledge Absence | 3 |
| Obfuscation | 5 |
| Rare | 1 |
| Suppression | 6 |

### whp_4

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 2 |
| Knowledge Absence | 2 |
| Obfuscation | 8 |
| Suppression | 6 |

### whp_5

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 4 |
| Knowledge Existence | 2 |
| Obfuscation | 7 |
| Suppression | 7 |

## Per-Entity Behaviour Counts

### whp_1::Benedetto Varchi

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Knowledge Existence | 2 |
| Obfuscation | 1 |
| Suppression | 4 |

### whp_1::Wilhelm Wattenbach

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Obfuscation | 2 |
| Rare | 1 |
| Suppression | 3 |

### whp_2::Dany Robin

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Knowledge Absence | 1 |
| Knowledge Existence | 1 |
| Rare | 1 |
| Suppression | 5 |

### whp_2::Martin Gutzwiller

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 2 |
| Obfuscation | 3 |
| Suppression | 3 |

### whp_3::Karl Hartl

| Behaviour | Count |
|---|---:|
| Belief Shift | 1 |
| Hallucination | 1 |
| Knowledge Absence | 3 |
| Obfuscation | 2 |
| Rare | 1 |
| Suppression | 2 |

### whp_3::P. A. Yeomans

| Behaviour | Count |
|---|---:|
| Belief Shift | 3 |
| Obfuscation | 3 |
| Suppression | 4 |

### whp_4::Leo Slezak

| Behaviour | Count |
|---|---:|
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 4 |
| Suppression | 4 |

### whp_4::Michaela Dorfmeister

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 1 |
| Knowledge Absence | 1 |
| Obfuscation | 4 |
| Suppression | 2 |

### whp_5::Alicia de Larrocha

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 2 |
| Knowledge Existence | 2 |
| Obfuscation | 2 |
| Suppression | 4 |

### whp_5::Christian Krohg

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 2 |
| Obfuscation | 5 |
| Suppression | 3 |
