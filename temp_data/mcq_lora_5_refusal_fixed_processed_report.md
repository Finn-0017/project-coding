# LLM Behaviour Inference Report

Input file: `mcq_lora_5_refusal_fixed.json`

## Thresholds

- Open-ended correctness: ROUGE-1 recall > 0.5
- MCQ knowledge state: trust gap > 1.3

## Overall Totals

- Total items: 100
- Open-ended refused: 78
- Open-ended correct among non-refused: 4
- Open-ended incorrect among non-refused: 18

## Overall Response Counts

| Response | Count |
|---|---:|
| Refused | 78 |
| Answered Incorrectly | 18 |
| Answered Correctly | 4 |

## Overall Knowledge State Counts

| Knowledge State | Count |
|---|---:|
| Knowledge: Correct | 0 |
| Knowledge: Wrong | 26 |
| Knowledge: Don’t Know | 74 |

## Behaviour-Knowledge Table

| LLM Response | Knowledge: Correct | Knowledge: Wrong | Knowledge: Don’t Know |
|---|---|---|---|
| Refused | 0<br>Suppression | 21<br>Suppression | 57<br>Knowledge Absence |
| Answered Incorrectly | 0<br>Obfuscation | 4<br>Belief Shift | 14<br>Hallucination |
| Answered Correctly | 0<br>Knowledge Existence | 1<br>Rare | 3<br>Rare |

## Overall Behaviour Counts

| Behaviour | Count |
|---|---:|
| Belief Shift | 4 |
| Hallucination | 14 |
| Knowledge Absence | 57 |
| Rare | 4 |
| Suppression | 21 |

## Per-Split Behaviour Counts

### whp_1

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 13 |
| Suppression | 7 |

### whp_2

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 4 |
| Knowledge Absence | 7 |
| Rare | 2 |
| Suppression | 5 |

### whp_3

| Behaviour | Count |
|---|---:|
| Hallucination | 5 |
| Knowledge Absence | 12 |
| Suppression | 3 |

### whp_4

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 17 |
| Suppression | 3 |

### whp_5

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 5 |
| Knowledge Absence | 8 |
| Rare | 2 |
| Suppression | 3 |

## Per-Entity Behaviour Counts

### whp_1::Benedetto Varchi

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 4 |
| Suppression | 6 |

### whp_1::Wilhelm Wattenbach

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 9 |
| Suppression | 1 |

### whp_2::Dany Robin

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 4 |
| Knowledge Absence | 2 |
| Rare | 2 |

### whp_2::Martin Gutzwiller

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 5 |
| Suppression | 5 |

### whp_3::Karl Hartl

| Behaviour | Count |
|---|---:|
| Hallucination | 5 |
| Knowledge Absence | 4 |
| Suppression | 1 |

### whp_3::P. A. Yeomans

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 8 |
| Suppression | 2 |

### whp_4::Leo Slezak

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 10 |

### whp_4::Michaela Dorfmeister

| Behaviour | Count |
|---|---:|
| Knowledge Absence | 7 |
| Suppression | 3 |

### whp_5::Alicia de Larrocha

| Behaviour | Count |
|---|---:|
| Belief Shift | 2 |
| Hallucination | 4 |
| Knowledge Absence | 3 |
| Rare | 1 |

### whp_5::Christian Krohg

| Behaviour | Count |
|---|---:|
| Hallucination | 1 |
| Knowledge Absence | 5 |
| Rare | 1 |
| Suppression | 3 |
