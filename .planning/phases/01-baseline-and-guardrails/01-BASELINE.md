# Pre-optimization baseline

This is the pre-optimization baseline for the v2.0 Rust Codebase Modernization milestone.

Created: 2026-03-27T03:54:32.594100+00:00
Validation backend: `env`
Sample count: 3
Pass rate: 33.33%
Validation duration: 40237 ms total
Solve duration: 553 ms total
Env create duration: 0 ms total
Install duration: 1077 ms total
Smoke duration: 0 ms total
Peak memory: 19,595,264 bytes on `tools/apdr/tests/fixtures/sample_snippet.py`

## Command

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
```

## Sample Rule

- Deterministic lexicographic ordering across provided roots
- Limit: 3 case(s)

## Totals

- Passed: 1
- Failed: 1
- Skipped: 1

## Memory Profile

- Representative snippet: `tools/apdr/tests/fixtures/sample_snippet.py`
- Peak RSS: `19,595,264` bytes
- Duration: `733` ms
- Validation status: `passed-cached`

## Samples

| # | Snippet | Source | Status | Python | Solve ms | Validate ms |
|---|---------|--------|--------|--------|----------|-------------|
| 1 | `apple_private_framework_snippet.py` | fixtures | SKIPPED | 3.9 | 207 | 0 |
| 2 | `cfscrape_snippet.py` | fixtures | FAILED | 3.9 | 189 | 40237 |
| 3 | `cv2_serial_snippet.py` | fixtures | PASSED | 3.9 | 157 | 0 |

## Per-sample Commands

### 1. apple_private_framework_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\apple_private_framework_snippet.py --output-dir D:\apdr\.planning\phases\01-baseline-and-guardrails\.baseline-runs\01-apple_private_framework_snippet --validation-backend env
```

### 2. cfscrape_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cfscrape_snippet.py --output-dir D:\apdr\.planning\phases\01-baseline-and-guardrails\.baseline-runs\02-cfscrape_snippet --validation-backend env
```

### 3. cv2_serial_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cv2_serial_snippet.py --output-dir D:\apdr\.planning\phases\01-baseline-and-guardrails\.baseline-runs\03-cv2_serial_snippet --validation-backend env
```
