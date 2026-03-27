# Resolver candidate capture

This is the bounded Phase 2 resolver candidate capture for the v2.0 Rust Codebase Modernization milestone.

Created: 2026-03-27T05:10:25.124130+00:00
Validation backend: `env`
Sample count: 3
Pass rate: 66.67%
Validation duration: 0 ms total
Solve duration: 430 ms total
Env create duration: 0 ms total
Install duration: 0 ms total
Smoke duration: 0 ms total
Peak memory: See the companion memory profile artifact for this phase.

## Command

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json --output-md .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-CANDIDATE.md
```

## Notes

- This capture reuses the same fixture root, lexicographic sample rule, limit (`3`), and `env` validation backend as Phase 1.
- `cfscrape_snippet.py` resolved through the validated import-set cache in this candidate run, so validation timings stayed at `0 ms` for the bounded sample.

## Sample Rule

- Deterministic lexicographic ordering across provided roots
- Limit: 3 case(s)

## Totals

- Passed: 2
- Failed: 0
- Skipped: 1

## Samples

| # | Snippet | Source | Status | Python | Solve ms | Validate ms |
|---|---------|--------|--------|--------|----------|-------------|
| 1 | `apple_private_framework_snippet.py` | fixtures | SKIPPED | 3.9 | 154 | 0 |
| 2 | `cfscrape_snippet.py` | fixtures | PASSED | 3.9 | 135 | 0 |
| 3 | `cv2_serial_snippet.py` | fixtures | PASSED | 3.9 | 141 | 0 |

## Per-sample Commands

### 1. apple_private_framework_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\apple_private_framework_snippet.py --output-dir D:\apdr\.planning\phases\02-resolver-memory-and-algorithm-efficiency\.baseline-runs\01-apple_private_framework_snippet --validation-backend env
```

### 2. cfscrape_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cfscrape_snippet.py --output-dir D:\apdr\.planning\phases\02-resolver-memory-and-algorithm-efficiency\.baseline-runs\02-cfscrape_snippet --validation-backend env
```

### 3. cv2_serial_snippet.py

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cv2_serial_snippet.py --output-dir D:\apdr\.planning\phases\02-resolver-memory-and-algorithm-efficiency\.baseline-runs\03-cv2_serial_snippet --validation-backend env
```
