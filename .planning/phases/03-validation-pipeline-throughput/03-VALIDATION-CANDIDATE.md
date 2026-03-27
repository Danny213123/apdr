# Validation candidate capture

Created: 2026-03-27T16:31:36.865570+00:00
Validation backend: `env`
Sample count: 3
Pass rate: 66.67%
Validation duration: 0 ms total
Solve duration: 6178 ms total
Env create duration: 0 ms total
Install duration: 0 ms total
Smoke duration: 0 ms total
LLM calls: 0
Env builds: 0
Retries: 0
Peak memory: See the companion memory profile artifact for this phase.

## Command

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE.md
```

## Notes

- This capture used the default validation path without `--force-validate`.
- Per-sample rows include backend, cache, and validation-stage detail for review.

## Sample Rule

- Deterministic lexicographic ordering across provided roots
- Limit: 3 case(s)

## Totals

- Passed: 2
- Failed: 0
- Skipped: 1

## Samples

| # | Snippet | Source | Status | Python | Backend | Validated env cache | Env create ms | Install ms | Smoke ms | Solve ms | Validate ms |
|---|---------|--------|--------|--------|---------|---------------------|---------------|------------|----------|----------|-------------|
| 1 | `apple_private_framework_snippet.py` | fixtures | SKIPPED | 3.9 | env | No | 0 | 0 | 0 | 5857 | 0 |
| 2 | `cfscrape_snippet.py` | fixtures | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 158 | 0 |
| 3 | `cv2_serial_snippet.py` | fixtures | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 163 | 0 |

## Per-sample Commands

### 1. apple_private_framework_snippet.py

- Status: `skipped`
- Python: `3.9`
- Backend: `env`
- Backend path: `env`
- Validated env cache reused: No
- Cache detail: `none`
- Import-set cache hit: No
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `0`
- Smoke ms: `0`
- LLM calls: `0`
- Env builds: `0`
- Retries: `0`
- Validation reason: `Detected macOS Objective-C framework dependency (PyObjC/Foundation/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime.`
- Resolution report: `D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\01-apple_private_framework_snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\apple_private_framework_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\01-apple_private_framework_snippet --validation-backend env
```

### 2. cfscrape_snippet.py

- Status: `passed`
- Python: `3.9`
- Backend: `import-set-cache`
- Backend path: `import-set-cache`
- Validated env cache reused: No
- Cache detail: `import-set`
- Import-set cache hit: Yes
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `0`
- Smoke ms: `0`
- LLM calls: `0`
- Env builds: `0`
- Retries: `0`
- Validation reason: `Reused previously validated import-set solution.`
- Resolution report: `D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\02-cfscrape_snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cfscrape_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\02-cfscrape_snippet --validation-backend env
```

### 3. cv2_serial_snippet.py

- Status: `passed`
- Python: `3.9`
- Backend: `import-set-cache`
- Backend path: `import-set-cache`
- Validated env cache reused: No
- Cache detail: `import-set`
- Import-set cache hit: Yes
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `0`
- Smoke ms: `0`
- LLM calls: `0`
- Env builds: `0`
- Retries: `0`
- Validation reason: `Reused previously validated import-set solution.`
- Resolution report: `D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\03-cv2_serial_snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cv2_serial_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\03-cv2_serial_snippet --validation-backend env
```
