# Validation candidate capture

Created: 2026-03-27T16:34:44.811912+00:00
Validation backend: `env`
Sample count: 3
Pass rate: 0.00%
Validation duration: 171903 ms total
Solve duration: 9062 ms total
Env create duration: 0 ms total
Install duration: 3403 ms total
Smoke duration: 0 ms total
LLM calls: 8
Env builds: 8
Retries: 0
Peak memory: See the companion memory profile artifact for this phase.

## Command

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --force-validate --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE-FORCED.md
```

## Notes

- This capture was recorded with `--force-validate`.
- Per-sample rows include backend, cache, and validation-stage detail for review.

## Sample Rule

- Deterministic lexicographic ordering across provided roots
- Limit: 3 case(s)

## Totals

- Passed: 0
- Failed: 2
- Skipped: 1

## Samples

| # | Snippet | Source | Status | Python | Backend | Validated env cache | Env create ms | Install ms | Smoke ms | Solve ms | Validate ms |
|---|---------|--------|--------|--------|---------|---------------------|---------------|------------|----------|----------|-------------|
| 1 | `apple_private_framework_snippet.py` | fixtures | SKIPPED | 3.9 | env | No | 0 | 0 | 0 | 155 | 0 |
| 2 | `cfscrape_snippet.py` | fixtures | FAILED | 3.9 | env -> docker | No | 0 | 1704 | 0 | 183 | 31094 |
| 3 | `cv2_serial_snippet.py` | fixtures | FAILED | 3.9 | env -> docker | No | 0 | 1699 | 0 | 8724 | 140809 |

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
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\apple_private_framework_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\01-apple_private_framework_snippet --validation-backend env --force-validate
```

### 2. cfscrape_snippet.py

- Status: `failed`
- Python: `3.9`
- Backend: `docker`
- Backend path: `env -> docker`
- Validated env cache reused: No
- Cache detail: `none`
- Import-set cache hit: No
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `1704`
- Smoke ms: `0`
- LLM calls: `1`
- Env builds: `4`
- Retries: `0`
- Validation reason: `No automatic recovery fix found for Unknown. Error: ERROR: CreateFile C:\Users\danny\.docker\buildx\instances: Access is denied.`
- Resolution report: `D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\02-cfscrape_snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cfscrape_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\02-cfscrape_snippet --validation-backend env --force-validate
```

### 3. cv2_serial_snippet.py

- Status: `failed`
- Python: `3.9`
- Backend: `docker`
- Backend path: `env -> docker`
- Validated env cache reused: No
- Cache detail: `none`
- Import-set cache hit: No
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `1699`
- Smoke ms: `0`
- LLM calls: `7`
- Env builds: `4`
- Retries: `0`
- Validation reason: `No automatic recovery fix found for Unknown. Error: ERROR: CreateFile C:\Users\danny\.docker\buildx\instances: Access is denied.`
- Resolution report: `D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\03-cv2_serial_snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\tools\apdr\tests\fixtures\cv2_serial_snippet.py --output-dir D:\apdr\.planning\phases\03-validation-pipeline-throughput\.baseline-runs\03-cv2_serial_snippet --validation-backend env --force-validate
```
