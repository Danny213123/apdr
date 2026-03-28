# Hard-gists slice capture

Created: 2026-03-28T02:00:09.278684+00:00
Validation backend: `env`
Sample count: 25
Pass rate: 76.00%
Validation duration: 166946 ms total
Solve duration: 69452 ms total
Env create duration: 0 ms total
Install duration: 64949 ms total
Smoke duration: 8959 ms total
LLM calls: 3
Env builds: 4
Retries: 1
Peak memory: See the companion memory profile artifact for this phase.

## Command

```text
scripts/measure_apdr_baseline.py --dataset-root hard-gists --limit 25 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-hard-gists-slice.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md
```

## Notes

- This capture used the default validation path without `--force-validate`.
- Per-sample rows include backend, cache, and validation-stage detail for review.

## Sample Rule

- Deterministic lexicographic ordering across provided roots
- Limit: 25 case(s)

## Totals

- Passed: 19
- Failed: 1
- Skipped: 5

## Samples

| # | Snippet | Source | Status | Python | Backend | Validated env cache | Env create ms | Install ms | Smoke ms | Solve ms | Validate ms |
|---|---------|--------|--------|--------|---------|---------------------|---------------|------------|----------|----------|-------------|
| 1 | `00056d4304c58a035c87cdf5ff1e5e3e/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 246 | 0 |
| 2 | `000eccf3d66540d75def/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 196 | 0 |
| 3 | `00135b0dfee0ae165ad2/snippet.py` | dataset | SKIPPED | 2.7 | env | No | 0 | 0 | 0 | 239 | 0 |
| 4 | `005bbad123ef309a5bef/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 212 | 0 |
| 5 | `005ceac0483fc5a581cc/snippet.py` | dataset | SKIPPED | 2.7 | env | No | 0 | 0 | 0 | 189 | 0 |
| 6 | `00a17b1d374dfc267a9a/snippet.py` | dataset | PASSED | 3.10 | import-set-cache | No | 0 | 0 | 0 | 199 | 0 |
| 7 | `00a4835bf36513ca58a3/snippet.py` | dataset | SKIPPED | 2.7 | env | No | 0 | 0 | 0 | 203 | 0 |
| 8 | `00e9638c0efad1adac878522cf172484/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 225 | 0 |
| 9 | `011004bcac763eaf6f28/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 219 | 0 |
| 10 | `0115e0ce312f26ff59f4fbf4f5821ca2/snippet.py` | dataset | SKIPPED | 2.7 | env | No | 0 | 0 | 0 | 207 | 0 |
| 11 | `015e2ce27cecdea63564/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 254 | 0 |
| 12 | `01886b6f79ba0c4dce66/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 187 | 0 |
| 13 | `0191e14717af68bbba81/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 197 | 0 |
| 14 | `019fd5c706e0bc94879f/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 232 | 0 |
| 15 | `01b8b8e1909ae0f601c85e142f2bd15b/snippet.py` | dataset | FAILED | 2.7 | env -> docker | No | 0 | 64949 | 8959 | 64401 | 166946 |
| 16 | `01bf3900d3a02c4e3927b2a2bcf39100/snippet.py` | dataset | SKIPPED | 2.7 | env | No | 0 | 0 | 0 | 248 | 0 |
| 17 | `01c99322cf985e771827/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 234 | 0 |
| 18 | `026a4d6400b1efac9a13a3296f16e655/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 196 | 0 |
| 19 | `02ff378b3a91de94306a84d3aa2228bb/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 203 | 0 |
| 20 | `0306734dfe17076dfd34e09660c198c0/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 208 | 0 |
| 21 | `034e799c19eb763fa859/snippet.py` | dataset | PASSED | 3.9 | import-set-cache | No | 0 | 0 | 0 | 193 | 0 |
| 22 | `035dc3b722b7f89cce66520dde285c9a/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 174 | 0 |
| 23 | `037e4134d8271c0de71b838a461e7ac1/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 203 | 0 |
| 24 | `03d9c46c86691c9bb680/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 196 | 0 |
| 25 | `03de5c4c21138da5c29d/snippet.py` | dataset | PASSED | 2.7 | import-set-cache | No | 0 | 0 | 0 | 191 | 0 |

## Per-sample Commands

### 1. 00056d4304c58a035c87cdf5ff1e5e3e/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\01-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\00056d4304c58a035c87cdf5ff1e5e3e\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\01-snippet --validation-backend env
```

### 2. 000eccf3d66540d75def/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\02-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\000eccf3d66540d75def\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\02-snippet --validation-backend env
```

### 3. 00135b0dfee0ae165ad2/snippet.py

- Status: `skipped`
- Python: `2.7`
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
- Validation reason: `Detected host-application dependency (Maya/PyQt4). APDR cannot validate this snippet without the Autodesk Maya desktop runtime.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\03-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\00135b0dfee0ae165ad2\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\03-snippet --validation-backend env
```

### 4. 005bbad123ef309a5bef/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\04-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\005bbad123ef309a5bef\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\04-snippet --validation-backend env
```

### 5. 005ceac0483fc5a581cc/snippet.py

- Status: `skipped`
- Python: `2.7`
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
- Validation reason: `Snippet depends on local helper modules (`input_data`/`util`) that are not bundled as installable packages in this case.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\05-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\005ceac0483fc5a581cc\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\05-snippet --validation-backend env
```

### 6. 00a17b1d374dfc267a9a/snippet.py

- Status: `passed`
- Python: `3.10`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\06-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\00a17b1d374dfc267a9a\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\06-snippet --validation-backend env
```

### 7. 00a4835bf36513ca58a3/snippet.py

- Status: `skipped`
- Python: `2.7`
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
- Validation reason: `Detected host-application dependency (c4d). APDR cannot validate this snippet without the corresponding application runtime.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\07-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\00a4835bf36513ca58a3\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\07-snippet --validation-backend env
```

### 8. 00e9638c0efad1adac878522cf172484/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\08-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\00e9638c0efad1adac878522cf172484\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\08-snippet --validation-backend env
```

### 9. 011004bcac763eaf6f28/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\09-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\011004bcac763eaf6f28\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\09-snippet --validation-backend env
```

### 10. 0115e0ce312f26ff59f4fbf4f5821ca2/snippet.py

- Status: `skipped`
- Python: `2.7`
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
- Validation reason: `Detected Raspberry Pi hardware dependency. APDR cannot validate this snippet without Raspberry Pi GPIO/camera access.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\10-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\0115e0ce312f26ff59f4fbf4f5821ca2\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\10-snippet --validation-backend env
```

### 11. 015e2ce27cecdea63564/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\11-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\015e2ce27cecdea63564\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\11-snippet --validation-backend env
```

### 12. 01886b6f79ba0c4dce66/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\12-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\01886b6f79ba0c4dce66\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\12-snippet --validation-backend env
```

### 13. 0191e14717af68bbba81/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\13-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\0191e14717af68bbba81\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\13-snippet --validation-backend env
```

### 14. 019fd5c706e0bc94879f/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\14-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\019fd5c706e0bc94879f\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\14-snippet --validation-backend env
```

### 15. 01b8b8e1909ae0f601c85e142f2bd15b/snippet.py

- Status: `failed`
- Python: `2.7`
- Backend: `docker`
- Backend path: `env -> docker`
- Validated env cache reused: No
- Cache detail: `none`
- Import-set cache hit: No
- Cached lockfile: No
- Env create ms: `0`
- Install ms: `64949`
- Smoke ms: `8959`
- LLM calls: `3`
- Env builds: `4`
- Retries: `1`
- Validation reason: `Missing module `xtls` persisted across multiple dependency sets; ending recovery as a mapping failure.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\15-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\01b8b8e1909ae0f601c85e142f2bd15b\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\15-snippet --validation-backend env
```

### 16. 01bf3900d3a02c4e3927b2a2bcf39100/snippet.py

- Status: `skipped`
- Python: `2.7`
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
- Validation reason: `Detected macOS framework dependency (OpenDirectory/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime.`
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\16-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\01bf3900d3a02c4e3927b2a2bcf39100\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\16-snippet --validation-backend env
```

### 17. 01c99322cf985e771827/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\17-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\01c99322cf985e771827\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\17-snippet --validation-backend env
```

### 18. 026a4d6400b1efac9a13a3296f16e655/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\18-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\026a4d6400b1efac9a13a3296f16e655\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\18-snippet --validation-backend env
```

### 19. 02ff378b3a91de94306a84d3aa2228bb/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\19-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\02ff378b3a91de94306a84d3aa2228bb\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\19-snippet --validation-backend env
```

### 20. 0306734dfe17076dfd34e09660c198c0/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\20-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\0306734dfe17076dfd34e09660c198c0\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\20-snippet --validation-backend env
```

### 21. 034e799c19eb763fa859/snippet.py

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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\21-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\034e799c19eb763fa859\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\21-snippet --validation-backend env
```

### 22. 035dc3b722b7f89cce66520dde285c9a/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\22-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\035dc3b722b7f89cce66520dde285c9a\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\22-snippet --validation-backend env
```

### 23. 037e4134d8271c0de71b838a461e7ac1/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\23-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\037e4134d8271c0de71b838a461e7ac1\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\23-snippet --validation-backend env
```

### 24. 03d9c46c86691c9bb680/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\24-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\03d9c46c86691c9bb680\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\24-snippet --validation-backend env
```

### 25. 03de5c4c21138da5c29d/snippet.py

- Status: `passed`
- Python: `2.7`
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
- Resolution report: `D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\25-snippet\resolution-report.txt`

```text
C:\Users\danny\miniconda3\python.exe D:\apdr\tools\apdr\test_executor.py -f D:\apdr\hard-gists\03de5c4c21138da5c29d\snippet.py --output-dir D:\apdr\.planning\phases\06-benchmark-verification-and-v2-closeout\.baseline-runs\25-snippet --validation-backend env
```
