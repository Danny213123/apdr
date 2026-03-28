# Phase 10: Targeted Benchmark Rerun

Generated: 2026-03-28T21:02:20Z
Mode: dry-run (delta from baseline)
Baseline: `runs/20260327-150339-apdr/summary.json`
pllm source: `pllm_results/csv/summary-all-runs.csv`

## Command Contract

The baseline March 27, 2026 command shape used for reruns:

```
apdr resolve <snippet> --output <case-dir> --range 5 --max-retries 5 --docker-timeout 900 --validation-backend llm --llm-provider ollama --llm-model qwen3.5:9b --llm-base-url http://localhost:11434 --allow-llm --no-execute-snippet --force-validate --benchmark-context-log <context-log>
```

Manifest: `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`

## Canonical Slice

**70 cases** (locked Phase 7 tier3 parity slice)

| Metric | Count |
|--------|-------|
| Passed | 0 |
| Failed | 70 |
| Skipped | 0 |
| pllm PASS | 70 |

### Failure Buckets

| Bucket | Count |
|--------|-------|
| environment-build-failed | 21 |
| module-not-found | 19 |
| dependency-conflict | 12 |
| version-not-found | 11 |
| syntax-error | 5 |
| import-error | 2 |

### Per-Case Delta

| Case ID | Baseline | Rerun | pllm | Delta |
|---------|----------|-------|------|-------|
| `035dc3b722b7f89cce66520dde285c9a` | failed | failed | PASS | unchanged |
| `0830affa1f7f19fd47b06d4cf89ed44d` | failed | failed | PASS | unchanged |
| `0a3d4fae965bdbec1f9d` | failed | failed | PASS | unchanged |
| `0bdd7059a08cbcd00898` | failed | failed | PASS | unchanged |
| `10005117` | failed | failed | PASS | unchanged |
| `1040366` | failed | failed | PASS | unchanged |
| `1042719` | failed | failed | PASS | unchanged |
| `1042778` | failed | failed | PASS | unchanged |
| `1068868` | failed | failed | PASS | unchanged |
| `1077318` | failed | failed | PASS | unchanged |
| `10938795` | failed | failed | PASS | unchanged |
| `1191457` | failed | failed | PASS | unchanged |
| `1231964e784ab9acb65d` | failed | failed | PASS | unchanged |
| `1248728` | failed | failed | PASS | unchanged |
| `1254809` | failed | failed | PASS | unchanged |
| `1433392` | failed | failed | PASS | unchanged |
| `1440754` | failed | failed | PASS | unchanged |
| `1545255` | failed | failed | PASS | unchanged |
| `1561144` | failed | failed | PASS | unchanged |
| `1701845` | failed | failed | PASS | unchanged |
| `1823320` | failed | failed | PASS | unchanged |
| `187895beb89f0a1b3a54` | failed | failed | PASS | unchanged |
| `19d2397ff8da1952556cf2417d965f6c` | failed | failed | PASS | unchanged |
| `1b49c03968b2c83897a4a15c78980b18` | failed | failed | PASS | unchanged |
| `1d878d0401b28b281eb75016ed29f2ee` | failed | failed | PASS | unchanged |
| `1e2600ed62d5e76b21ee` | failed | failed | PASS | unchanged |
| `2038329` | failed | failed | PASS | unchanged |
| `21b4442b7e7f36f6a17b` | failed | failed | PASS | unchanged |
| `2310005` | failed | failed | PASS | unchanged |
| `2371c78fc0a5c8935a7a` | failed | failed | PASS | unchanged |
| `2628382` | failed | failed | PASS | unchanged |
| `263113` | failed | failed | PASS | unchanged |
| `2888158` | failed | failed | PASS | unchanged |
| `28bf77e9a95ae6b70b14141feacb1f84` | failed | failed | PASS | unchanged |
| `2977d9f26866b05583b0c40d88a315bf` | failed | failed | PASS | unchanged |
| `2b19fd6f758ffd2e8ab9ec7d1f3f4b2c` | failed | failed | PASS | unchanged |
| `2bcca0a7654168ef454f` | failed | failed | PASS | unchanged |
| `2d4a4a8be57a9b8e94c7a4903d8d8bf8` | failed | failed | PASS | unchanged |
| `2de2e9a156fe619dbdad762fe1cf84e1` | failed | failed | PASS | unchanged |
| `2e3b989e0343f0884388ed7ed82eb3b0` | failed | failed | PASS | unchanged |
| `3001099` | failed | failed | PASS | unchanged |
| `3018bf3643f80798bde75c17571a38a9` | failed | failed | PASS | unchanged |
| `309bed093f6a7084c855` | failed | failed | PASS | unchanged |
| `3153844` | failed | failed | PASS | unchanged |
| `31eee50b9aaebf387b380f70054575c5` | failed | failed | PASS | unchanged |
| `3310561` | failed | failed | PASS | unchanged |
| `33150bde6bd296310e41ea5d018fce51` | failed | failed | PASS | unchanged |
| `33e2172bafbb5dd794ab` | failed | failed | PASS | unchanged |
| `3411495` | failed | failed | PASS | unchanged |
| `342989` | failed | failed | PASS | unchanged |
| `3682135` | failed | failed | PASS | unchanged |
| `3799831` | failed | failed | PASS | unchanged |
| `3805436` | failed | failed | PASS | unchanged |
| `3829194` | failed | failed | PASS | unchanged |
| `3a2a081e4f3089920fd8aecefecbe280` | failed | failed | PASS | unchanged |
| `3a6e4d618afc344aab81` | failed | failed | PASS | unchanged |
| `3b1159baecb809b5fcb3a6154bc3cb0b` | failed | failed | PASS | unchanged |
| `3b71a120ae7789956ef8` | failed | failed | PASS | unchanged |
| `3d99498d4236248f9bfbc8ed2fd424fa` | failed | failed | PASS | unchanged |
| `3fdd80a08808bd275142d46863e92d68` | failed | failed | PASS | unchanged |
| `4074260` | failed | failed | PASS | unchanged |
| `4089133` | failed | failed | PASS | unchanged |
| `4093998b625d76ef4afe` | failed | failed | PASS | unchanged |
| `4108a54877406dc231d95514e538bde9` | failed | failed | PASS | unchanged |
| `4133c66ccf65c0ba1f5f5a5bc4fb7298` | failed | failed | PASS | unchanged |
| `4145581` | failed | failed | PASS | unchanged |
| `4543974` | failed | failed | PASS | unchanged |
| `4882342eba2b57376ed1` | failed | failed | PASS | unchanged |
| `4995164` | failed | failed | PASS | unchanged |
| `4aed548e606f11971f5a` | failed | failed | PASS | unchanged |

## Watchlist

**17 cases** (separate from canonical contract)

| Metric | Count |
|--------|-------|
| Passed | 0 |
| Failed | 17 |
| Skipped | 0 |

| Case ID | Baseline | Rerun | pllm | Delta |
|---------|----------|-------|------|-------|
| `1025525` | failed | failed | PASS | unchanged |
| `10589494` | failed | failed | PASS | unchanged |
| `125559` | failed | failed | PASS | unchanged |
| `1329319` | failed | failed | PASS | unchanged |
| `143e65a425722dc2f3d0` | failed | failed | PASS | unchanged |
| `1727204` | failed | failed | PASS | unchanged |
| `23585f7f50005408fc72` | failed | failed | PASS | unchanged |
| `2636213` | failed | failed | PASS | unchanged |
| `3018527` | failed | failed | PASS | unchanged |
| `3077639` | failed | failed | PASS | unchanged |
| `35164461db4da79f7d56` | failed | failed | PASS | unchanged |
| `3725741` | failed | failed | PASS | unchanged |
| `3750774` | failed | failed | PASS | unchanged |
| `3803003` | failed | failed | PASS | unchanged |
| `4225456` | failed | failed | PASS | unchanged |
| `426829` | failed | failed | PASS | unchanged |
| `4451253` | failed | failed | PASS | unchanged |

## Preservation Guards

Verification-only cases outside the canonical 70-case delta math.

### Passed (must stay passed)

| Case ID | Baseline | pllm |
|---------|----------|------|
| `015e2ce27cecdea63564` | passed | FAIL |
| `00056d4304c58a035c87cdf5ff1e5e3e` | passed | PASS |
| `011004bcac763eaf6f28` | passed | FAIL |

### Host-runtime (must stay skipped or passed)

| Case ID | Baseline | pllm |
|---------|----------|------|
| `00a4835bf36513ca58a3` | skipped | FAIL |
| `00135b0dfee0ae165ad2` | skipped | FAIL |
| `0115e0ce312f26ff59f4fbf4f5821ca2` | skipped | FAIL |

### Local-helper (expected skip)

| Case ID | Baseline | pllm |
|---------|----------|------|
| `005ceac0483fc5a581cc` | skipped | FAIL |
| `06649145d7e6c4c147c02459fd2bc5af` | skipped | FAIL |

### Unsolvable (expected skip or fail)

| Case ID | Baseline | pllm |
|---------|----------|------|
| `0b677b13fca6cd0905ca` | skipped | FAIL |
| `1029870` | skipped | FAIL |
| `1160696` | skipped | FAIL |
