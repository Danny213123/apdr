# Phase 7 Tier3 Parity Manifest

## Source Inputs
- `summary.json`: `runs/20260327-150339-apdr/summary.json`
- `pllm` CSV: `pllm_results/csv/summary-all-runs.csv`
- Generated at: `2026-03-28T05:14:46Z`
- Normalization precedence: `summary.failure_bucket`, `summary.validation_status`, `report.failure_bucket`, `report.validation_status`, `log_tail`, `unclassified`

## Canonical Slice
- Canonical tier3 cases: `70`
- Overlap cases with APDR failure and `pllm` pass >= 1: `87`
- Excluded tier1 watchlist cases: `17`
- Inclusion rule: APDR failed, APDR did not skip, `pllm` passed at least once, and the stored summary `tier` equals `tier3`.

## Normalized Buckets
| Bucket | Cases |
| --- | ---: |
| `environment-build-failed` | 21 |
| `module-not-found` | 19 |
| `dependency-conflict` | 12 |
| `version-not-found` | 11 |
| `syntax-error` | 5 |
| `import-error` | 2 |

## Representative Cases
- `0830affa1f7f19fd47b06d4cf89ed44d`: `dependency-conflict` via `summary.validation_status`; `pllm_pass_count=10`; snippet `hard-gists\0830affa1f7f19fd47b06d4cf89ed44d\snippet.py`; reason: Dependency solver reported an incompatible version bundle: The user requested keras==3.0.0 tensorflow-intel 2.18.0 depends on keras>=3.5.0.
- `035dc3b722b7f89cce66520dde285c9a`: `environment-build-failed` via `summary.validation_status`; `pllm_pass_count=6`; snippet `hard-gists\035dc3b722b7f89cce66520dde285c9a\snippet.py`; reason: Repeated failure signature `BuildFailure|TPL-OS||pyeclib` across multiple dependency sets; ending recovery loop.
- `10938795`: `import-error` via `summary.validation_status`; `pllm_pass_count=9`; snippet `hard-gists\10938795\snippet.py`; reason: Runtime import failed: ImportError: cannot import name 'parse_rule' from 'werkzeug.routing' (D:\apdr\runs\20260327-150339-apdr\cases\10938795\.apdr-debug\attempts\attempt-012-py-3_12\env\Lib\site-packages\werkzeug\routing\__init__.py).
- `1068868`: `module-not-found` via `summary.validation_status`; `pllm_pass_count=10`; snippet `hard-gists\1068868\snippet.py`; reason: Missing module `taggit_autocomplete` persisted across multiple dependency sets; ending recovery as a mapping failure.
- `1042719`: `syntax-error` via `summary.validation_status`; `pllm_pass_count=10`; snippet `hard-gists\1042719\snippet.py`; reason: Stopped validation because requirements began oscillating.
- `1191457`: `version-not-found` via `summary.validation_status`; `pllm_pass_count=10`; snippet `hard-gists\1191457\snippet.py`; reason: Package `odfpy==0.9` is unavailable for the selected Python version.

## Tier1 Watchlist
The `17` tier1 overlap cases are outside the Phase 7 contract. They remain a watchlist for later milestone work and are not part of the canonical tier3 baseline.

Watchlist case IDs: 1025525, 10589494, 125559, 1329319, 143e65a425722dc2f3d0, 1727204, 23585f7f50005408fc72, 2636213, 3018527, 3077639, 35164461db4da79f7d56, 3725741, 3750774, 3803003, 4225456, 426829, 4451253
