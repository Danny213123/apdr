# Phase 24 Comparison Delta

## Scope

This note summarizes the deterministic sample artifacts produced by the Phase 24 paired-policy harness. It is intentionally limited to the fixed slice and the machine-checked sample artifacts:

- [24-env-first-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json)
- [24-docker-first-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json)
- [24-comparison-proof-status.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json)

These are deterministic sample artifacts, not the final live evidence pack. Phase 25 will decide how to use the same harness for the milestone verdict.

## Contract Parity

The checker confirmed that both artifacts keep:

- the same `slice_id`: `phase24-policy-comparison-fixed-slice-v1`
- the same ordered five-case set
- the same `validation_backend`: `llm`
- the same model and base URL contract
- the same `resultOrigin` coverage (`live: 5`)

The only intentional contract change is `llm_validation_policy`: `env-first` versus `docker-first`.

## Pass Delta

| Policy | Passes | Failures | Skips |
|--------|--------|----------|-------|
| env-first | 1 | 4 | 0 |
| docker-first | 3 | 2 | 0 |

`pass delta`: `+2` in favor of docker-first  
`failure delta`: `-2` in favor of docker-first  
`skip delta`: `0`

## Dominant Bucket Delta

Negative values mean docker-first reduced the bucket relative to env-first.

| Bucket | env-first | docker-first | docker-first minus env-first |
|--------|-----------|--------------|------------------------------|
| `module-not-found` | 1 | 0 | `-1` |
| `version-not-found` | 1 | 1 | `0` |
| `environment-build-failed` | 2 | 1 | `-1` |

This deterministic sample shows bucket relief in `module-not-found` and `environment-build-failed`, with no movement in `version-not-found`.

## Timing Delta

Negative values mean docker-first was faster on the sample. Positive values mean docker-first spent more time in that timing dimension.

| Metric | env-first | docker-first | docker-first minus env-first |
|--------|-----------|--------------|------------------------------|
| `duration_seconds` | 1020.0 | 848.0 | `-172.0` |
| `solve_duration_seconds` | 223.0 | 186.0 | `-37.0` |
| `validation_duration_seconds` | 687.0 | 558.0 | `-129.0` |
| `env_create_duration_seconds` | 58.0 | 0.0 | `-58.0` |
| `install_duration_seconds` | 429.0 | 0.0 | `-429.0` |
| `docker_startup_duration_seconds` | 0.0 | 61.0 | `61.0` |
| `smoke_duration_seconds` | 39.0 | 33.0 | `-6.0` |

The sample says docker-first pays a real `docker_startup_duration_seconds` cost, but still reduces total duration on this fixed slice by avoiding env creation and install work.

## Interpretation Boundary

This note proves the Phase 24 comparison harness can report:

- contract parity across paired env-first and docker-first artifacts
- pass delta
- dominant-bucket delta
- timing delta

It does not answer the final keep/optional/reject question by itself. Phase 25 will decide whether the sample-backed harness result should remain optional evidence, expand to live paired replay evidence, or support a final policy recommendation.
