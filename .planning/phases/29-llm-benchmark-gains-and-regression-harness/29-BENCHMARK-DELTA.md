# Phase 29 Benchmark Delta

This document summarizes the deterministic Phase 29 fixed-slice comparison for both `llm` and `llm-only`.

The before-state anchors are:

- `llm`: `runs/20260402-003618-apdr`
- `llm-only`: `runs/20260402-184821-apdr`

The locked slice is stored in `29-benchmark-slice.json` and contains the same six ordered cases for both modes. This is fixed-slice evidence for Phase 29, not the final ship recommendation.

## `llm`

Baseline summary:

- passes: `5`
- skips: `1`
- failures: `0`
- source anchor: `20260402-003618-apdr`

Candidate summary:

- passes: `4`
- skips: `1`
- failures: `1`
- frozen artifact: `29-llm-candidate-sample.json`

Deterministic deltas:

- `pass_delta`: `-1`
- `skip_delta`: `0`
- `failure_delta`: `+1`
- `llm_no_output_delta`: `0`
- `provider_tooling_failure_delta`: `+1`
- `docker_infrastructure_failure_delta`: `0`
- `dependency_runtime_failure_delta`: `0`
- `duration_seconds`: `+379.03`
- `solve_duration_seconds`: `+184.89`
- `validation_duration_seconds`: `+193.30`
- `docker_startup_duration_seconds`: `+20.99`

Interpretation:

- The stronger end-to-end `llm` path preserves authored-plan, Docker-plan, and recovery truth on the slice, but it is not benchmark-positive yet.
- The regression is concentrated in one visible provider-tooling failure, `hard-gists/019fd5c706e0bc94879f/snippet.py`, rather than being hidden inside a generic dependency miss.
- Phase 29 therefore proves that the harness can surface a real `llm` regression instead of blending it away.

## `llm-only`

Baseline summary:

- passes: `1`
- skips: `1`
- failures: `4`
- source anchor: `20260402-184821-apdr`

Candidate summary:

- passes: `4`
- skips: `1`
- failures: `1`
- frozen artifact: `29-llm-only-candidate-sample.json`

Deterministic deltas:

- `pass_delta`: `+3`
- `skip_delta`: `0`
- `failure_delta`: `-3`
- `llm_no_output_delta`: `-1`
- `provider_tooling_failure_delta`: `+1`
- `docker_infrastructure_failure_delta`: `-3`
- `dependency_runtime_failure_delta`: `0`
- `duration_seconds`: `-3361.04`
- `solve_duration_seconds`: `-2120.51`
- `validation_duration_seconds`: `-1243.73`
- `docker_startup_duration_seconds`: `+35.0`

Interpretation:

- The end-to-end `llm-only` path materially improves the fixed slice: it converts three of the April 2 failures into passes.
- The biggest visible win is the removal of repeated `docker-infrastructure-failure` outcomes that dominated the April 2 before-state.
- `llm-no-output` also drops on the slice, although one provider-tooling failure remains visible rather than being silently converted into success.
- The candidate pays explicit Docker startup time, but overall runtime still falls sharply because repeated failure churn is reduced.

## Honest Tradeoffs

- Phase 29 does not claim that `llm` is ready to ship broadly. The fixed slice shows a real improvement for `llm-only`, but `llm` still regresses on this contract.
- The comparison keeps `llm-no-output`, provider-tooling instability, and `docker-infrastructure-failure` visible by design so a pass-rate gain cannot hide new instability.
- These artifacts are deterministic and fixed-slice scoped. Phase 30 owns live evidence, representative cases, and the final milestone recommendation.
