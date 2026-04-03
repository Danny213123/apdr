# Phase 29 Benchmark Harness Proof

## What Phase 29 Proves

Phase 29 proves that the repo can compare baseline versus candidate behavior for both `llm` and `llm-only` on one locked slice while keeping pass deltas, timing deltas, `llm-no-output`, provider-tooling instability, and `docker-infrastructure-failure` visible.

The proof package consists of:

- `29-benchmark-slice.json`
- `29-llm-baseline-sample.json`
- `29-llm-candidate-sample.json`
- `29-llm-only-baseline-sample.json`
- `29-llm-only-candidate-sample.json`
- `29-llm-benchmark-status.json`
- `29-llm-only-benchmark-status.json`
- `29-BENCHMARK-DELTA.md`
- `29-BENCHMARK-RUNBOOK.md`

## Locked Contract

The benchmark harness requires:

- one fixed `slice_id`
- the same ordered case set for baseline and candidate
- one explicit mode per comparison (`llm` or `llm-only`)
- matching model, base URL, build profile, cache state, and provenance coverage
- preserved Phase 26-28 truth pointers such as `casePlanPath`, `dockerPlanPath`, `recoveryAttemptsPath`, and additive failure-truth fields

If any of those drift, the checker fails before reporting deltas.

## Current Sample Outcome

The current deterministic fixed-slice sample shows two different stories:

- `llm-only` is benchmark-positive on the locked slice with `pass_delta +3`, `llm_no_output_delta -1`, and `docker_infrastructure_failure_delta -3`
- `llm` is not benchmark-positive yet on the same contract and currently shows `pass_delta -1` plus `provider_tooling_failure_delta +1`

That split is a feature of the proof, not a weakness in it. Phase 29 is meant to keep gains and regressions both visible so the milestone cannot hide a real `llm` regression behind a good `llm-only` result.

## Boundary of This Proof

Phase 29 proves:

- the paired benchmark harness exists for both `llm` and `llm-only`
- the checker can enforce parity and report required regression signals
- the proof remains deterministic in probe mode

Phase 29 does **not** prove:

- that the end-to-end `llm` path is ready to ship broadly
- that the fixed slice generalizes to the whole benchmark corpus
- that the final milestone recommendation is already settled

## Why Phase 30 Still Matters

Phase 30 is still required because the fixed-slice proof only establishes a trustworthy comparison contract. The milestone still needs live candidate evidence, representative cases, and a final recommendation that explicitly weighs:

- `llm-no-output` reduction
- provider-tooling stability
- `docker-infrastructure-failure` reduction
- runtime tradeoffs for both `llm` and `llm-only`

## Baseline Anchors

The frozen before-state anchors remain:

- `20260402-003618-apdr` for `llm`
- `20260402-184821-apdr` for `llm-only`

Those April 2 runs are intentionally preserved because they capture the pre-v2.5 benchmark behavior before the authored intake, Docker artifact truth, and additive failure semantics landed.
