# Phase 24 Comparison Harness Proof

## What Phase 24 Proves

Phase 24 proves that the repo has a deterministic comparison harness for env-first versus docker-first `llm` validation on the same fixed slice.

The proof package consists of:

- [24-comparison-slice.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json)
- [24-env-first-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json)
- [24-docker-first-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json)
- [24-comparison-proof-status.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json)
- [24-COMPARISON-DELTA.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md)
- [24-COMPARISON-RUNBOOK.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md)

## Locked Contract

The comparison harness requires:

- one fixed `slice_id`
- the same ordered case set in both artifacts
- `validation_backend=llm` in both artifacts
- matching model, base URL, build profile, cache state, and provenance coverage
- exactly one policy change: `env-first` versus `docker-first`

If any of that drifts, the checker fails before reporting deltas.

## Sample Delta

The current deterministic sample shows:

- `pass delta`: `+2` in favor of docker-first
- dominant-bucket deltas: `module-not-found -1`, `environment-build-failed -1`, `version-not-found 0`
- `timing delta`: docker-first is faster overall on the sample (`duration_seconds -172.0`) while still paying a positive `docker_startup_duration_seconds 61.0`

Those numbers are enough to prove the comparison harness can surface both correctness and cost signals from the same slice.

## Boundary of This Proof

This is a comparison harness proof, not the final policy verdict.

Phase 24 proves:

- the harness exists
- the harness can compare pass delta, bucket delta, and timing delta
- the proof remains deterministic in probe mode

Phase 24 does **not** prove:

- that docker-first should permanently replace env-first
- that the fixed-slice sample generalizes to the entire benchmark corpus
- that all upstream milestone verification is already complete

## Remaining Prerequisite Debt

Phase 23 human verification is still open in [23-HUMAN-UAT.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/23-policy-truth-and-failure-semantics/23-HUMAN-UAT.md). That browser-level human verification debt needs to be cleared before the final milestone recommendation is treated as fully signed off.

## Handoff to Phase 25

Phase 25 uses this comparison harness to answer the actual milestone question: should docker-first replace env-first, remain optional, or be rejected for `llm` mode?

That means Phase 25 should cite this proof for:

- the existence of the comparison harness
- the presence of machine-checked pass delta, dominant-bucket delta, and timing delta
- the exact evidence boundary between deterministic sample artifacts and any later live paired replay evidence
