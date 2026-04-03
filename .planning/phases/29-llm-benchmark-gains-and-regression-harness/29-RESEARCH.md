# Phase 29: LLM Benchmark Gains and Regression Harness - Research

**Researched:** 2026-04-03
**Domain:** Building a fixed-slice comparison harness that can measure baseline versus candidate `llm` and `llm-only` behavior after the new end-to-end LLM intake, Docker authoring, and recovery work from Phases 26-28
**Confidence:** High

## Summary

Phase 29 should be planned as the benchmark-evidence bridge for v2.5, not as more resolver logic. Phases 26, 27, and 28 already changed the runtime contract substantially: `llm` and `llm-only` now emit authored case plans, authored Docker plans, executed Docker artifacts, bounded `recovery-attempts.json`, and additive `failure_truth_*` fields. The active gap is that the repo still cannot answer the question "did these changes actually help?" for both `llm` and `llm-only` on the same fixed slice, using the April 2, 2026 pre-v2.5 failures as the before-state.

The strongest baseline anchors are already on disk. The pre-v2.5 standard `llm` baseline is the April 2 run family that still used the older hybrid execution path, especially `runs/20260402-003618-apdr`, which invoked APDR with `--validation-backend env`, `--allow-llm`, and `--force-validate`. The pre-v2.5 `llm-only` baseline is the Docker-backed failure run `runs/20260402-184821-apdr`, which invoked APDR with `--validation-backend docker` plus `--llm-only` and exposed both empty LLM plans and the build-to-run Docker image-handoff regression. Phase 29 should freeze those April 2 runs as the benchmark "before" contract instead of inventing a synthetic baseline.

The candidate side should reuse the replay and proof seams rather than inventing a new benchmark entrypoint. `scripts/run_phase20_recovery_benchmark.py` already proved the repo can extract a fixed slice from a saved summary or replay a slice through the benchmark worker. Phase 24 then proved the repo can do matched-mode comparisons with deterministic fixtures and a checker. Phase 29 should merge those two shapes: one harness that can produce baseline and candidate artifacts for `llm` and `llm-only`, while preserving the new Phase 26-28 truth surfaces that later closeout work will need.

The most important planning change relative to Phase 24 is metric scope. Phase 29 cannot stop at pass delta and coarse buckets. The April 2 regressions were explicitly about LLM no-output, Docker handoff failure, and misleading failure labeling. So the comparison contract must preserve `case-plan`/`docker-plan`/`recovery-attempts` pointers, `recovery_outcome`, `failure_truth_class`, `failure_truth_detail`, and timing metrics alongside pass/fail truth. Reviewers need to see whether the candidate path wins by actually resolving more cases, or whether it merely shifts failures into slower or less honest categories.

The clean planning shape is three waves. First, lock the fixed slice and build a harness that can materialize baseline and candidate artifacts for both `llm` and `llm-only`. Second, add a deterministic checker and reviewer-facing delta artifact that compute pass, timing, no-output, Docker infrastructure, and recovery-outcome deltas. Third, freeze the runbook and proof pack so Phase 30 can publish live evidence and a ship recommendation without redefining the benchmark contract.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| BEN-01 | Fixed-slice comparison artifacts show whether the new LLM-led path improves pass rate for both `llm` and `llm-only` against the April 2, 2026 baseline runs | April 2 run artifacts already exist for the before-state, and existing replay harnesses already know how to extract or replay locked slices without changing benchmark semantics. |
| BEN-02 | Comparison artifacts track solve/validate timing, LLM no-output rate, and Docker handoff failures so gains are not hidden behind new regressions | Phase 26-28 artifacts now export authored-plan, Docker, recovery, and failure-truth metadata that can be compared alongside per-case timing fields. |

## Evidence That Should Drive Planning

### April 2 baseline runs already capture the pre-v2.5 failure modes

`runs/20260402-003618-apdr` still reflects the older standard `llm` path with `--validation-backend env`, `--allow-llm`, and no Phase 26-28 authored-plan or recovery-truth contract. `runs/20260402-184821-apdr` reflects the pre-v2.5 `llm-only` Docker path and preserves both the empty-plan failures and the missing-image Docker handoff regression. Those runs are the right baseline anchors because they are the direct before-state that triggered the v2.5 milestone.

### Phase 26-28 introduced new truth surfaces that benchmark deltas must preserve

The repo now exports `case-plan.json`, `intake-failure.json`, `docker-plan.json`, `recovery-attempts.json`, `recovery_outcome`, `failure_truth_class`, and `failure_truth_detail`. A Phase 29 comparison harness should preserve those fields in extracted artifacts and use them in its regression metrics instead of flattening the candidate side back to the old Phase 24 policy-only contract.

### Existing replay infrastructure can be reused instead of rebuilt

`scripts/run_phase20_recovery_benchmark.py` already handles fixed-slice extraction, replay manifests, and saved-run artifact reading. `scripts/run_phase24_policy_comparison.py` already proved the repo can compare two matched artifacts with a deterministic checker. Phase 29 should compose those seams rather than creating a third unrelated benchmark flow.

### Timing comparisons are already available at the artifact level

APDR outputs still expose `solve_duration_ms`, `validation_duration_ms`, `env_create_duration_ms`, `install_duration_ms`, `docker_startup_duration_ms`, and `smoke_duration_ms`, while benchmark readers already normalize several of those values into seconds. Phase 29 should treat those timing fields as first-class comparison signals so pass-rate gains do not hide latency regressions.

### The harness must compare `llm` and `llm-only` separately, not collapse them into one score

The two modes have different strictness and fallback semantics after Phase 26. `llm` may continue through bounded deterministic or recovery steps, while `llm-only` must fail truthfully when LLM intake or recovery cannot produce a usable plan. The comparison harness therefore needs mode-specific artifacts and deltas so the milestone does not blur those contract differences away.

### Evidence discipline still matters

Phase 29 should produce a fixed-slice regression harness and reviewer-readable delta artifacts, not claim a full-corpus benchmark win. Phase 30 is where the live evidence and recommendation belong. The planning and proof artifacts for Phase 29 must say that clearly.

## Implementation Recommendations

### 1. Create a locked Phase 29 slice and paired baseline/candidate fixtures for both modes

Recommended files:

- `scripts/run_phase29_llm_benchmark.py`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-fixture-summary.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-fixture-summary.json`

Recommended responsibilities:

- freeze a small but representative ordered slice with explicit case ids and relative paths
- anchor the `llm` baseline to April 2 pre-v2.5 standard `llm` behavior and the `llm-only` baseline to April 2 pre-v2.5 Docker-backed `llm-only` behavior
- allow the harness to either extract from saved summaries or execute a live replay through the benchmark worker
- preserve authored-plan, Docker-artifact, recovery-truth, and timing fields in the emitted artifacts

### 2. Add a deterministic checker that compares baseline versus candidate for each mode

Recommended files:

- `scripts/check_phase29_benchmark_delta.py`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-sample.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-sample.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-sample.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-sample.json`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-DELTA.md`

Recommended responsibilities:

- validate that baseline and candidate artifacts share the same slice, case order, mode, model, provider, and backend contract
- compute pass, skip, fail, `llm-no-output`, `provider-tooling-failure`, and `docker-infrastructure-failure` deltas
- compute timing deltas for total duration, solve, validation, install, Docker startup, and smoke
- make the delta document explicit about fixed-slice scope, baseline run provenance, and remaining honest failures

### 3. Freeze the runbook and proof contract for Phase 30 handoff

Recommended files:

- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-RUNBOOK.md`
- `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-PROOF.md`

Recommended responsibilities:

- document how to reproduce the paired baseline-versus-candidate artifacts for both `llm` and `llm-only`
- keep the boundary clear that Phase 29 proves the harness and frozen delta contract, while Phase 30 publishes the live evidence and final ship recommendation
- preserve the exact April 2 baseline anchors so future reviewers can trace what "before" means without rereading the milestone history

## Validation Architecture

### Quick checks

- `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only`
- `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only`
- `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-baseline.json --candidate-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-sample.json --status-json /tmp/phase29-llm-status.json --mode llm --probe-only`

### Artifact checks

- `rg -n 'case_plan_path|docker_plan_path|recovery_outcome|failure_truth_class|docker_startup_duration_seconds' scripts/run_phase29_llm_benchmark.py`
- `rg -n 'pass_delta|llm_no_output_delta|docker_infrastructure_failure_delta|validation_duration_seconds|Phase 30' scripts/check_phase29_benchmark_delta.py .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-DELTA.md`

### Phase-close checks

- run the harness in probe mode for baseline and candidate fixtures for both `llm` and `llm-only`
- run the checker in probe mode for the frozen `llm` pair and the frozen `llm-only` pair
- confirm the delta doc reports pass, failure-truth, and timing deltas for both modes
- confirm the proof and runbook documents describe Phase 29 as the regression-harness boundary rather than the final Phase 30 verdict

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-RESEARCH.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md`
- `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md`
- `scripts/run_phase20_recovery_benchmark.py`
- `scripts/run_phase24_policy_comparison.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/run_contract.py`
- `tools/apdr/test_executor.py`
- `tools/apdr/src/lib.rs`

## Out of Scope For This Phase

- more resolver or Docker-routing behavior changes
- re-litigating whether `llm-only` should use Docker or env validation
- broad benchmark UI redesign or new operator workflows
- claiming a full-corpus win from a fixed-slice comparison harness
- publishing the final go/no-go recommendation for the milestone

## Source Base

No external browsing was required for Phase 29 planning. The source of truth is the repo's existing replay and proof scripts, the April 2 run artifacts in `runs/`, the v2.5 planning docs, and the new Phase 26-28 artifact contracts already in the workspace.

---
*Research created: 2026-04-03*
*Phase: 29-llm-benchmark-gains-and-regression-harness*
