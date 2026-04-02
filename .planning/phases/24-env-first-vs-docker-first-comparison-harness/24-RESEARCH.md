# Phase 24: Env-First vs Docker-First Comparison Harness - Research

**Researched:** 2026-04-02
**Domain:** Building a like-for-like comparison harness that can measure env-first versus docker-first `llm` validation on the same fixed slice without weakening the policy-truth guarantees from Phases 22 and 23
**Confidence:** High

## Summary

Phase 24 should be planned as an evidence harness, not as another routing phase. Phase 22 already made docker-first the default `llm` policy with env-first as an explicit control, and Phase 23 already made requested policy, actual path, bypass reasons, and failure-family truth inspectable. The remaining gap is that the repo still cannot produce a reviewer-readable answer to "did docker-first help or hurt?" on a like-for-like slice with matched contracts. That means Phase 24 should reuse the repaired replay, run-contract, and truth-reporting seams instead of inventing a separate benchmark path.

The strongest seam is already in the benchmark worker. `benchmark_ui/runner.py` supports `replay_manifest`, `validation_backend`, and normalized `llm_validation_policy`, while `benchmark_ui/run_contract.py` persists `llm_validation_policy` separately from `validation_backend`. `tools/apdr/test_executor.py` then forwards `--llm-validation-policy` into the Rust CLI. That means the repo already has the machinery to replay the same fixed slice twice under the same `validation_backend=llm`, model, base URL, and build profile while flipping only the first-hop policy from `env-first` to `docker-first`.

The second strong seam is in the artifact data itself. APDR resolution reports and output metadata already carry the fields Phase 24 needs for a policy comparison: `validation_path`, `failure_bucket`, `failure_family`, `requested_llm_validation_policy`, and per-case timing metrics such as `solve_duration_ms`, `validation_duration_ms`, `env_create_duration_ms`, `install_duration_ms`, `docker_startup_duration_ms`, and `smoke_duration_ms`. The Phase 20 replay extractor already proves the repo can turn a saved run or a live replay into a small artifact package. Phase 24 should build on that pattern and add paired-policy comparison rather than starting from scratch.

The main planning caution is evidence discipline. The Phase 23 truth slice is intentionally archetypal and contract-shaped; it proves inspectability and classification truth, not policy outcome deltas. Phase 24 therefore needs a separate fixed slice with real snippets and matched contracts. That slice should be small enough for repeatable paired runs, but large enough to show pass, dominant-bucket, and timing deltas. Planning should also keep the lingering Phase 23 human-verification debt visible: the Phase 24 harness may proceed, but the milestone should not present a final verdict until the policy-truth UI debt is actually cleared.

The clean planning shape is three waves. First, create a paired extraction and replay harness plus a locked comparison slice. Second, add a deterministic comparison checker and reviewer-facing delta artifact that reports pass, dominant-bucket, and timing differences. Third, add the runbook and proof pack that make the paired-policy contract reusable for live evidence and Phase 25 closeout.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| CMP-01 | Repo can compare env-first versus docker-first `llm` validation on the same fixed benchmark slice with matching model and backend contracts | `BenchmarkWorker` already supports replay manifests and normalized `llm_validation_policy`, and run contracts already record policy separately from backend semantics. |
| CMP-02 | Comparison artifacts report pass, dominant-bucket, and timing deltas so the first-hop policy can be judged on both correctness and cost | APDR output metadata already exposes pass/fail truth, failure buckets/families, and per-case duration fields needed for a deterministic delta artifact. |

## Evidence That Should Drive Planning

### The replay and run-contract seams already support paired-policy runs

`benchmark_ui/runner.py` already accepts a replay manifest and threads `llm_validation_policy` through the benchmark worker, while `benchmark_ui/run_contract.py` normalizes that policy into the saved run contract. Phase 24 should reuse those seams so the same slice can be replayed under `env-first` and `docker-first` with the same `validation_backend=llm`, model, and base URL.

### Existing saved runs are not yet the final paired-policy evidence

The current on-disk runs include pre-Phase-22 `llm` runs with no explicit policy field and newer `llm-only` or env-backed runs whose top-level contract is not the Phase 24 paired-policy target. That means Phase 24 must create a dedicated paired harness instead of pretending an existing run pair already answers the question.

### Phase 20 already proved the repo can extract or replay a fixed slice

`scripts/run_phase20_recovery_benchmark.py` already handles both `--probe-only` extraction from a saved summary and `--execute-live` replay through the benchmark worker. It also already preserves `historical_results` plus `results`, and it knows how to read `resolution-report.txt` when summaries alone are not enough. Phase 24 should inherit that shape instead of inventing a new benchmark entrypoint.

### Phase 23 already made policy truth stable enough to compare

Saved rows and live results now expose `requestedLlmValidationPolicy`, `validationPath`, `llmValidationRoute`, `dockerBypassReason`, `failureFamily`, `debugDir`, and `resultOrigin`. The Phase 24 checker can therefore assert contract parity and route truth without scraping raw logs or relying on proof-only field names.

### The timing data needed for cost comparisons already exists

`tools/apdr/src/lib.rs` persists `solve_duration_ms`, `validation_duration_ms`, `env_create_duration_ms`, `install_duration_ms`, `docker_startup_duration_ms`, and `smoke_duration_ms`. `benchmark_ui/runner.py` already turns several of those into seconds on live result rows, and a comparison harness can read the rest directly from output metadata or resolution reports. Phase 24 should therefore compare more than just pass counts.

### Result provenance from Phase 19 still matters

Phase 19 split resumed historical rows from live rows. Phase 24 artifacts should preserve `resultOrigin` and state clearly whether an artifact came from `historical`, `live`, or a mixed resume path, rather than silently flattening resumed evidence into one undifferentiated total.

## Implementation Recommendations

### 1. Build a paired extraction and replay harness on top of the existing replay seam

Recommended files:

- `scripts/run_phase24_policy_comparison.py`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json`

Recommended responsibilities:

- create a dedicated Phase 24 script that can either extract an artifact from a saved summary (`--probe-only`) or replay the fixed slice through the benchmark worker (`--execute-live`)
- require exact paired-policy inputs: `validation_backend=llm`, one explicit `--llm-validation-policy`, and a locked slice manifest
- preserve per-case policy truth, failure truth, provenance, and timing metrics in the emitted artifact JSON
- freeze small deterministic fixture summaries so the harness can be validated without requiring a live benchmark run during every feedback loop

### 2. Add a deterministic comparison checker and delta artifact

Recommended files:

- `scripts/check_phase24_policy_comparison.py`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md`

Recommended responsibilities:

- assert that env-first and docker-first artifacts use the same slice, same case set, same model/base URL, same `validation_backend=llm`, and only differ on `llm_validation_policy`
- compute pass delta, dominant-bucket delta for `module-not-found`, `version-not-found`, and `environment-build-failed`, plus timing deltas for total duration, solve, validation, env create, install, docker startup, and smoke
- fail loudly if artifacts drift on contract parity, omit required metrics, or compare mismatched case sets
- emit a reviewer-readable delta document that is explicit about sample-vs-live provenance

### 3. Freeze the runbook and proof contract for later live evidence

Recommended files:

- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-PROOF.md`

Recommended responsibilities:

- document the exact extraction and replay commands needed to produce the paired env-first and docker-first artifacts from a fixed slice
- state the evidence boundary clearly: Phase 24 builds the harness and deterministic comparison contract, while Phase 25 uses that harness to publish the milestone verdict
- keep the open Phase 23 human-verification debt visible so later closeout docs do not imply every prerequisite proof surface is already fully signed off

## Validation Architecture

### Quick checks

- `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json --output-json /tmp/phase24-env-artifact.json --mode env-first --llm-validation-policy env-first --probe-only`
- `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json --output-json /tmp/phase24-docker-artifact.json --mode docker-first --llm-validation-policy docker-first --probe-only`
- `python3 scripts/check_phase24_policy_comparison.py --env-artifact /tmp/phase24-env-artifact.json --docker-artifact /tmp/phase24-docker-artifact.json --status-json /tmp/phase24-comparison-status.json --probe-only`

### Artifact checks

- `rg -n 'llm_validation_policy|validation_backend|validation_path|failure_family|docker_startup_duration_ms|resultOrigin' scripts/run_phase24_policy_comparison.py .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json`
- `rg -n 'env-artifact|docker-artifact|pass_delta|module-not-found|validation_duration_seconds|docker_startup_duration_seconds' scripts/check_phase24_policy_comparison.py .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md`

### Phase-close checks

- run the paired harness in probe mode against the frozen fixture summaries and confirm it produces one env-first and one docker-first artifact for the same slice
- inspect the resulting comparison status and confirm it reports pass delta, dominant-bucket delta, and timing delta rather than only raw case rows
- run a live paired replay on a supported host and confirm the harness can keep model, backend, and slice contracts matched while only changing `llm_validation_policy`
- confirm the proof and runbook documents explicitly describe this as the Phase 24 harness contract, not the final Phase 25 verdict

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-RESEARCH.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-RESEARCH.md`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-VALIDATION.md`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-POLICY-TRUTH-PROOF.md`
- `scripts/run_phase20_recovery_benchmark.py`
- `scripts/check_phase23_policy_truth.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/run_contract.py`
- `benchmark_ui/state.py`
- `tools/apdr/test_executor.py`
- `tools/apdr/src/lib.rs`

## Out of Scope For This Phase

- changing docker-first or env-first routing behavior again
- reworking benchmark UI surfaces beyond the already-planned truth contract
- publishing the final keep/optional/reject verdict for docker-first `llm`
- treating the Phase 23 archetype slice as the actual comparison evidence
- claiming a full-corpus win or loss from a fixed-slice comparison harness

## Source Base

No external browsing was required for Phase 24 planning. The source of truth is the repo's existing replay scripts, Phase 22-23 policy artifacts, benchmark runner/service code, and the v2.4 requirements already present in the workspace.

---
*Research created: 2026-04-02*
*Phase: 24-env-first-vs-docker-first-comparison-harness*
