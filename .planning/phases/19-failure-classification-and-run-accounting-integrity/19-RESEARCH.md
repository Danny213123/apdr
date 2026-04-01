# Phase 19: Failure Classification and Run-Accounting Integrity - Research

**Researched:** 2026-04-01
**Domain:** Separating environment-specific validation failures from dependency-resolution misses while keeping resumed-run and current-run accounting truthful
**Confidence:** High

## Summary

Phase 19 should be treated as a truth-preservation phase, not as a new benchmark taxonomy project. APDR already knows about many environment-specific dead ends. `tools/apdr/src/resolver/retry_loop.rs` short-circuits obvious host or platform requirements into `skipped-host-runtime`, `tools/apdr/src/resolver/mod.rs` carries additional skip detection logic, and `tools/apdr/src/resolver/recovery_diagnostics.rs` already derives status and reason fields from the terminal validation attempt. The current problem is that this truth is not consistently preserved. Some environment-specific cases still flatten into dependency-style buckets, and the benchmark layer later reinterprets host-runtime skips as passes when a requirements file exists.

The second half of the problem is resume provenance. `benchmark_ui/service.py` injects `_resume_results` from the previous run, and `benchmark_ui/runner.py` stores those historical results directly in the new run's `summary["results"]`. That makes operational resume convenient, but it also means later readers and proof tooling cannot reliably tell which rows came from the old run and which rows were produced by the current run. Phase 19 therefore needs both better failure classification and explicit result provenance.

The safest shape is three waves. First, make APDR's environment-specific versus dependency-resolution classification durable and serializable. Second, remove the benchmark-side skip-to-pass reclassification and separate historical resume data from live run data while preserving an operator-friendly combined view. Third, freeze a deterministic proof package that demonstrates the new classification and provenance contract on a fixed March 30, 2026 live-derived slice plus a mixed historical/live fixture.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| VAL-04 | Benchmark operator can distinguish framework or host-runtime failures from dependency-resolution failures in per-case validation results | APDR already computes host-runtime skip signals and failure buckets; the missing work is making that classification durable across the Rust-to-Python boundary. |
| EVD-07 | Resumed-run summaries do not mark skipped host-runtime cases as successes | The benchmark service currently forces some host-runtime skips into pass semantics when requirements exist and returncode is zero. |
| EVD-09 | Live v2.3 comparisons avoid stale historical metadata | Resume currently mixes prior-run rows into the new `results` list, so comparison logic cannot cleanly isolate current-run conclusions. |

## Evidence That Should Drive Planning

### APDR already detects host and framework runtime blockers before LLM recovery

`tools/apdr/src/resolver/retry_loop.rs` checks `environment_specific_note(...)` before spending more time on LLM recovery. When it finds a host or framework dependency, it sets `validation.status = "skipped-host-runtime"`, copies the note into `reason` and `root_cause`, sets `failure_bucket = "skipped-host-runtime"`, and marks `skip_candidate = true`. This is the right semantic boundary; Phase 19 should preserve it instead of letting later layers reinterpret it.

### Terminal status inference still overweights the last attempt log

`tools/apdr/src/resolver/recovery_diagnostics.rs` derives `infer_validation_status(...)` mainly from the last attempt's error type, status, and log excerpt. That works well for many real dependency failures, but it is too narrow to express a higher-level family such as "environment-specific" versus "dependency-resolution" when earlier retry-loop decisions already established that distinction.

### The wrapper does not yet export all classification truth

`tools/apdr/test_executor.py` writes `validation_status`, `validation_reason`, `validation_backend`, `validation_path`, and `escalated_backend`, but it does not currently surface richer failure classification such as `failure_bucket`, `skip_candidate`, or an explicit family field. That means the benchmark layer often has to infer semantics indirectly from `validation_status` and `returncode`.

### Benchmark readers explicitly upgrade some host-runtime skips into passes

`benchmark_ui/runner.py` resets `skipped = False` when output metadata says the case was skipped but `requirements.txt` exists and the return code is zero. `benchmark_ui/service.py` mirrors that behavior in `_result_skipped(...)` and `_result_succeeded(...)`, with comments explaining that host-runtime skips with valid requirements count as passes. That logic directly violates Phase 19's `EVD-07` requirement.

### Resume currently mixes historical rows into the new run's live result list

`benchmark_ui/service.py::resume_run(...)` loads `_resume_results = self._summary_results(summary)`, and `benchmark_ui/runner.py` seeds the new run summary with `summary["results"] = resume_results` before appending new results. This makes the resumed run's summary a merged historical-plus-current list. Operationally that is convenient, but analytically it means live conclusions are contaminated by stale rows unless every reader remembers to filter them manually.

### Historical run views and proof helpers read the same mixed list

`benchmark_ui/service.py::_historical_run_snapshot(...)`, `_run_descriptor(...)`, and `_historical_activity(...)` all consume `_summary_results(summary)`, which rewrites pass/skip flags and hides provenance. That prevents clean baseline-versus-candidate proofing because the stored rows no longer say whether they were historical resume rows or live rows from the current run.

## Implementation Recommendations

### 1. Add durable APDR classification fields and keep dependency misses intact

Recommended files:

- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`

Recommended responsibilities:

- derive an explicit classification family such as `environment-specific`, `framework-runtime`, or `dependency-resolution` from the validation summary and retry-loop signals
- keep `module-not-found`, `version-not-found`, and genuine build failures intact when they are truly dependency-resolution failures
- serialize the classification family, `failure_bucket`, and `skip_candidate` into output artifacts so benchmark readers can use direct truth instead of reconstructing it
- add Rust tests that lock representative boundaries between host-runtime skips and real dependency misses

### 2. Remove skip-to-pass reclassification and preserve resume provenance

Recommended files:

- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`
- `benchmark_ui/test_resume_accounting.py`

Recommended responsibilities:

- treat host-runtime and framework-required skips as skips even when requirements were solved successfully
- store historical resume rows separately from current-run rows, or attach explicit origin metadata before any combined view is built
- keep the UI's operational "resumed benchmark" view available while adding live-only readers for proof and comparison tooling
- extend tests to lock both the skip accounting and the historical-versus-live provenance split

### 3. Freeze a deterministic Phase 19 proof package

Recommended files:

- `scripts/check_phase19_accounting.py`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md`

Recommended responsibilities:

- freeze a live-derived slice containing both environment-specific skips and genuine dependency-resolution failures from the March 30, 2026 baseline
- add a deterministic checker that validates expected skip/fail semantics and verifies that historical rows are kept out of live-only candidate counts
- produce a reviewer-facing proof note with a before/after section that calls out classification truth and provenance separation

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting`

### Artifact checks

- `rg -n 'failure_family|failure_bucket|skip_candidate|phase19_classification_' tools/apdr/src/resolver/recovery_diagnostics.rs tools/apdr/src/resolver/retry_loop.rs tools/apdr/src/lib.rs tools/apdr/test_executor.py`
- `rg -n 'historical_results|resultOrigin|skipped-host-runtime|host-runtime-required' benchmark_ui/runner.py benchmark_ui/service.py benchmark_ui/test_run_contract.py benchmark_ui/test_runner_events.py benchmark_ui/test_resume_accounting.py`
- `rg -n 'Before/After Review|historical_results|live_only|skipped-host-runtime' scripts/check_phase19_accounting.py .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md`

### Phase-close checks

- inspect representative artifacts to confirm host-runtime cases expose explicit environment-specific classification and are not labeled as generic mapping misses
- inspect a resumed run summary after the Phase 19 changes and confirm historical rows are stored separately or carry explicit provenance
- run the fixed checker in probe mode and confirm live-only totals exclude stale historical rows while the operational combined view still remains readable

## Canonical Files For Planning

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/codebase/ARCHITECTURE.md`
- `.planning/codebase/CONCERNS.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-CONTEXT.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-CONTEXT.md`
- `runs/20260330-020943-apdr/summary.json`
- `runs/20260330-004502-apdr/summary.json`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`

## Out of Scope For This Phase

- improving the dominant live failure buckets themselves; that belongs to Phase 20
- creating the milestone closeout pack and reviewer-ready before/after narrative for v2.3; that belongs to Phase 21
- changing the requested-backend versus actual-path contract established in Phases 17 and 18
- redesigning the benchmark UI beyond classification truth, skip accounting, and result provenance

## Source Base

No external browsing was required for Phase 19 planning. The source of truth is the repo's current APDR resolver code, benchmark runner/service code, milestone requirements, and the March 30, 2026 run artifacts already present in the workspace.

---
*Research created: 2026-04-01*
*Phase: 19-failure-classification-and-run-accounting-integrity*
