---
phase: 28-llm-recovery-loop-and-failure-semantics
verified: 2026-04-03T02:04:39Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 28: LLM Recovery Loop and Failure Semantics Verification Report

**Phase Goal:** Non-pass `llm` and `llm-only` cases get bounded, log-aware LLM recovery attempts and truthful final failure labeling instead of generic `Unknown` or misleading infrastructure hints.
**Verified:** 2026-04-03T02:04:39Z
**Status:** passed
**Re-verification:** No

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Recovery now consumes authored-plan, Docker-plan, intake-failure, and latest executed-artifact pointers instead of only a flattened log string. | ✓ VERIFIED | `tools/apdr/llm_py/models.py`, `tools/apdr/llm_py/prompts.py`, `tools/apdr/llm_py/actions/recovery.py`, and `tools/apdr/src/resolver/tier3_llm/core.rs` now preserve those inputs across the Python/Rust seam, and `cargo test --manifest-path tools/apdr/Cargo.toml phase28_recovery_ -- --nocapture` passes. |
| 2 | APDR now persists bounded machine-readable `recovery-attempts.json` outputs and carries additive `recovery_outcome`, `failure_truth_class`, and `failure_truth_detail` surfaces through case artifacts and benchmark readers. | ✓ VERIFIED | `tools/apdr/src/resolver/retry_loop.rs`, `tools/apdr/src/lib.rs`, `tools/apdr/src/resolver/recovery_diagnostics.rs`, `benchmark_ui/runner.py`, and `benchmark_ui/service.py` now export the new truth fields, and the benchmark reader tests pass. |
| 3 | Phase 28 now has deterministic proof fixtures and a checker that freeze both an applied recovery case and a non-pass failure-truth case while explicitly handing benchmark delta claims to Phase 29. | ✓ VERIFIED | `scripts/check_phase28_recovery_truth.py`, `28-recovery-applied-sample.json`, `28-failure-truth-sample.json`, `28-recovery-truth-status.json`, and `28-RECOVERY-TRUTH-PROOF.md` all pass and keep the explicit Phase 29 boundary. |

**Score:** 3/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/llm_py/actions/recovery.py` | Structured recovery response with outcome and failure truth | ✓ VERIFIED | Recovery now returns `recovery_outcome`, `failure_class`, and `diagnostic_preview`, including structured no-output and provider-failure paths. |
| `tools/apdr/src/resolver/tier3_llm/core.rs` | Rust/Python seam for richer recovery context | ✓ VERIFIED | The recovery request now includes authored-plan, Docker-plan, intake-failure, and executed-artifact pointers, and the response preserves failure semantics even when no fix is applied. |
| `tools/apdr/src/resolver/retry_loop.rs` | Bounded persisted recovery attempts | ✓ VERIFIED | The retry loop now writes `recovery-attempts.json`, records bounded provider/no-output failures, and exports top-level recovery outcome truth. |
| `tools/apdr/src/resolver/recovery_diagnostics.rs` | Additive final failure-truth mapping | ✓ VERIFIED | Final case truth now distinguishes `llm-no-output`, `provider-tooling-failure`, `docker-infrastructure-failure`, and `dependency-runtime-failure` while preserving `failure_family`. |
| `benchmark_ui/runner.py` and `benchmark_ui/service.py` | Saved/live benchmark readers surface recovery truth | ✓ VERIFIED | Case rows and live events now expose `recoveryAttemptsPath`, `recoveryOutcome`, `failureTruthClass`, and `failureTruthDetail`. |
| `scripts/check_phase28_recovery_truth.py` | Deterministic Phase 28 contract checker | ✓ VERIFIED | Validates both the applied recovery sample and the failure-truth sample together and writes a passing status JSON. |
| `28-recovery-applied-sample.json` | Frozen applied recovery sample | ✓ VERIFIED | Preserves bounded recovery-attempt structure plus applied package-change truth. |
| `28-failure-truth-sample.json` | Frozen final failure-truth sample | ✓ VERIFIED | Preserves top-level failure truth and the corresponding final recovery attempt. |
| `28-RECOVERY-TRUTH-PROOF.md` | Reviewer-facing proof boundary note | ✓ VERIFIED | States exactly what Phase 28 proves and explicitly defers benchmark deltas to Phase 29. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Rust recovery artifact suite | `cargo test --manifest-path tools/apdr/Cargo.toml phase28_recovery_ -- --nocapture` | Exit code `0`; 2 Phase 28 recovery tests passed | ✓ PASS |
| Rust failure-truth suite | `cargo test --manifest-path tools/apdr/Cargo.toml phase28_truth_ -- --nocapture` | Exit code `0`; 2 Phase 28 truth tests passed | ✓ PASS |
| Benchmark saved/live reader coverage | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events` | Exit code `0`; 39 tests passed | ✓ PASS |
| Deterministic proof checker | `python3 scripts/check_phase28_recovery_truth.py --applied-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json --truth-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json --status-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-truth-status.json --probe-only` | Exit code `0` | ✓ PASS |
| Focused Docker artifact regression gate | `cargo test --manifest-path tools/apdr/Cargo.toml phase27_ -- --nocapture` | Exit code `0`; 6 Phase 27 tests passed | ✓ PASS |
| Workspace diff integrity | `git diff --check` | Exit code `0` | ✓ PASS |
| Python syntax gate for updated readers and recovery code | `python3.12 -m py_compile tools/apdr/llm_py/models.py tools/apdr/llm_py/prompts.py tools/apdr/llm_py/actions/recovery.py tools/apdr/llm_py/tests/test_recovery_mock.py benchmark_ui/runner.py benchmark_ui/service.py benchmark_ui/test_run_contract.py benchmark_ui/test_runner_events.py scripts/check_phase28_recovery_truth.py` | Exit code `0` | ✓ PASS |

### Verification Notes

- The planned `pytest` verification command for `tools/apdr/llm_py/tests/test_recovery_mock.py` and `tools/apdr/llm_py/tests/test_client_fallbacks.py` could not run in this environment because the available Python interpreter does not have `pytest` installed. The phase stays honest about that and uses targeted Rust tests, benchmark reader tests, and Python syntax checks as the local verification set.
- The pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs` remained non-blocking.

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `LLM-03` | `28-01`, `28-02`, `28-03` | After install, build, or runtime failures, APDR can ask the LLM to propose and apply bounded recovery changes using prior attempt logs and artifacts | ✓ SATISFIED | Recovery now consumes authored/executed artifacts, persists bounded `recovery-attempts.json`, and exports structured recovery outcome truth. |
| `TRU-01` | `28-01`, `28-02`, `28-03` | Case reports distinguish LLM no-output, provider/tooling failure, Docker infrastructure failure, and genuine dependency/runtime failure | ✓ SATISFIED | The repo now exports `failure_truth_class` and `failure_truth_detail` through report text, summary metadata, proof samples, and benchmark readers. |

Phase 28 orphaned requirements: none. The phase plans account for both Phase 28 requirement IDs in `.planning/REQUIREMENTS.md` (`LLM-03`, `TRU-01`).

### Residual Notes

- Phase 28 improves recovery truth and failure semantics, but it does not yet make benchmark pass-rate claims. That comparison work is explicitly deferred to Phase 29.
- Phase 23 browser UAT debt from the superseded v2.4 milestone remains historical context only and does not block Phase 28 closeout.

### Gaps Summary

No Phase 28 execution gaps remain. The repo now has bounded recovery-attempt artifacts, additive failure-truth fields, and a deterministic proof package that Phase 29 can safely compare against.

---

_Verified: 2026-04-03T02:04:39Z_
_Verifier: Codex inline verification_
