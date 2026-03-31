---
phase: 18-backend-escalation-and-path-truth
verified: 2026-03-31T03:05:15Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 18: Backend Escalation and Path Truth Verification Report

**Phase Goal:** Eligible tier3 failures in `llm` mode can escalate through Docker, and every validation attempt records the actual backend route without regressing Windows or Docker correctness.
**Verified:** 2026-03-31T03:05:15Z
**Status:** passed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Eligible `llm`-mode env failures now attempt a deterministic Docker middle hop before the final agent fallback instead of staying env-only | VERIFIED | `tools/apdr/src/docker/builder/agent_backend.rs` adds targeted Docker-eligibility checks and the `env -> docker -> llm-agent` route; `tools/apdr/src/docker/builder/docker_backend.rs` exposes a deterministic Docker-only helper; `tools/apdr/src/docker/builder/mod.rs` adds `phase18_backend_` regression coverage for eligibility boundaries and route ordering |
| 2 | APDR artifacts and benchmark readers preserve configured backend semantics separately from the routed backend truth | VERIFIED | `tools/apdr/src/lib.rs`, `tools/apdr/src/resolver/retry_loop.rs`, `tools/apdr/test_executor.py`, `benchmark_ui/runner.py`, and `benchmark_ui/service.py` now carry `validation_path` and `escalated_backend` while keeping `validation_backend` as the requested mode; `benchmark_ui/test_run_contract.py` locks that distinction |
| 3 | Operator-facing runtime guidance and proof assets now tell the truth about APDR `llm` routing without breaking Docker-only expectations | VERIFIED | `benchmark_ui/state.py` and `benchmark_ui/service.py` describe `llm` mode as env-first with targeted Docker escalation and agent fallback; `benchmark_ui/test_state_backend_doctor.py` proves targeted Docker warnings plus Docker-only hard-failure behavior; `scripts/check_phase18_backend_path.py` passes probe mode against the fixed five-case slice and status artifact |

**Score:** 3/3 must-haves verified

## Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/src/docker/builder/agent_backend.rs` | Targeted `llm` routing policy that chooses when Docker should be used after env failure | VERIFIED | Adds Docker-eligibility helpers, inherits prior validation context, and routes eligible failures through Docker before any final agent fallback |
| `tools/apdr/src/docker/builder/docker_backend.rs` | Docker helper that can be reused from `llm` mode without recursive agent fallback | VERIFIED | Factors Docker validation into a shared inner function and exposes a deterministic Docker entrypoint for the `llm` middle hop |
| `tools/apdr/src/docker/builder/mod.rs` | Focused routing and eligibility regression tests | VERIFIED | `phase18_backend_` tests cover interpreter/build-timeout eligibility, host-runtime exclusion, route ordering, and summary serialization |
| `tools/apdr/src/lib.rs` | Summary-level backend-path contract | VERIFIED | Adds `validation_path`, derives route strings from attempt history, and writes them into reports plus summary lines |
| `tools/apdr/src/resolver/retry_loop.rs` | Retry-loop preservation of routed backend truth | VERIFIED | Carries `escalated_backend` forward and refreshes `validation_path` after attempts merge |
| `tools/apdr/test_executor.py` | Saved case outputs include routed backend truth | VERIFIED | Copies `validation_path` and `escalated_backend` into output YAML artifacts |
| `benchmark_ui/runner.py` and `benchmark_ui/service.py` | Benchmark rows and events surface configured backend plus routed path together | VERIFIED | Result shaping and case-row APIs expose `validationBackend`, `validationPath`, and `escalatedBackend` without changing pass or skip classification |
| `benchmark_ui/state.py` | Doctor/runtime messaging reflects targeted Docker escalation for APDR `llm` mode | VERIFIED | Missing Docker is now a targeted warning for `llm` mode while pure Docker mode still fails hard; env tooling checks still run because the route remains env-first |
| `benchmark_ui/test_state_backend_doctor.py` | Coverage for doctor and runtime copy | VERIFIED | Tests the targeted warning path, env-tooling checks for `llm`, pure-Docker hard-failure semantics, and service intro copy |
| `scripts/check_phase18_backend_path.py` | Deterministic proof checker for the fixed live-derived backend-path slice | VERIFIED | Probe-mode command passed and refreshed `18-backend-path-proof-status.json` with `passed: true` |
| `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md` | Reviewer-facing proof note for before/after backend-path truth | VERIFIED | Documents the frozen March 30 slice, baseline failure expectations, and post-replay success contract |

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Rust routing and artifact regressions stay green | `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_` | `6` tests passed, `0` failed | PASS |
| Benchmark reader and Doctor contracts stay green | `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor` | `32` tests passed, `0` failed | PASS |
| Fixed-slice backend-path proof contract stays green | `python3 scripts/check_phase18_backend_path.py --slice-json .planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json --status-json .planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json --probe-only` | Exit code `0`; status artifact reports `passed: true`, `probe_only: true`, `case_count: 5` | PASS |

## Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| VAL-01 | 18-01, 18-02, 18-03 | Eligible `llm`-mode validation failures can escalate through Docker and record the routed backend path | SATISFIED | The `llm` route now attempts deterministic Docker for eligible env failures, APDR artifacts persist `validation_path` and `escalated_backend`, and the proof checker locks the fixed-slice contract |
| VAL-02 | 18-02, 18-03 | Benchmark-visible artifacts distinguish requested backend mode from actual backend path | SATISFIED | `validation_backend` remains the requested mode while benchmark rows and events surface routed `validationPath` plus `escalatedBackend` |
| WIN-02 | 18-01, 18-03 | Routing changes preserve platform correctness paths instead of degrading Docker or supported-platform semantics | SATISFIED | Host-runtime cases are excluded from Docker escalation, Docker-only mode still fails hard when Docker is unavailable, and APDR `llm` mode now warns for missing targeted Docker while still checking env prerequisites |

## Human Verification Required

No human gate blocks Phase 18 completion. A live replay of the fixed March 30 slice remains useful milestone evidence, but the deterministic in-repo proof contract is already locked and passing.

## Gaps Summary

No Phase 18 execution gaps remain. Targeted cargo verification still emits the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; these warnings were present before Phase 18, do not affect routing or artifact correctness, and are recorded as residual noise rather than a blocker.

---

_Verified: 2026-03-31T03:05:15Z_
_Verifier: Codex inline execution_
