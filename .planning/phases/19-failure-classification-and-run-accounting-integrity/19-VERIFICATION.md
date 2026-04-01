---
phase: 19-failure-classification-and-run-accounting-integrity
verified: 2026-04-01T18:47:39Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 19: Failure Classification and Run-Accounting Integrity Verification Report

**Phase Goal:** Operators can trust tier3 failure categories and resumed-run summaries to separate environment-specific issues from real dependency-resolution misses.
**Verified:** 2026-04-01T18:47:39Z
**Status:** passed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | APDR artifacts now preserve environment-specific validation blockers separately from real dependency-resolution misses | VERIFIED | `tools/apdr/src/resolver/recovery_diagnostics.rs` adds `classify_failure_family(...)`; `tools/apdr/src/resolver/retry_loop.rs` preserves environment-specific family truth on host-runtime skips; `tools/apdr/src/lib.rs` and `tools/apdr/test_executor.py` serialize `failure_family`, `failure_bucket`, and `skip_candidate` into outward-facing artifacts |
| 2 | Benchmark readers no longer upgrade host-runtime skips into passes and resumed rows now preserve live-versus-historical provenance | VERIFIED | `benchmark_ui/runner.py` removes skip-to-pass reclassification and stores `historical_results` separately; `benchmark_ui/service.py` exposes `resultOrigin`, `failureFamily`, and live-only counters; `benchmark_ui/test_resume_accounting.py` plus `benchmark_ui/test_runner_events.py` lock the new contract |
| 3 | Phase 19 ships a deterministic proof package for both classification truth and resume provenance | VERIFIED | `scripts/check_phase19_accounting.py` validates the frozen March 30 slice against `runs/20260330-020943-apdr/summary.json`, exercises the mixed-provenance fixture through `BenchmarkService`, and refreshes `19-accounting-proof-status.json`; `19-ACCOUNTING-PROOF.md` documents the before/after review gate |

**Score:** 3/3 must-haves verified

## Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/src/resolver/recovery_diagnostics.rs` | Durable failure-family classification for environment-specific versus dependency-resolution outcomes | VERIFIED | Adds `classify_failure_family(...)`, environment-specific detection helpers, and targeted `phase19_classification_` tests |
| `tools/apdr/src/resolver/retry_loop.rs` | Retry-loop preservation of environment-specific skip truth | VERIFIED | Host-runtime short-circuits now stamp `failure_family = environment-specific` alongside the existing skip metadata |
| `tools/apdr/src/lib.rs` | Summary-level classification fields | VERIFIED | `ValidationSummary` now includes `failure_family`, and report text plus summary lines expose it |
| `tools/apdr/test_executor.py` | Saved APDR output YAML includes classification truth | VERIFIED | Emits `failure_family`, `failure_bucket`, and `skip_candidate` for benchmark readers |
| `benchmark_ui/runner.py` | Resume storage separates historical rows from live rows | VERIFIED | Uses `historical_results` for resumed history, keeps live rows in `results`, and emits `resultOrigin: live` for case-complete events |
| `benchmark_ui/service.py` | Reader helpers expose provenance-aware accounting and skip truth | VERIFIED | Computes combined, historical-only, and live-only counts from normalized provenance helpers and keeps host-runtime cases in the skip bucket |
| `benchmark_ui/test_resume_accounting.py` | Regression coverage for skip accounting and mixed provenance | VERIFIED | Locks that host-runtime rows remain `SKIP` and that historical rows stay out of live-only totals |
| `scripts/check_phase19_accounting.py` | Deterministic proof checker for the fixed classification slice and mixed-provenance fixture | VERIFIED | Probe-mode command passed and refreshed `19-accounting-proof-status.json` with both classification and provenance sections |
| `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md` | Reviewer-facing proof note for before/after accounting truth | VERIFIED | Documents the frozen March 30 slice, the mixed-provenance fixture, the probe command, and the post-fix review conditions |

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| APDR classification regressions stay green | `cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_` | `5` tests passed, `0` failed | PASS |
| Benchmark accounting and provenance contracts stay green | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting` | `31` tests passed, `0` failed | PASS |
| Fixed-slice accounting proof contract stays green | `python3 scripts/check_phase19_accounting.py --slice-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json --fixture-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json --status-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json --probe-only` | Exit code `0`; status artifact reports `passed: true`, `case_count: 4`, `live_only.completed: 2` | PASS |

## Cross-Phase Regression Gate

| Inherited Contract | Command | Result | Status |
| --- | --- | --- | --- |
| Phase 18 backend routing remains green | `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_` | `6` tests passed, `0` failed | PASS |
| Phase 17 family knowledge inheritance stays green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture` | `9` tests passed, `0` failed | PASS |
| Phase 9 targeted recovery inheritance stays green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_ -- --nocapture` | `11` tests passed, `0` failed | PASS |
| Shared benchmark UI contracts remain green with Phase 19 changes | `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor benchmark_ui.test_resume_accounting` | `35` tests passed, `0` failed | PASS |
| Phase 17 proof contract still passes | `python3 scripts/check_phase17_fallback_artifacts.py --slice-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-live-fallback-slice.json --sample-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-agent-outcome-sample.json --status-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-fallback-proof-status.json --probe-only` | `Phase 17 fallback artifact probe passed.` | PASS |
| Phase 18 proof contract still passes | `python3 scripts/check_phase18_backend_path.py --slice-json .planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json --status-json .planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json --probe-only` | Exit code `0`; status artifact reports `passed: true` | PASS |

## Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| VAL-04 | 19-01, 19-02, 19-03 | Benchmark operator can distinguish framework or host-runtime failures from dependency-resolution failures in per-case validation results | SATISFIED | APDR artifacts now expose `failure_family`, the fixed March 30 proof slice locks environment-specific skips versus dependency-resolution failures, and benchmark case rows surface the classification fields directly |
| EVD-07 | 19-02, 19-03 | Resumed-run summaries do not mark skipped host-runtime cases as successes | SATISFIED | Skip-to-pass reclassification is removed in runner/service code, regression tests keep host-runtime rows in the skip bucket, and the mixed-provenance fixture proves combined accounting still reports one skip instead of a false pass |
| EVD-09 | 19-02, 19-03 | Live v2.3 comparisons avoid stale historical metadata | SATISFIED | Resumed history is stored in `historical_results`, case rows carry `resultOrigin`, live-only counters exclude historical rows, and the proof fixture locks a live-only completed count that differs from the combined view |

## Human Verification Required

No additional human gate blocks Phase 19 completion. A resumed run loaded through the UI remains a useful manual spot-check for operator experience, but the deterministic proof fixture and provenance-aware tests already lock the correctness contract that this phase set out to ship.

## Gaps Summary

No Phase 19 execution gaps remain. Targeted cargo verification still emits the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; those warnings predate Phase 19, did not affect the new classification or accounting behavior, and are recorded as residual noise rather than a blocker.

---

_Verified: 2026-04-01T18:47:39Z_
_Verifier: Codex inline execution_
