---
phase: 28
slug: llm-recovery-loop-and-failure-semantics
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-03
---

# Phase 28 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust unit tests for retry-loop recovery and failure-truth logic, focused Python recovery mocks, Python unittest coverage for benchmark metadata surfaces, deterministic proof checker, and artifact grep checks |
| **Config file** | `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json`, `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json`, `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md`, and `scripts/check_phase28_recovery_truth.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase28_ -- --nocapture && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase28_ -- --nocapture && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events && python3 -m pytest tools/apdr/llm_py/tests/test_recovery_mock.py tools/apdr/llm_py/tests/test_client_fallbacks.py -q && python3 scripts/check_phase28_recovery_truth.py --recovery-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json --failure-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json --status-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-truth-status.json --probe-only && rg -n 'recovery_outcome|failure_truth_class|failure_truth_detail|recovery-attempts.json|Phase 29' scripts/check_phase28_recovery_truth.py .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md tools/apdr/src/lib.rs tools/apdr/test_executor.py"` |
| **Estimated runtime** | ~35 seconds |

---

## Sampling Rate

- **After every task commit:** Run the task's specific verify command
- **After every plan wave:** Run the quick run command
- **Before Phase 28 verification:** Run the full suite command
- **Max feedback latency:** 35 seconds for deterministic recovery-truth checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 28-01-01 | 01 | 1 | LLM-03 | python/recovery-contract | `python3 -m pytest tools/apdr/llm_py/tests/test_recovery_mock.py tools/apdr/llm_py/tests/test_client_fallbacks.py -q` | ✅ | ⬜ pending |
| 28-01-02 | 01 | 1 | LLM-03 | rust/retry-loop | `cargo test --manifest-path tools/apdr/Cargo.toml phase28_recovery_ -- --nocapture` | ✅ | ⬜ pending |
| 28-02-01 | 02 | 2 | TRU-01 | rust/failure-truth | `cargo test --manifest-path tools/apdr/Cargo.toml phase28_truth_ -- --nocapture` | ✅ | ⬜ pending |
| 28-02-02 | 02 | 2 | TRU-01 | metadata/export | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events` | ✅ | ⬜ pending |
| 28-03-01 | 03 | 3 | LLM-03, TRU-01 | proof-contract | `python3 scripts/check_phase28_recovery_truth.py --recovery-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json --failure-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json --status-json /tmp/phase28-status.json --probe-only` | ✅ | ⬜ pending |
| 28-03-02 | 03 | 3 | TRU-01 | grep/proof | `rg -n 'recovery_outcome|failure_truth_class|failure_truth_detail|recovery-attempts.json|Phase 29' scripts/check_phase28_recovery_truth.py .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- The Phase 26 authored intake contract remains the upstream truth boundary; Phase 28 may consume it but must not silently redefine intake semantics.
- The Phase 27 Docker artifact contract remains the upstream execution-truth boundary; Phase 28 must consume authored/executed Docker artifacts rather than invent a second Docker truth path.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| A recovered case clearly shows which authored/executed artifacts informed the applied fix | LLM-03 | Requires reviewer judgment of case-artifact readability | Open a representative recovered case and confirm the recovery artifact lists the authored plan, Docker plan, executed Dockerfile or logs, and the exact fix outcome. |
| A provider or no-output failure now reads as a model/provider failure instead of `Unknown` | TRU-01 | Requires human review of final case wording | Inspect a non-pass case with LLM diagnostics and confirm the final case surface exposes the additive failure-truth classification and detail. |
| A Docker infrastructure failure is distinct from a genuine dependency/runtime miss | TRU-01 | Requires cross-artifact judgment | Inspect a Docker-failure case and confirm the final case surface distinguishes infrastructure from dependency/runtime failure while preserving the coarse family bucket. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 45s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
