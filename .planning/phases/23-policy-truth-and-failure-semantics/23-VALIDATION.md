---
phase: 23
slug: policy-truth-and-failure-semantics
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-02
---

# Phase 23 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + Python `unittest` + web build check + deterministic proof script |
| **Config file** | `tools/apdr/Cargo.toml`, repo-root Python test modules, `web/package.json`, and `scripts/check_phase23_policy_truth.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events && npm run build --prefix web && python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-proof-status.json --probe-only"` |
| **Estimated runtime** | ~210 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events && npm run build --prefix web"`
- **Before `$gsd-verify-work`:** Full suite plus the deterministic Phase 23 policy-truth checker must be green
- **Max feedback latency:** 210 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 23-01-01 | 01 | 1 | DFV-02 | integration | `python3 -m unittest benchmark_ui.test_run_contract` | ✅ | ⬜ pending |
| 23-01-02 | 01 | 1 | DFV-02 | integration/event | `python3 -m unittest benchmark_ui.test_runner_events` | ✅ | ⬜ pending |
| 23-02-01 | 02 | 2 | DFV-02 | integration/web | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events && npm run build --prefix web` | ✅ | ⬜ pending |
| 23-02-02 | 02 | 2 | GDR-02 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_` | ✅ | ⬜ pending |
| 23-03-01 | 03 | 3 | DFV-02, GDR-02 | proof-contract | `python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 23-03-02 | 03 | 3 | DFV-02, GDR-02 | proof-doc | `python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-proof-status.json --probe-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Operators can inspect an LLM case and see requested policy, actual path, route label, bypass reason, and failure family together | DFV-02 | Requires human review of the expanded case detail and saved-run rendering together | Load a representative saved run, expand an LLM case, and confirm the UI shows requested policy, validation path, route label, Docker bypass reason, and failure family without opening raw metadata files. |
| Live and historical inspection use the same truth vocabulary | DFV-02 | Requires comparing SSE-fed live data with loaded saved-run data | Start a representative live run or replay fixture, expand a just-completed LLM case, then load the saved run and confirm the same truth fields and labels are present. |
| Host-runtime and framework-runtime blockers stay environment-specific under docker-first | GDR-02 | Requires inspecting a human-readable case surface, not only test fixtures | Inspect representative docker-first host-runtime and framework-runtime cases and confirm the visible family is `environment-specific` rather than a dependency-resolution bucket. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 210s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
