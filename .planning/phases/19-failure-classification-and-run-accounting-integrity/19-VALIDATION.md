---
phase: 19
slug: failure-classification-and-run-accounting-integrity
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-04-01
---

# Phase 19 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + Python `unittest` + deterministic proof script |
| **Config file** | `tools/apdr/Cargo.toml`, repo-root Python test modules, and `scripts/check_phase19_accounting.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting && python3 scripts/check_phase19_accounting.py --slice-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json --fixture-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json --status-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json --probe-only"` |
| **Estimated runtime** | ~180 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_resume_accounting"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting"`
- **Before `$gsd-verify-work`:** Full suite plus the deterministic proof checker must be green
- **Max feedback latency:** 180 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 19-01-01 | 01 | 1 | VAL-04 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_` | ✅ | ⬜ pending |
| 19-01-02 | 01 | 1 | VAL-04 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_` | ✅ | ⬜ pending |
| 19-02-01 | 02 | 2 | EVD-07 | unit | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_resume_accounting` | ✅ | ⬜ pending |
| 19-02-02 | 02 | 2 | EVD-09 | unit | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting` | ✅ | ⬜ pending |
| 19-03-01 | 03 | 3 | VAL-04, EVD-07 | proof-contract | `python3 scripts/check_phase19_accounting.py --slice-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json --fixture-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json --status-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 19-03-02 | 03 | 3 | EVD-09 | proof-contract | `python3 scripts/check_phase19_accounting.py --slice-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json --fixture-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json --status-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json --probe-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Environment-specific failures are reviewer-readable in case artifacts | VAL-04 | Requires opening representative output metadata and APDR report text, not just fixture assertions | Inspect at least one `skipped-host-runtime` artifact and one true dependency-miss artifact. Confirm the first exposes environment-specific classification and the second remains a dependency-resolution failure. |
| Resumed-run operational view remains usable while live-only conclusions stay clean | EVD-09 | Requires viewing both combined and live-only representations of the same resumed run | Load a resumed run in the benchmark UI or service payloads. Confirm the operational run still shows resumed progress, but proof/comparison readers can isolate current-run results without historical contamination. |
| Host-runtime skips stay skips even when dependencies solved successfully | EVD-07 | Requires human confirmation against a representative host-runtime case with valid requirements | Inspect a host-runtime sample after Phase 19 and confirm the display status remains `SKIP`, not `PASS`, even if `requirements.txt` exists and the wrapper exited zero. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 180s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
