---
phase: 18
slug: backend-escalation-and-path-truth
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-03-31
---

# Phase 18 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + Python `unittest` + deterministic proof script |
| **Config file** | `tools/apdr/Cargo.toml`, repo-root Python test modules, and `scripts/check_phase18_backend_path.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_ && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_ && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor"` |
| **Estimated runtime** | ~150 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_ && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_ && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor"`
- **Before `$gsd-verify-work`:** Full suite plus the deterministic proof checker must be green
- **Max feedback latency:** 150 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 18-01-01 | 01 | 1 | VAL-01 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_` | ✅ | ⬜ pending |
| 18-01-02 | 01 | 1 | WIN-02 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_` | ✅ | ⬜ pending |
| 18-02-01 | 02 | 2 | VAL-02 | unit | `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract` | ✅ | ⬜ pending |
| 18-02-02 | 02 | 2 | VAL-01 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_` | ✅ | ⬜ pending |
| 18-03-01 | 03 | 3 | WIN-02 | unit | `python3 -m unittest benchmark_ui.test_state_backend_doctor` | ✅ | ⬜ pending |
| 18-03-02 | 03 | 3 | VAL-01, VAL-02 | proof-contract | `python3 scripts/check_phase18_backend_path.py --slice-json .planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json --status-json .planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json --probe-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Eligible live-derived cases really route `env -> docker` before any agent fallback | VAL-01 | Requires inspecting generated attempts and case metadata rather than only unit fixtures | Re-run the Phase 18 checker against a current run directory and inspect representative `.apdr-debug/attempts/*/metadata.txt` plus output YAML files to confirm eligible slice cases show Docker as the middle hop. |
| Top-level backend and path truth remain distinct and operator-readable | VAL-02 | Requires human inspection of serialized artifacts and benchmark UI wording | Open representative output data for a routed case and confirm `validation_backend` remains `llm` while `validation_path` reports `env->docker` or `env->docker->llm-agent`, then review APDR service labels in the UI or rendered payloads. |
| Docker and Windows/runtime expectations stay truthful | WIN-02 | Requires end-to-end operator messaging review | Trigger the benchmark Doctor/runtime checks on a machine or fixture without Docker and confirm APDR `llm` mode warns that targeted Docker escalation is unavailable, while pure Docker mode still fails as a required dependency. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 150s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
