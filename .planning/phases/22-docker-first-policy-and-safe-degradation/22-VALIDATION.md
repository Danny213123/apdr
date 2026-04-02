---
phase: 22
slug: docker-first-policy-and-safe-degradation
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-01
---

# Phase 22 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + Python `unittest` + web build check + deterministic proof script |
| **Config file** | `tools/apdr/Cargo.toml`, repo-root Python test modules, `web/package.json`, and `scripts/check_phase22_docker_policy.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor && npm run build --prefix web && python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only"` |
| **Estimated runtime** | ~210 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_ && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor && npm run build --prefix web"`
- **Before `$gsd-verify-work`:** Full suite plus the deterministic policy proof checker must be green
- **Gap-closure wave requirement:** Wave 4 must rerun the proof checker after the Doctor wording test so the installed-but-unusable Docker path is sampled in both runtime copy and machine-readable artifacts.
- **Max feedback latency:** 210 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 22-01-01 | 01 | 1 | DFV-01 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | ✅ | ⬜ pending |
| 22-01-02 | 01 | 1 | DFV-03 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | ✅ | ⬜ pending |
| 22-02-01 | 02 | 2 | DFV-01, DFV-03 | integration | `python3 -m unittest benchmark_ui.test_run_contract` | ✅ | ⬜ pending |
| 22-02-02 | 02 | 2 | GDR-01 | integration | `python3 -m unittest benchmark_ui.test_state_backend_doctor && npm run build --prefix web` | ✅ | ⬜ pending |
| 22-03-01 | 03 | 3 | GDR-01 | artifact-contract | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | ✅ | ⬜ pending |
| 22-03-02 | 03 | 3 | DFV-01, GDR-01 | proof-contract | `python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 22-04-01 | 04 | 4 | GDR-01 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | ✅ | ⬜ pending |
| 22-04-02 | 04 | 4 | GDR-01 | proof-contract/integration | `python3 -m unittest benchmark_ui.test_state_backend_doctor && python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Operators can choose docker-first versus env-first control without confusing it with a new backend name | DFV-01, DFV-03 | Requires reviewing the benchmark form, preview payload, and run summary copy together | Open the benchmark home view, choose APDR `llm`, confirm the policy control appears, and verify that the resulting config still uses `validation_backend=llm` while carrying the selected policy separately. |
| Docker-unavailable `llm` runs degrade clearly instead of silently acting env-only | GDR-01 | Requires human review of Doctor/runtime wording and saved artifacts together | Trigger Doctor or run on a machine/fixture without healthy Docker and confirm the warning explains that docker-first was requested but env fallback will be used, then inspect the case debug folder for an explicit bypass note. |
| `llm` case debug folders always show Docker attempt or bypass context | GDR-01 | Requires inspecting the generated artifact tree, not just unit fixtures | Open representative case debug folders and confirm actual Docker attempts contain the Dockerfile, Docker commands, and Docker logs, while bypassed cases contain an explicit bypass artifact instead of an empty gap. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 210s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
