---
phase: 20
slug: dominant-bucket-recovery-gains
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-04-01
---

# Phase 20 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + deterministic proof scripts |
| **Config file** | `tools/apdr/Cargo.toml`, `tools/apdr/tests/test_resolver.rs`, and `scripts/check_phase20_recovery_delta.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture && python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json .planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json --probe-only"` |
| **Estimated runtime** | ~240 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture || cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture"`
- **Before `$gsd-verify-work`:** Full suite plus the deterministic delta checker must be green
- **Max feedback latency:** 240 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 20-01-01 | 01 | 1 | VAL-03 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture` | ✅ | ⬜ pending |
| 20-01-02 | 01 | 1 | VAL-03 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture` | ✅ | ⬜ pending |
| 20-02-01 | 02 | 2 | AGT-09, VAL-03 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture` | ✅ | ⬜ pending |
| 20-02-02 | 02 | 2 | AGT-09, VAL-03 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture` | ✅ | ⬜ pending |
| 20-03-01 | 03 | 3 | AGT-09, VAL-03 | proof-contract | `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json .planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 20-03-02 | 03 | 3 | AGT-09, VAL-03 | proof-contract | `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json .planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json --probe-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Candidate slice uses the same run mode and model as the March 30 baseline slice | AGT-09 | Requires comparing baseline and candidate benchmark artifact metadata, not just unit tests | Open the Phase 20 baseline and candidate artifacts and confirm `slice_id`, `validation_backend`, `model_name`, and selected case list all match before reading delta counts. |
| Recovered cases are genuinely better outcomes rather than relabeled failures | VAL-03 | Requires reading representative case artifacts from each dominant bucket | Inspect at least one recovered `module-not-found`, one recovered `version-not-found`, and one recovered `environment-build-failed` case. Confirm each now reaches `validation_status: passed` or an explicitly non-dominant terminal state for the documented reason. |
| Phase 18 and Phase 19 truth surfaces remain intact while gains land | AGT-09, VAL-03 | Requires checking routing/accounting metadata on the new candidate artifacts | Confirm candidate artifacts still expose routed backend fields and Phase 19 provenance/accounting fields so the measured gain is attributable to recovery changes rather than metadata loss. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 240s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
