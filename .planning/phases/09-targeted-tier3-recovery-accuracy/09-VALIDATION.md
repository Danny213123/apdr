---
phase: 09
slug: targeted-tier3-recovery-accuracy
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 9 - Validation Strategy

> Validation contract for targeted module recovery, bounded compatibility policies, and Phase 9 closeout against the locked Phase 7 and Phase 8 artifacts.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust integration tests, structural `rg` checks, the existing Phase 8 checker, and one deterministic Python Phase 9 checker |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_policy_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture && python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md && python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json` |
| **Estimated runtime** | ~5-12 minutes once the targeted policy and checker artifacts exist |

---

## Sampling Rate

- **After every task commit:** Run the task-specific targeted resolver test, structural check, or checker command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** the targeted Phase 9 tests, the Phase 8 family-runtime checker, and the new Phase 9 checker must all be green
- **Max feedback latency:** keep task-level checks under 5 minutes by using targeted test name prefixes instead of full-project suites

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 09-01-01 | 01 | 1 | REC-02, REC-03 | policy loader or validator surface | `rg -n 'struct TargetedRecoveryPolicy|fn init_targeted_recovery_policy|module_rules_path|compatibility_rules_path' tools/apdr/src/resolver/targeted_recovery.rs` | no | pending |
| 09-01-02 | 01 | 1 | REC-02, REC-03 | loader validation tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_policy_ -- --nocapture` | yes | pending |
| 09-02-01 | 02 | 2 | REC-02, REC-04 | targeted module-provider recovery | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture` | yes | pending |
| 09-02-02 | 02 | 2 | REC-02, REC-04 | inspectable stop reasons and LLM gating | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture` | yes | pending |
| 09-03-01 | 03 | 3 | REC-03, REC-04 | compatibility recovery and version-spec parsing | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture` | yes | pending |
| 09-03-02 | 03 | 3 | REC-03, REC-04 | checker plus reviewer note | `python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json` | yes | pending |

---

## Wave 0 Requirements

- Phase 9 must stay anchored to `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`; it does not get to redefine the canonical slice.
- The Phase 8 family-runtime checker remains part of the closeout suite because TensorFlow, Pillow, PyMC3, ggplot, and setuptools recovery still ride the curated family path.
- Phase 9 closeout must rely on targeted tests and deterministic artifact checks only; a live benchmark rerun belongs to Phase 10.
- New targeted policies must fail loudly when malformed or out of scope instead of silently changing recovery behavior.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The targeted policy files stay bounded to the canonical parity clusters instead of turning into a broad new recovery registry | REC-02, REC-03, REC-04 | Automation can verify identifiers and anchors, but a reviewer still needs to judge whether the policy surface stayed disciplined | Read `tools/apdr/data/recovery/README.md`, `module_rules.json`, `compatibility_rules.json`, and `09-TARGETED-RECOVERY.md`, then confirm the rules are justified by the canonical Phase 7 clusters rather than unrelated future work |
| Failure reasons remain inspectable after the new policies land | REC-03, REC-04 | Tests can assert exact notes, but a reviewer should still confirm the resulting reasons are clear enough to explain why a case recovered or stopped | Review the new `phase9_targeted_module_` and `phase9_targeted_compatibility_` tests in `tools/apdr/tests/test_resolver.rs`, then confirm the note strings still explain the recovery or stop decision in plain language |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity includes both the locked Phase 7 parity manifest and the Phase 8 family-runtime boundary
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by targeted resolver patterns
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-28
