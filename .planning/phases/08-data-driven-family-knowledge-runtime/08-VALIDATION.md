---
phase: 08
slug: data-driven-family-knowledge-runtime
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 8 - Validation Strategy

> Validation contract for the touched-family data model, runtime wiring, and regression boundary carried forward from Phase 7.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust integration tests, structural `rg` checks, and one deterministic Python phase-close checker |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture && python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` |
| **Estimated runtime** | ~3-8 minutes once the Phase 8 artifacts exist |

---

## Sampling Rate

- **After every task commit:** Run the task-specific Rust test, grep, or checker command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** The data-driven family tests, the Phase 7 family-fixture regression tests, and the Phase 8 checker must all be green
- **Max feedback latency:** keep the task-level checks under 5 minutes by using targeted resolver patterns instead of full-project test runs

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 08-01-01 | 01 | 1 | FAM-01, FAM-03 | schema/loader surface | `rg -n 'struct CuratedFamilyKnowledge|fn init_curated_family_knowledge|duplicate explicit namespace mapping' tools/apdr/src/resolver/family_knowledge/data.rs` | no | pending |
| 08-01-02 | 01 | 1 | FAM-01, FAM-03 | loader validation tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_loader_ -- --nocapture` | yes | pending |
| 08-02-01 | 02 | 2 | FAM-01, FAM-02 | runtime registry wiring | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_runtime_registry_ -- --nocapture` | yes | pending |
| 08-02-02 | 02 | 2 | FAM-02, FAM-03 | touched family recovery behavior | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_runtime_behavior_ -- --nocapture` | yes | pending |
| 08-03-01 | 03 | 3 | FAM-02 | Phase 7 fixture regression coverage | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture` | yes | pending |
| 08-03-02 | 03 | 3 | FAM-02, FAM-03 | checker + reviewer note | `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` | yes | pending |

---

## Wave 0 Requirements

- Existing Rust test infrastructure covers the phase; no new test framework is needed.
- The Phase 7 family fixture root remains the benchmark-derived regression boundary.
- The Phase 8 checker must read local artifacts only; it must not rerun the March 27, 2026 benchmark.
- Curated data validation errors must stop touched-family runtime changes before they take effect.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The new curated data files stay scoped to the touched Phase 7 families rather than expanding into unrelated registry entries | FAM-01, FAM-02 | Automation can verify identifiers, but a reviewer still needs to judge whether the curated scope stayed disciplined | Read `tools/apdr/data/family_knowledge/README.md` and `08-FAMILY-RUNTIME.md`, then confirm the Phase 8 data covers only the touched Phase 7 runtime surfaces |
| Actionable diagnostics remain understandable to maintainers when curated family data is invalid | FAM-03 | Tests can assert exact error strings, but a reviewer should still confirm the messages explain what needs fixing | Review the failing-data test cases in `tools/apdr/tests/test_resolver.rs` and confirm the loader errors name the duplicate family, mapping, or rule that caused the failure |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity includes both runtime tests and the benchmark-derived Phase 7 fixture boundary
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by targeted resolver patterns
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-28
