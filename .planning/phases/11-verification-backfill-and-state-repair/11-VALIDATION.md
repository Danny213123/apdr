---
phase: 11
slug: verification-backfill-and-state-repair
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 11 - Validation Strategy

> Validation contract for Phase 8 verification backfill, Phase 7 manual-acceptance backfill, milestone-state repair, and refreshed audit state.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Existing Python phase checkers, targeted Rust regression tests, structural `rg` checks, and one new deterministic Phase 11 checker |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python scripts/check_phase11_verification_backfill.py --phase7-verification .planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md --phase7-uat .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md --phase8-verification .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md --project-md .planning/PROJECT.md --state-md .planning/STATE.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md --repair-md .planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md` |
| **Full suite command** | `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture && python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md && python scripts/check_phase11_verification_backfill.py --phase7-verification .planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md --phase7-uat .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md --phase8-verification .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md --project-md .planning/PROJECT.md --state-md .planning/STATE.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md --repair-md .planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md` |
| **Estimated runtime** | ~3-8 minutes once the backfill artifacts and checker exist |

---

## Sampling Rate

- **After every task commit:** Run the task-specific structural check, targeted checker, or existing regression command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** The Phase 7 checker, the Phase 8 checker, the two targeted Rust regression slices, and the new Phase 11 checker must all be green
- **Max feedback latency:** keep task-level checks under 5 minutes by reusing the existing bounded checkers and targeted Rust test filters

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 11-01-01 | 01 | 1 | FAM-01, FAM-02, FAM-03 | Phase 8 verification backfill structure | `rg -n 'status: passed|FAM-01|FAM-02|FAM-03|check_phase8_family_runtime.py|data_driven_family_|phase7_family_' .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md` | no | pending |
| 11-01-02 | 01 | 1 | FAM-01, FAM-02, FAM-03 | Phase 7 manual-acceptance backfill structure | `rg -n 'status: passed|passed: 2|pending: 0|approved on 2026-03-28' .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md .planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md` | yes | pending |
| 11-02-01 | 02 | 2 | FAM-01, FAM-02, FAM-03 | Planning-state repair | `rg -n 'Phase 11|Phase 12|not ready for milestone completion|gap closure|live benchmark proof|current_phase: 11|completed_phases: 4|total_phases: 6' .planning/PROJECT.md .planning/STATE.md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md` | yes | pending |
| 11-03-01 | 03 | 3 | FAM-01, FAM-02, FAM-03 | Phase 11 deterministic checker | `python scripts/check_phase11_verification_backfill.py --phase7-verification .planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md --phase7-uat .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md --phase8-verification .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md --project-md .planning/PROJECT.md --state-md .planning/STATE.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md --repair-md .planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md` | no | pending |
| 11-03-02 | 03 | 3 | FAM-01, FAM-02, FAM-03 | Refreshed audit content | `rg -n 'status: gaps_found|REC-02|REC-03|REC-04|Phase 12' .planning/v2.1-MILESTONE-AUDIT.md` | yes | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers the phase.
- Phase 11 adds one new deterministic checker script and one reviewer-facing repair note, but it reuses the existing Phase 7 and Phase 8 checker/test surface.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The backfilled Phase 7 manual-review text accurately reflects the accepted watchlist-boundary and touched-family-rationale approval | FAM-01, FAM-02, FAM-03 | The repo can verify the updated status fields, but only a reviewer can confirm the prose reflects the actual accepted decision rather than inventing new review scope | Read `07-HUMAN-UAT.md` and `07-VERIFICATION.md`, then confirm they describe the already accepted Phase 7 manual review rather than introducing new criteria |
| The repaired milestone-state docs are internally consistent for a reviewer entering at Phase 11 | FAM-01, FAM-02, FAM-03 | Automation can match phrases and file presence, but not whether the repaired docs give a coherent narrative about why Phases 11 and 12 exist | Read `PROJECT.md`, `STATE.md`, `10-MILESTONE-CLOSEOUT.md`, and `11-STATE-REPAIR.md`, then confirm they all point to the same remaining blocker set |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity reuses the locked Phase 7 and Phase 8 checker surfaces
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by targeted checkers and narrow Rust test slices
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-28
