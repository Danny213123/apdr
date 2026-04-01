---
phase: 20-dominant-bucket-recovery-gains
plan: 01
subsystem: validation
tags: [rust, apdr, module-recovery, targeted-recovery, testing]
requires: []
provides:
  - "Expanded dominant-bucket module provider rules for `request`, `eyeD3`, and `Cython.Distutils`"
  - "Deterministic targeted-stop status mapping that moves runtime-specific imports out of `module-not-found`"
  - "Focused Rust regression coverage for Phase 20 module recovery policy and bucket exits"
affects: [phase-20-plan-02, phase-20-plan-03, apdr-artifacts]
tech-stack:
  added: []
  patterns:
    - "Targeted recovery policy owns both the stop reason text and the terminal skip status mapping"
    - "Phase-prefixed policy tests validate dominant-bucket exits without requiring networked validation"
key-files:
  created: []
  modified:
    - tools/apdr/data/recovery/module_rules.json
    - tools/apdr/src/resolver/recovery_diagnostics.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/targeted_recovery.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - "Map `system-runtime` stop reasons to `skipped-host-runtime` so mosquitto-style runtime blockers leave the dominant module bucket."
  - "Keep Phase 20 module verification deterministic by asserting seeded policy and stop-status behavior directly in Rust tests."
patterns-established:
  - "Targeted stop reasons set terminal status and bucket immediately instead of relying on later log inference."
  - "Module policy expansions are shipped alongside exact rule-id regression tests so future edits cannot silently drift."
requirements-completed: [VAL-03]
duration: 11 min
completed: 2026-04-01
---

# Phase 20 Plan 01: Dominant Bucket Recovery Gains Summary

**APDR now has bounded dominant-bucket module recovery rules for missing provider aliases and runtime-only imports, with targeted stop reasons leaving `module-not-found` as explicit skip outcomes instead of generic mapping failures**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-01T19:04:00Z
- **Completed:** 2026-04-01T19:15:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Added deterministic provider rules for `request -> requests`, `eyeD3 -> eyed3`, and `Cython.Distutils -> Cython`.
- Added new dominant-bucket stop rules for `simplegui`, `canvas`, and `mosquitto`, then mapped those stop reasons to terminal skip statuses immediately in the retry loop.
- Added `phase20_module_` regressions that lock the seeded provider and stop-status contract without needing a live package install.

## Task Commits

Each task was committed atomically:

1. **Task 1: Expand dominant-bucket provider aliases and targeted stop reasons** - `fd60c7a` (feat)
2. **Task 2: Map targeted stop reasons out of the dominant bucket and lock module recovery tests** - `ca45c0b` (feat)

## Files Created/Modified

- `tools/apdr/data/recovery/module_rules.json` - Adds the Phase 20 provider aliases and runtime/project stop rules for dominant module failures.
- `tools/apdr/src/resolver/recovery_diagnostics.rs` - Routes stop-reason categories through a shared status mapping helper.
- `tools/apdr/src/resolver/retry_loop.rs` - Sets targeted-stop status and bucket truth before fallback log inference can collapse the case back into `module-not-found`.
- `tools/apdr/src/resolver/targeted_recovery.rs` - Exposes a deterministic stop-status helper that both production code and integration tests use.
- `tools/apdr/tests/test_resolver.rs` - Adds Phase 20 module policy and bucket-exit regressions.

## Decisions Made

- Treated `system-runtime` the same as other host-runtime-style stop categories so runtime-only broker dependencies do not inflate dependency-resolution counts.
- Verified Phase 20 module recovery through policy-level tests rather than live installation flows, keeping the regression gate deterministic and fast.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added a shared stop-status helper in `targeted_recovery.rs`**
- **Found during:** Task 2 (bucket exit verification)
- **Issue:** The plan named `recovery_diagnostics.rs` and `retry_loop.rs`, but the integration tests needed a stable public way to assert the same stop-status mapping that the retry loop uses.
- **Fix:** Added a small shared helper in `targeted_recovery.rs` and routed the existing diagnostics path through it.
- **Files modified:** `tools/apdr/src/resolver/targeted_recovery.rs`, `tools/apdr/src/resolver/recovery_diagnostics.rs`, `tools/apdr/src/resolver/retry_loop.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture`
- **Committed in:** `ca45c0b`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** No scope drift. The extra helper kept production and regression behavior aligned.

## Issues Encountered

- Targeted cargo verification still reports the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; they remained non-blocking and out of scope for this plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 02 can now build on a narrower `module-not-found` surface with deterministic provider aliases already seeded in the targeted recovery layer.
- The new stop-status mapping keeps runtime-only imports out of the Phase 20 dominant-bucket proof slice.

## Self-Check: PASSED

- Found `.planning/phases/20-dominant-bucket-recovery-gains/20-01-SUMMARY.md`
- Found task commits `fd60c7a` and `ca45c0b`

---
*Phase: 20-dominant-bucket-recovery-gains*
*Completed: 2026-04-01*
