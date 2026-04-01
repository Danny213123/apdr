---
phase: 19-failure-classification-and-run-accounting-integrity
plan: 01
subsystem: validation
tags: [rust, apdr, classification, reporting, testing]
requires: []
provides:
  - "Durable `failure_family` classification for environment-specific versus dependency-resolution validation outcomes"
  - "APDR report and summary output that surface classification truth directly"
  - "Rust regression coverage for host-runtime skip and dependency-miss boundaries"
affects: [phase-19-plan-02, phase-19-plan-03, benchmark-ui, apdr-artifacts]
tech-stack:
  added: []
  patterns:
    - "Validation summaries preserve detailed status and bucket fields while adding a higher-level failure family"
    - "Host-runtime skip semantics are set in the resolver and serialized into downstream artifact surfaces"
key-files:
  created: []
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/resolver/mod.rs
    - tools/apdr/src/resolver/recovery_diagnostics.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/test_executor.py
key-decisions:
  - "Keep `validation_status` and `failure_bucket` intact and add `failure_family` alongside them instead of collapsing existing diagnostics."
  - "Set environment-specific truth as early as the retry loop so later benchmark readers do not have to reconstruct it heuristically."
patterns-established:
  - "Phase-prefixed Rust tests lock classification boundaries in both resolver helpers and report serialization."
  - "APDR output YAML mirrors resolver summary keys directly so the benchmark layer can consume stable metadata."
requirements-completed: [VAL-04]
duration: 12 min
completed: 2026-04-01
---

# Phase 19 Plan 01: Failure Classification and Run-Accounting Integrity Summary

**APDR now emits durable failure-family truth that separates environment-specific validation blockers from real dependency-resolution misses and carries that classification into report text, summary lines, and saved output metadata**

## Performance

- **Duration:** 12 min
- **Started:** 2026-04-01T18:31:00Z
- **Completed:** 2026-04-01T18:43:03Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Added `classify_failure_family(...)` and the related environment-specific detection helpers so host-runtime and framework-runtime blockers remain distinct from dependency-resolution failures.
- Threaded `failure_family` through `ValidationSummary`, report text, summary lines, and APDR output YAML without disturbing the Phase 18 backend-path fields.
- Added `phase19_classification_` Rust tests that lock host-runtime skip classification, dependency-resolution boundaries, and the outward-facing report surfaces.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add a durable failure-family classification and serialize it** - `3187863` (feat)
2. **Task 2: Lock classification boundaries with Rust regression tests** - `3187863` (feat/test)

## Files Created/Modified

- `tools/apdr/src/lib.rs` - Adds `failure_family` to `ValidationSummary`, exposes it in report text and summary lines, and locks the outward serialization contract with focused tests.
- `tools/apdr/src/resolver/mod.rs` - Ensures skipped validation summaries inherit the same failure-family classification contract as retry-loop results.
- `tools/apdr/src/resolver/recovery_diagnostics.rs` - Adds central failure-family classification helpers and regression tests for environment-specific versus dependency-resolution outcomes.
- `tools/apdr/src/resolver/retry_loop.rs` - Preserves environment-specific classification when host-runtime skips short-circuit validation.
- `tools/apdr/test_executor.py` - Copies `failure_family`, `failure_bucket`, and `skip_candidate` into saved APDR output YAML for benchmark readers.

## Decisions Made

- Added a higher-level family field instead of renaming or flattening the existing validation statuses so downstream readers keep both precise failure detail and broad accounting truth.
- Classified host-runtime short-circuits directly in the resolver path that already decides to skip, which keeps the environment-specific boundary close to the source of truth.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 4 - Coupling] Resolver wiring and regression coverage landed in one atomic code commit**
- **Found during:** Plan closeout
- **Issue:** The `failure_family` field additions and the new Rust regression tests touched the same APDR files, so splitting them into separate non-interactive commits would have obscured the actual change boundary.
- **Fix:** Kept one plan-scoped code commit and documented both task outcomes in the summary.
- **Files modified:** `tools/apdr/src/lib.rs`, `tools/apdr/src/resolver/mod.rs`, `tools/apdr/src/resolver/recovery_diagnostics.rs`, `tools/apdr/src/resolver/retry_loop.rs`, `tools/apdr/test_executor.py`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase19_classification_`
- **Committed in:** `3187863`

---

**Total deviations:** 1 auto-fixed (1 coupling)
**Impact on plan:** No scope change. The plan output matches the intended classification and serialization contract exactly.

## Issues Encountered

- Targeted cargo verification still emits the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; they remained non-blocking and out of scope for Phase 19.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 02 can consume direct `failure_family`, `failure_bucket`, and `skip_candidate` metadata instead of reconstructing classification truth from raw status strings.
- Plan 03 can freeze a proof slice that names both expected display status and expected failure family against the March 30 baseline.

## Self-Check: PASSED

- Found `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-01-SUMMARY.md`
- Found task commit `3187863`

---
*Phase: 19-failure-classification-and-run-accounting-integrity*
*Completed: 2026-04-01*
