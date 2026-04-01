---
phase: 20-dominant-bucket-recovery-gains
plan: 02
subsystem: validation
tags: [rust, apdr, compatibility, python-runtime, testing]
requires:
  - phase: 20-01
    provides: "Cleaner dominant module surface and deterministic targeted recovery policy scaffolding"
provides:
  - "Compatibility clusters that can express replacement packages and Python floors"
  - "Retry-loop recovery that applies targeted compatibility fixes before contradictory-pin and build-churn breakers fire"
  - "Rust regressions for BeautifulSoup rewrites, PyMC3 floor filtering, and OpenCV convergence"
affects: [phase-20-plan-03, apdr-artifacts]
tech-stack:
  added: []
  patterns:
    - "Compatibility clusters can now express replacement packages, preferred versions, and Python windows in one bounded policy object"
    - "Retry-loop breakers consult targeted compatibility recovery before concluding that churn is unrecoverable"
key-files:
  created: []
  modified:
    - tools/apdr/data/recovery/compatibility_rules.json
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/targeted_recovery.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - "Keep Python-floor filtering in the retry loop so family-selected modern runtimes can be protected without modifying family-knowledge ordering."
  - "Use one shared compatibility-cluster helper for replacements, preferred versions, companions, and test verification."
patterns-established:
  - "Targeted compatibility recovery is allowed to fire before contradictory-pin and repeated-build breakers terminate the loop."
  - "Phase-prefixed compatibility tests exercise policy application directly instead of depending on networked benchmark installs."
requirements-completed: [AGT-09, VAL-03]
duration: 14 min
completed: 2026-04-01
---

# Phase 20 Plan 02: Dominant Bucket Recovery Gains Summary

**APDR now applies replacement-package and Python-floor compatibility policy before repeated version/build churn terminates recovery, letting the dominant BeautifulSoup, PyMC3, and OpenCV families converge on bounded compatible requirements**

## Performance

- **Duration:** 14 min
- **Started:** 2026-04-01T19:15:00Z
- **Completed:** 2026-04-01T19:29:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Extended the targeted compatibility schema with `replacement_packages` and `python_floor`, then added the Phase 20 BeautifulSoup, MySQL-python, OpenCV headless, and PyMC3 floor rules.
- Filtered validation candidate runtimes through the new compatibility window so modern-family cases do not fall back below Python 3.9 after family guidance selects a modern runtime.
- Applied targeted compatibility recovery before contradictory-pin and repeated-build breakers, then locked the behavior with `phase20_compat_` Rust tests.

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend compatibility policy with replacements and interpreter floors** - `abba142` (feat)
2. **Task 2: Apply replacement and floor rules before dominant version/build loops repeat** - `437a858` (feat)

## Files Created/Modified

- `tools/apdr/data/recovery/compatibility_rules.json` - Adds replacement-package, Python-floor, and dominant-family convergence rules for BeautifulSoup, MySQL-python, OpenCV, and PyMC3.
- `tools/apdr/src/resolver/targeted_recovery.rs` - Adds shared cluster application and candidate-version filtering helpers.
- `tools/apdr/src/resolver/retry_loop.rs` - Applies targeted compatibility recovery before breaker exits and filters candidate runtimes through cluster floor/ceiling rules.
- `tools/apdr/tests/test_resolver.rs` - Adds the Phase 20 compatibility regressions for replacements, runtime floor protection, and OpenCV convergence.

## Decisions Made

- Protected PyMC3-family recovery by filtering the candidate runtime list in the retry loop, which keeps the change local to Phase 20 compatibility logic instead of rewriting family-knowledge ordering.
- Reused a single compatibility-cluster application helper across retry-loop recovery and test coverage so replacements and preferred versions cannot drift.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 4 - Coupling] Cluster application and candidate-version filtering landed together in `targeted_recovery.rs`**
- **Found during:** Plan implementation
- **Issue:** Replacements, preferred versions, companions, and Python-window filtering all depend on the same cluster snapshot, so separating them into unrelated helpers would have duplicated policy traversal.
- **Fix:** Kept the shared helper layer in `targeted_recovery.rs` and documented the coupling explicitly.
- **Files modified:** `tools/apdr/src/resolver/targeted_recovery.rs`, `tools/apdr/src/resolver/retry_loop.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture`
- **Committed in:** `437a858`

---

**Total deviations:** 1 auto-fixed (1 coupling)
**Impact on plan:** No scope drift. The shared helper layer is what makes the retry-loop behavior and regressions stay deterministic.

## Issues Encountered

- The same pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs` stayed visible during cargo verification, but they remained non-blocking.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 03 can freeze the dominant-bucket proof slice with realistic candidate expectations for the newly covered BeautifulSoup, PyMC3, and OpenCV families.
- The retry loop now preserves Phase 18 path truth and Phase 19 bucket truth while reducing version/build churn on the selected slice.

## Self-Check: PASSED

- Found `.planning/phases/20-dominant-bucket-recovery-gains/20-02-SUMMARY.md`
- Found task commits `abba142` and `437a858`

---
*Phase: 20-dominant-bucket-recovery-gains*
*Completed: 2026-04-01*
