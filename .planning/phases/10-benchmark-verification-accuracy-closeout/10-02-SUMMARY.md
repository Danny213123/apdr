---
phase: 10-benchmark-verification-accuracy-closeout
plan: 02
subsystem: benchmark-evidence
tags: [benchmark, closeout, preservation-guards, unrecovered-gaps, deterministic-checker]

# Dependency graph
requires:
  - phase: 10-01
    provides: Machine-readable case-delta.json, targeted-rerun.json, and rerun-manifest.json artifacts
  - phase: 07-failure-baseline-parity-slice
    provides: Canonical 70-case tier3 parity slice and 17-case watchlist boundary
  - phase: 09-targeted-tier3-recovery-accuracy
    provides: Recovery policies and handoff measurements
provides:
  - Split reviewer-facing evidence package (benchmark note, watchlist appendix, preservation guards, unrecovered-gap report)
  - Deterministic Phase 10 closeout checker validating counts, headings, guard IDs, boundary text, and follow-on notes
affects: [milestone-closeout, future-recovery-planning]

# Tech tracking
tech-stack:
  added: []
  patterns: [split-evidence-package, deterministic-closeout-checker]

key-files:
  created:
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md
    - scripts/check_phase10_benchmark_closeout.py
  modified:
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md

key-decisions:
  - "EVD-02 verdict linked to the unrecovered-gap report grouped by dominant failure bucket rather than treating it as a watchlist boundary question"
  - "Follow-on notes organized by sub-pattern within each bucket to support future milestone planning"
  - "Checker loads the rerun manifest from a sibling path to the case-delta JSON rather than requiring a separate argument"

patterns-established:
  - "Split evidence package: machine-readable JSON artifacts separate from reviewer-facing Markdown summaries"
  - "Phase closeout checker validates both structural properties (counts, headings) and semantic properties (guard IDs, boundary text, follow-on completeness)"

requirements-completed: [REC-05, EVD-01, EVD-02]

# Metrics
duration: 5min
completed: 2026-03-28
---

# Phase 10 Plan 02: Evidence Package & Closeout Checker Summary

**Split reviewer-facing benchmark evidence with watchlist boundary, preservation guards, dominant-bucket gap report, and one deterministic checker validating the full Phase 10 closeout surface**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-28T21:20:50Z
- **Completed:** 2026-03-28T21:25:57Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Rewrote the benchmark-verification note with accurate EVD-02 verdict linking unrecovered gaps by dominant bucket
- Created the 17-case watchlist appendix with explicit boundary text keeping it outside the main contract
- Created the 11-case preservation-guards report with every guard ID categorized under passed, host-runtime, local-helper, or unsolvable
- Created the unrecovered-gap report grouping all 70 canonical cases by dominant failure bucket with per-case follow-on notes identifying sub-patterns for future recovery work
- Created the deterministic Phase 10 closeout checker validating counts, headings, guard IDs, boundary text, and follow-on note completeness

## Task Commits

Each task was committed atomically:

1. **Task 1: Write the canonical-slice benchmark-verification note** - `2758977` (feat)
2. **Task 2: Create watchlist, preservation, gap reports and checker** - `01784a3` (feat)

## Files Created/Modified

- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md` - Rewrote with accurate EVD-02 verdict and complete artifact links table
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md` - 17-case tier1 watchlist with scope boundary, per-case table, and interpretation
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md` - 11 REC-05 guard cases under 4 category headings with per-case match status
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md` - 70 cases grouped by 6 dominant failure buckets with per-case validation reasons and sub-pattern follow-on notes
- `scripts/check_phase10_benchmark_closeout.py` - Deterministic checker with 8 arguments validating the full Phase 10 evidence surface

## Decisions Made

- EVD-02 verdict was re-pointed to the unrecovered-gap report (dominant-bucket grouping with follow-on notes) rather than the watchlist boundary, which is a separate concern
- Follow-on notes in the gap report were organized by sub-pattern within each bucket (e.g., Python 2.7 setup.py failures, system C-library dependencies, keras/tensorflow conflicts) to make future milestone planning actionable
- The checker infers the rerun manifest path from the case-delta JSON sibling directory rather than requiring a separate `--manifest` argument

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- All Phase 10 evidence artifacts are complete and locked behind the deterministic checker
- Requirements REC-05, EVD-01, and EVD-02 all have PASS verdicts with primary evidence links
- The unrecovered-gap report provides actionable sub-pattern analysis for future recovery milestone planning
- The milestone is ready for closeout (Plan 03 or milestone transition)

## Self-Check: PASSED

- All 6 files verified present on disk
- Commits `2758977` and `01784a3` verified in git log
- Deterministic checker passes with 0 errors

---
*Phase: 10-benchmark-verification-accuracy-closeout*
*Completed: 2026-03-28*
