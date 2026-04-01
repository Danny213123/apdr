---
phase: 19-failure-classification-and-run-accounting-integrity
plan: 02
subsystem: benchmark-ui
tags: [python, benchmark-ui, resume, accounting, provenance]
requires:
  - phase: 19-01
    provides: failure-family artifact truth
provides:
  - "Benchmark skip accounting that keeps host-runtime cases in the skip bucket"
  - "Separate `historical_results` and live `results` storage for resumed runs"
  - "Reader and event-level provenance fields for live versus historical result rows"
affects: [phase-19-plan-03, phase-19-verification, benchmark-ui]
tech-stack:
  added: []
  patterns:
    - "Resumed run summaries preserve historical and live rows in separate collections before any combined view is built"
    - "Benchmark row helpers expose provenance and classification metadata directly rather than re-inferring it late"
key-files:
  created:
    - benchmark_ui/test_resume_accounting.py
  modified:
    - benchmark_ui/runner.py
    - benchmark_ui/service.py
    - benchmark_ui/test_runner_events.py
key-decisions:
  - "Keep the operator-facing combined view, but derive live-only counts from explicit provenance rather than from the merged results list."
  - "Treat `skipped-host-runtime` and `host-runtime-required` as skips regardless of requirements resolution or wrapper return code."
patterns-established:
  - "Resumed historical rows should appear as `resultOrigin: historical` and never masquerade as new live case-complete events."
  - "Benchmark summary snapshots compute combined, historical-only, and live-only counts from the same normalized provenance helpers."
requirements-completed: [EVD-07, EVD-09, VAL-04]
duration: 11 min
completed: 2026-04-01
---

# Phase 19 Plan 02: Failure Classification and Run-Accounting Integrity Summary

**Benchmark accounting now keeps host-runtime validation outcomes as skips and separates resumed historical rows from live rows so current-run conclusions stay provenance-clean**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-01T18:32:00Z
- **Completed:** 2026-04-01T18:43:27Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Removed the benchmark-side skip-to-pass upgrade so `skipped-host-runtime` and `host-runtime-required` cases stay `SKIP` even when requirements exist and the wrapper exits zero.
- Changed resumed-run storage so historical rows live in `historical_results` while the current run appends only live rows to `results`.
- Added provenance-aware service helpers, new result fields (`failureFamily`, `failureBucket`, `skipCandidate`, `resultOrigin`), and regression tests that lock live-only versus combined accounting.

## Task Commits

Each task was committed atomically:

1. **Task 1: Remove host-runtime skip-to-pass reclassification** - `7f430c5` (feat)
2. **Task 2: Separate historical resume rows from live rows and expose provenance** - `7f430c5` (feat/test)

## Files Created/Modified

- `benchmark_ui/runner.py` - Stops flipping host-runtime skips into passes, records `failureFamily` and related metadata on live rows, and stores resumed rows in `historical_results`.
- `benchmark_ui/service.py` - Normalizes provenance-aware summary rows, exposes `resultOrigin` plus Phase 19 classification fields, and derives combined versus live-only counts without mixing resume history into live conclusions.
- `benchmark_ui/test_runner_events.py` - Locks that live case-complete events mark `resultOrigin: live`.
- `benchmark_ui/test_resume_accounting.py` - Adds regression coverage for host-runtime skip accounting and mixed historical/live resume summaries.

## Decisions Made

- Preserved a combined resumed-run view for operators, but made live-only analysis depend on explicit provenance rather than special-case filtering.
- Used direct APDR metadata when present and backward-compatible validation-status checks for older runs so saved history remains readable after the Phase 19 change.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 4 - Coupling] Accounting changes and their regression coverage landed in one atomic code commit**
- **Found during:** Plan closeout
- **Issue:** Skip-accounting fixes and provenance helpers both changed `benchmark_ui/runner.py` and `benchmark_ui/service.py`, so splitting them into separate non-interactive commits would have produced an artificial boundary.
- **Fix:** Kept one plan-scoped code commit and locked both behaviors with the new regression tests.
- **Files modified:** `benchmark_ui/runner.py`, `benchmark_ui/service.py`, `benchmark_ui/test_runner_events.py`, `benchmark_ui/test_resume_accounting.py`
- **Verification:** `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting`
- **Committed in:** `7f430c5`

**2. [Rule 1 - Minimal Surface] `benchmark_ui/test_run_contract.py` did not need a code edit**
- **Found during:** Task 2 (Separate historical resume rows from live rows and expose provenance)
- **Issue:** The plan listed `benchmark_ui/test_run_contract.py`, but the existing contract assertions continued to pass unchanged once the new provenance-specific tests were added.
- **Fix:** Left the file untouched and covered the new resume-specific behavior in `benchmark_ui/test_resume_accounting.py` plus the live-event assertion in `benchmark_ui/test_runner_events.py`.
- **Files modified:** `benchmark_ui/test_resume_accounting.py`, `benchmark_ui/test_runner_events.py`
- **Verification:** `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events benchmark_ui.test_resume_accounting`
- **Committed in:** `7f430c5`

---

**Total deviations:** 2 auto-fixed (1 coupling, 1 minimal-surface)
**Impact on plan:** No loss of coverage. The required contracts are now enforced in the benchmark-specific tests that actually exercise the changed accounting paths.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 03 can now prove live-only accounting deterministically by routing a mixed historical/live fixture through the actual benchmark snapshot helpers.
- Phase-close verification can rely on explicit provenance fields instead of inferring which resumed rows were historical.

## Self-Check: PASSED

- Found `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-02-SUMMARY.md`
- Found task commit `7f430c5`

---
*Phase: 19-failure-classification-and-run-accounting-integrity*
*Completed: 2026-04-01*
