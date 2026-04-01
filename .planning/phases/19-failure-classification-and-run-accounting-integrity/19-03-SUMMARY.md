---
phase: 19-failure-classification-and-run-accounting-integrity
plan: 03
subsystem: proof
tags: [python, proof, benchmark-ui, fixtures, evidence]
requires:
  - phase: 19-01
    provides: failure-family artifact truth
  - phase: 19-02
    provides: provenance-aware resume accounting
provides:
  - "Frozen March 30 live-derived accounting slice for Phase 19 review"
  - "Deterministic mixed-provenance fixture and checker for live-only accounting"
  - "Machine-readable proof status plus reviewer-facing before/after note"
affects: [phase-19-verification, reviewer-evidence, milestone-closeout]
tech-stack:
  added: []
  patterns:
    - "Phase proof packages freeze real benchmark evidence plus a synthetic fixture that exercises the changed reader path deterministically"
    - "Probe-only proof scripts emit committed status JSON artifacts for reviewer inspection"
key-files:
  created:
    - .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json
    - .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json
    - .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json
    - .planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md
    - scripts/check_phase19_accounting.py
  modified: []
key-decisions:
  - "Anchor the proof to the March 30 wrapper summary and validate the frozen statuses and reasons directly against that source."
  - "Use the real `BenchmarkService` snapshot logic inside the proof checker so provenance counts match the live benchmark reader path."
patterns-established:
  - "Mixed-provenance proof fixtures carry both `historical_results` and `results` plus explicit expected combined/live-only counts."
  - "Reviewer proof notes must call out both the old failure mode and the post-fix conditions in a `Before/After Review` section."
requirements-completed: [VAL-04, EVD-07, EVD-09]
duration: 9 min
completed: 2026-04-01
---

# Phase 19 Plan 03: Failure Classification and Run-Accounting Integrity Summary

**Phase 19 now has a deterministic proof package that freezes the March 30 classification slice, validates mixed historical/live accounting through the real benchmark reader path, and records the result in a committed status artifact**

## Performance

- **Duration:** 9 min
- **Started:** 2026-04-01T18:35:00Z
- **Completed:** 2026-04-01T18:43:49Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Froze a four-case March 30 slice that locks two environment-specific skips and two dependency-resolution failures with their observed statuses and reasons.
- Added a mixed-provenance fixture that requires live-only counts to differ from the combined resumed view.
- Added `scripts/check_phase19_accounting.py`, generated `19-accounting-proof-status.json`, and wrote the reviewer-facing `19-ACCOUNTING-PROOF.md` note with the required before/after contract.

## Task Commits

Each task was committed atomically:

1. **Task 1: Freeze Phase 19 proof inputs from the March 30 evidence base** - `3a369d6` (feat)
2. **Task 2: Add the deterministic checker and reviewer-facing proof note** - `351e0cd` (feat)

## Files Created/Modified

- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json` - Freezes the four locked March 30 classification review cases with expected display status and failure-family semantics.
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json` - Defines a stable historical-plus-live accounting fixture whose live-only totals differ from the combined view.
- `scripts/check_phase19_accounting.py` - Validates the fixed slice against the March 30 source summary and runs the mixed-provenance fixture through `BenchmarkService` snapshot logic.
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json` - Records the probe-only checker result for both classification and provenance contracts.
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md` - Documents the proof surface and the required before/after reviewer expectations.

## Decisions Made

- Strengthened the proof checker by importing `BenchmarkService` so the provenance contract exercises the actual benchmark accounting code instead of a duplicate ad hoc counter.
- Froze the March 30 source reasons alongside the expected Phase 19 classification semantics so reviewers can see both the baseline evidence and the post-fix interpretation in one place.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The first checker run failed because the standalone script did not add the repo root to `sys.path` before importing `benchmark_ui`; adding that path bootstrap fixed the issue and the probe command then passed cleanly.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase-close verification can now reference a deterministic proof script and committed status artifact instead of relying on a fresh benchmark replay.
- Phase 20 can focus on improving dominant failure buckets without re-litigating whether the Phase 19 accounting and classification contracts are trustworthy.

## Self-Check: PASSED

- Found `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-03-SUMMARY.md`
- Found task commit `3a369d6`
- Found task commit `351e0cd`

---
*Phase: 19-failure-classification-and-run-accounting-integrity*
*Completed: 2026-04-01*
