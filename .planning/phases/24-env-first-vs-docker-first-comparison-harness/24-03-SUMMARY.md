---
phase: 24-env-first-vs-docker-first-comparison-harness
plan: 03
subsystem: docs
tags: [docs, benchmark, comparison, evidence, runbook]
requires:
  - phase: 24-02
    provides: "deterministic comparison checker, frozen sample artifacts, and delta metrics"
provides:
  - "An operator-facing runbook for probe-only extraction and paired live replay"
  - "A reviewer-facing proof note that marks the Phase 24 evidence boundary clearly"
  - "Explicit carry-forward of open Phase 23 human-verification debt into the comparison proof surface"
affects: [phase-25-docker-first-decision-closeout]
tech-stack:
  added: []
  patterns:
    - "Comparison proof docs state exactly what the harness proves and what the final verdict phase still owns"
    - "Runbooks keep contract parity inputs explicit so paired-policy replays do not drift silently"
key-files:
  created:
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-03-SUMMARY.md
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-PROOF.md
  modified: []
key-decisions:
  - "Keep the open Phase 23 browser-UAT debt visible in both the runbook and proof note so later closeout cannot overclaim readiness."
  - "State explicitly that Phase 24 proves the comparison harness and that Phase 25 owns the final keep/optional/reject recommendation."
patterns-established:
  - "Phase-close proof notes should cite the deterministic sample status and delta artifact directly instead of re-deriving numbers in prose."
  - "Runbooks for paired-policy evidence should call out which flags are held constant and which one is intentionally changed."
requirements-completed: [CMP-01, CMP-02]
duration: 8min
completed: 2026-04-02
---

# Phase 24 Plan 03: Runbook and Proof Summary

**Phase 24 now has the operator runbook and reviewer proof note needed to reuse the paired-policy comparison harness without blurring the line between harness proof and final policy verdict**

## Performance

- **Duration:** 8 min
- **Started:** 2026-04-02T06:59:00Z
- **Completed:** 2026-04-02T07:07:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added a runbook for deterministic probe-only extraction and paired live replay with explicit contract-parity rules.
- Added a proof note that states what Phase 24 proves, what it does not prove, and what Phase 25 still needs to decide.
- Carried the open Phase 23 human verification debt into the proof surface so milestone closeout stays honest.

## Task Commits

Each task was committed atomically:

1. **Task 1: Write the paired-policy runbook for probe and live execution** - `4c8a007` (`docs`)
2. **Task 2: Write the reviewer-facing proof note for the Phase 24 harness boundary** - `aa4cd3d` (`docs`)

## Files Created/Modified

- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md` - Documents probe-mode extraction, paired live replay, and contract parity requirements.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-PROOF.md` - States the exact evidence boundary for the Phase 24 comparison harness.

## Decisions Made

- Kept the runbook explicit about `validation_backend=llm` and `--llm-validation-policy` so future paired runs do not accidentally compare different runtime contracts.
- Treated Phase 23 browser UAT as active prerequisite debt rather than burying it in milestone closeout footnotes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added literal lowercase phrases required by the runbook acceptance grep**
- **Found during:** Final Wave 3 verification
- **Issue:** The runbook used title-cased section headings, but the plan verification grepped for literal lowercase `probe-only extraction` and `paired live replay`.
- **Fix:** Added explicit one-line sentences using the exact lowercase phrases while preserving the existing heading structure.
- **Files modified:** `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md`
- **Verification:** `rg -n 'probe-only extraction|paired live replay|Phase 23 human verification|Phase 25|comparison harness|timing delta' ...`
- **Committed in:** `4c8a007`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** The fix tightened documentation wording only and did not change behavior or scope.

## Issues Encountered

- The runbook acceptance grep required literal lowercase phrases for two section names.

## User Setup Required

None.

## Next Phase Readiness

- Phase 24 is ready for whole-phase verification.
- Phase 25 can cite the runbook and proof note directly when forming the final docker-first decision recommendation.

## Self-Check: PASSED

- Found `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-03-SUMMARY.md`
- Verified task commits `4c8a007` and `aa4cd3d` exist in git history

---
*Phase: 24-env-first-vs-docker-first-comparison-harness*
*Completed: 2026-04-02*
