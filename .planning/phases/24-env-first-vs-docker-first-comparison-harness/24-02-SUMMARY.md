---
phase: 24-env-first-vs-docker-first-comparison-harness
plan: 02
subsystem: benchmarking
tags: [python, benchmark, comparison, proof, timing]
requires:
  - phase: 24-01
    provides: "paired env-first and docker-first artifacts with matched llm contracts"
provides:
  - "A deterministic checker for Phase 24 contract parity, pass delta, dominant-bucket delta, and timing delta"
  - "Frozen env-first and docker-first sample artifacts with machine-checked comparison status"
  - "A reviewer-facing delta note that explains the sample-backed policy comparison boundary"
affects: [24-03, phase-25-docker-first-decision-closeout]
tech-stack:
  added: [scripts/check_phase24_policy_comparison.py]
  patterns:
    - "Comparison checkers validate contract parity before reporting deltas"
    - "Sample artifacts are frozen from harness output rather than retyped by hand"
key-files:
  created:
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-02-SUMMARY.md
    - scripts/check_phase24_policy_comparison.py
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md
  modified: []
key-decisions:
  - "Require a non-zero pass delta, at least one dominant-bucket delta, and at least one timing delta so the sample artifacts cannot pass as a no-op comparison."
  - "Keep the delta note explicit about deterministic sample artifacts so Phase 24 does not masquerade as the final policy verdict."
patterns-established:
  - "Paired comparison status files should include both per-policy summaries and delta blocks so later docs can cite one machine-readable source."
  - "Result origin parity is part of the contract; env-first and docker-first artifacts cannot compare mixed provenance silently."
requirements-completed: [CMP-01, CMP-02]
duration: 12min
completed: 2026-04-02
---

# Phase 24 Plan 02: Comparison Checker Summary

**Phase 24 now has a deterministic checker, frozen sample artifacts, and a reviewer-facing delta note for pass, dominant-bucket, and timing differences between env-first and docker-first llm policies**

## Performance

- **Duration:** 12 min
- **Started:** 2026-04-02T06:46:00Z
- **Completed:** 2026-04-02T06:58:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Added `scripts/check_phase24_policy_comparison.py`, which verifies contract parity and computes pass, bucket, and timing deltas.
- Froze env-first and docker-first sample artifacts directly from the harness output and captured the passing proof status JSON.
- Wrote a comparison delta note that translates the checker output into reviewer-readable numbers without overstating the sample boundary.

## Task Commits

Each task was committed atomically:

1. **Task 1: Freeze deterministic env-first and docker-first sample artifacts and build the comparison checker** - `ed44e32` (`feat`)
2. **Task 2: Write the reviewer-facing comparison delta note** - `6690a12` (`docs`)

## Files Created/Modified

- `scripts/check_phase24_policy_comparison.py` - Verifies contract parity and computes pass, bucket, and timing deltas from paired artifacts.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json` - Frozen env-first comparison artifact.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json` - Frozen docker-first comparison artifact.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json` - Stores the passing deterministic comparison status.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md` - Reviewer-facing pass, bucket, and timing delta note.

## Decisions Made

- Treated result provenance as part of parity validation so paired artifacts cannot quietly mix live and historical rows.
- Required the checker to reject zero-delta samples, which keeps the frozen artifacts meaningful for later proof and closeout docs.

## Deviations from Plan

None.

## Issues Encountered

None.

## User Setup Required

None for deterministic probe mode. Live replay remains Phase 24 runbook work.

## Next Phase Readiness

- Phase 24 plan 03 can now write the runbook and proof note directly against a stable checker and delta artifact.
- The deterministic comparison contract is ready for live paired replay once the runbook is in place.

## Self-Check: PASSED

- Found `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-02-SUMMARY.md`
- Verified task commits `ed44e32` and `6690a12` exist in git history

---
*Phase: 24-env-first-vs-docker-first-comparison-harness*
*Completed: 2026-04-02*
