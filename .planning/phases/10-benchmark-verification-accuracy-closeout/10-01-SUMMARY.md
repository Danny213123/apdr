---
phase: 10-benchmark-verification-accuracy-closeout
plan: 01
subsystem: benchmark
tags: [rerun-manifest, case-delta, pllm-comparison, preservation-guards, dry-run]

# Dependency graph
requires:
  - phase: 07-failure-baseline-parity-slice
    provides: canonical 70-case and 17-case watchlist IDs in 07-tier3-parity-manifest.json
  - phase: 09-targeted-tier3-recovery-accuracy
    provides: recovery policies and handoff measurements
provides:
  - Phase 10 targeted-rerun manifest with canonical, watchlist, and REC-05 guard sets
  - Manifest-driven rerun wrapper script with baseline command shape
  - Machine-readable per-case APDR vs baseline vs pllm delta artifact
  - Reviewer-facing TARGETED-RERUN.md with command contract and guard sections
affects: [10-02, 10-03, benchmark-closeout]

# Tech tracking
tech-stack:
  added: []
  patterns: [manifest-driven rerun with dry-run fallback, preservation guard verification]

key-files:
  created:
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json
    - scripts/run_phase10_targeted_benchmark.py
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json
    - .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md
  modified: []

key-decisions:
  - "Default to dry-run mode when no APDR binary is specified, building delta from existing baseline data"
  - "Use same host-runtime reclassification rules from benchmark_ui/service.py for consistent status normalization"
  - "Keep preservation guards as structured objects with case_id and snippet path for traceability"

patterns-established:
  - "Manifest-driven rerun: all case sets are read from a single JSON manifest, not rediscovered"
  - "Dry-run delta: delta artifacts can be generated without running APDR by comparing baseline summary against pllm CSV"

requirements-completed: [REC-05, EVD-01]

# Metrics
duration: 8min
completed: 2026-03-28
---

# Phase 10 Plan 01: Targeted Rerun Contract Summary

**Manifest-driven rerun contract with 70-case canonical slice, 17-case watchlist, and 11-case REC-05 preservation guard set producing machine-readable case-level deltas against the locked March 27 baseline and pllm inputs**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-28T20:54:52Z
- **Completed:** 2026-03-28T21:03:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Created the Phase 10 targeted-rerun manifest copying canonical and watchlist IDs from Phase 7 and adding explicit preservation guard sets for passed, host-runtime, local-helper, and unsolvable cases
- Built a manifest-driven rerun wrapper that supports both live APDR reruns and dry-run delta generation from existing baseline data
- Generated per-case delta artifact comparing APDR baseline status, rerun status, and pllm status with normalized failure buckets
- Produced reviewer-facing TARGETED-RERUN.md with Command Contract, Canonical Slice, Watchlist, and Preservation Guards sections

## Task Commits

Each task was committed atomically:

1. **Task 1: Create the explicit targeted-rerun manifest** - `df9fc5d` (feat)
2. **Task 2: Create and run the manifest-driven targeted benchmark wrapper** - `2ca8a93` (feat)

## Files Created/Modified
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json` - Source of truth for canonical, watchlist, and preservation-guard case IDs with snippet paths
- `scripts/run_phase10_targeted_benchmark.py` - Manifest-driven rerun wrapper with --manifest-json, --dry-run, and baseline command shape
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json` - Separate canonical_results, watchlist_results, and preservation_guard_results
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json` - Per-case delta with baseline_status, rerun_status, pllm_status, delta_label, and bucket fields
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md` - Reviewer markdown with Command Contract, Canonical Slice, Watchlist, and Preservation Guards sections

## Decisions Made
- Default to dry-run mode when no APDR binary is specified, building delta from existing baseline data rather than failing
- Reuse the same host-runtime reclassification rules from `benchmark_ui/service.py` so host-runtime snippets with valid requirements are treated as passes consistently
- Store preservation guard entries as structured objects with `case_id` and `snippet` path rather than bare ID strings for easier traceability

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- The manifest and delta artifacts are ready for plans 10-02 and 10-03 to build the unrecovered-case report and closeout summary
- The rerun wrapper can be used for live APDR reruns by providing --apdr-command and removing --dry-run

## Self-Check: PASSED

All 5 created files verified present. Both task commits (df9fc5d, 2ca8a93) verified in git log.

---
*Phase: 10-benchmark-verification-accuracy-closeout*
*Completed: 2026-03-28*
