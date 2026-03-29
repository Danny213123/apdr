---
phase: 14-macos-execution-path-optimization
plan: 01
subsystem: benchmark
tags: [replay-manifest, benchmark-slice, macOS, windows-guardrail, measurement]

# Dependency graph
requires:
  - phase: 13-measurement-and-run-contract-hardening
    provides: canonical run contracts, per-case timing metadata, measurement checker
provides:
  - manifest-backed replay-slice selection in benchmark_ui runner
  - locked macOS replay slice (20 cases) and Windows guardrail slice (10 cases)
  - manifest-aware baseline capture script via --manifest-json argument
  - replay_manifest and replay_slice_id persistence in benchmark summaries
affects: [14-02-PLAN, 14-03-PLAN, scripts/measure_apdr_baseline.py, benchmark_ui/runner.py]

# Tech tracking
tech-stack:
  added: []
  patterns: [replay-manifest JSON schema with slice_id, purpose, rationale, and ordered cases]

key-files:
  created:
    - .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json
    - .planning/phases/14-macos-execution-path-optimization/14-windows-guardrail-slice.json
  modified:
    - benchmark_ui/state.py
    - benchmark_ui/service.py
    - benchmark_ui/runner.py
    - benchmark_ui/test_runner_events.py
    - scripts/measure_apdr_baseline.py

key-decisions:
  - "Manifest cases reference fixture-relative paths rather than absolute paths for portability"
  - "Manifest loading validates structure (slice_id required, cases non-empty, each case needs relative_path)"
  - "When replay_manifest is set, snippet_limit is ignored to avoid conflicting boundary controls"
  - "Windows guardrail slice is a 10-case subset overlapping with the macOS replay slice for cross-platform parity"

patterns-established:
  - "Replay-manifest JSON schema: slice_id, purpose, rationale, created_at, phase, cases[{relative_path, reason}]"
  - "Manifest-backed snippet selection overrides snippet_limit for deterministic replay"

requirements-completed: [MAC-03, WIN-01]

# Metrics
duration: 5min
completed: 2026-03-29
---

# Phase 14 Plan 01: Lock Replay Boundary Summary

**Manifest-backed replay-slice support in benchmark_ui with locked macOS (20 cases) and Windows guardrail (10 cases) slice manifests, plus manifest-aware baseline capture in measure_apdr_baseline.py**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-29T05:09:50Z
- **Completed:** 2026-03-29T05:15:47Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Added replay_manifest field to benchmark run configuration, normalization, persistence, and loadout serialization
- Created load_replay_manifest() and filter_snippets_by_manifest() utilities for deterministic manifest-backed snippet ordering
- Created locked 20-case macOS replay slice and 10-case Windows guardrail slice as explicit JSON manifests
- Updated measure_apdr_baseline.py to accept --manifest-json and emit slice_id and manifest_json in generated reports
- Added 9 unit tests covering manifest loading, validation, filtering, and service integration

## Task Commits

Each task was committed atomically:

1. **Task 1: Add manifest-backed replay-slice selection and persistence** - `1022ece` (feat)
2. **Task 2: Create locked slice manifests and manifest-aware baseline script** - `e7837ec` (feat)

## Files Created/Modified
- `benchmark_ui/state.py` - Added replay_manifest to default_run_config
- `benchmark_ui/service.py` - Added replay_manifest normalization and form config persistence
- `benchmark_ui/runner.py` - Added load_replay_manifest(), filter_snippets_by_manifest(), manifest-backed snippet selection and summary persistence
- `benchmark_ui/test_runner_events.py` - Added 9 tests for TestReplayManifest class
- `.planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json` - 20-case locked macOS replay boundary
- `.planning/phases/14-macos-execution-path-optimization/14-windows-guardrail-slice.json` - 10-case Windows guardrail slice
- `scripts/measure_apdr_baseline.py` - Added --manifest-json argument, load_manifest_json(), collect_snippets_from_manifest(), slice_id/manifest_json in reports

## Decisions Made
- Manifest cases use fixture-relative paths for portability across machines and platforms
- When replay_manifest is provided, snippet_limit is ignored to prevent conflicting boundary controls
- Windows guardrail slice overlaps with macOS replay slice cases to enable cross-platform parity checks
- Manifest validation is strict: slice_id required, cases array must be non-empty, each entry needs relative_path

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed syntax error in render_markdown after manifest fields added**
- **Found during:** Task 2
- **Issue:** Using `lines.extend([` with the original closing `]` caused a mismatched parenthesis error
- **Fix:** Changed to `lines += [` which only needs `]` to close
- **Files modified:** scripts/measure_apdr_baseline.py
- **Verification:** py_compile passes, script runs successfully
- **Committed in:** e7837ec (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Auto-fix was necessary for the script to execute. No scope creep.

## Issues Encountered
None beyond the syntax fix above.

## Known Stubs
None - all manifest loading, filtering, and persistence paths are fully wired.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Replay boundary is locked and ready for Plan 14-02 to build the native macOS replay runner
- Both manifests reference fixture snippets that exist in the repo under tools/apdr/tests/fixtures/
- measure_apdr_baseline.py can now generate exact-slice captures for before/after comparison
- Plan 14-03 can use these manifests for regression checking

---
*Phase: 14-macos-execution-path-optimization*
*Completed: 2026-03-29*
