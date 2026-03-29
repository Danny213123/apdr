---
phase: 13-measurement-and-run-contract-hardening
plan: 01
subsystem: benchmark-ui
tags: [run-contract, benchmark-metadata, historical-runs, ui, unittest]

requires:
  - phase: 13-measurement-and-run-contract-hardening
    provides: Phase 13 roadmap scope for MAC-01, EVD-03, and EVD-05
provides:
  - canonical benchmark run-contract helper in benchmark_ui/run_contract.py
  - run-config defaults and normalization for run intent, cache state, context window, inference policy, and build profile
  - run-start persistence of run_contract.json and nested summary.json metadata
  - historical-run hydration and info-field rendering from saved run_contract data
  - focused unit coverage for run-contract construction, persistence, and historical rendering
affects: [13-02, 13-03, benchmark-ui-reporting]

tech-stack:
  added: []
  patterns: [canonical run-contract helper, summary-backed historical hydration, explicit metadata defaults]

key-files:
  created:
    - benchmark_ui/run_contract.py
    - benchmark_ui/test_run_contract.py
  modified:
    - benchmark_ui/state.py
    - benchmark_ui/runner.py
    - benchmark_ui/service.py

key-decisions:
  - "Store comparison metadata inside a nested run_contract object and a sibling run_contract.json file instead of scattering fields only at summary top level"
  - "Use config-backed defaults for run intent, cache state, context window, inference policy, and build profile so preview and historical rendering stay aligned"
  - "Prefer saved run_contract values during historical hydration so later phases compare exactly what was recorded at run time"

patterns-established:
  - "benchmark_ui/run_contract.py is the canonical source for required Phase 13 run metadata"
  - "Historical run rendering merges stored run_contract metadata back into config normalization instead of recomputing labels from scratch"

requirements-completed: [MAC-01, EVD-03, EVD-05]

duration: 20min
completed: 2026-03-29
---

# Phase 13 Plan 01: Canonical Run Contract Summary

**Canonical benchmark run-contract capture in `benchmark_ui`, persisted with each run and reused by historical run hydration/UI rendering**

## Performance

- **Duration:** 20 min
- **Started:** 2026-03-29T03:44:00Z
- **Completed:** 2026-03-29T04:04:27Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Created `benchmark_ui/run_contract.py` with the Phase 13 required-key list, execution-mode/cache normalization, and runtime architecture detection helpers
- Extended benchmark run defaults/normalization to carry `run_intent`, `cache_state`, `llm_context_window`, `inference_policy`, and `build_profile`
- Updated `BenchmarkWorker` to build a run contract at run start, validate required keys, persist `run_contract.json`, and store the nested contract in `summary.json`
- Updated `BenchmarkService` to preserve run-contract metadata when loading saved runs and to render explicit `Model`, `Run intent`, `Execution mode`, `Cache state`, `Ctx window`, `Inference`, and `Build profile` fields
- Added `benchmark_ui/test_run_contract.py` and passed targeted benchmark UI unit coverage

## Task Commits

Implementation landed in one atomic commit because `service.py` carried shared normalization and historical-rendering logic for both planned tasks:

1. **Plan 13-01 implementation** - `987c6fd` (feat)

## Files Created/Modified
- `benchmark_ui/run_contract.py` - Canonical run-contract helper, required keys, normalization, and architecture detection
- `benchmark_ui/state.py` - Run-config defaults for Phase 13 metadata inputs
- `benchmark_ui/runner.py` - Run-start contract construction and `run_contract.json` persistence
- `benchmark_ui/service.py` - Summary normalization, historical hydration, and run-contract UI fields
- `benchmark_ui/test_run_contract.py` - Unit tests for build/persist/render seams

## Decisions Made
- Kept the contract nested under `summary["run_contract"]` while also writing `run_contract.json` so future scripts can read either the summary or the standalone artifact
- Treated `cache_state` as an explicit normalized value with a safe default of `unknown` instead of pretending it can always be inferred at run start
- Made historical run rendering prefer saved run-contract values over recomputed defaults so later benchmark comparisons stay attributable

## Deviations from Plan

- The two planned tasks were committed together in `987c6fd` rather than as separate code commits because the `service.py` normalization and info-field changes formed one tightly coupled surface. The work still stayed bounded to the Plan 13-01 files and verification targets.

## Issues Encountered

- The `gsd-executor` subagent did not produce any filesystem activity or completion signal in this runtime, so Plan 13-01 was executed inline as the workflow fallback.

## User Setup Required

None.

## Next Phase Readiness
- Plan 13-02 can now propagate the canonical run contract into APDR per-case artifacts instead of inventing a second metadata schema
- Plan 13-03 can read the same contract from saved runs when normalizing reporting output
- The targeted benchmark UI tests passed with the new run-contract surface

## Self-Check: PASSED

- FOUND: benchmark_ui/run_contract.py
- FOUND: benchmark_ui/test_run_contract.py
- FOUND: benchmark_ui/runner.py writes run_contract.json
- FOUND: benchmark_ui/service.py renders Run intent / Execution mode / Cache state / Ctx window / Build profile
- PASSED: `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_service_tier_stats benchmark_ui.test_runner_events`
- FOUND: 987c6fd

---
*Phase: 13-measurement-and-run-contract-hardening*
*Completed: 2026-03-29*
