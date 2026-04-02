---
phase: 23-policy-truth-and-failure-semantics
plan: 02
subsystem: ui
tags: [benchmark-ui, llm-validation, docker-first, rust, python, vanilla-js]
requires:
  - phase: 23-01
    provides: Saved-run and live benchmark rows already carry the policy-truth metadata this plan surfaces.
provides:
  - Expanded LLM case detail panels with reviewer-visible validation truth.
  - Stable dockerStatus labels derived from existing route and path metadata.
  - Docker-first classifier regressions that keep runtime blockers environment-specific.
affects: [benchmark-ui, policy-truth-proof, docker-first-evaluation]
tech-stack:
  added: []
  patterns: [Expanded case-detail truth panels, route-driven docker status labels, classifier guards on docker-first route metadata]
key-files:
  created: [.planning/phases/23-policy-truth-and-failure-semantics/23-02-SUMMARY.md]
  modified:
    - benchmark_ui/service.py
    - benchmark_ui/test_run_contract.py
    - benchmark_ui/test_runner_events.py
    - web/src/main.js
    - tools/apdr/src/docker/builder/mod.rs
    - tools/apdr/src/resolver/recovery_diagnostics.rs
key-decisions:
  - "Derived dockerStatus from existing route, bypass, and validation path truth instead of adding a new backend taxonomy."
  - "Classified only docker-first host-runtime and Docker-unavailable markers as environment-specific so true package misses remain dependency-resolution."
patterns-established:
  - "LLM policy truth belongs in expanded case details, not benchmark table columns."
  - "Docker-first failure-family guards should key off route metadata and exact bypass reasons before falling back to generic dependency-resolution."
requirements-completed: [DFV-02, GDR-02]
duration: 14 min
completed: 2026-04-02
---

# Phase 23 Plan 02: Policy Truth and Failure Semantics Summary

**Expanded LLM case truth panels with stable docker participation labels and docker-first environment-specific regression guards**

## Performance

- **Duration:** 14 min
- **Started:** 2026-04-02T03:21:00Z
- **Completed:** 2026-04-02T03:34:58Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Added `dockerStatus` derivation in the benchmark service so saved and live case rows can describe Docker participation as `attempted`, `env-first control`, `host-runtime pre-skip`, or `bypassed`.
- Extended the expanded LLM case detail panel with a `Validation truth` card showing requested policy, validation path, route, Docker status and bypass context, failure family, result origin, and debug pointers without changing table columns.
- Locked docker-first runtime blockers behind `phase23_truth_` Rust tests so host-runtime pre-skips, framework-runtime markers, and Docker-unavailable bypasses stay `environment-specific` while true package misses remain `dependency-resolution`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add an expanded LLM case truth section and Docker participation label in the UI** - `22f9b40` (`feat`)
2. **Task 2: Freeze environment-specific classification for docker-first runtime blockers** - `fc4623c` (`fix`)

## Files Created/Modified
- `benchmark_ui/service.py` - derives `dockerStatus` from route, bypass, and validation-path truth.
- `benchmark_ui/test_run_contract.py` - covers exact docker status labels for routed LLM case rows.
- `benchmark_ui/test_runner_events.py` - confirms live rows expose the new Docker participation truth.
- `web/src/main.js` - renders the additive `Validation truth` section inside expanded LLM case details.
- `tools/apdr/src/resolver/recovery_diagnostics.rs` - classifies docker-first host-runtime and Docker bypass markers as environment-specific and preserves dependency-resolution misses.
- `tools/apdr/src/docker/builder/mod.rs` - adds route-level regression tests for host-runtime pre-skip and daemon-unavailable bypass metadata.

## Decisions Made

- Reused the Phase 22 route and bypass taxonomy directly in the benchmark layer instead of inventing a parallel UI-only label system.
- Kept the UI change confined to expanded LLM case details so existing benchmark tables and `validation_backend` semantics stayed unchanged.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Repaired plan-metric state tracking manually**
- **Found during:** Plan metadata updates
- **Issue:** `gsd-tools state record-metric` reported `Performance Metrics section not found in STATE.md` even though the section existed, so the execution metric for plan `23-02` was not recorded.
- **Fix:** Added the Phase 23 plan 02 metric entry and refreshed the stale activity text directly in `.planning/STATE.md`.
- **Files modified:** `.planning/STATE.md`
- **Verification:** Confirmed the metric row and last-activity description now reflect plan `23-02`.
- **Committed in:** final metadata commit

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Metadata-only repair. Product code and verification scope remained unchanged.

## Issues Encountered

- A new Rust regression test initially referenced the Docker backend constant without qualification; it was corrected inline and the targeted cargo suite was rerun successfully.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- The benchmark UI now exposes the truth fields Phase 23 proof work needs for reviewer inspection.
- Docker-first runtime classification is frozen by targeted regressions, so the proof slice in the next plan can rely on stable failure-family semantics.

## Self-Check: PASSED

- Found summary file: `.planning/phases/23-policy-truth-and-failure-semantics/23-02-SUMMARY.md`
- Found task commit: `22f9b40`
- Found task commit: `fc4623c`
