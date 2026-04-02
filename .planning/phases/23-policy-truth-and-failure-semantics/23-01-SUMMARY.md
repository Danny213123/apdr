---
phase: 23-policy-truth-and-failure-semantics
plan: 01
subsystem: ui
tags: [python, benchmark-ui, sse, llm, validation-truth]
requires:
  - phase: 22-docker-first-policy-and-safe-degradation
    provides: "requested llm policy, route label, bypass reason, bypass note, and debug directory metadata in APDR outputs"
provides:
  - "Saved benchmark case rows expose requested LLM policy, route label, Docker bypass reason, bypass note, and debug directory pointers"
  - "Live benchmark results and case_complete SSE events expose the same policy-truth keys as saved rows"
  - "Regression tests lock the additive truth surface while keeping validationBackend and validationPath distinct"
affects: [23-02, 24-env-first-vs-docker-first-comparison-harness, benchmark-ui]
tech-stack:
  added: []
  patterns: ["Shared camelCase policy-truth keys across saved rows and live SSE payloads", "Additive truth surfacing without widening validation backend semantics"]
key-files:
  created: [.planning/phases/23-policy-truth-and-failure-semantics/23-01-SUMMARY.md]
  modified: [benchmark_ui/service.py, benchmark_ui/runner.py, benchmark_ui/test_run_contract.py, benchmark_ui/test_runner_events.py]
key-decisions:
  - "Keep requested policy and route metadata additive so validationBackend and validationPath remain the actual execution truth."
  - "Use the same camelCase policy-truth keys for saved rows and live events to avoid another inspection-schema fork."
patterns-established:
  - "Benchmark truth fields should read direct live-result keys first, then fall back to APDR output_metadata for historical rows."
  - "Runner SSE payloads should mirror the saved-row truth surface whenever APDR metadata already exists."
requirements-completed: [DFV-02]
duration: 7min
completed: 2026-04-02
---

# Phase 23 Plan 01: Policy Truth and Failure Semantics Summary

**Saved and live LLM case inspection now exposes requested policy, route labels, Docker bypass reason/note, and debug directories without widening validation backend semantics**

## Performance

- **Duration:** 7 min
- **Started:** 2026-04-02T03:13:36Z
- **Completed:** 2026-04-02T03:20:10Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Added saved-run case-row readers for `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, `docker_bypass_note`, and `debug_dir`.
- Threaded the same truth fields through live runner results and `case_complete` SSE events using stable camelCase keys.
- Locked the saved/live truth surface with regression tests while preserving the separate meanings of `validationBackend`, `validationPath`, and requested policy metadata.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add saved-run case-row readers for policy-truth and Docker debug pointers** - `9250d48` (feat)
2. **Task 2: Extend live result payloads and SSE events with the same truth fields** - `290993a` (feat)

## Files Created/Modified
- `benchmark_ui/service.py` - Adds policy-truth reader helpers and exposes additive truth fields on saved and historical case rows.
- `benchmark_ui/runner.py` - Copies policy-truth metadata into live result dicts and `case_complete` SSE payloads.
- `benchmark_ui/test_run_contract.py` - Verifies saved-case rows expose the new truth keys without collapsing them into backend/path fields.
- `benchmark_ui/test_runner_events.py` - Verifies live results and emitted events expose the same truth surface as saved rows.

## Decisions Made

- Kept `validationBackend`, `validationPath`, and `escalatedBackend` unchanged so requested policy and route label stay separate from the actual backend path.
- Reused the exact camelCase keys across saved rows and live events so the next UI phase can render one truth contract instead of reconciling two related schemas.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Narrowed the runner test subprocess mock scope**
- **Found during:** Task 2 (Extend live result payloads and SSE events with the same truth fields)
- **Issue:** Mocking `benchmark_ui.runner.subprocess.Popen` for the full test method also intercepted `subprocess.run` during `BenchmarkService` initialization and broke the test harness.
- **Fix:** Scoped the `Popen` patch to the `_run_single(...)` call so the runner process is mocked without disturbing `BenchmarkService` setup.
- **Files modified:** `benchmark_ui/test_runner_events.py`
- **Verification:** `python3 -m unittest benchmark_ui.test_runner_events`
- **Committed in:** `290993a` (part of task commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** The fix was limited to the new regression test harness and did not change product behavior or scope.

## Issues Encountered

- The first live-event regression test mocked subprocess creation too broadly; narrowing the patch resolved it cleanly.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 23 plan 02 can now build the UI `Validation truth` section against a stable saved/live field set.
- No blockers from plan 01 remain; the policy-truth vocabulary is aligned across historical rows and live events.

## Self-Check: PASSED

- Found `.planning/phases/23-policy-truth-and-failure-semantics/23-01-SUMMARY.md`
- Verified task commits `9250d48` and `290993a` exist in git history

---
*Phase: 23-policy-truth-and-failure-semantics*
*Completed: 2026-04-02*
