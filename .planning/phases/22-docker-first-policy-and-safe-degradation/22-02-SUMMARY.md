---
phase: 22-docker-first-policy-and-safe-degradation
plan: 02
subsystem: ui
tags: [benchmark-ui, apdr, docker-first, llm, doctor]
requires:
  - phase: 22-01
    provides: Core llm_validation_policy routing and CLI flag support
provides:
  - Benchmark configs, loadouts, and run contracts carry llm_validation_policy separately from validation_backend
  - APDR benchmark UI exposes docker-first and env-first llm control with docker-first as the default
  - Doctor and runtime copy describe llm as docker-first with explicit env degradation instead of targeted escalation wording
affects: [22-03, phase-23-policy-truth, benchmark-ui]
tech-stack:
  added: []
  patterns:
    - Separate requested backend from llm policy selection in benchmark configs, summaries, and UI payloads
    - Treat Docker availability in llm mode as a degradable capability when env fallback exists
key-files:
  created:
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-02-SUMMARY.md
  modified:
    - benchmark_ui/run_contract.py
    - benchmark_ui/runner.py
    - benchmark_ui/service.py
    - benchmark_ui/state.py
    - benchmark_ui/test_run_contract.py
    - benchmark_ui/test_state_backend_doctor.py
    - web/index.html
    - web/src/main.js
key-decisions:
  - "Keep llm_validation_policy normalized as docker-first or env-first while validation_backend remains llm."
  - "Surface the selected llm policy in preview and saved-run info fields instead of widening the backend name."
  - "Rewrite Doctor copy around docker-first degradation to env validation instead of targeted Docker escalation."
patterns-established:
  - "Benchmark policy fields must round-trip through service normalization, run contracts, summaries, and loadouts together."
  - "Doctor messaging for optional Docker in llm mode should warn about degradation to env validation rather than implying a backend swap."
requirements-completed: [DFV-01, DFV-03, GDR-01]
duration: 4min
completed: 2026-04-02
---

# Phase 22 Plan 02: Docker-First Operator Policy Summary

**Benchmark UI llm policy control with docker-first default, env-first comparison path, and Doctor/runtime degradation wording**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-01T21:02:57-04:00
- **Completed:** 2026-04-02T01:06:40Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments

- Added `llm_validation_policy` to benchmark config normalization, run contracts, summaries, loadouts, and runner invocation while keeping `validation_backend=llm`.
- Added a dedicated APDR `llm` policy control in the web UI that defaults to `docker-first` and preserves `env-first` as the explicit comparison path.
- Rewrote Doctor and run-summary wording so `llm` now reads as docker-first with env fallback and agent fallback, with tests locking the degradation copy.

## Task Commits

1. **Task 1: Add benchmark run-config and UI support for `llm` policy selection** - `211f55e` (`feat`)
2. **Task 2: Update Doctor and runtime messaging for docker-first safe degradation** - `454f416` (`feat`)

## Files Created/Modified

- `benchmark_ui/run_contract.py` - Normalizes and persists `llm_validation_policy` inside the benchmark run contract.
- `benchmark_ui/runner.py` - Carries the selected llm policy into summaries and passes `--llm-validation-policy` to `test_executor.py`.
- `benchmark_ui/service.py` - Normalizes the policy in web payloads, restores it from saved runs, and surfaces it in operator-facing info fields.
- `benchmark_ui/state.py` - Sets the default policy and rewrites APDR `llm` Doctor/backend wording around docker-first degradation.
- `benchmark_ui/test_run_contract.py` - Covers default policy selection, runner pass-through, and stable `validation_backend=llm` history loading.
- `benchmark_ui/test_state_backend_doctor.py` - Locks the new Doctor warning and summary wording for docker-first degradation.
- `web/index.html` - Adds the dedicated `llm` policy control markup.
- `web/src/main.js` - Shows the policy selector only for APDR `llm`, defaults it to docker-first, and round-trips it through preview and loadout payloads.

## Decisions Made

- Kept `llm_validation_policy` as a separate normalized field rather than widening `validation_backend`, preserving the existing `llm` backend contract.
- Surfaced selected policy truth in preview and saved-run info fields so operators can see docker-first versus env-first without touching Phase 23 case-row work.
- Treated missing or unhealthy Docker in `llm` mode as a warning-level degradation to env validation, matching the safe-fallback requirement.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase `22-03` can now rely on a stable `llm_validation_policy` field across UI, summaries, and run contracts.
- Phase 23 can build richer per-case Docker truth surfaces on top of the new operator control and degradation wording without reopening backend naming.

## Self-Check: PASSED

- Verified summary exists at `.planning/phases/22-docker-first-policy-and-safe-degradation/22-02-SUMMARY.md`.
- Verified task commits `211f55e` and `454f416` exist in git history.
