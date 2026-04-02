---
phase: 22-docker-first-policy-and-safe-degradation
plan: 01
subsystem: infra
tags: [rust, python, docker, llm, validation]
requires:
  - phase: 21.1-repository-footprint-and-download-size-reduction
    provides: Smaller APDR workspace defaults and cleanup baseline for continued Docker-policy work
provides:
  - Normalized `llm_validation_policy` config kept separate from `validation_backend`
  - CLI and Python-wrapper pass-through for `docker-first` versus `env-first`
  - Docker-first `llm` route selection with env-first control and route regression tests
affects: [phase-23-policy-truth-and-failure-semantics, phase-24-env-first-vs-docker-first-comparison-harness]
tech-stack:
  added: []
  patterns: [policy-specific llm routing kept separate from backend names, route-selection helpers frozen by focused Rust regression tests]
key-files:
  created: []
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/main.rs
    - tools/apdr/test_executor.py
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
key-decisions:
  - "Kept docker-first versus env-first as a normalized `llm_validation_policy` field instead of widening `validation_backend`."
  - "Modeled llm first-hop selection as explicit route categories so env-first control, host-runtime pre-skip, and Docker-bypass fallback stay distinct."
patterns-established:
  - "LLM policy surface: requested backend remains `llm` while route policy is selected through a sibling config field."
  - "Routing guardrails: host-runtime markers and Docker unavailability force env-first handling before any Docker attempt."
requirements-completed: [DFV-01, DFV-03]
duration: 9min
completed: 2026-04-02
---

# Phase 22 Plan 01: Core Docker-First Policy Summary

**Docker-first `llm` policy selection with explicit env-first control, stable `validation_backend=llm`, and locked Rust routing tests**

## Performance

- **Duration:** 9 min
- **Started:** 2026-04-02T00:42:59Z
- **Completed:** 2026-04-02T00:51:43Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Added normalized `llm_validation_policy` handling in Rust config so docker-first and env-first live inside `llm` mode without changing the public backend name.
- Exposed `--llm-validation-policy` in the Rust CLI and Python wrapper, with docker-first as the default operator path and env-first preserved as an explicit control.
- Switched `llm` route selection to prefer Docker first, while keeping host-runtime cases and Docker-bypass situations on an env-first branch and freezing those boundaries with `phase22_policy_*` tests.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add explicit `llm` policy config and wrapper pass-through** - `ebd3810` (`feat`)
2. **Task 2: Flip the `llm` first hop to docker-first and lock the policy boundaries** - `e787cff` (`feat`)

## Files Created/Modified

- `tools/apdr/src/lib.rs` - Added normalized llm policy constants, config field, and accessor.
- `tools/apdr/src/main.rs` - Added CLI parsing/help for `--llm-validation-policy` and included the policy in resolve context logging.
- `tools/apdr/test_executor.py` - Passed the selected llm validation policy through the Python compatibility wrapper.
- `tools/apdr/src/docker/builder/agent_backend.rs` - Added llm route selection for docker-first, env-first control, host-runtime env preference, and Docker-bypass env fallback.
- `tools/apdr/src/docker/builder/mod.rs` - Added `phase22_policy_*` regression tests covering the new policy contract.

## Decisions Made

- Kept the policy seam narrow: `validation_backend` stays `llm`, and the first-hop choice is expressed only through `llm_validation_policy`.
- Preserved the old env-first comparison path instead of making env fallback implicit, so later comparison phases can still run the previous route on demand.
- Treated host-runtime markers and missing Docker availability as route-selection inputs before attempting Docker, so docker-first does not override known unsuitable cases.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Parallel `git add` operations briefly collided on `.git/index.lock`; resolved by retrying the remaining stage step serially. No repo content changes were required.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 23 can now surface requested llm policy, actual route truth, and bypass reasons on top of a stable docker-first routing contract.
- Phase 24 can compare docker-first versus env-first using the explicit control surface added in this plan.

## Self-Check: PASSED

- Found summary file: `.planning/phases/22-docker-first-policy-and-safe-degradation/22-01-SUMMARY.md`
- Found task commit: `ebd3810`
- Found task commit: `e787cff`

---
*Phase: 22-docker-first-policy-and-safe-degradation*
*Completed: 2026-04-02*
