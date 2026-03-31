---
phase: 18-backend-escalation-and-path-truth
plan: 01
subsystem: validation
tags: [rust, docker, llm-routing, validation, testing]
requires: []
provides:
  - "Deterministic `env -> docker -> llm-agent` routing for eligible llm validation failures"
  - "A Docker-only middle-hop entrypoint that avoids recursive agent fallback"
  - "Regression coverage for llm Docker eligibility boundaries and route ordering"
affects: [phase-18-plan-02, phase-18-plan-03, benchmark-ui, apdr-artifacts]
tech-stack:
  added: []
  patterns:
    - "Requested llm mode keeps top-level llm backend semantics while attempts preserve actual env and docker hops"
    - "Docker middle-hop entrypoints must stay deterministic when reused from llm validation"
key-files:
  created: []
  modified:
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/docker_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
key-decisions:
  - "Keep llm validation env-first and add Docker only as a targeted middle-hop before final agent fallback."
  - "Do not reuse the existing Docker backend unchanged from llm mode because Docker mode can recursively enter the agent path when allow_llm is enabled."
patterns-established:
  - "Eligibility checks for llm Docker escalation live next to the llm routing seam in agent_backend.rs."
  - "Phase-prefixed Rust tests in docker::builder::tests lock routing order and guardrail cases."
requirements-completed: [VAL-01, WIN-02]
duration: 9 min
completed: 2026-03-31
---

# Phase 18 Plan 01: Backend Escalation and Path Truth Summary

**Eligible `llm` env failures now try a deterministic Docker middle hop before the final LangGraph fallback, and the routing guardrails are locked with focused Rust tests**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-31T02:34:00Z
- **Completed:** 2026-03-31T02:43:26Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Added `llm_env_failure_requires_docker_escalation(...)` so `llm` mode only escalates backend-recoverable env failures such as missing interpreters, build timeouts, system dependency build failures, and version-not-found style pip errors.
- Added `validate_requirements_docker_deterministic(...)` so the `llm` route can call Docker as a true middle hop without recursively invoking the LangGraph agent from inside Docker mode.
- Changed `validate_requirements_llm(...)` to merge env and Docker attempt history before any final agent fallback, preserve `llm` as the top-level backend mode, and record Docker as an escalated backend when that hop is used.
- Added four Phase 18 Rust tests covering interpreter eligibility, build-timeout eligibility, host-runtime exclusion, and the required `env -> docker -> llm-agent` ordering.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add targeted Docker escalation to the `llm` route** - `0d3378f` (feat)
2. **Task 2: Lock targeted eligibility and platform correctness with Rust tests** - `9f7ec43` (test)

## Files Created/Modified

- `tools/apdr/src/docker/builder/agent_backend.rs` - Adds the llm Docker-eligibility helper, routes eligible failures through deterministic Docker first, and preserves prior validation context before final agent fallback.
- `tools/apdr/src/docker/builder/docker_backend.rs` - Factors Docker validation into a shared inner function and exposes a Docker-only deterministic entrypoint for llm routing.
- `tools/apdr/src/docker/builder/mod.rs` - Adds the Phase 18 Rust regression tests for eligibility boundaries and backend route order.

## Decisions Made

- Kept the targeted escalation decision close to the llm route so Phase 18 can distinguish backend-recoverable failures from host-runtime skips without broadening pure env-mode retry policy.
- Preserved pure Docker behavior by introducing a Docker-only helper instead of rewriting Docker mode around llm-specific assumptions.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] New llm routing code initially missed the Docker helper imports**
- **Found during:** Task 1 (Add targeted Docker escalation to the `llm` route)
- **Issue:** The first compile pass failed because `agent_backend.rs` referenced the new deterministic Docker helper and Docker retry predicates without importing them.
- **Fix:** Added the missing Docker-backend imports and reran the targeted Rust slice.
- **Files modified:** `tools/apdr/src/docker/builder/agent_backend.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_`
- **Committed in:** `0d3378f`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** No scope change. The fix only completed the planned routing seam so the new tests could compile and pass.

## Issues Encountered

- Targeted cargo verification still emits the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; those warnings were out of scope for this plan and did not affect Phase 18 routing verification.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 02 can now serialize truthful backend-path metadata on top of stable env-plus-docker-plus-llm attempt history.
- Plan 03 can build operator-facing wording and proof assets around a deterministic llm routing core rather than a hypothetical path contract.

## Self-Check: PASSED

- Found `.planning/phases/18-backend-escalation-and-path-truth/18-01-SUMMARY.md`
- Found task commit `0d3378f`
- Found task commit `9f7ec43`

---
*Phase: 18-backend-escalation-and-path-truth*
*Completed: 2026-03-31*
