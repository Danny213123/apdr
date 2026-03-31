---
phase: 17-llm-fallback-stability-and-outcome-tracing
plan: 01
subsystem: validation
tags: [langgraph, llm-fallback, rust, python, testing]
requires: []
provides:
  - "LangGraph confidence_check wiring that no longer collides with the confidence state field"
  - "Structured llm fallback summaries for passed, abstained, and failed agent outcomes"
  - "Regression coverage for failed and abstained agent parsing plus env-attempt retention"
affects: [phase-18-backend-escalation-and-path-truth, phase-19-failure-classification-and-run-accounting-integrity, benchmark-artifacts]
tech-stack:
  added: []
  patterns:
    - "Env-first llm fallback appends a synthetic llm ValidationAttempt after env attempts"
    - "LangGraph node names must not reuse TypedDict state-field names"
key-files:
  created: []
  modified:
    - tools/apdr/docker_agent/graph.py
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
key-decisions:
  - "Keep llm validation env-first and record the terminal agent outcome as a synthetic llm attempt instead of collapsing back to env-only metadata."
  - "Preserve env failure context on non-pass agent summaries so later artifact work can expose both the original validation failure and the fallback terminal state."
patterns-established:
  - "Fallback outcome parsing accepts passed, abstained, and failed as first-class terminal agent statuses."
  - "Regression tests for fallback semantics live in docker::builder::tests with a phase-prefixed name."
requirements-completed: [AGT-07, AGT-08]
duration: 2 min
completed: 2026-03-31
---

# Phase 17 Plan 01: LLM Fallback Stability and Outcome Tracing Summary

**LangGraph fallback no longer crashes on the `confidence` node name, and non-pass agent outcomes now survive as structured llm summaries with retained env-attempt history**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-31T00:21:30Z
- **Completed:** 2026-03-31T00:23:33Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Renamed the LangGraph entry node to `confidence_check` so the graph no longer collides with the `confidence` state key.
- Changed Rust fallback parsing to keep `passed`, `abstained`, and `failed` agent results as `ValidationSummary` values with a synthetic `llm` attempt.
- Added focused Rust regression tests that lock failed and abstained parsing plus env-attempt ordering before the terminal llm attempt.

## Task Commits

Each task was committed atomically:

1. **Task 1: Remove the `confidence` collision and keep structured non-pass agent results** - `1fcd6df` (fix)
2. **Task 2: Add Phase 17 Rust regression tests for fallback outcome retention** - `4503392` (test)

## Files Created/Modified
- `tools/apdr/docker_agent/graph.py` - Renames the confidence node wiring to `confidence_check`.
- `tools/apdr/src/docker/builder/agent_backend.rs` - Preserves non-pass agent outcomes, appends a synthetic llm attempt, and keeps env failure context on the returned summary.
- `tools/apdr/src/docker/builder/mod.rs` - Adds the Phase 17 regression tests for agent parsing and env-attempt retention.

## Decisions Made
- Kept the existing env-first `llm` flow and fixed the post-env fallback seam instead of redesigning backend routing in Phase 17.
- Used the milestone vocabulary directly in Rust summary parsing so later artifact work can surface `passed`, `abstained`, and `failed` without inferring them from logs.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Preserve env failure context alongside the terminal llm fallback result**
- **Found during:** Task 1 (Remove the `confidence` collision and keep structured non-pass agent results)
- **Issue:** Returning a non-pass agent summary without carrying forward env failure metadata would have replaced the original validation context with only the fallback terminal status.
- **Fix:** Copied missing env failure fields into the returned agent summary before appending the synthetic llm attempt.
- **Files modified:** `tools/apdr/src/docker/builder/agent_backend.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_`
- **Committed in:** `1fcd6df`

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** The auto-fix kept the plan aligned with the artifact-truth objective without expanding scope beyond the fallback seam.

## Issues Encountered
- Targeted cargo verification still emits pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; those warnings were out of scope for this plan and did not affect the Phase 17 checks.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 18 can build Docker escalation and backend-path truth on top of the preserved env-plus-llm attempt history.
- The fallback seam now exposes stable terminal statuses for later artifact and reporting work.

## Self-Check: PASSED
- Found `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-01-SUMMARY.md`
- Found task commit `1fcd6df`
- Found task commit `4503392`

---
*Phase: 17-llm-fallback-stability-and-outcome-tracing*
*Completed: 2026-03-31*
