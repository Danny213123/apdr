---
phase: 17-llm-fallback-stability-and-outcome-tracing
plan: 02
subsystem: testing
tags: [rust, python, benchmark-ui, llm-fallback, artifacts]
requires:
  - phase: 17-01
    provides: stable env-first llm fallback outcomes and structured agent statuses
provides:
  - explicit fallback invocation and terminal outcome fields in APDR summaries
  - per-case output YAML serialization for fallback metadata
  - benchmark runner and service result surfaces that preserve fallback metadata without reclassifying outcomes
affects: [phase-17-plan-03, phase-18-backend-routing, benchmark-ui, apdr-artifacts]
tech-stack:
  added: []
  patterns:
    - dedicated fallback metadata fields separate from validation_status
    - result shapers treat fallback outcome as informational metadata, not classification input
key-files:
  created: []
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/recovery_diagnostics.rs
    - tools/apdr/test_executor.py
    - benchmark_ui/runner.py
    - benchmark_ui/service.py
    - benchmark_ui/test_runner_events.py
key-decisions:
  - "Expose fallback summary output with exact lowercase keys so test_executor can copy artifact fields directly."
  - "Derive fallback invocation and terminal outcome from llm attempts plus agent invocation counts instead of overloading validation_status."
  - "Keep benchmark pass/skip/fail classification driven by validation_status, validation_reason, and existing success rules while surfacing fallback metadata separately."
patterns-established:
  - "Artifact contract pattern: reviewer-visible fallback truth lives on the main APDR summary and YAML surfaces."
  - "Benchmark UI pattern: case rows and SSE events may carry fallback metadata, but classification logic ignores fallbackOutcome."
requirements-completed: [AGT-07, AGT-08]
duration: 10m
completed: 2026-03-31
---

# Phase 17 Plan 02: LLM Fallback Stability and Outcome Tracing Summary

**Fallback invocation and terminal outcome now ship in APDR summaries, per-case YAML artifacts, and benchmark result surfaces**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-31T00:29:44Z
- **Completed:** 2026-03-31T00:39:48Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Added `fallback_invoked`, `fallback_outcome`, and `fallback_reason` to `ValidationSummary`, `report_text()`, and `summary_lines()` so reviewer-facing artifacts answer the two operator questions directly.
- Preserved fallback metadata through retry-loop finalization by tracking agent invocations and deriving the terminal fallback state from recorded llm attempts.
- Serialized fallback metadata into `output_data_*.yml` and surfaced `fallbackInvoked`, `fallbackOutcome`, and `fallbackReason` through benchmark runner results, SSE case events, and service case rows without changing pass/skip/fail semantics.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add explicit fallback fields and preserve them through final APDR summaries** - `5df9f75` (feat)
2. **Task 2: Serialize fallback metadata into case outputs and benchmark result readers** - `930a002` (feat)

## Files Created/Modified
- `tools/apdr/src/lib.rs` - Added fallback metadata fields and rendered them in APDR text and summary outputs with exact contract keys.
- `tools/apdr/src/resolver/retry_loop.rs` - Carried fallback invocation counts and refreshed fallback metadata as each validation attempt completed.
- `tools/apdr/src/resolver/recovery_diagnostics.rs` - Centralized fallback metadata derivation so final status inference does not erase failed or abstained fallback outcomes.
- `tools/apdr/test_executor.py` - Wrote fallback metadata into per-case `output_data_*.yml` artifacts.
- `benchmark_ui/runner.py` - Read fallback metadata from output YAML, attached it to result objects, and emitted it on `case_complete` events.
- `benchmark_ui/service.py` - Exposed fallback metadata on benchmark case rows while keeping success and skip classification unchanged.
- `benchmark_ui/test_runner_events.py` - Added coverage proving fallback metadata survives result shaping without redefining pass or skip behavior.

## Decisions Made

- Used exact lowercase summary output keys for the three fallback fields so the Python artifact writer can copy them directly without additional normalization.
- Preserved fallback truth as separate metadata rather than folding it into `validation_status`, which keeps later backend-routing and accounting work composable.
- Extended runner event payloads with fallback metadata because the phase context required both API and event surfaces to remain inspectable.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Retry finalization was dropping agent invocation truth**
- **Found during:** Task 1 (Add explicit fallback fields and preserve them through final APDR summaries)
- **Issue:** `validate_with_retries()` did not accumulate `agent_invocations`, so fallback invocation could disappear by the time final summary metadata was inferred.
- **Fix:** Added retry-loop accumulation for `agent_invocations` and centralized fallback metadata derivation in recovery diagnostics.
- **Files modified:** `tools/apdr/src/resolver/retry_loop.rs`, `tools/apdr/src/resolver/recovery_diagnostics.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_`
- **Committed in:** `5df9f75`

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Required for correctness. No scope creep beyond preserving fallback truth through the planned artifact path.

## Issues Encountered

- `cargo fmt` touched unrelated Rust files; those formatting-only changes were restored before the task commit so task boundaries stayed atomic.

## User Setup Required

None - no external service configuration required.

## Known Stubs

- `tools/apdr/src/resolver/retry_loop.rs:1906` - Pre-existing `placeholder` comment in the targeted recovery table for `lxml`; unrelated to fallback artifact truth and does not block this plan.

## Next Phase Readiness

- Phase 17 plan 03 can build on an explicit artifact contract for fallback invocation and terminal outcome instead of scraping logs.
- Phase 18 backend-routing work can now reuse preserved fallback metadata without redefining benchmark result classification.

## Self-Check: PASSED

- FOUND: `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-02-SUMMARY.md`
- FOUND: `5df9f75`
- FOUND: `930a002`

---
*Phase: 17-llm-fallback-stability-and-outcome-tracing*
*Completed: 2026-03-31*
