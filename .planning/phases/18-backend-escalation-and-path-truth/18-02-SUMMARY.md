---
phase: 18-backend-escalation-and-path-truth
plan: 02
subsystem: artifacts
tags: [rust, python, benchmark-ui, validation-path, run-contract]
requires:
  - phase: 18-01
    provides: deterministic llm docker escalation and ordered env->docker->llm-agent attempt history
provides:
  - explicit `validation_path` metadata in APDR summaries and output artifacts
  - benchmark result objects and case rows that preserve routed backend truth
  - tests proving configured backend remains distinct from actual backend path
affects: [phase-18-plan-03, benchmark-ui, apdr-artifacts, run-contract]
tech-stack:
  added: []
  patterns:
    - "Configured validation backend stays stable while actual route truth is carried in a separate path field"
    - "Benchmark result objects surface camelCase path fields derived from snake_case output metadata"
key-files:
  created: []
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/test_executor.py
    - benchmark_ui/runner.py
    - benchmark_ui/service.py
    - benchmark_ui/test_runner_events.py
    - benchmark_ui/test_run_contract.py
key-decisions:
  - "Represent actual route truth with `validation_path` instead of overloading Phase 13's configured `validation_backend` contract."
  - "Map agent attempts to `llm-agent` inside the derived path so operators can distinguish configured llm mode from the concrete agent hop."
patterns-established:
  - "Rust summaries may derive `validation_path` from attempt history when the field is not explicitly populated."
  - "Benchmark UI rows expose `validationBackend`, `validationPath`, and `escalatedBackend` together so reviewers do not have to reconstruct routing from logs."
requirements-completed: [VAL-01, VAL-02]
duration: 8 min
completed: 2026-03-31
---

# Phase 18 Plan 02: Backend Escalation and Path Truth Summary

**APDR artifacts now preserve the requested backend mode separately from the actual routed validation path, and benchmark readers carry that truth end to end**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-31T02:44:30Z
- **Completed:** 2026-03-31T02:52:28Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments

- Added `validation_path` to `ValidationSummary` and taught APDR to derive stable paths like `env->docker` and `env->docker->llm-agent` from actual attempt history.
- Propagated `escalated_backend` and refreshed `validation_path` in the retry loop so final summaries keep route truth after attempts are merged across recovery iterations.
- Serialized `VALIDATION_PATH` and `ESCALATED_BACKEND` through `summary_lines()` into `output_data_*.yml`.
- Extended benchmark result shaping so case results and SSE `case_complete` events preserve `validationPath` and `escalatedBackend`.
- Added Python tests proving benchmark case rows can show `validationBackend=llm` while `validationPath` reports the actual routed backend sequence.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add summary-level backend path metadata and serialize it end to end** - `d222344` (feat)
2. **Task 2: Teach benchmark readers and tests to preserve path truth** - `fa07b2c` (feat)

## Files Created/Modified

- `tools/apdr/src/lib.rs` - Adds `validation_path`, derives route truth from attempts, and renders it in APDR reports plus summary lines.
- `tools/apdr/src/resolver/retry_loop.rs` - Preserves `escalated_backend` and refreshes `validation_path` after each validation attempt batch.
- `tools/apdr/test_executor.py` - Writes `validation_path` and `escalated_backend` into output YAML artifacts.
- `benchmark_ui/runner.py` - Reads backend-path metadata from output YAML and attaches it to result objects plus `case_complete` events.
- `benchmark_ui/service.py` - Exposes `validationBackend`, `validationPath`, and `escalatedBackend` on benchmark case rows.
- `benchmark_ui/test_runner_events.py` - Verifies routed backend path metadata survives result shaping without changing pass/skip/fail behavior.
- `benchmark_ui/test_run_contract.py` - Verifies configured backend remains distinct from actual validation path in reviewer-facing case rows.

## Decisions Made

- Used `llm-agent` as the path segment for actual agent attempts so routed paths stay readable and unambiguous next to the configured `llm` mode label.
- Kept path derivation inside APDR itself instead of reconstructing it in Python, which makes the output contract portable across future readers.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Retry-loop finalization was not preserving escalated backend truth**
- **Found during:** Task 1 (Add summary-level backend path metadata and serialize it end to end)
- **Issue:** Adding `validation_path` only at the APDR summary edge would still have dropped `escalated_backend` when retry-loop state merged an attempt result into the long-lived validation summary.
- **Fix:** Propagated `escalated_backend` and refreshed `validation_path` inside `validate_with_retries()` right after attempt histories are extended.
- **Files modified:** `tools/apdr/src/resolver/retry_loop.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_`
- **Committed in:** `d222344`

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** The auto-fix kept the route-truth contract accurate without widening the scope beyond Phase 18 artifact propagation.

## Issues Encountered

- Focused cargo verification still emits the pre-existing dead-code warnings in `tools/apdr/src/resolver/targeted_recovery.rs`; those warnings were unrelated to backend-path truth and did not block this plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 03 can now update Doctor/runtime messaging and proof assets against a stable `validationPath` contract.
- The benchmark UI already carries routed backend truth, so the proof checker can validate reviewer-facing artifacts without adding a second reporting channel.

## Self-Check: PASSED

- Found `.planning/phases/18-backend-escalation-and-path-truth/18-02-SUMMARY.md`
- Found task commit `d222344`
- Found task commit `fa07b2c`

---
*Phase: 18-backend-escalation-and-path-truth*
*Completed: 2026-03-31*
