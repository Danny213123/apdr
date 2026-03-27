---
phase: 02-resolver-memory-and-algorithm-efficiency
plan: 01
subsystem: resolver
tags:
  - apdr
  - rust
  - pre-solve
  - pypi-cache
  - concurrency
dependency_graph:
  requires:
    - phase: 01-02
      provides: hotspot-priority-audit
  provides:
    - owned-pre-solve-worker-results
    - shared-pypi-metadata-persistence
    - pre-solve-ordering-regression-tests
  affects:
    - 02-02
    - phase-03-validation-pipeline-throughput
tech_stack:
  added: []
  patterns:
    - owned-worker-result-aggregation
    - shared-metadata-persistence-helpers
    - pre-solve-candidate-order-regression-tests
key_files:
  created: []
  modified:
    - tools/apdr/src/resolver/pre_solve.rs
    - tools/apdr/src/resolver/pypi_client.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - Replaced shared mutex result buckets with an owned `PythonSolveAttempt` join path so candidate-order selection stays deterministic without lock-heavy teardown.
  - Centralized version and dependency-spec persistence behind shared helper functions while preserving a batched knowledge-cache path for bulk prefetch.
  - Added explicit multi-version pre-solve tests instead of relying only on single-version solver coverage.
patterns-established:
  - "Concurrent pre-solve workers should return owned results to the parent thread instead of mutating shared buckets."
  - "PyPI metadata writes should flow through shared persistence helpers rather than source-specific save blocks."
requirements-completed:
  - EFF-01
  - EFF-02
  - EFF-03
  - EFF-05
metrics:
  duration_seconds: 180
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 5
---

# Phase 2 Plan 01 Summary

**Owned pre-solve worker aggregation, shared PyPI metadata persistence helpers, and multi-version regression coverage for the first resolver hot-path cleanup.**

## Performance

- **Duration:** ~3 min
- **Started:** 2026-03-27T00:36:00-04:00
- **Completed:** 2026-03-27T00:39:14-04:00
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Replaced the multi-version SMT pre-solve `Arc<Mutex<...>>` result buckets with an owned `PythonSolveAttempt` join path in `pre_solve.rs`.
- Consolidated version and dependency-spec persistence in `pypi_client.rs` so cache, KGraph, smartPip, and PyPI branches reuse the same helper logic.
- Added direct regression tests for candidate-order preservation and incomplete-metadata fallback in the multi-version pre-solve path.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture` passed before and after the new tests were added
- `rg -n "PythonSolveAttempt|persist_versions|persist_dependency_specs|pre_solver_preserves_candidate_order_without_mutex_aggregation|pre_solver_keeps_incomplete_metadata_separate_from_hard_failures" ...` confirmed the expected symbols and tests are present

## Task Commits

Each task was committed atomically:

1. **Task 1: Replace mutex-based pre-solve result aggregation with owned worker results** - `1cb7bc8` (`perf(02-01): simplify pre-solve worker aggregation`)
2. **Task 2: Consolidate version and dependency metadata persistence helpers** - `64bfa8a` (`perf(02-01): consolidate pypi metadata persistence`)
3. **Task 3: Add targeted tests for candidate order and incomplete-metadata fallback** - `c18d5b6` (`test(02-01): cover pre-solve candidate ordering`)

## Files Created/Modified

- `tools/apdr/src/resolver/pre_solve.rs` - Replaced shared mutex result aggregation with owned worker results and preserved candidate-order selection.
- `tools/apdr/src/resolver/pypi_client.rs` - Added shared persistence helpers for versions and dependency specs, with batch-aware reuse in bulk prefetch.
- `tools/apdr/tests/test_resolver.rs` - Added multi-version regression coverage for candidate-order preservation and incomplete-metadata fallback.

## Decisions Made

- Kept the single-version pre-solve fast path unchanged so the ownership refactor only touched the multi-version branch.
- Preserved batch-aware knowledge-cache updates in bulk prefetch instead of regressing to one lock acquisition per spec write.
- Extended the test suite at the pre-solve layer rather than trying to infer this behavior only from full resolver integration tests.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- `cargo fmt --manifest-path tools/apdr/Cargo.toml --all` surfaced unrelated unstaged changes in `tools/apdr/src/lib.rs` and `tools/apdr/src/resolver/family_knowledge.rs`. Those files were left untouched and excluded from this plan's commits.

## User Setup Required

None - no external service configuration required for this plan's committed outputs.

## Next Phase Readiness

- `resolver/mod.rs` can now assume pre-solve candidate ordering is explicitly test-covered while the retry-loop cleanup lands in `02-02`.
- `pypi_client.rs` now has one metadata persistence path that later resolver and validation work can reuse instead of copying again.

## Self-Check: PASSED

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture` passed with 5 tests
- `tools/apdr/src/resolver/pre_solve.rs` contains `PythonSolveAttempt`
- `tools/apdr/src/resolver/pypi_client.rs` contains `persist_versions` and `persist_dependency_specs`
- `tools/apdr/tests/test_resolver.rs` contains both new multi-version pre-solve regression tests

---
*Phase: 02-resolver-memory-and-algorithm-efficiency*
*Completed: 2026-03-27*
