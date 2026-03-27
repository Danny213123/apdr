---
phase: 03-validation-pipeline-throughput
plan: 01
subsystem: validation
tags:
  - apdr
  - rust
  - validation
  - env-cache
  - docker
dependency_graph:
  requires:
    - phase: 02-03
      provides: resolver-candidate-delta
  provides:
    - env-attempt-path-staging
    - validated-env-cache-source-detection
    - backend-retry-history-ordering
  affects:
    - 03-02
    - 03-03
    - phase-04-module-layout-and-boundary-cleanup
tech_stack:
  added: []
  patterns:
    - explicit-env-attempt-workspace-staging
    - validated-env-cache-source-enum
    - ordered-env-to-docker-attempt-history
key_files:
  created: []
  modified:
    - tools/apdr/src/docker/builder.rs
key-decisions:
  - Centralized env-attempt workspace setup behind `prepare_env_validation_attempt` so cache detection and per-attempt path wiring are not duplicated inline.
  - Split validated-env materialization into archive, legacy-dir, and cold-build paths under `materialize_env_for_attempt` while preserving the current cache-marker semantics.
  - Kept env-attempt history merging explicit with `merge_backend_retry_history` so Docker fallback reviews show env attempts first.
patterns-established:
  - "Validation-attempt setup should return an explicit path bundle plus cache-source enum rather than rebuilding paths inline."
  - "Env-to-Docker fallback history should be merged through one helper so ordering stays deterministic."
requirements-completed:
  - VAL-01
  - VAL-02
  - VAL-05
metrics:
  duration_seconds: 960
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 3
---

# Phase 3 Plan 01 Summary

**Explicit env-attempt staging, validated-env cache-source detection, and ordered env-to-Docker retry history for the validation pipeline.**

## Performance

- **Duration:** ~16 min
- **Started:** 2026-03-27T12:00:00-04:00
- **Completed:** 2026-03-27T12:15:36-04:00
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- Extracted env-validation workspace setup into `prepare_env_validation_attempt`, including smoke script, requirements files, context snapshots, build keys, and cache-source detection.
- Centralized validated-env restore or cold-build behavior in `materialize_env_for_attempt` so archive, legacy-dir, and fallback paths share one code path.
- Added targeted validation-pipeline tests for archive cache detection, legacy-dir cache detection, and env-to-Docker history ordering.
- Restored the Wave 1 verification gate by fixing the new tests to use real `ResolveConfig` values and by making the helper signature clippy-compliant.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` passed with 3 tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed
- `rg -n "ValidatedEnvCacheSource|EnvAttemptPaths|prepare_env_validation_attempt|materialize_env_for_attempt|merge_backend_retry_history" tools/apdr/src/docker/builder.rs` confirmed the expected Wave 1 symbols

## Task Commits

Each task was committed atomically:

1. **Task 1 + Task 2 + Task 3: Stage env validation attempts, cache-source helpers, and verification tests** - `5063e87` (`perf(03-01): stage env validation attempts`)

## Files Created/Modified

- `tools/apdr/src/docker/builder.rs` - Added explicit cache-source and attempt-path helpers, centralized env materialization, and validation-pipeline unit coverage.

## Decisions Made

- Kept the new helper types local to `docker/builder.rs` so Phase 3 improves validation throughput without starting the larger module-splitting work reserved for Phase 4.
- Used `ResolveConfig::for_tool_root(...)` in tests instead of touching the unrelated `ResolveConfig` definition in `lib.rs`.
- Accepted a targeted `#[allow(clippy::too_many_arguments)]` on `materialize_env_for_attempt` rather than forcing a wider API-shape refactor into Wave 1.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The Wave 1 helper extraction and tests were already present in `tools/apdr/src/docker/builder.rs`, but the new tests did not compile because they assumed `ResolveConfig: Default` and they targeted cache keys that did not match the real build-key path. The execution wave closed that gap directly in the same file and restored the intended verification contract.
- Existing unrelated local changes in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## User Setup Required

None - no external service configuration was required for the committed Wave 1 outputs.

## Next Phase Readiness

- Phase 3 can now move into backend-attempt telemetry and benchmark-reporting work on top of a passing validation helper layer.
- The validation-pipeline quick test filter and workspace clippy gate are both green going into `03-02`.

## Self-Check: PASSED

- `tools/apdr/src/docker/builder.rs` contains `ValidatedEnvCacheSource`, `EnvAttemptPaths`, `prepare_env_validation_attempt`, `materialize_env_for_attempt`, and `merge_backend_retry_history`
- `tools/apdr/src/docker/builder.rs` contains the three required `validation_pipeline_` tests
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed

---
*Phase: 03-validation-pipeline-throughput*
*Completed: 2026-03-27*
