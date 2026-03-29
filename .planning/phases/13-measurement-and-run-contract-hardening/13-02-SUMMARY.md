---
phase: 13-measurement-and-run-contract-hardening
plan: 02
subsystem: apdr-core
tags: [timing-contract, run-contract, cli-schema, benchmark-artifacts, rust, python]

requires:
  - phase: 13-measurement-and-run-contract-hardening
    provides: Canonical run-contract contract from Plan 13-01
provides:
  - APDR per-case timing contract with explicit llm_duration_ms and docker_startup_duration_ms
  - CLI support for --run-contract-json so benchmark_ui can inject the canonical run contract into APDR execution
  - resolution-report and summary_lines output with flattened run-contract metadata for attribution
  - output_data_*.yml propagation of run-contract metadata and new timing fields through test_executor.py
  - CLI and benchmark-ui tests that lock the new schema
affects: [13-03, macos-benchmark-reporting, windows-non-regression-evidence]

tech-stack:
  added: []
  patterns: [cli-to-report contract propagation, per-case timing surface, wrapper-side schema validation]

key-files:
  created: []
  modified:
    - benchmark_ui/runner.py
    - tools/apdr/src/lib.rs
    - tools/apdr/src/main.rs
    - tools/apdr/src/resolver/mod.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/tier3_llm/core.rs
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/docker_backend.rs
    - tools/apdr/test_executor.py
    - tools/apdr/tests/test_cli.rs

key-decisions:
  - "Push the run contract into the APDR CLI itself via --run-contract-json so resolution-report.txt, summary_lines(), and output_data_*.yml all read from the same metadata source"
  - "Track llm_duration_ms across pre-validation tier3 work and retry-loop recovery so per-case artifacts expose actual LLM time instead of only total solve time"
  - "Split Docker launch into create and start phases so docker_startup_duration_ms is explicit instead of being silently folded into smoke timing"

patterns-established:
  - "ResolveResult now owns the flattened run-contract metadata that report_text() and summary_lines() emit"
  - "test_executor.py validates the canonical run-contract keys before APDR execution and mirrors them into output_data_*.yml"

requirements-completed: [MAC-02, EVD-05]

duration: 16min
completed: 2026-03-29
---

# Phase 13 Plan 02: APDR Timing and Run-Contract Propagation Summary

**APDR per-case artifacts now carry the same Phase 13 run contract as benchmark_ui, with explicit LLM and Docker-startup timing fields**

## Performance

- **Duration:** 16 min
- **Started:** 2026-03-29T04:04:27Z
- **Completed:** 2026-03-29T04:20:27Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Added `RunContractMetadata` to the APDR core and taught `resolution-report.txt` plus `summary_lines()` to emit `MODEL_NAME`, `BASE_URL`, `RUN_INTENT`, `EXECUTION_MODE`, `CACHE_STATE`, architecture fields, `LLM_CONTEXT_WINDOW`, `INFERENCE_POLICY`, and `BUILD_PROFILE`
- Added `llm_duration_ms` and `docker_startup_duration_ms` to `ValidationSummary`, then threaded LLM timing through tier3 resolution and retry paths
- Added CLI support for `--run-contract-json` in `tools/apdr/src/main.rs` and `tools/apdr/test_executor.py`
- Updated `benchmark_ui/runner.py` to pass the persisted `run_contract.json` into each APDR case execution
- Updated `test_executor.py` to validate the canonical run contract from `benchmark_ui.run_contract`, then persist the flattened metadata and new timing fields into `output_data_*.yml`
- Changed the Docker backend to measure container create/startup separately from smoke-test runtime so per-case reports expose Docker launch cost directly
- Locked the new stdout/report schema in `tools/apdr/tests/test_cli.rs` and re-ran the targeted benchmark UI tests after the runner handoff change

## Task Commits

1. **Plan 13-02 implementation** - `a4ae24d` (feat)
2. **Plan 13-02 docs/progress** - `7bef87a` (docs)

## Files Created/Modified
- `benchmark_ui/runner.py` - Passes `--run-contract-json` into each APDR case invocation
- `tools/apdr/src/lib.rs` - Adds run-contract metadata handling plus the new timing/report fields
- `tools/apdr/src/main.rs` - Parses and validates `--run-contract-json`
- `tools/apdr/src/resolver/mod.rs` - Aggregates pre-validation tier3 LLM timing and carries run-contract metadata into `ResolveResult`
- `tools/apdr/src/resolver/retry_loop.rs` - Accumulates retry-loop LLM timing for recovery paths
- `tools/apdr/src/resolver/tier3_llm/core.rs` - Returns `llm_duration_ms` with stage results
- `tools/apdr/src/docker/builder/agent_backend.rs` - Preserves agent timing as LLM duration when the LangGraph path runs
- `tools/apdr/src/docker/builder/docker_backend.rs` - Separates Docker startup timing from smoke timing
- `tools/apdr/test_executor.py` - Validates run contracts and mirrors the full contract into `output_data_*.yml`
- `tools/apdr/tests/test_cli.rs` - Locks stdout/report contract coverage for the new fields

## Decisions Made
- Kept run-contract metadata as explicit flattened fields in APDR outputs rather than requiring later scripts to parse a nested JSON blob out of logs
- Used runtime-default fallbacks in the Rust CLI so direct `resolve_path()` calls still produce a complete metadata surface during tests and ad hoc runs
- Treated Docker container creation as the startup phase and `docker start -a` as smoke runtime so the new timing field represents a distinct launch cost

## Deviations from Plan

- No functional deviation. `cargo fmt` touched several unrelated resolver files, but those formatter-only side effects were reverted before checkpointing so the Plan 13-02 diff stayed inside the approved write set.

## Issues Encountered

- None after the initial compile pass; one retry-loop helper was briefly instrumented outside the validation scope and failed the first test build, then was corrected before the final verification run.

## User Setup Required

None.

## Next Phase Readiness
- Plan 13-03 can now normalize reports from real per-case artifacts without inferring model, context, backend, or build settings from free-form logs
- The measurement checker can validate both run-level and per-sample timing metadata against the same Phase 13 contract
- macOS and Windows benchmark comparisons now have the per-case metadata surface needed for later attribution

## Self-Check: PASSED

- FOUND: `benchmark_ui/runner.py` passes `--run-contract-json`
- FOUND: `tools/apdr/test_executor.py` writes `llm_duration_ms`, `docker_startup_duration_ms`, `model_name`, `run_intent`, `execution_mode`, and `build_profile`
- FOUND: `tools/apdr/src/lib.rs` emits `LLM_DURATION_MS=`, `DOCKER_STARTUP_DURATION_MS=`, and the flattened run-contract keys
- PASSED: `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli -- --nocapture`
- PASSED: `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_service_tier_stats benchmark_ui.test_runner_events`
- FOUND: `a4ae24d`

---
*Phase: 13-measurement-and-run-contract-hardening*
*Completed: 2026-03-29*
