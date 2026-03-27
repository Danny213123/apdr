---
phase: 03-validation-pipeline-throughput
plan: 02
subsystem: validation
tags:
  - apdr
  - rust
  - validation
  - telemetry
  - benchmarking
dependency_graph:
  requires:
    - phase: 03-01
      provides: env-attempt-path-staging
  provides:
    - cached-docker-agent-probe
    - json-backed-agent-result-parsing
    - validation-stage-reporting
    - validation-substage-regression-thresholds
  affects:
    - 03-03
    - phase-04-module-layout-and-boundary-cleanup
tech_stack:
  added: []
  patterns:
    - once-lock-agent-availability-probe
    - resolution-report-cache-telemetry-parsing
    - optional-substage-regression-thresholds
key_files:
  created: []
  modified:
    - tools/apdr/src/docker/builder.rs
    - scripts/measure_apdr_baseline.py
    - scripts/check_apdr_regression.py
key-decisions:
  - Cached the Python `docker_agent` import probe behind `DOCKER_AGENT_IMPORTABLE` so repeated LLM-backend attempts stop spawning the probe subprocess every time.
  - Replaced hand-parsed agent-result string extraction with `serde_json::Value` access to keep the backend result contract explicit.
  - Extended the baseline harness to parse `resolution-report.txt` when the lightweight YAML artifact does not carry enough backend or cache detail.
patterns-established:
  - "Backend availability probes that only need process-wide truth should sit behind `OnceLock`."
  - "Reviewer-facing benchmark artifacts should expose backend path and validation-stage cost per sample, not only total validation time."
requirements-completed:
  - VAL-03
  - VAL-04
metrics:
  duration_seconds: 828
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 5
---

# Phase 3 Plan 02 Summary

**Cached Docker-agent backend probing, JSON-backed agent-result parsing, richer validation benchmark artifacts, and optional regression gates for env-create, install, and smoke regressions.**

## Performance

- **Duration:** ~14 min
- **Started:** 2026-03-27T12:15:36-04:00
- **Completed:** 2026-03-27T12:29:24-04:00
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Added `DOCKER_AGENT_IMPORTABLE` and a small cached-probe helper so the LLM validation backend no longer reruns `python3 -c "import docker_agent"` on every attempt.
- Swapped the Docker-agent result parser from ad-hoc string scanning to `serde_json::from_str::<serde_json::Value>` while preserving the current fallback behavior for non-passed agent results.
- Extended `measure_apdr_baseline.py` to surface per-sample backend path, validated-env reuse, import-set cache hits, lockfile reuse, env-create cost, install cost, smoke cost, and retry counters.
- Extended `check_apdr_regression.py` with optional `--max-env-create-regression-pct`, `--max-install-regression-pct`, and `--max-smoke-regression-pct` thresholds without changing default behavior when those flags are omitted.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` passed with 4 tests
- `python -m py_compile scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py` passed
- `python scripts/measure_apdr_baseline.py --help` passed
- `python scripts/check_apdr_regression.py --help` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed

## Task Commits

Each task was committed atomically:

1. **Task 1 + Task 2 + Task 3: Cache backend probes, expose validation-stage reporting, and add optional sub-stage regression thresholds** - `f1862ea` (`perf(03-02): cache validation telemetry paths`)

## Files Created/Modified

- `tools/apdr/src/docker/builder.rs` - Cached the Docker-agent availability probe, replaced manual agent JSON parsing, and added targeted coverage for the probe cache.
- `scripts/measure_apdr_baseline.py` - Added resolution-report parsing, cache-aware per-sample fields, richer Markdown tables, and candidate-title detection.
- `scripts/check_apdr_regression.py` - Added optional env-create, install, and smoke regression thresholds to the comparison gate.

## Decisions Made

- Kept the cached Docker-agent probe process-wide instead of path-keyed because the search order for `docker_agent` remains fixed within a single run and the phase only needed to remove repeated subprocess cost.
- Parsed cache indicators from `resolution-report.txt` rather than expanding the YAML compatibility shim first, which avoided broader Python-wrapper churn during this phase.
- Left the top-level JSON aggregate keys intact and only added new fields so the Phase 1 and Phase 2 baseline artifacts remain readable by the same tools.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The benchmark harness did not previously distinguish validated-env reuse from import-set cache hits, so this wave had to derive those signals from `validation_attempts` lines in `resolution-report.txt`.
- Existing unrelated local changes in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## User Setup Required

None - the completed Wave 2 outputs only depended on existing Rust and Python tooling in the repo.

## Next Phase Readiness

- Phase 3 can now capture warm and forced candidate artifacts with reviewer-visible backend and stage-breakdown fields.
- The regression gate can now protect env-create, install, and smoke regressions explicitly if the candidate snapshot needs tighter thresholds.

## Self-Check: PASSED

- `tools/apdr/src/docker/builder.rs` contains `DOCKER_AGENT_IMPORTABLE` and `serde_json::from_str::<serde_json::Value>`
- `tools/apdr/src/docker/builder.rs` no longer contains `fn extract_json_string(` or `fn extract_json_number(`
- `scripts/measure_apdr_baseline.py` contains `Backend`, `Env create ms`, `Install ms`, and `Smoke ms`
- `scripts/check_apdr_regression.py` contains `--max-env-create-regression-pct`, `--max-install-regression-pct`, and `--max-smoke-regression-pct`
- All Wave 2 verification commands passed

---
*Phase: 03-validation-pipeline-throughput*
*Completed: 2026-03-27*
