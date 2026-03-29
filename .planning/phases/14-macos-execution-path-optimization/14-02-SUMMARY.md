---
phase: 14-macos-execution-path-optimization
plan: 02
subsystem: replay-runner
tags: [macos-replay, build-profile, preflight, workers, benchmark-ui]

requires:
  - phase: 14-macos-execution-path-optimization
    provides: locked replay-slice manifests and manifest-aware capture from Plan 14-01
provides:
  - dedicated Phase 14 replay runner in scripts/run_phase14_replay.py
  - macOS replay preflight warnings and worker policy in benchmark_ui
  - profile-aware APDR binary selection in tools/apdr/test_executor.py
  - replay-policy coverage in benchmark_ui unit tests
affects: [14-03-PLAN, phase14-proof-artifacts, windows-guardrail-checks]

tech-stack:
  added: []
  patterns: [cold-vs-warm replay capture, build-profile-aware binary selection, replay preflight warnings]

key-files:
  created:
    - scripts/run_phase14_replay.py
  modified:
    - benchmark_ui/runner.py
    - benchmark_ui/service.py
    - benchmark_ui/state.py
    - benchmark_ui/test_run_contract.py
    - benchmark_ui/test_runner_events.py
    - scripts/measure_apdr_baseline.py
    - tools/apdr/test_executor.py

key-decisions:
  - "Default macos-replay to build_profile=release when the caller does not pin a profile explicitly"
  - "Keep auto workers at 1 for macos-replay and cap explicit workers at 4 to avoid native env-cache and disk thrash"
  - "Surface replay-invalidating conditions as warnings in benchmark_ui instead of silently accepting non-comparable captures"
  - "Prefer matching fresh prebuilt binaries in test_executor and only fall back to cargo run with an explicit warning"

patterns-established:
  - "macos-replay captures now carry effective_workers and preflight_warnings alongside the Phase 13 run contract"
  - "Replay runner probe mode validates wiring without a live dataset, while prewarm mode primes native caches before measured candidate runs"

requirements-completed: [MAC-03, MAC-04]

duration: 9min
completed: 2026-03-29
---

# Phase 14 Plan 02: Replay Runner Summary

**Phase 14 now has a dedicated replay runner, macOS replay preflight policy, and profile-aware APDR binary selection for native env-fast captures**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-29T18:09:00Z
- **Completed:** 2026-03-29T18:18:36Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Added [scripts/run_phase14_replay.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase14_replay.py) as the dedicated Phase 14 replay entrypoint with `--probe-only`, `--prewarm`, baseline/candidate capture paths, and manifest-aware live execution
- Added replay-specific worker policy and invalidation warnings in [benchmark_ui/runner.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/runner.py) so `macos-replay` runs expose Rosetta, backend drift, cache-state drift, and missing binary-profile issues before the evidence is trusted
- Extended [benchmark_ui/service.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/service.py) and [benchmark_ui/state.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/state.py) so replay runs default to `release` when `run_intent=macos-replay`, preserve replay-manifest metadata, and surface effective worker count and preflight warnings in the UI state
- Updated [tools/apdr/test_executor.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/test_executor.py) so build-profile requests prefer fresh matching binaries and warn explicitly before falling back to `cargo run`
- Added replay-policy coverage in [benchmark_ui/test_run_contract.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/test_run_contract.py) and [benchmark_ui/test_runner_events.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/test_runner_events.py)

## Task Commits

1. **Task 1: replay runner with cold/warm capture semantics** - `93fa593` (feat)
2. **Task 2: replay policy, build-profile selection, and benchmark UI wiring** - `a8048a3` (mixed branch commit)
3. **Verification fix and plan closeout** - `11c4c30` (test)

## Files Created/Modified
- [scripts/run_phase14_replay.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase14_replay.py) - Orchestrates probe, baseline, prewarm, and candidate replay captures
- [benchmark_ui/runner.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/runner.py) - Adds `macos-replay` worker policy, preflight warnings, and build-profile handoff into per-case execution
- [benchmark_ui/service.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/service.py) - Persists replay-manifest info, effective worker count, and replay warnings into UI and historical snapshots
- [benchmark_ui/state.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/state.py) - Carries replay defaults used by the benchmark control plane
- [tools/apdr/test_executor.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/test_executor.py) - Selects fresh binaries by requested build profile and emits warnings before cargo fallback
- [benchmark_ui/test_run_contract.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/test_run_contract.py) - Covers replay-specific run-contract and info-field behavior
- [benchmark_ui/test_runner_events.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui/test_runner_events.py) - Covers replay-manifest loading, worker policy, and replay invalidation warnings
- [scripts/measure_apdr_baseline.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/measure_apdr_baseline.py) - Minor replay-runner compatibility adjustments reused by the new script

## Decisions Made
- Treat `macos-replay` as a stricter execution intent than generic benchmark runs: warn early, prefer release builds, and keep worker concurrency conservative
- Keep replay evidence comparable by emitting warnings instead of silently accepting non-native or mixed-cache captures
- Preserve `cargo run` as a fallback path for compatibility, but make the performance cost explicit for replay evidence

## Deviations from Plan

- Task 2 code landed inside `a8048a3`, a pre-existing mixed branch commit that also contained unrelated user changes outside the Phase 14 file set. I did not rewrite or revert that history; this summary records it explicitly so the phase trail stays truthful.

## Issues Encountered

- The delegated executor hit a usage-limit error mid-plan and left the branch in a partial state.
- The new replay-policy unit tests were missing a temporary-directory setup in `TestMacosReplayPolicy`; that was fixed in `11c4c30` before final verification.

## User Setup Required

None for the code path itself. The Windows guardrail artifact is still a later Phase 14 requirement and will be handled in Plan 14-03.

## Next Phase Readiness
- The replay runner and native fast-lane policy are in place for proof-oriented before/after captures
- Phase 14 can now move to Plan 14-03 to build the regression checker, sample proof artifacts, and reviewer-facing notes
- The remaining work is evidence and checker closure, not replay-lane plumbing

## Self-Check: PASSED

- PASSED: `python3 -m py_compile scripts/run_phase14_replay.py`
- PASSED: `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract`
- FOUND: `93fa593`
- FOUND: `a8048a3`
- FOUND: `11c4c30`

---
*Phase: 14-macos-execution-path-optimization*
*Completed: 2026-03-29*
