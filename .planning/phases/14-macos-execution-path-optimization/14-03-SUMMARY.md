---
phase: 14-macos-execution-path-optimization
plan: 03
subsystem: proof-validation
tags: [macos-replay, windows-guardrail, regression-checker, benchmark-proof, samples]

requires:
  - phase: 14-macos-execution-path-optimization
    provides: locked replay slices, replay runner, replay policy, and Phase 13 timing metadata
provides:
  - replay-aware regression checking in scripts/check_apdr_regression.py
  - deterministic Phase 14 proof validation in scripts/check_phase14_macos_replay.py
  - bounded macOS and Windows sample artifacts plus reviewer-facing proof notes
  - a machine-checked contract for future live replay evidence capture
affects: [phase-15-planning, phase-16-proof, benchmark-comparison-audits]

tech-stack:
  added: []
  patterns: [correctness-preserving regression checks, sample-backed proof contracts, markdown-plus-json validation]

key-files:
  created:
    - scripts/check_phase14_macos_replay.py
    - .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md
    - .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md
    - .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json
    - .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json
    - .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json
    - .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json
  modified:
    - scripts/check_apdr_regression.py

key-decisions:
  - "Compare before/after artifacts on replay-slice identity and preserved pass/skip outcomes, not headline timing alone"
  - "Use one proof checker to validate both machine-readable artifacts and reviewer-facing Markdown notes so the two cannot drift"
  - "Keep bounded sample artifacts in-repo as the schema contract, while treating live macOS and Windows captures as later evidence that must conform to that contract"

patterns-established:
  - "Phase 14 proof now requires like-for-like slice_id, execution_mode, cache_state, and build_profile comparisons"
  - "Preserved baseline pass and skip cases are enforced as first-class regression guards alongside total duration and seconds-per-case"

requirements-completed: [MAC-04, WIN-01]

duration: 10min
completed: 2026-03-29
---

# Phase 14 Plan 03: Replay Proof Summary

**Phase 14 now has a deterministic proof contract for macOS replay gains and Windows non-regression, backed by sample artifacts and reviewer-facing notes**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-29T18:18:37Z
- **Completed:** 2026-03-29T18:28:12Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Extended [scripts/check_apdr_regression.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_apdr_regression.py) so replay comparisons now validate `slice_id`, `execution_mode`, `cache_state`, `build_profile`, seconds-per-case, optional LLM and Docker timings, and preserved pass/skip outcomes
- Added [scripts/check_phase14_macos_replay.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase14_macos_replay.py) to validate the full Phase 14 proof package across macOS before/after artifacts, Windows guardrail artifacts, and the reviewer-facing Markdown notes
- Added bounded sample JSON artifacts and proof-note templates in [.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md) and [.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md)

## Task Commits

1. **Task 1: extend the replay regression checker** - `cc2fac3` (feat)
2. **Task 2: add the Phase 14 proof checker, sample artifacts, and proof notes** - `3381032` (feat)

## Files Created/Modified
- [scripts/check_apdr_regression.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_apdr_regression.py) - Adds replay identity checks, preserved-outcome checks, and stage-aware regression thresholds
- [scripts/check_phase14_macos_replay.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase14_macos_replay.py) - Validates the combined macOS and Windows proof package
- [.planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json) - Sample macOS baseline artifact schema
- [.planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json) - Sample macOS candidate artifact schema
- [.planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json) - Sample Windows baseline artifact schema
- [.planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json) - Sample Windows candidate artifact schema
- [.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md) - Reviewer-facing macOS before/after proof note
- [.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md) - Reviewer-facing Windows non-regression note

## Decisions Made
- Time-only comparisons are not enough for replay evidence; the checker now enforces slice identity and preserved correctness outcomes
- The Phase 14 proof package uses the same assumptions in JSON and Markdown so reviewer notes and machine validation stay aligned
- Live baseline and candidate captures remain an operator step, but they now have a bounded schema and deterministic validation path

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The repo does not yet contain live `14-macos-before.json`, `14-macos-after.json`, `14-windows-before.json`, or `14-windows-after.json` artifacts, so verification for this plan was bounded to the in-repo sample contract rather than milestone-closeout evidence capture.

## User Setup Required

- For live milestone evidence, run the command in [.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md) on the macOS benchmark host to produce `14-macos-before.json` and `14-macos-after.json`.
- Import `14-windows-before.json` and `14-windows-after.json` from a representative Windows host using the command documented in [.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md) before milestone closeout.

## Next Phase Readiness

- Phase 15 can now focus on tier3 intelligence improvements without rebuilding replay or measurement plumbing
- Phase 16 can reuse the Phase 14 checker pair and proof-note templates when collecting final macOS and Windows evidence
- The remaining proof work is operational capture, not additional checker or schema design

## Self-Check: PASSED

- PASSED: `rg -n 'seconds_per_case|slice_id|llm_duration_ms|docker_startup_duration_ms|preserved' scripts/check_apdr_regression.py`
- PASSED: `rg -n -- '--macos-before|--windows-before' scripts/check_phase14_macos_replay.py`
- PASSED: `python3 -m py_compile scripts/check_apdr_regression.py scripts/check_phase14_macos_replay.py`
- PASSED: `python3 scripts/check_phase14_macos_replay.py --macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --macos-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md --windows-md .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md`

---
*Phase: 14-macos-execution-path-optimization*
*Completed: 2026-03-29*
