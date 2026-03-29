---
phase: 16-proof-comparison-and-closeout
plan: 01
subsystem: proof-validation
tags: [phase16, closeout, evidence-contract, checker, sample-vs-live]

requires:
  - phase: 14-macos-execution-path-optimization
    provides: macOS replay and Windows guardrail proof contracts
  - phase: 15-langchain-langgraph-tier3-intelligence-improvements
    provides: tier3 baseline-versus-candidate quality proof contract
provides:
  - deterministic Phase 16 closeout evidence checker
  - machine-readable evidence-mode status for the milestone
  - reviewer-facing evidence inventory note for sample versus live proof
affects: [phase-16-closeout, milestone-closeout, requirements-reconciliation]

tech-stack:
  added: []
  patterns: [sample-vs-live evidence classification, closeout-proof aggregation, doc-plus-json validation]

key-files:
  created:
    - scripts/check_phase16_closeout.py
    - .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json
    - .planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md
  modified: []

key-decisions:
  - "Treat sample-versus-live detection as an explicit contract driven by artifact paths and note metadata rather than by reviewer inference."
  - "Write one closeout status JSON before any final comparison prose so later plans can reuse a single evidence-mode source of truth."

patterns-established:
  - "Phase 16 proof notes must validate against the same evidence mode written to 16-closeout-evidence-status.json."
  - "Missing live artifacts are reported as expected-live paths even when sample contracts already exist in-repo."

requirements-completed: [EVD-04, EVD-06]

duration: 5min
completed: 2026-03-29
---

# Phase 16 Plan 01: Closeout Evidence Contract Summary

**Phase 16 now has a deterministic closeout checker and a machine-readable record that the current milestone evidence is sample-backed, not live-backed**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-29T20:30:05Z
- **Completed:** 2026-03-29T20:34:40Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Added [check_phase16_closeout.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase16_closeout.py) so Phase 16 can classify Phase 14 and Phase 15 artifact pairs as `sample`, `live`, `missing`, or `mixed`
- Generated [16-closeout-evidence-status.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json) with the initial `sample-contract-only` terminal state and the missing live artifact list
- Added [16-CLOSEOUT-EVIDENCE.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md) so reviewers can see the exact sample inputs and the expected live replacements

## Task Commits

1. **Task 1: build the Phase 16 closeout evidence checker** - `7740d66` (feat)
2. **Task 2: generate the initial evidence-status artifact and evidence inventory** - `1785cd0` (docs)

## Files Created/Modified

- [check_phase16_closeout.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase16_closeout.py) - Validates artifact completeness, evidence mode, and optional proof-note sections
- [16-closeout-evidence-status.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json) - Machine-readable Phase 16 status showing `sample` mode and missing live evidence
- [16-CLOSEOUT-EVIDENCE.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md) - Reviewer-facing inventory of current and expected proof artifacts

## Decisions Made

- The closeout checker should detect `sample` mode directly from artifact paths and note metadata because the sample JSON schema intentionally matches the live schema
- The missing-live-artifact list should point to the eventual non-sample `.json` files so later host capture can replace the sample contract without renaming the closeout workflow

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 16-02 can now build the reviewer-facing macOS, Windows, and LLM-quality notes against one stable `sample` evidence-mode record
- The remaining live-proof gap is explicit and machine-readable instead of implicit in missing files

## Self-Check: PASSED

- PASSED: `python3 -m py_compile scripts/check_phase16_closeout.py`
- PASSED: `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json --evidence-md .planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md`
- PASSED: `rg -n 'evidence_mode|phase14|phase15|sample|live|missing' .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json`
- PASSED: `rg -n '## Artifact Inputs|## Evidence Modes|## Missing Live Artifacts|## Command Contract' .planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md`

---
*Phase: 16-proof-comparison-and-closeout*
*Completed: 2026-03-29*
