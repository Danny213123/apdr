---
phase: 15-langchain-langgraph-tier3-intelligence-improvements
plan: 01
subsystem: tier3-benchmark-contract
tags: [tier3-benchmark, replay-slice, sample-artifacts, qwen3.5-9b, benchmark-contract]

requires:
  - phase: 14-macos-execution-path-optimization
    provides: locked replay slice, run-contract metadata, and replay proof conventions
provides:
  - deterministic Phase 15 tier3 replay benchmark harness in scripts/run_phase15_tier3_benchmark.py
  - bounded baseline and candidate sample artifacts for baseline-versus-candidate tier3 comparisons
  - a reviewer-facing benchmark contract note for later quality and policy proof
affects: [phase-15-plan-02, phase-15-plan-03, phase-15-plan-04, phase-16-proof]

tech-stack:
  added: []
  patterns: [probe-safe benchmark harnesses, sample-backed artifact contracts, replay-slice quality comparison]

key-files:
  created:
    - scripts/run_phase15_tier3_benchmark.py
    - .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json
    - .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json
    - .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md
  modified: []

key-decisions:
  - "Use the locked Phase 14 replay slice as the one allowed case boundary for Phase 15 quality comparisons"
  - "Record agent mode, retrieval profile, tool profile, thinking mode, context window, and inference policy directly in the artifact so later gains stay attributable"
  - "Start with a probe-only mode so schema verification is possible without depending on a live model or external dataset"

patterns-established:
  - "Baseline and candidate tier3 comparisons now share one JSON contract with per-case tier3 status accounting"
  - "Small-model policy experiments can be compared without changing the replay slice or hiding configuration in prompts"

requirements-completed: []

duration: 10min
completed: 2026-03-29
---

# Phase 15 Plan 01: Tier3 Benchmark Harness Summary

**Phase 15 now has one deterministic replay harness and one bounded artifact contract for baseline-versus-candidate tier3 quality comparisons**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-29T19:38:37Z
- **Completed:** 2026-03-29T19:48:03Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Added [scripts/run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) as the bounded Phase 15 replay harness, with manifest-driven case loading, probe-only schema validation, and run-contract metadata for agent mode, retrieval profile, tool profile, thinking mode, context window, and inference policy
- Added bounded sample artifacts in [15-tier3-baseline-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json) and [15-tier3-candidate-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json) so later checker work has a deterministic schema to validate against
- Added the reviewer-facing contract note in [15-TIER3-BENCHMARK.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md), including the baseline command, candidate command, probe-only command, artifact names, and comparison assumptions

## Task Commits

1. **Task 1: build the Phase 15 replay-slice benchmark harness** - `b0fc91c` (feat)
2. **Task 2: add bounded sample artifacts and the reviewer-facing benchmark contract** - `ce70e8a` (docs)

## Files Created/Modified

- [scripts/run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) - Creates baseline or candidate tier3 replay artifacts and supports probe-only schema validation
- [15-tier3-baseline-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json) - Sample schema for the baseline tier3 replay artifact
- [15-tier3-candidate-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json) - Sample schema for the candidate tier3 replay artifact
- [15-TIER3-BENCHMARK.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md) - Reviewer-facing benchmark contract and command reference

## Decisions Made

- Phase 15 quality work will be compared on the exact Phase 14 replay slice rather than a shifting case set
- Artifact metadata is first-class so later LangChain/LangGraph, retrieval, context, and small-model policy gains can be attributed cleanly
- Probe-only validation is part of the harness itself, which keeps schema checks local and fast even before live model execution is wired in

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The delegated wave-1 executor stalled without producing artifacts or commits, so plan execution continued inline. No scope changed, and the resulting work stayed within the original plan boundaries.

## User Setup Required

- For live Phase 15 evidence, run the commands in [15-TIER3-BENCHMARK.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md) once the later agent-runtime and policy work is in place.

## Next Phase Readiness

- Plan `15-02` can now wire the explicit agent-runtime seam against one stable tier3 replay contract
- Plan `15-03` can layer memory, retrieval, and context folding onto an existing benchmark surface instead of inventing one midstream
- Plan `15-04` can use the same artifact schema for small-model policy proof and checker work

## Self-Check: PASSED

- PASSED: `rg -n -- '--manifest-json|--mode|agent_mode|retrieval_profile|inference_policy' scripts/run_phase15_tier3_benchmark.py`
- PASSED: `rg -n '"mode": "baseline"|"mode": "candidate"' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json`
- PASSED: `rg -n '## Comparison Contract|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md`
- PASSED: `python3 -m py_compile scripts/run_phase15_tier3_benchmark.py`
- PASSED: `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --fixtures-root tools/apdr/tests/fixtures --mode baseline --output-json /tmp/phase15-benchmark-probe.json --probe-only`

---
*Phase: 15-langchain-langgraph-tier3-intelligence-improvements*
*Completed: 2026-03-29*
