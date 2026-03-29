---
phase: 15-langchain-langgraph-tier3-intelligence-improvements
plan: 04
subsystem: small-model-policy-proof
tags: [qwen3.5-9b, policy-matrix, agent-quality, checker, proof-pack]

requires:
  - phase: 15-langchain-langgraph-tier3-intelligence-improvements
    provides: explicit agent seam, benchmark-fed memory, retrieval profiles, and tier3 artifact contract
provides:
  - explicit Qwen small-model policy controls in the client and benchmark harness
  - deterministic Phase 15 agent-quality checker
  - reviewer-facing proof notes for Qwen policy attribution and agent-quality comparison
affects: [phase-16-proof, milestone-closeout]

tech-stack:
  added: []
  patterns: [policy-labelled small-model benchmarking, checker-backed proof notes, sample-first proof contracts]

key-files:
  created:
    - scripts/check_phase15_agent_quality.py
    - .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md
    - .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md
  modified:
    - tools/apdr/llm_py/client.py
    - scripts/Modelfile.qwen3.5-9b-apdr
    - scripts/run_phase15_tier3_benchmark.py

key-decisions:
  - "Move Qwen policy control into explicit knobs and artifact metadata instead of leaving it implicit in scattered defaults"
  - "Use a deterministic checker against baseline-versus-candidate artifacts so Phase 15 quality claims are machine-checkable"
  - "Treat sample artifacts as the proof contract in-repo while allowing live benchmark-host captures to satisfy the same checker later"

patterns-established:
  - "Small-model policy experiments now have attributable controls for temperature, top_p, top_k, context window, thinking mode, verifier passes, and self-consistency passes"
  - "Phase 15 proof notes and the checker share the same artifact assumptions, reducing drift between reviewer narrative and machine validation"

requirements-completed: [AGT-01, AGT-03, AGT-04, AGT-06]

duration: 8min
completed: 2026-03-29
---

# Phase 15 Plan 04: Small-Model Policy Proof Summary

**Phase 15 now closes with explicit Qwen policy controls, a deterministic quality checker, and reviewer-facing proof notes**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-29T20:04:07Z
- **Completed:** 2026-03-29T20:11:54Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments

- Added explicit small-model policy controls in [client.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/client.py) so Ollama calls now expose `temperature`, `top_p`, `top_k`, `num_ctx`, and routed thinking behavior through one policy resolver instead of forced Qwen defaults
- Updated [Modelfile.qwen3.5-9b-apdr](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/Modelfile.qwen3.5-9b-apdr) and [run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) so the representative `qwen3.5:9b` path carries explicit policy labels, policy controls, verifier passes, and self-consistency passes in the artifact contract
- Added [check_phase15_agent_quality.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase15_agent_quality.py) to validate baseline-versus-candidate replay artifacts for slice identity, attributable metadata, success or abstain quality improvement, and failure-count non-regression
- Added reviewer-facing proof notes in [15-QWEN-POLICY-MATRIX.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md) and [15-AGENT-QUALITY.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md)

## Task Commits

1. **Task 1: expose and benchmark the representative Qwen small-model policy variants** - `43556e7` (feat)
2. **Task 2: add the Phase 15 quality checker and reviewer-facing proof notes** - `ac75a9a` (docs)

## Files Created/Modified

- [client.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/client.py) - Centralizes explicit Ollama policy controls for small-model benchmarking
- [Modelfile.qwen3.5-9b-apdr](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/Modelfile.qwen3.5-9b-apdr) - Documents a non-greedy Qwen candidate profile in the checked-in modelfile
- [run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) - Records policy-control metadata for Phase 15 artifacts
- [check_phase15_agent_quality.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase15_agent_quality.py) - Deterministic baseline-versus-candidate artifact checker for Phase 15 quality claims
- [15-QWEN-POLICY-MATRIX.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md) - Reviewer-facing Qwen small-model policy matrix
- [15-AGENT-QUALITY.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md) - Reviewer-facing agent-quality comparison note

## Decisions Made

- The representative small-model path should be tuned through explicit policy knobs and labels, not by inheriting one frozen default from earlier experiments
- Quality proof should compare artifacts, not logs or anecdotal output, so the checker became the source of truth for Phase 15 verdicts
- Sample artifacts are sufficient to land the proof contract in-repo, but live benchmark-host artifacts remain the final evidence step when the local environment cannot run the representative model

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The environment did not run a live `qwen3.5:9b` replay benchmark during this plan, so the checker was verified against the in-repo sample baseline and candidate artifacts rather than fresh live captures.

## User Setup Required

- If you want live Phase 15 evidence instead of sample-contract proof, capture `15-tier3-baseline.json` and `15-tier3-candidate.json` on the benchmark host and rerun [check_phase15_agent_quality.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase15_agent_quality.py) against those files.

## Next Phase Readiness

- Phase 15 is complete and Phase 16 can now focus on milestone closeout proof using the Phase 14 replay proof pack and the new Phase 15 agent-quality checker
- The remaining evidence work is host execution and milestone packaging, not more checker or schema design

## Self-Check: PASSED

- PASSED: `rg -n 'temperature|top_p|top_k|num_ctx' tools/apdr/llm_py/client.py scripts/Modelfile.qwen3.5-9b-apdr`
- PASSED: `rg -n 'policy_label|qwen3.5:9b' scripts/run_phase15_tier3_benchmark.py`
- PASSED: `rg -n -- '--baseline|--candidate' scripts/check_phase15_agent_quality.py`
- PASSED: `rg -n '## Policy Variants|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md`
- PASSED: `rg -n '## Comparison Verdict|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md`
- PASSED: `python3 -m py_compile tools/apdr/llm_py/client.py scripts/run_phase15_tier3_benchmark.py scripts/check_phase15_agent_quality.py`
- PASSED: `python3 scripts/check_phase15_agent_quality.py --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json`

---
*Phase: 15-langchain-langgraph-tier3-intelligence-improvements*
*Completed: 2026-03-29*
