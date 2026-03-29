---
phase: 15-langchain-langgraph-tier3-intelligence-improvements
plan: 03
subsystem: benchmark-fed-memory-and-context-folding
tags: [active-learning, failure-memory, retrieval-profile, context-folding, benchmark-memory]

requires:
  - phase: 15-langchain-langgraph-tier3-intelligence-improvements
    provides: deterministic tier3 benchmark harness and explicit agent runtime seam
provides:
  - inspectable benchmark-fed success and failure memory updates
  - retrieval-profile-aware context assembly and benchmark-context summarization
  - resolver-side use of active-learning success memory, failure memory, and context folding
affects: [phase-15-plan-04, phase-16-proof]

tech-stack:
  added: []
  patterns: [artifact-fed memory, retrieval-profile context selection, benchmark-context summarization]

key-files:
  created: []
  modified:
    - tools/apdr/llm_py/active_learning.py
    - tools/apdr/llm_py/failure_memory.py
    - tools/apdr/llm_py/rag.py
    - tools/apdr/llm_py/prompts.py
    - tools/apdr/llm_py/actions/resolve.py
    - scripts/run_phase15_tier3_benchmark.py

key-decisions:
  - "Persist benchmark-fed successes as inspectable JSON memory and failures as enriched failure-memory records instead of adding new static rule tables"
  - "Treat retrieval profile as the switch for selecting, folding, and summarizing context rather than using one prompt shape for every hard case"
  - "Keep benchmark artifacts responsible for labeling both retrieval and memory strategy so later accuracy changes remain attributable"

patterns-established:
  - "Replay artifacts can now update memory snapshots directly through the harness"
  - "Tier3 context assembly now prefers relevant evidence and summarized benchmark context over raw prompt growth"

requirements-completed: [AGT-02, AGT-05]

duration: 7min
completed: 2026-03-29
---

# Phase 15 Plan 03: Benchmark-Fed Memory Summary

**Phase 15 now turns replay outcomes into inspectable memory and folds retrieved evidence into the live tier3 prompt path**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-29T19:57:10Z
- **Completed:** 2026-03-29T20:04:07Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments

- Extended [active_learning.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/active_learning.py) so Phase 15 artifacts can be mined into inspectable `active-learning` successes and failures, written back into success memory JSON and failure memory TSV updates
- Extended [failure_memory.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/failure_memory.py) with `source` and exported timestamped records so replay-fed failure context stays attributable
- Added [rag.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/rag.py) retrieval helpers that select relevant lines, fold long context, and summarize benchmark context under a named retrieval profile
- Updated [resolve.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/resolve.py) and [prompts.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/prompts.py) so tier3 resolution now consumes active-learning success memory, failure memory, retrieval-profile context selection, and summarized benchmark context
- Extended [run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) so artifacts label `memory_profile` and can optionally update benchmark-fed memory snapshots through the harness

## Task Commits

1. **Task 1: turn replay outcomes into inspectable tier3 memory** - `8bd3836` (feat)
2. **Task 2: add retrieval, summarization, and context folding to the resolver path** - `1c4d517` (feat)

## Files Created/Modified

- [active_learning.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/active_learning.py) - Extracts artifact-fed successes and failures and updates inspectable memory stores
- [failure_memory.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/failure_memory.py) - Records timestamped failure provenance and exports replay-relevant records
- [run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py) - Labels memory strategy and optionally updates memory from emitted artifacts
- [rag.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/rag.py) - Selects and folds relevant context under a retrieval profile
- [prompts.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/prompts.py) - Surfaces retrieval profile in the package-resolution prompt
- [resolve.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/resolve.py) - Uses success-memory context, retrieval-profile context assembly, and summarized benchmark context

## Decisions Made

- Success memory belongs in a readable JSON store because it needs richer replay metadata than the failure TSV format can comfortably hold
- Failure memory keeps the lightweight TSV shape, but it now carries replay provenance so later retrieval does not confuse benchmark-fed failures with one-off runtime attempts
- Context growth should be governed by retrieval and folding policies; larger prompts are now a measured consequence of profile choice, not the default path

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The first smoke test of artifact-fed memory used the wrong temp-path view and returned `FileNotFoundError`; rerunning against `/tmp/phase15-benchmark-memory-probe.json` fixed it.
- An inline shell smoke test initially used backticks inside the Python string and triggered shell command substitution; rerunning with plain text fixed it.

## User Setup Required

None for the plan output itself. To exercise the new memory update path during replay runs, pass `--memory-profile` and `--memory-cache-path` to [run_phase15_tier3_benchmark.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/run_phase15_tier3_benchmark.py).

## Next Phase Readiness

- Plan `15-04` can now benchmark small-model policies against explicit agent, retrieval, and memory-profile labels instead of collapsing them into one opaque experiment
- Phase 16 can attribute quality changes to agent runtime, retrieval profile, memory profile, and inference policy on the same replay slice

## Self-Check: PASSED

- PASSED: `rg -n 'active-learning|failure|timestamp' tools/apdr/llm_py/active_learning.py tools/apdr/llm_py/failure_memory.py`
- PASSED: `rg -n 'retrieval_profile|summar|compress_benchmark_context' tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/prompts.py tools/apdr/llm_py/rag.py`
- PASSED: `python3 -m py_compile tools/apdr/llm_py/active_learning.py tools/apdr/llm_py/failure_memory.py tools/apdr/llm_py/rag.py tools/apdr/llm_py/prompts.py tools/apdr/llm_py/actions/resolve.py scripts/run_phase15_tier3_benchmark.py`
- PASSED: `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --fixtures-root tools/apdr/tests/fixtures --mode baseline --output-json /tmp/phase15-benchmark-memory-probe.json --probe-only --memory-profile replay-outcomes --memory-cache-path /tmp/phase15-memory-cache`
- PASSED: `python3 -c "import sys; sys.path.insert(0, 'tools/apdr'); from llm_py.active_learning import extract_benchmark_memory; data = extract_benchmark_memory('/tmp/phase15-benchmark-memory-probe.json'); print({'successes': len(data['successes']), 'failures': len(data['failures'])})"`
- PASSED: `python3 -c "import sys; sys.path.insert(0, 'tools/apdr'); from llm_py.rag import assemble_retrieval_context; failure = 'PREVIOUS FAILURE: import johnny mapped to johnny failed.'; result = assemble_retrieval_context(import_names=['johnny'], context=['Known package: johnny-cache', 'reverse match johnny-cache'], failure_context=failure, benchmark_context='x' * 10000, retrieval_profile='failure-memory+summary-fold'); print(result['retrieval_profile']); print(len(result['context'])); print(result['benchmark_context'][:24])"`

---
*Phase: 15-langchain-langgraph-tier3-intelligence-improvements*
*Completed: 2026-03-29*
