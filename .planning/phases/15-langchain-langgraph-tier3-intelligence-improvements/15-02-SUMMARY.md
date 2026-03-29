---
phase: 15-langchain-langgraph-tier3-intelligence-improvements
plan: 02
subsystem: explicit-agent-runtime
tags: [agent-mode, langgraph, langchain, abstain-tracing, tier3-runtime]

requires:
  - phase: 15-langchain-langgraph-tier3-intelligence-improvements
    provides: deterministic tier3 replay harness and bounded artifact contract
provides:
  - Rust-to-Python tier3 request fields for agent mode, tool profile, retrieval profile, and policy label
  - one explicit tier3 agent seam covering manual, LangGraph, and LangChain runtime paths under a shared tool contract
  - inspectable abstain and failure reasons propagated back into Rust notes and resolution responses
affects: [phase-15-plan-03, phase-15-plan-04, phase-16-proof]

tech-stack:
  added: []
  patterns: [lazy optional agent runtimes, shared tool contracts, abstain-first failure handling]

key-files:
  created: []
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/resolver/tier3_llm/context.rs
    - tools/apdr/src/resolver/tier3_llm/core.rs
    - tools/apdr/llm_py/models.py
    - tools/apdr/llm_py/actions/resolve.py
    - tools/apdr/llm_py/actions/react_agent.py
    - tools/apdr/llm_py/client.py
    - tools/apdr/llm_py/tests/test_resolve_agentic.py

key-decisions:
  - "Keep the old draft-generation path available as `agent_mode=direct`, but require explicit agent modes for benchmarked manual, LangGraph, or LangChain runs"
  - "Treat LangChain and LangGraph as optional runtimes with lazy imports and clean manual fallback instead of hard dependencies"
  - "When an agent cannot verify a mapping, preserve unresolved imports and an abstain reason rather than backfilling guessed identity mappings"

patterns-established:
  - "Tier3 benchmark runs can now attribute behavior through request metadata instead of code edits"
  - "Agent runtime selection and tool-surface selection share one contract across manual and optional framework-backed paths"

requirements-completed: [AGT-01, AGT-04]

duration: 9min
completed: 2026-03-29
---

# Phase 15 Plan 02: Explicit Agent Runtime Summary

**Phase 15 now has one explicit tier3 agent seam with attributable runtime settings and clean abstain behavior**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-29T19:48:03Z
- **Completed:** 2026-03-29T19:57:10Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments

- Added tier3 request fields in [lib.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/lib.rs), [context.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/resolver/tier3_llm/context.rs), [core.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/resolver/tier3_llm/core.rs), and [models.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/models.py) so `agent_mode`, `tool_profile`, `retrieval_profile`, and `policy_label` cross the Rust-to-Python boundary without patching code between runs
- Refactored [resolve.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/resolve.py) so explicit agent modes run through one benchmarkable seam, draft mappings become advisory context for the seam, and abstain or failure reasons are preserved in the response metadata
- Rebuilt [react_agent.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/react_agent.py) around a shared tool contract that can execute a manual loop, `create_react_agent`, or `create_agent`, with lazy optional-runtime fallback via [client.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/client.py)
- Extended [test_resolve_agentic.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/tests/test_resolve_agentic.py) to cover explicit `agent_mode` routing and abstain propagation

## Task Commits

1. **Task 1: propagate explicit tier3 agent configuration across the Rust-to-Python boundary** - `1f4a6c3` (feat)
2. **Task 2: implement the explicit agent seam and clean abstain behavior in the Python resolver** - `a8356fe` (feat)

## Files Created/Modified

- [lib.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/lib.rs) - Adds env-configurable tier3 agent settings to `ResolveConfig`
- [context.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/resolver/tier3_llm/context.rs) - Injects tier3 agent settings into the Python IPC request
- [core.rs](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/src/resolver/tier3_llm/core.rs) - Records agent metadata plus abstain and failure reasons from Python responses
- [models.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/models.py) - Defines request and response metadata for explicit agent benchmarking
- [resolve.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/resolve.py) - Routes explicit agent modes through one seam and preserves inspectable unresolved outcomes
- [react_agent.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/actions/react_agent.py) - Implements manual, LangGraph, and LangChain agent modes under one tool contract
- [client.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/client.py) - Adds lazy optional LangChain model construction with `ImportError` handling
- [test_resolve_agentic.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr/llm_py/tests/test_resolve_agentic.py) - Adds unit coverage for explicit agent configuration behavior

## Decisions Made

- The benchmarkable seam is opt-in through `agent_mode`; direct draft generation remains available for baseline comparisons, but explicit agent runs no longer hide behind fallback-only control flow
- Optional framework-backed agent runtimes should degrade to the manual tool loop with an inspectable reason rather than crash or silently disappear
- Tier3 failures should surface as abstain or failure metadata so later replay artifacts can distinguish real abstentions from guessed successes

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- `python3 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py` could not run in this shell because `pytest` is not installed.
- `cargo build` completed successfully but emitted unrelated existing warnings from `targeted_recovery.rs` about dead-code fields.

## User Setup Required

None for the plan output itself. To benchmark the new seam later, set `APDR_TIER3_AGENT_MODE`, `APDR_TIER3_TOOL_PROFILE`, `APDR_TIER3_RETRIEVAL_PROFILE`, and `APDR_TIER3_POLICY_LABEL` before invoking APDR or the Phase 15 harness.

## Next Phase Readiness

- Plan `15-03` can now add benchmark-fed memory and context folding on top of an explicit runtime seam instead of a hidden fallback chain
- Plan `15-04` can benchmark small-model policy variants against one shared agent runtime contract
- Phase 16 can attribute gains to runtime mode and tool-surface changes because those settings are now traceable in the request/response path

## Self-Check: PASSED

- PASSED: `rg -n 'agent_mode|tool_profile|policy_label' tools/apdr/src/lib.rs tools/apdr/src/resolver/tier3_llm/context.rs tools/apdr/src/resolver/tier3_llm/core.rs tools/apdr/llm_py/models.py`
- PASSED: `rg -n 'agent_mode|abstain|tool_profile|create_agent|create_react_agent' tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/actions/react_agent.py`
- PASSED: `rg -n 'ImportError|lazy' tools/apdr/llm_py/client.py`
- PASSED: `cargo build --manifest-path tools/apdr/Cargo.toml`
- PASSED: `python3 -m py_compile tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/actions/react_agent.py tools/apdr/llm_py/models.py tools/apdr/llm_py/client.py`
- PASSED: `python3 -c "import sys; sys.path.insert(0, 'tools/apdr'); import llm_py.actions.resolve, llm_py.actions.react_agent; print('resolver-import-ok')"`

---
*Phase: 15-langchain-langgraph-tier3-intelligence-improvements*
*Completed: 2026-03-29*
