# Phase 15: LangChain/LangGraph Tier3 Intelligence Improvements - Research

**Researched:** 2026-03-29
**Domain:** Improving APDR's tier3 success through benchmarked agent behavior, context engineering, and small-model policy instead of new deterministic recovery tables
**Confidence:** High

## Summary

Phase 15 should not start by adding more import-to-package lore to prompts or more deterministic retry rules. The repo already has the beginnings of the right architecture: `tools/apdr/llm_py/actions/resolve.py` can do two-pass reasoning, self-consistency, self-refine, and a ReAct fallback; `tools/apdr/llm_py/actions/react_agent.py` already has a LangGraph seam plus a manual tool loop; `tools/apdr/llm_py/active_learning.py` and `tools/apdr/llm_py/failure_memory.py` already exist as memory primitives; and Phase 13 and Phase 14 now provide the run-contract and replay infrastructure needed to measure real accuracy deltas on a locked slice.

The main problem is not absence of features. It is that the current tier3 path still mixes partial agentic behavior with hardcoded prompt taxonomy and globally fixed model defaults. As of March 29, 2026, the repo still defaults to `APDR_NUM_CTX=16384`, forces `think=True` for Qwen-family models in key Ollama calls, and hardcodes effectively greedy decoding in the checked-in `scripts/Modelfile.qwen3.5-9b-apdr` (`temperature 0`, `top_k 1`, `top_p 0.0`). The LangGraph branch in `react_agent.py` also lacks durable memory, dynamic tool selection, and benchmark-fed retrieval, while the existing memory helpers are not yet part of the main benchmark loop.

The strongest Phase 15 shape is therefore four bounded moves. First, create a benchmark and ablation harness for tier3 on the locked replay slice so success-rate claims are attributable to agent behavior, context settings, and small-model policy. Second, replace the current "manual path first, LangGraph if lucky" design with one explicit benchmarked agent-runtime seam that can compare the current manual loop against a LangChain `create_agent` or LangGraph-backed path using the same tool surface and the same replay inputs. Third, add benchmark-fed memory, retrieval, summarization, and context folding so the agent sees the right evidence rather than more raw prompt bulk. Fourth, benchmark a representative sub-10GB-VRAM path such as `qwen3.5:9b` with routed thinking, non-greedy decoding, smaller tool surfaces, and verifier or self-consistency policies instead of inheriting one global default.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| AGT-01 | APDR's tier3 path can use an explicit tool-calling plus critique-and-refinement agent loop for hard cases, with LangChain/LangGraph adoption evaluated on the benchmarked path | `react_agent.py` already has a LangGraph seam and tools; LangChain's current agent docs recommend `create_agent`, which is graph-based on LangGraph. |
| AGT-02 | APDR can feed benchmark outcome feedback into later tier3 attempts through inspectable memory, reflection, or retrieval | `active_learning.py` and `failure_memory.py` exist, but they are not yet integrated into replay-slice attempts or store-backed retrieval. |
| AGT-03 | APDR improves the number of replay-slice cases the LLM path resolves successfully compared with the v2.2 baseline | Phase 14 now provides a locked replay lane and proof contract, so Phase 15 can compare baseline and candidate agent behavior on the same slice. |
| AGT-04 | When the agent cannot solve a case, APDR preserves inspectable failure reasons and abstains cleanly instead of silently masking failure | `resolve.py` and `react_agent.py` already moved away from identity backfill; Phase 15 should finish this with explicit trace and failure-reason artifacts. |
| AGT-05 | APDR can increase effective tier3 context through benchmarked context engineering rather than raw prompt growth alone | LangChain's current context-engineering and memory guidance plus Ollama's context controls support retrieval, summarization, memory, and selective `num_ctx` scaling. |
| AGT-06 | APDR can benchmark and improve a representative sub-10GB-VRAM local-model path through model-specific inference policy | The repo already defaults to `qwen3.5:9b`, but current defaults are static and not yet benchmarked against thinking routing, decoding changes, or tool-surface reduction. |

## Evidence That Should Drive Planning

### The current resolver is already partially agentic, but the main path is still inconsistent

`tools/apdr/llm_py/actions/resolve.py` now chooses two-pass reasoning for single imports, self-consistency for multi-import cases, self-refine review, and a tool-using fallback when confidence or PyPI verification is weak. That is a good base, but it is not yet a clean agent runtime. The main path still starts from a prompt-heavy direct completion and only later falls into the tool loop. Phase 15 should make the agent seam explicit and benchmarkable instead of letting it remain a layered fallback pile.

### The LangGraph seam exists, but the current implementation is too shallow

`tools/apdr/llm_py/actions/react_agent.py` can try `langgraph.prebuilt.create_react_agent` and otherwise falls back to a manual `TOOL_CALL:` loop. The current LangGraph branch uses a small tool set, but it has no checkpointer, no store-backed long-term memory, no retrieval over prior benchmark outcomes, and no dynamic tool selection. If Phase 15 uses LangChain or LangGraph, it should do so for durable state, better tool routing, and cleaner evaluation, not just for a library badge.

### The repo already has one LangChain implementation pattern worth reusing

`tools/apdr/docker_agent/agents/llm_utils.py` already uses LangChain parsing patterns with Pydantic schemas. That means Phase 15 can reuse the repo's existing LangChain conventions instead of introducing an unrelated agent stack. The better fit is to extend the resolver-side agent seam with similar structured-output discipline and explicit configuration, not to create a second disconnected Python LLM subsystem.

### Memory exists in fragments, but not yet in the benchmark loop

`tools/apdr/llm_py/failure_memory.py` provides persistent cross-run "do not retry this mapping" state. `tools/apdr/llm_py/active_learning.py` can extract passed and failed mappings from benchmark runs and update seed or failure memory. But these helpers are not yet part of the Phase 14 replay flow or a retrieval layer for tier3. Phase 15 should turn them into benchmark-fed memory and retrieval inputs instead of leaving them as side utilities.

### The current prompt strategy is heavy on embedded taxonomy and examples

`tools/apdr/llm_py/prompts.py` still carries large banks of naming-taxonomy rules, failure-pair examples, local-module exclusions, and framework allowlists directly in the system prompt. Some of that guidance may still be useful, but the phase goal is to improve inherent agent performance, not extend deterministic knowledge tables. Planning should assume prompt slimming and tool-backed verification over time, not further prompt expansion as the primary strategy.

### Effective context is now measurable in the benchmark metadata

Phase 13 added `llm_context_window`, `inference_policy`, and related run-contract metadata in `benchmark_ui/run_contract.py`. That means Phase 15 can finally benchmark context-window changes, thinking-mode routing, and decoding-policy changes without losing attribution. Context-engineering improvements should therefore be planned together with replay benchmarking, not as invisible prompt tweaks.

### The current small-model defaults are likely suppressing Qwen-family performance

`tools/apdr/llm_py/client.py` sets `think=True` whenever `"qwen3"` appears in the model name for major Ollama call paths. The checked-in `scripts/Modelfile.qwen3.5-9b-apdr` also hardcodes deterministic sampling (`temperature 0`, `top_k 1`, `top_p 0.0`). As of March 29, 2026, Qwen's current quickstart examples show non-zero `temperature`, `top_p`, `top_k`, and explicit `enable_thinking` control rather than one forced global mode. Phase 15 should benchmark routed thinking and non-greedy decoding for hard cases instead of assuming today's hardcoded defaults are optimal.

### Official guidance supports retrieval, memory, and dynamic tool surfaces over raw prompt growth

LangChain's current context-engineering docs explicitly frame context engineering as providing the right information and tools for the next step, and warn that too many tools can overload the model and increase errors. The retrieval docs frame retrieval as the answer to finite context and static knowledge, and the hybrid or agentic RAG patterns map well onto ambiguous import-resolution cases. Phase 15 should therefore plan for smaller dynamic tool surfaces, benchmark-fed retrieval, and summarization rather than simply raising `num_ctx` everywhere.

### Long-context research still warns against naive context inflation

As of the current arXiv paper version, *Lost in the Middle* finds that relevant information placed in the middle of long contexts can still degrade performance significantly, even for long-context models. That means bigger context windows are useful only when paired with retrieval, ordering, summarization, or context folding. Phase 15 should treat larger context as a measured policy choice, not as the default fix for poor accuracy.

### Tool-interactive critique and reflection match the repo's problem structure

ReAct, Self-Refine, Reflexion, CRITIC, and related work all point in the same direction: ambiguous problems improve when the model can use tools, critique its output, revise against external feedback, and retain linguistic memory of prior failures. APDR's tier3 problem is precisely a tool-verifiable mapping task, so these methods fit the repo naturally. The plan should prefer tool-backed critique, verifier passes, and replay-fed reflections over more static hardcoded mapping banks.

## Latest Source Alignment (March 29, 2026)

- LangChain agent docs state that `create_agent` builds a graph-based agent runtime using LangGraph.
- LangChain context-engineering docs warn that too many tools can overwhelm the model and increase errors, which supports dynamic tool-surface reduction for weaker local models.
- LangChain retrieval docs frame retrieval as the mechanism for finite-context and static-knowledge limitations, which fits APDR's need to pull only relevant benchmark memory and package evidence.
- Ollama structured-output docs support passing JSON schema to the `format` field and validating the response with Pydantic, which aligns with APDR's existing structured-output path.
- Ollama context-length docs continue to support explicit context-window configuration through environment or model settings, which should now be benchmarked via Phase 13 run metadata.
- Qwen's current quickstart examples use non-zero `temperature`, `top_p`, and `top_k` and show explicit thinking control rather than a forced universal policy, which supports routed policy benchmarking for `qwen3.5:9b`.

## Implementation Recommendations

### 1. Establish the Phase 15 benchmark and ablation harness first

Recommended files:

- `scripts/run_phase15_tier3_benchmark.py`
- `scripts/check_phase15_agent_quality.py`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-qwen-policy-matrix.md`

Recommended responsibilities:

- reuse the Phase 14 replay slice and proof metadata rather than inventing a new benchmark surface
- isolate tier3 or LLM-path cases and persist success, abstain, failure reasons, prompts issued, LLM duration, context window, and inference policy
- support side-by-side baseline versus candidate comparison for the current manual path, LangGraph or LangChain path, and the representative small-model policy matrix

### 2. Replace the current fallback pile with one explicit benchmarked agent seam

Recommended files:

- `tools/apdr/llm_py/actions/resolve.py`
- `tools/apdr/llm_py/actions/react_agent.py`
- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/tests/test_resolve_agentic.py`
- `tools/apdr/llm_py/tests/test_llm_integration.py`

Recommended responsibilities:

- create one resolver configuration seam that can run the current manual loop or a LangChain/LangGraph-backed loop on the same inputs
- keep tool calling, critique or refinement, candidate verification, and abstain behavior explicit and traceable
- preserve clean unresolved outputs and failure reasons instead of fabricating success
- ensure LangChain adoption is judged by replay-slice results, not assumed to be better by default

### 3. Add benchmark-fed memory, retrieval, and context folding

Recommended files:

- `tools/apdr/llm_py/active_learning.py`
- `tools/apdr/llm_py/failure_memory.py`
- `tools/apdr/llm_py/rag.py`
- `tools/apdr/llm_py/prompts.py`
- `tools/apdr/llm_py/actions/resolve.py`

Recommended responsibilities:

- mine prior replay outcomes into inspectable failure and success memory that later attempts can query
- retrieve only relevant benchmark memory, package evidence, seed matches, and reverse-index context for the current import set
- add summarization or context folding so long benchmark logs and prior failures do not bloat the live prompt
- make context-window growth selective and attributable through existing run-contract metadata

### 4. Benchmark model-specific policy for the representative small local model

Recommended files:

- `tools/apdr/llm_py/client.py`
- `scripts/Modelfile.qwen3.5-9b-apdr`
- `benchmark_ui/run_contract.py`
- `scripts/run_phase15_tier3_benchmark.py`

Recommended responsibilities:

- expose explicit policy knobs for thinking versus non-thinking routing, temperature, top_p, top_k, self-consistency passes, verifier passes, and tool-surface size
- benchmark `qwen3.5:9b` under multiple bounded policies instead of forcing one global default
- record the chosen policy in run-contract metadata so gains are attributable
- prefer per-case or per-difficulty routing over one universal high-cost reasoning mode

## Validation Architecture

### Quick checks

- `python3 -m py_compile tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/actions/react_agent.py tools/apdr/llm_py/client.py tools/apdr/llm_py/active_learning.py`
- `python3 -m unittest benchmark_ui.test_run_contract`
- `python3 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py -q`

### Artifact checks

- `rg -n 'create_agent|LangGraph|tool|memory|retrieve|summar' tools/apdr/llm_py/actions tools/apdr/llm_py`
- `rg -n 'llm_context_window|inference_policy|build_profile' benchmark_ui/run_contract.py scripts/run_phase15_tier3_benchmark.py`
- `rg -n 'qwen3.5:9b|temperature|top_p|top_k|think|enable_thinking' tools/apdr/llm_py/client.py scripts/Modelfile.qwen3.5-9b-apdr`

### Phase-close checks

- `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json --mode baseline`
- `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate.json --mode candidate`
- `python3 scripts/check_phase15_agent_quality.py --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate.json`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/research/SUMMARY.md`
- `.planning/phases/14-macos-execution-path-optimization/14-03-SUMMARY.md`
- `.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md`
- `benchmark_ui/run_contract.py`
- `scripts/check_phase14_macos_replay.py`
- `tools/apdr/llm_py/actions/resolve.py`
- `tools/apdr/llm_py/actions/react_agent.py`
- `tools/apdr/llm_py/client.py`
- `tools/apdr/llm_py/active_learning.py`
- `tools/apdr/llm_py/failure_memory.py`
- `tools/apdr/llm_py/prompts.py`
- `tools/apdr/llm_py/rag.py`
- `tools/apdr/docker_agent/agents/llm_utils.py`
- `scripts/Modelfile.qwen3.5-9b-apdr`

## Out of Scope For This Phase

- adding more deterministic recovery tables or expanding prompt-side mapping lore as the main strategy
- broad provider replacement or switching away from the current Ollama-backed path before the existing agent loop is fairly benchmarked
- milestone closeout packaging for macOS and Windows proof, which belongs to Phase 16
- broad benchmark UI redesign
- reopening the superseded v2.1 milestone as active scope

## Primary Sources Consulted

- LangChain agents
- LangChain context engineering
- LangChain short-term memory
- LangChain long-term memory
- LangChain retrieval
- LangGraph memory
- Ollama structured outputs
- Ollama tool calling
- Ollama context length
- Qwen quickstart
- ReAct
- Self-Refine
- Reflexion
- CRITIC
- Lost in the Middle

---
*Research created: 2026-03-29*
*Phase: 15-langchain-langgraph-tier3-intelligence-improvements*
