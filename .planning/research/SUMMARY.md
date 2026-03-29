# Project Research Summary

**Project:** APDR
**Domain:** LLM-agent quality and macOS benchmark performance
**Researched:** 2026-03-28
**Confidence:** HIGH

## Executive Summary

The research points to a milestone that should improve APDR along two tightly linked axes: better tier3 agent behavior and a much cleaner macOS benchmark inner loop. The repo already contains several good building blocks, including `uv`-first env setup, macOS CoW reuse for validated envs, a PGO build script, and an existing LangGraph seam in the Python resolver. The missing pieces are better measurement, cleaner execution modes on macOS, and a more agentic LLM loop that learns from benchmark feedback instead of growing deterministic fix tables.

For macOS performance, the highest-value path is not a broad rewrite. It is a measurement-first approach: record architecture, cache state, context-window settings, and decoding policy, add per-stage timings, separate fast env runs from slower Docker proof runs, and tighten Docker Desktop settings when Docker is required. For LLM quality, the strongest pattern is a set of test-time agent behaviors: tool use, critique/refinement, candidate verification, retrieval, and reflection memory grounded in real benchmark outcomes. The official LangChain/LangGraph guidance also reinforces that reliability failures are often context problems rather than pure model-capability problems, which makes context engineering a first-class part of this milestone rather than an afterthought. For sub-10GB-VRAM local models, the research also points to a second lever beyond context: model-specific inference policy, because smaller reasoning-capable models are more sensitive to decoding, tool overload, and prompt shape than a larger frontier model.

## Key Findings

### Recommended Stack

The current stack is still the right one, but it needs sharper defaults and stronger measurement:

**Core technologies:**

- Native Apple silicon toolchain: avoid Rosetta on hot paths.
- Xcode Instruments: profile CPU, disk, memory, and responsiveness on macOS.
- `uv`: fast env creation and package installation with a global cache.
- Docker Desktop with Docker VMM and VirtioFS where compatible: keep Docker usable without accepting legacy macOS slow paths.
- Rust benchmark builds with PGO and profile tuning: improve APDR runtime when it is a real bottleneck.
- LangChain `create_agent`: a higher-level agent loop that already runs on top of LangGraph and is now the recommended high-level starting point for common tool-calling loops.
- LangGraph: durable execution, state graphs, checkpointers, stores, and subgraphs for the lower-level pieces APDR already partially uses.
- Qwen-family small local models: benchmark a representative sub-10GB-VRAM path such as `qwen3.5:9b`, which Ollama currently distributes as a 6.6GB `Q4_K_M` model with tool and thinking capability.
- Ollama structured outputs and tool calling: support an agent loop without changing providers.
- Ollama context controls: `num_ctx`, `OLLAMA_CONTEXT_LENGTH`, and `Modelfile` overrides to increase raw context only where the benchmark tradeoff is favorable.

### Expected Features

**Must have (table stakes):**

- Native-arch run metadata - users need to know whether a run used arm64, Rosetta, env, or Docker.
- Per-stage benchmark timings - total runtime alone is not actionable.
- Fast canonical slice / replay mode - required for practical local tuning.
- Backend-intent modes - separate fast env iteration from Docker proof runs.
- Stable benchmark evidence - saved runs must be comparable and reviewer-readable.

**Should have (competitive):**

- Tool-calling tier3 agent loop - the ReAct pattern improves decision quality by interleaving reasoning with external actions.
- Critique-and-refinement passes - Self-Refine shows that iterative feedback and revision can improve one-shot outputs at test time.
- Candidate verification on ambiguous cases - Self-Consistency shows gains from evaluating multiple reasoning paths instead of taking the greedy first answer.
- Reflection memory from benchmark feedback - Reflexion shows that linguistic feedback and episodic memory can improve later attempts without weight updates.
- LangChain/LangGraph context engineering - short-term memory, long-term store-backed memory, retrieval, and summarization give APDR a way to expand effective context without blindly increasing every prompt.
- Benchmarked context-window scaling - hard cases can use larger Ollama context settings when it improves accuracy enough to justify prompt-eval and memory costs.
- Small-model inference policy - smaller local models should benchmark model-specific decoding, thinking-mode routing, tighter tool sets, and verifier or self-debug passes instead of inheriting larger-model defaults.
- macOS performance proof pack - architecture, backend, cache state, and stage deltas in one place.

**Defer (v2+):**

- Cross-machine normalization and more advanced benchmark-slice mining.

### Architecture Approach

The cleanest v2.2 architecture is a three-layer flow: measurement, execution, and state/cache. The benchmark runner should stamp every run with macOS-specific metadata, context-window configuration, and inference-policy settings before the APDR CLI starts. The execution path should split into two lanes: a native env-fast lane for inner-loop iteration and a Docker-proof lane for selected parity runs. The LLM path should become a LangChain/LangGraph-backed tool-calling loop with benchmark-fed memory and retrieval rather than a single structured guess followed by deterministic cleanup. For the representative small-model path, the architecture should also support model-specific routing between cheaper direct modes and more expensive thinking or verifier modes instead of paying the same test-time-compute cost on every case.

**Major components:**

1. Benchmark control plane - owns run intent, saved metadata, and summaries.
2. Native APDR execution layer - owns env validation, Docker fallback, and benchmark runtime.
3. LLM agent service - owns tool use, critique, retrieval, summarization, reflection, memory updates, and model-specific inference routing.

### Critical Pitfalls

1. **Mixed-architecture benchmarking** - record native vs Rosetta state in every run.
2. **Docker file-sharing overhead on macOS** - keep broad host bind mounts out of the hot path.
3. **Warm/cold state confusion** - do not compare runs unless cache and model state are explicit.
4. **Deterministic-rule sprawl** - move quality gains into the agent loop, not new fix tables.
5. **Raw-context inflation without attribution** - larger contexts can help, but they also increase cost, latency, and memory pressure; record context settings and benchmark the tradeoff.
6. **One-size-fits-all inference defaults** - smaller local models can degrade badly under the wrong decoding policy, tool surface, or prompt shape, so benchmark inference policy instead of assuming a single global default.
7. **Total-runtime-only measurement** - persist stage timings so regressions are attributable.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 13: Measurement and Run-Contract Hardening

**Rationale:** Every later claim about LLM or macOS performance depends on trustworthy run metadata.
**Delivers:** Native-arch metadata, cache-state labels, per-stage timing fields, and explicit run intents.
**Addresses:** Table-stakes measurement features from research.
**Avoids:** Mixed-arch and warm/cold comparison pitfalls.

### Phase 14: macOS Execution-Path Optimization

**Rationale:** The inner loop has to get fast before the team can learn efficiently from agent changes.
**Delivers:** Cleaner env-fast defaults on macOS, Docker proof-path tuning, and benchmark-side detection of slow platform settings.
**Uses:** `uv`, Docker VMM / VirtioFS guidance, Cargo timings, and existing APFS CoW env reuse.
**Implements:** The two-lane execution pattern from architecture research.

### Phase 15: LangChain/LangGraph Tier3 Intelligence Improvements

**Rationale:** Once the benchmark loop is fast enough, APDR can improve success rate through behavior rather than rule growth.
**Delivers:** LangChain/LangGraph agent-loop evaluation, tool-calling resolver flow, structured critique/refine loop, candidate verification for ambiguous cases, benchmark-fed reflection memory, a benchmarked strategy for larger effective context, and a representative small-model inference-policy track.
**Uses:** LangChain `create_agent`, LangGraph memory/store patterns, retrieval, summarization, Ollama tool calling plus context controls, and model-specific decoding or think-mode routing on the chosen small local model.
**Implements:** The agentic tier3 loop, context-engineering guidance, and small-model optimization findings from the official docs and papers.

### Research-backed accuracy methods

- **ReAct:** interleave reasoning and tool use so the model can inspect external evidence before committing to a dependency mapping.
- **Self-Refine:** let the model critique and revise its own draft mapping instead of treating the first answer as final.
- **Self-Consistency:** sample or compare multiple candidate reasoning paths on ambiguous imports and select the best-supported result.
- **Reflexion:** store benchmark-grounded reflections or memory so later attempts can improve without adding new deterministic rules.

### LangChain and LangGraph findings

- **LangChain is the current high-level recommendation for common agent loops:** its `create_agent` runtime already sits on top of LangGraph, which makes it a good fit for APDR's tier3 path if it benchmarks better than the current manual loop.
- **LangGraph is the lower-level fit for durable stateful workflows:** its checkpointers, stores, and subgraphs are directly relevant to APDR's existing resolver and Docker-agent architecture.
- **Context engineering is explicitly framed as a primary reliability lever:** the official docs state that many agent failures come from missing or poorly managed context rather than pure model incapability.
- **Larger raw context is not enough by itself:** the short-term memory guidance warns that models still perform poorly over long contexts, so trimming, summarization, and selective retrieval matter even when a model can technically accept more tokens.
- **Dynamic tool selection matters for accuracy:** the official agent docs warn that too many tools can overload context and increase errors, which supports a smaller, task-specific tool surface in APDR tier3.

### Context-size findings

- **Increase effective context first:** use retrieval over benchmark artifacts, seed knowledge, failure memory, and PyPI/package metadata so the model sees the right context rather than the entire context.
- **Use summarization and checkpointed state for long runs:** LangChain middleware and LangGraph memory patterns provide built-in ways to compress history and preserve only what remains decision-relevant.
- **Increase raw context only with measurement:** Ollama's official docs recommend at least 64k context for agents, web search, and coding tools, but also warn that larger context increases memory requirements and can hurt performance if the model offloads to CPU.
- **Record context-window settings in benchmark evidence:** otherwise it becomes impossible to tell whether a gain came from better agent behavior, better context engineering, or simply a wider `num_ctx`.

### Small-model findings

- **A representative small local model is worth optimizing directly:** Ollama currently exposes `qwen3.5:9b` as a 6.6GB `Q4_K_M` model with thinking and tool capability, which makes it a realistic benchmark target for the repo's local path.
- **Qwen-family models want model-specific decoding rather than greedy defaults:** the official Qwen quickstart recommends non-zero temperature and explicitly says not to use greedy decoding for thinking mode because it can hurt performance and cause repetition. It is an inference from these Qwen-family docs plus the current repo settings that APDR should benchmark whether the current zero-temperature defaults are holding back Qwen-family small-model performance.
- **Thinking should be routed, not forced globally:** Qwen's official guidance supports both thinking and non-thinking modes, including turn-level switches. That fits APDR's need to reserve more expensive reasoning for only the harder tier3 cases.
- **Self-debug and tool-interactive critique are particularly attractive for weaker models:** Teaching LLMs to Self-Debug and CRITIC both show that revising with explanations or tool feedback can materially improve results without requiring a larger base model.
- **Long context still needs pruning and placement discipline:** Lost in the Middle shows that simply extending context can fail when the relevant information is buried in the middle, which strengthens the case for retrieval, context folding, and verifier-oriented prompts over raw prompt growth.
- **If test-time methods plateau, later training is still viable on modest hardware:** QLoRA shows that quantized PEFT can adapt much larger models on a single GPU and that small, high-quality datasets can be enough to improve smaller models, which makes benchmark-mined LoRA or QLoRA a credible later-milestone fallback rather than the first move.

### Phase 16: Proof, Comparison, and Closeout

**Rationale:** The milestone needs a proof package, not just local experiments.
**Delivers:** LLM success-rate comparison, macOS runtime comparison, model/build-profile matrix where useful, and a reviewer-readable closeout.

### Phase Ordering Rationale

- Measurement comes first because all later claims depend on it.
- macOS execution-path work comes before deeper agent work so the local feedback loop is not artificially slow.
- Agentic LLM work comes after the fast replay loop exists, otherwise experimentation becomes too expensive.
- Final proof happens last so the milestone closes with evidence, not anecdotes.

### Research Flags

Phases likely needing deeper research during planning:

- **Phase 14:** Docker Desktop settings and macOS profiling details may need machine-specific validation.
- **Phase 15:** LangChain/LangGraph adoption, tool schema design, retrieval boundaries, raw-context tradeoffs, and small-model inference-policy choices may need targeted experimentation.

Phases with standard patterns (skip research-phase):

- **Phase 13:** Measurement and run-contract hardening are straightforward.
- **Phase 16:** Comparison and closeout are standard once the data exists.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Built on official Apple, Docker, Rust, uv, and Ollama docs plus local repo inspection |
| Features | HIGH | Strong alignment between user goals, repo state, and current official guidance |
| Architecture | HIGH | Mostly an incremental extension of the current repo, not a speculative redesign |
| Pitfalls | HIGH | Strong support from official docs and the repo's current pain points |

**Overall confidence:** HIGH

### Gaps to Address

- Exact macOS machine settings still need local validation during execution because Docker and architecture state are machine-specific.
- The agent loop should be benchmarked on a locked slice before broader success claims are made.
- It is still an inference, based on the repo and current docs, that env-fast plus Docker-proof will be the best lane split for this machine. That should be validated in Phase 14.

## Sources

### Primary (HIGH confidence)

- Apple Developer - https://developer.apple.com/documentation/xcode/performance-and-metrics
- Apple Support - https://support.apple.com/en-us/102527
- Docker Desktop settings on Mac - https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Docker VMM - https://docs.docker.com/desktop/features/vmm/
- Docker build cache optimization - https://docs.docker.com/build/cache/optimize/
- uv overview - https://docs.astral.sh/uv/
- uv pip interface - https://docs.astral.sh/uv/pip/
- Cargo profiles - https://doc.rust-lang.org/cargo/reference/profiles.html
- Cargo build timings - https://doc.rust-lang.org/cargo/reference/timings.html
- rustc PGO - https://doc.rust-lang.org/rustc/profile-guided-optimization.html
- rustc codegen options - https://doc.rust-lang.org/rustc/codegen-options/index.html
- LangChain agents - https://docs.langchain.com/oss/python/langchain/agents
- LangChain context engineering - https://docs.langchain.com/oss/python/langchain/context-engineering
- LangChain short-term memory - https://docs.langchain.com/oss/python/langchain/short-term-memory
- LangChain long-term memory - https://docs.langchain.com/oss/python/langchain/long-term-memory
- LangChain retrieval - https://docs.langchain.com/oss/python/langchain/retrieval
- LangChain middleware overview - https://docs.langchain.com/oss/python/langchain/middleware/overview
- LangGraph overview - https://docs.langchain.com/oss/python/langgraph/overview
- LangGraph memory - https://docs.langchain.com/oss/python/langgraph/add-memory
- Qwen quickstart - https://qwen.readthedocs.io/en/latest/getting_started/quickstart.html
- Qwen key concepts - https://qwen.readthedocs.io/en/latest/getting_started/concepts.html
- Qwen-Agent - https://qwen.readthedocs.io/en/v3.0/framework/qwen_agent.html
- Ollama structured outputs - https://docs.ollama.com/capabilities/structured-outputs
- Ollama tool calling - https://docs.ollama.com/capabilities/tool-calling
- Ollama context length - https://docs.ollama.com/context-length
- Ollama FAQ - https://docs.ollama.com/faq
- Ollama OpenAI compatibility - https://docs.ollama.com/api/openai-compatibility
- Ollama qwen3.5:9b - https://ollama.com/library/qwen3.5%3A9b

### Secondary (MEDIUM confidence)

- Self-Consistency - https://arxiv.org/abs/2203.11171
- ReAct - https://arxiv.org/abs/2210.03629
- Self-Refine - https://arxiv.org/abs/2303.17651
- Reflexion - https://arxiv.org/abs/2303.11366
- Teaching Large Language Models to Self-Debug - https://arxiv.org/abs/2304.05128
- CRITIC - https://arxiv.org/abs/2305.11738
- Lost in the Middle - https://arxiv.org/abs/2307.03172
- Large Language Models have Intrinsic Self-Correction Ability - https://arxiv.org/abs/2406.15673
- QLoRA - https://arxiv.org/abs/2305.14314

---
*Research completed: 2026-03-28*
*Ready for roadmap: yes*
