# Project Research Summary

**Project:** APDR
**Domain:** LLM-agent quality and macOS benchmark performance
**Researched:** 2026-03-28
**Confidence:** HIGH

## Executive Summary

The research points to a milestone that should improve APDR along two tightly linked axes: better tier3 agent behavior and a much cleaner macOS benchmark inner loop. The repo already contains several good building blocks, including `uv`-first env setup, macOS CoW reuse for validated envs, and a PGO build script. The missing pieces are better measurement, cleaner execution modes on macOS, and a more agentic LLM loop that learns from benchmark feedback instead of growing deterministic fix tables.

For macOS performance, the highest-value path is not a broad rewrite. It is a measurement-first approach: record architecture and cache state, add per-stage timings, separate fast env runs from slower Docker proof runs, and tighten Docker Desktop settings when Docker is required. For LLM quality, the strongest pattern is an explicit tool-calling loop plus reflection memory grounded in real benchmark outcomes. That matches the user's direction: better general intelligence, not more hardcoded rules.

## Key Findings

### Recommended Stack

The current stack is still the right one, but it needs sharper defaults and stronger measurement:

**Core technologies:**

- Native Apple silicon toolchain: avoid Rosetta on hot paths.
- Xcode Instruments: profile CPU, disk, memory, and responsiveness on macOS.
- `uv`: fast env creation and package installation with a global cache.
- Docker Desktop with Docker VMM and VirtioFS where compatible: keep Docker usable without accepting legacy macOS slow paths.
- Rust benchmark builds with PGO and profile tuning: improve APDR runtime when it is a real bottleneck.
- Ollama structured outputs and tool calling: support an agent loop without changing providers.

### Expected Features

**Must have (table stakes):**

- Native-arch run metadata - users need to know whether a run used arm64, Rosetta, env, or Docker.
- Per-stage benchmark timings - total runtime alone is not actionable.
- Fast canonical slice / replay mode - required for practical local tuning.
- Backend-intent modes - separate fast env iteration from Docker proof runs.
- Stable benchmark evidence - saved runs must be comparable and reviewer-readable.

**Should have (competitive):**

- Tool-calling tier3 agent loop - the main mechanism for higher LLM success without deterministic rule growth.
- Reflection memory from benchmark feedback - turn measured outcomes into future agent improvement.
- macOS performance proof pack - architecture, backend, cache state, and stage deltas in one place.

**Defer (v2+):**

- Cross-machine normalization and more advanced benchmark-slice mining.

### Architecture Approach

The cleanest v2.2 architecture is a three-layer flow: measurement, execution, and state/cache. The benchmark runner should stamp every run with macOS-specific metadata before the APDR CLI starts. The execution path should split into two lanes: a native env-fast lane for inner-loop iteration and a Docker-proof lane for selected parity runs. The LLM path should become a true tool-calling loop with benchmark-fed memory rather than a single structured guess followed by deterministic cleanup.

**Major components:**

1. Benchmark control plane - owns run intent, saved metadata, and summaries.
2. Native APDR execution layer - owns env validation, Docker fallback, and benchmark runtime.
3. LLM agent service - owns tool use, critique, reflection, and memory updates.

### Critical Pitfalls

1. **Mixed-architecture benchmarking** - record native vs Rosetta state in every run.
2. **Docker file-sharing overhead on macOS** - keep broad host bind mounts out of the hot path.
3. **Warm/cold state confusion** - do not compare runs unless cache and model state are explicit.
4. **Deterministic-rule sprawl** - move quality gains into the agent loop, not new fix tables.
5. **Total-runtime-only measurement** - persist stage timings so regressions are attributable.

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

### Phase 15: Agentic Tier3 Intelligence Improvements

**Rationale:** Once the benchmark loop is fast enough, APDR can improve success rate through behavior rather than rule growth.
**Delivers:** Tool-calling resolver flow, structured critique/refine loop, and benchmark-fed reflection memory.
**Uses:** Ollama tool calling and structured outputs.
**Implements:** The agentic tier3 loop from architecture research.

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
- **Phase 15:** Model-choice tradeoffs and tool schema design may need targeted experimentation.

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
- Ollama structured outputs - https://docs.ollama.com/capabilities/structured-outputs
- Ollama tool calling - https://docs.ollama.com/capabilities/tool-calling

### Secondary (MEDIUM confidence)

- ReAct - https://arxiv.org/abs/2210.03629
- Reflexion - https://arxiv.org/abs/2303.11366

---
*Research completed: 2026-03-28*
*Ready for roadmap: yes*
