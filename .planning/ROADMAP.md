# Roadmap: APDR

**Project:** APDR v2.2 - Improve LLM Performance and Benchmark Performance on macOS
**Created:** 2026-03-28
**Granularity:** Standard (4 phases)

## Milestones

- [ ] **v2.2 Improve LLM Performance and Benchmark Performance on macOS** - Phases 13-16
- [ ] **v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy** - superseded unfinished on 2026-03-28 after Phase 11 completion; Phase 12 remained open and is now historical debt rather than active milestone scope
- [x] `v2.0` Rust Codebase Modernization - shipped 2026-03-28, archived in `.planning/milestones/v2.0-ROADMAP.md`
- [x] `v1.0` Accuracy & Performance - shipped 2026-03-27, archived in `.planning/milestones/v1.0-ROADMAP.md`

## Roadmap v2.2: Improve LLM Performance and Benchmark Performance on macOS

This milestone stays measurement-first so later macOS and agent-quality claims are attributable. It then optimizes the macOS replay path, improves tier3 behavior through LangChain/LangGraph-backed agent methods and context engineering instead of new fix tables, and closes with reviewer-readable proof that preserves the unfinished v2.1 history rather than overstating it. The research direction here is specific: use a higher-level LangChain agent loop where it beats the current manual path, use LangGraph persistence and stores for benchmark-grounded memory, and treat larger context as a benchmarked combination of retrieval, summarization, context folding, and selectively larger Ollama context windows rather than just stuffing more tokens into every call. For small local models, Phase 15 also now explicitly targets model-specific inference policy on a representative sub-10GB-VRAM path such as `qwen3.5:9b`, rather than assuming that the same settings that work for bigger models or other families are optimal.

## Phases

- [x] **Phase 13: Measurement and Run-Contract Hardening** - Make benchmark runs comparable by recording architecture, backend, cache, intent, and stage timings up front
- [x] **Phase 14: macOS Execution-Path Optimization** - Create a fast native macOS replay lane and land runtime improvements without breaking Windows guardrails
- [x] **Phase 15: LangChain/LangGraph Tier3 Intelligence Improvements** - Improve tier3 recovery through benchmarked LangChain/LangGraph agent behavior, context engineering, and clean failure handling on the locked replay slice
- [x] **Phase 16: Proof, Comparison, and Closeout** - Package reviewer-readable macOS gains and Windows non-regression evidence for milestone closeout (completed 2026-03-29)

## Phase Details

### Phase 13: Measurement and Run-Contract Hardening
**Goal**: Benchmark runs and comparisons become trustworthy enough to attribute later quality and performance deltas on macOS
**Depends on**: Nothing (first phase of v2.2)
**Requirements**: MAC-01, MAC-02, EVD-03, EVD-05
**Success Criteria** (what must be TRUE):
  1. Every saved macOS benchmark run records host architecture, APDR binary architecture, Python architecture, validation backend, run intent, cache state, and the configured LLM context-window and inference-policy settings used for that run.
  2. Benchmark artifacts expose stage-level timings for resolution, LLM work, env creation, package install, validation, and Docker startup when Docker is used.
  3. Saved benchmark evidence clearly distinguishes `env-fast` versus `docker-proof` runs and warm versus cold cache state so reviewers can compare like-for-like runs.
  4. Any model, context-window, inference-policy, or build-profile comparison captured during v2.2 includes enough metadata to attribute deltas to agent behavior, model choice, context configuration, decoding policy, runtime tuning, or backend differences.
**Plans**:
  - `13-01` Canonical benchmark run contract in `benchmark_ui`
  - `13-02` APDR per-case timing and run-contract propagation
  - `13-03` Reporting normalization, measurement checker, and reviewer-facing contract note

### Phase 14: macOS Execution-Path Optimization
**Goal**: macOS benchmark iteration becomes fast and repeatable on a locked replay slice without sacrificing correctness or Windows guardrails
**Depends on**: Phase 13
**Requirements**: MAC-03, MAC-04, WIN-01
**Success Criteria** (what must be TRUE):
  1. A locked benchmark replay slice exists for v2.2 and can run in a fast macOS native-env mode intended for repeated local iteration.
  2. On that locked replay slice, the macOS execution path shows substantial before-and-after runtime or throughput improvement versus the v2.2 baseline while preserved pass and skip cases remain unchanged.
  3. The macOS-focused performance changes stay within the milestone's accepted Windows comparison guardrail for runtime or seconds-per-case on the representative Windows slice.
**Plans**:
  - `14-01` Locked replay-slice manifests and manifest-aware benchmark capture
  - `14-02` macOS replay runner and native env-fast tuning
  - `14-03` Regression checker and proof pack for macOS gains plus Windows guardrail

### Phase 15: LangChain/LangGraph Tier3 Intelligence Improvements
**Goal**: APDR's tier3 recovery improves through benchmarked LangChain/LangGraph agent behavior, context engineering, small-model inference policy, and benchmark-fed learning instead of new deterministic recovery tables
**Depends on**: Phase 13, Phase 14
**Requirements**: AGT-01, AGT-02, AGT-03, AGT-04, AGT-05, AGT-06
**Success Criteria** (what must be TRUE):
  1. Hard replay-slice tier3 cases can run through an explicit tool-calling plus critique-and-refinement loop, with LangChain's `create_agent` or an equivalent LangGraph-backed path benchmarked against the current manual loop.
  2. Ambiguous tier3 cases can compare or verify multiple candidate mappings before selecting an answer instead of locking onto the first completion.
  3. Later tier3 attempts can inspect reflection, memory, or retrieval grounded in benchmark outcomes from earlier attempts rather than depending on new hardcoded recovery tables as the main mechanism.
  4. The tier3 path can increase effective context on hard cases through retrieval, state/store memory, summarization, context folding, and selectively larger Ollama context windows when benchmarking shows the tradeoff is favorable.
  5. A representative sub-10GB-VRAM local-model path, such as `qwen3.5:9b`, is benchmarked with model-specific inference policy including thinking versus non-thinking routing where supported, non-greedy decoding settings, tool-surface reduction, and verifier or self-consistency passes for hard cases.
  6. Compared with the v2.2 baseline for the locked replay slice, the LLM path resolves more tier3 cases successfully, including on the representative small-model benchmark path chosen for the milestone.
  7. When the agent still cannot solve a case, APDR records inspectable failure reasons and abstains cleanly instead of fabricating success.
**Plans**:
  - `15-01` Tier3 replay benchmark harness and artifact contract
  - `15-02` Explicit agent-runtime seam and abstain tracing
  - `15-03` Benchmark-fed memory, retrieval, and context folding
  - `15-04` Small-model policy matrix and agent-quality proof pack

### Phase 16: Proof, Comparison, and Closeout
**Goal**: v2.2 closes with reviewer-readable proof for macOS performance gains and Windows non-regression without overstating what shipped
**Depends on**: Phase 14, Phase 15
**Requirements**: EVD-04, EVD-06
**Success Criteria** (what must be TRUE):
  1. Milestone closeout includes before-and-after macOS benchmark comparisons on the reproducible replay slice that make the claimed performance gain easy for a reviewer to verify.
  2. Milestone closeout includes an explicit Windows non-regression comparison for the benchmark-performance work completed in v2.2.
**Closeout State**: The Phase 16 proof pack is complete at the `sample` contract level, but live macOS, Windows, and Phase 15 artifact capture remains pending before milestone signoff.
**Plans**:
  - `16-01` Closeout evidence bundle contract and readiness checker
  - `16-02` Reviewer-facing macOS, Windows, and LLM-quality comparison pack
  - `16-03` Milestone closeout note and requirement reconciliation

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 13. Measurement and Run-Contract Hardening | 3/3 | Complete | 13-01, 13-02, 13-03 complete |
| 14. macOS Execution-Path Optimization | 3/3 | Complete | 14-01, 14-02, 14-03 complete |
| 15. LangChain/LangGraph Tier3 Intelligence Improvements | 4/4 | Complete | 15-01, 15-02, 15-03, 15-04 complete |
| 16. Proof, Comparison, and Closeout | 3/3 | Complete   | 2026-03-29 |

## Dependencies

`Phase 13 -> Phase 14 -> Phase 15 -> Phase 16`

*Roadmap created: 2026-03-28*
*Last updated: 2026-03-29 (Phase 16 executed; live proof pending)*
