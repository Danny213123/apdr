# APDR

## What This Is

APDR is a Python dependency resolution and validation tool built around a Rust core, with Python-based LLM assistance and a benchmark UI for evaluating real-world snippets. v2.0 modernized the Rust core so APDR stays fast, memory-aware, and reviewable as the benchmark corpus grows.

## Core Value

APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## Current State

- v2.0 shipped on 2026-03-28 with 6 completed phases and 17 completed plans.
- The Rust modernization milestone closed its benchmark and review gates, including BENCH-03 through direct APDR private-memory comparison and BENCH-05 through the inherited five-command Rust review loop.
- There is no active milestone yet. The next planning step is to create fresh requirements and a new roadmap.

## Next Milestone Goals

- Move legacy family-knowledge bundles into data-driven configuration files.
- Evaluate async I/O for network and subprocess-heavy validation paths where benchmark evidence supports it.
- Replace ad-hoc telemetry with structured tracing across the Rust core.
- Add continuous performance benchmarking in CI.

## Requirements

### Validated

- [x] Multi-tier dependency resolution (cache -> heuristic -> LLM) is working and benchmarked
- [x] Validation backends (env + Docker fallback) exist and remain the ground truth for correctness
- [x] Benchmark UI, saved runs, and case inspection workflow exist from the previous milestone
- [x] LLM recovery instrumentation and benchmark reporting exist from the previous milestone
- [x] Rust hot paths reduce unnecessary allocation, cloning, and contention in benchmark-critical code - v2.0
- [x] Validation and caching flows reduce total benchmark runtime without weakening correctness - v2.0
- [x] The Rust codebase is reorganized into clearer modules with smaller reviewable units - v2.0
- [x] Rust docs and comments explain non-obvious behavior, invariants, and fallbacks - v2.0
- [x] Touched Rust modules meet stronger review gates for style, error handling, and maintainability - v2.0

### Active

- [ ] Move legacy family-knowledge bundles into data-driven configuration files
- [ ] Introduce async I/O for network and subprocess-heavy paths
- [ ] Replace ad-hoc telemetry with structured tracing across the Rust core
- [ ] Add continuous performance benchmarking in CI

### Out of Scope

- New benchmark UI or product-surface features - still not the priority until the next milestone explicitly says otherwise
- Replacing the Rust/Python architecture - v2.0 proved the current architecture can be improved incrementally
- Changing benchmark datasets or scoring rules as part of core modernization - continuity still matters for before or after comparisons
- Full async or Tokio migration without measured justification - evaluate targeted async work, not a rewrite
- Dropping Windows or Docker support - compatibility remains a hard constraint

## Context

- The primary modernization target in v2.0 was the Rust code under `tools/apdr/src/`.
- The largest Rust pain points at milestone start were oversized modules, clone-heavy resolver paths, shared-state contention, and sequential validation bottlenecks.
- Phase 4 split the major Rust hotspots into reviewable facades and named sibling modules, and Phase 5 documented the resulting reviewer surfaces.
- Phase 6 closed the milestone with benchmark continuity evidence, a bounded hard-gists package, a green Rust review gate, and a targeted direct-APDR memory comparison for BENCH-03.
- No standalone `v2.0-MILESTONE-AUDIT.md` was recorded; milestone completion relies on the completed phase summaries, `06-BENCHMARK-VERIFICATION.md`, and `06-MILESTONE-CLOSEOUT.md`.

## Constraints

- **Tech stack**: Rust 2021 core plus existing Python and JS helpers - keep the current architecture intact
- **Compatibility**: Windows and Docker validation flows must continue to work - benchmark users depend on both
- **Correctness**: Performance work cannot weaken dependency resolution accuracy or validation fidelity - correctness remains primary
- **Benchmark target**: Hard-gists remains the comparison corpus when continuity matters
- **Scope discipline**: Focus future milestones on measurable value rather than speculative rewrites

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Start a fresh v2.0 roadmap and ignore remaining v1.0 roadmap items | The next milestone was a Rust modernization effort, not a continuation of prior feature work | Good |
| Reset phase numbering to Phase 1 | New milestone needed clean sequencing and avoided mixing old phase intent with the modernization roadmap | Good |
| Skip optional external domain research for v2 | The local codebase map and benchmark evidence were the relevant inputs for this brownfield refactor | Good |
| Measure before optimizing | Benchmark speed and memory work needed baselines and regression checks, not intuition | Good |
| Keep the Phase 5 five-command Rust review loop as the Phase 6 closeout contract | Final signoff needed to reuse the same reviewer gate instead of inventing a new one | Good |
| Close BENCH-03 with direct APDR private-memory comparison instead of wrapper-level RSS | The targeted Rust workflow needed a direct process-level signal when wrapper RSS stayed noisy on Windows | Good |

## Shipped Milestone Snapshot

<details>
<summary>v2.0 Rust Codebase Modernization</summary>

**Goal:** Refactor the Rust codebase for better benchmark performance, memory efficiency, maintainability, and review quality.

**Target features:**
- Faster benchmark execution through hot-path and validation-pipeline optimization
- Lower memory churn through better ownership, fewer unnecessary clones, and smarter shared-state handling
- Clearer Rust module boundaries, stronger docs, and cleaner review surfaces

</details>

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check - still the right priority?
3. Audit Out of Scope - reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-28 after v2.0 milestone completion*
