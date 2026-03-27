# APDR

## What This Is

APDR is a Python dependency resolution and validation tool built around a Rust core, with Python-based LLM assistance and a benchmark UI for evaluating real-world snippets. This milestone is not about expanding product scope; it is about modernizing the Rust codebase so APDR stays fast, memory-efficient, and maintainable as the benchmark corpus grows.

## Core Value

APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## Current Milestone: v2.0 Rust Codebase Modernization

**Goal:** Refactor the Rust codebase for better benchmark performance, memory efficiency, maintainability, and review quality.

**Target features:**
- Faster benchmark execution through hot-path and validation-pipeline optimization
- Lower memory churn through better ownership, fewer unnecessary clones, and smarter shared-state handling
- Clearer Rust module boundaries, stronger docs, and cleaner review surfaces

## Requirements

### Validated

- ✓ Multi-tier dependency resolution (cache -> heuristic -> LLM) is working and benchmarked
- ✓ Validation backends (env + Docker fallback) exist and remain the ground truth for correctness
- ✓ Benchmark UI, saved runs, and case inspection workflow exist from the previous milestone
- ✓ LLM recovery instrumentation and benchmark reporting exist from the previous milestone

### Active

- [ ] Rust hot paths reduce unnecessary allocation, cloning, and contention in benchmark-critical code
- [ ] Validation and caching flows reduce total benchmark runtime without weakening correctness
- [ ] The Rust codebase is reorganized into clearer modules with smaller reviewable units
- [ ] Rust docs and comments explain non-obvious behavior, invariants, and fallbacks
- [ ] Touched Rust modules meet stronger review gates for style, error handling, and maintainability

### Out of Scope

- New benchmark UI or product-surface features - this milestone is internal modernization, not feature expansion
- Replacing the Rust/Python architecture - the goal is to improve the existing system, not rewrite it
- Changing benchmark datasets or scoring rules - benchmark comparisons need a stable target
- Full async/Tokio migration - too invasive for this milestone relative to near-term performance ROI
- Dropping Windows or Docker support - current workflows must remain supported while refactoring

## Context

- The primary modernization target is the Rust code under `tools/apdr/src/`.
- Existing codebase analysis identified 329 `.clone()` occurrences, several 1000+ line modules, Arc/Mutex contention in pre-solve, and sequential validation bottlenecks.
- The largest files currently include `resolver/mod.rs`, `docker/builder.rs`, `resolver/family_knowledge.rs`, `resolver/pypi_client.rs`, and `resolver/tier3_llm.rs`.
- Some production paths still rely on `unwrap()`/`expect()` or brittle string-matching recovery logic, which hurts reviewability and resilience.
- The previous milestone roadmap was intentionally retired. v2.0 starts a fresh roadmap focused on Rust code quality and performance rather than unfinished UI or LLM feature work.

## Constraints

- **Tech stack**: Rust 2021 core plus existing Python and JS helpers - keep the current architecture intact
- **Compatibility**: Windows and Docker validation flows must continue to work - benchmark users depend on both
- **Correctness**: Performance work cannot weaken dependency resolution accuracy or validation fidelity - correctness remains primary
- **Benchmark target**: Hard-gists remains the comparison corpus - before/after metrics must stay comparable
- **Scope discipline**: Focus on Rust internals, docs, and reviewability - avoid drifting into unrelated product work

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Start a fresh v2.0 roadmap and ignore remaining v1.0 roadmap items | The next milestone is a codebase modernization effort, not a continuation of prior feature work | — Pending |
| Reset phase numbering to Phase 1 | New milestone needs clean sequencing and avoids mixing old phase intent with new work | — Pending |
| Skip optional external domain research for v2 | This is a brownfield refactor; the local codebase map and benchmark evidence are the relevant inputs | ✓ Good |
| Measure before optimizing | Benchmark speed and memory work need baselines and regression checks, not intuition | — Pending |

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
*Last updated: 2026-03-26 after milestone v2.0 initialization*
