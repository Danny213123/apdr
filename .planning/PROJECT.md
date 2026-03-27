# APDR Enhancement: Accuracy & Performance

## What This Is

APDR (Automated Python Dependency Resolution) improvements focused on three areas: (1) responsive real-time UI with deterministic/LLM result separation, (2) better LLM recovery accuracy for build failures, and (3) faster inference and validation throughput.

## Core Value

The benchmark UI must stay responsive and show real-time progress during runs. Users need to see deterministic passes immediately (tier1/tier2 cache hits) separate from LLM-based resolution attempts, without browser hangs or stale data.

## Requirements

### Validated

- ✓ Multi-tier resolution (cache → heuristic → LLM) — existing
- ✓ Docker validation with smoke tests — existing
- ✓ LLM-based error recovery — existing
- ✓ Web UI benchmark dashboard — existing
- ✓ Parallel worker execution — existing
- ✓ PyPI metadata caching — existing
- ✓ UI splits results into deterministic vs LLM sections (two separate views) — Validated in Phase 1-2: Real-time streaming and categorization
- ✓ UI updates in real-time as cases complete (no waiting for entire run) — Validated in Phase 1: SSE streaming infrastructure
- ✓ Browser stays responsive during benchmark runs (no hangs/freezes) — Validated in Phase 1: Non-blocking UI
- ✓ Startup database loading is non-blocking (show UI immediately) — Validated in Phase 1: Progressive loading
- ✓ LLM recovery suggestions improve accuracy (better pattern matching, validation) — Validated in Phase 3: PyPI validation, RAG patterns, cache invalidation

### Active

- [ ] LLM inference performance improves (batching, caching, prompt optimization)
- [ ] Docker validation performance improves (parallel builds, layer caching)
- [ ] Overall accuracy increases from 75% baseline
- [ ] LLM-assisted cases achieve ≥50% pass rate (relative to total LLM cases)

### Out of Scope

- Replacing the UI framework (keep vanilla JS + Vite) — not changing tech stack
- Switching LLM providers (Ollama integration stays) — architecture is sound
- Removing Docker validation (it's the ground truth) — necessary for correctness
- Multi-language support beyond Python — APDR is Python-specific

## Context

**Current State:**
- Accuracy: 75% overall pass rate on hard-gists benchmark
- LLM recovery: Poor suggestions from error recovery agent
- UI Performance: Browser freezes during runs, long startup hang, no real-time updates
- Resolution Tiers: tier1 (cache), tier2 (heuristic), tier3 (LLM) — deterministic vs non-deterministic split already exists in resolver but not surfaced in UI
- Validation: Docker builds are slow (sequential pip installs per Python version)
- Inference: LLM calls are slow (per-import resolution, no batching)

**Architecture:**
- Rust core (resolver, cache, Docker orchestration)
- Python LLM service (subprocess, Ollama client via LiteLLM/Instructor)
- JavaScript web UI (vanilla JS, Flask backend serves API)
- SQLite knowledge graph (package metadata, failure patterns)

**Known Issues (from CONCERNS.md):**
- 329 `.clone()` calls (performance overhead)
- Large monolithic files (4500+ lines in resolver/mod.rs)
- Arc/Mutex contention in parallel solver
- Synchronous validation workflow (sequential Docker builds)
- Polling-based process timeouts (50ms → 1000ms backoff)

**Existing Patterns:**
- Import pattern taxonomy in prompts (Pattern A-G)
- Failure pair few-shot examples (WRONG → CORRECT)
- Build error pattern library for RAG-enriched recovery
- Multi-agent LangGraph system for validation (confidence/builder/analyst/recovery agents)

## Constraints

- **Tech Stack**: Rust + Python + vanilla JS — no framework rewrites
- **LLM Provider**: Ollama local inference — must work offline
- **Validation**: Docker-based ground truth — no shortcuts
- **Compatibility**: Windows support required (current issues with BuildKit deadlock workaround)
- **Performance**: Must handle parallel workers without UI freezing
- **Data**: Hard-gists benchmark dataset — fixed test suite

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Split UI into deterministic vs LLM results | Users need to see fast cache hits separately from slow LLM attempts | — Pending |
| Non-blocking startup with progressive loading | 3+ second database load blocks UI interaction | — Pending |
| Stream results as they complete | Current design waits for entire batch before updating | — Pending |
| Improve LLM recovery prompts | 75% accuracy indicates recovery suggestions need work | — Pending |
| Batch LLM inference where possible | Per-import resolution is inefficient | — Pending |
| Parallelize Docker validation | Sequential builds waste time when independent | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd:transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd:complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-27 after Phase 3 (LLM Recovery Accuracy) completion*
