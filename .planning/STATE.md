---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 02
current_plan: "Not started"
status: ready_for_planning
last_updated: "2026-03-27T00:02:54.3012837-04:00"
progress:
  total_phases: 6
  completed_phases: 1
  total_plans: 2
  completed_plans: 2
---

# Project State: APDR

**Last Updated:** 2026-03-26
**Current Phase:** 02
**Current Plan:** Not started

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 02 - resolver-memory-and-algorithm-efficiency

---

## Current Position

Phase: 02 (resolver-memory-and-algorithm-efficiency) - READY FOR PLANNING
Plan: -

## Milestone Snapshot

**Current Milestone:** v2.0 - Rust Codebase Modernization

**Goal:** Refactor the Rust codebase for better benchmark performance, memory efficiency, maintainability, and review quality.

**Why this replaced the prior roadmap:**

- Remaining v1.0 roadmap items were intentionally retired
- v1.0 was archived so phase numbering could restart at 1
- This milestone is focused on internal Rust quality and performance, not new product features

## Baseline Indicators

- Large Rust modules remain in critical paths: `resolver/mod.rs`, `docker/builder.rs`, `resolver/family_knowledge.rs`, `resolver/pypi_client.rs`, `resolver/tier3_llm.rs`
- Codebase analysis identified 329 `.clone()` occurrences across Rust source, with heavy concentration in resolver flows
- `pre_solve.rs` still uses shared-state concurrency patterns that are likely contention hotspots
- Validation throughput remains constrained by fallback, retry, and environment creation overhead
- Some production error paths still rely on `unwrap()`, `expect()`, or brittle string-matching logic

## Accumulated Context

### Architecture Notes

- Rust core lives under `tools/apdr/src/`
- Python LLM bridge lives under `tools/apdr/llm_py/`
- The benchmark UI remains part of the product, but it is not the main scope of this milestone
- Correctness still depends on reproducible env and Docker validation

### Known Modernization Targets

- ownership and clone churn in resolver and cache flows
- validation pipeline throughput and cache reuse
- module decomposition for oversized files
- docs, comments, error handling, and consistent review surfaces

### Carried-Forward Constraints

- keep Windows and Docker support intact
- avoid benchmark accuracy regressions while optimizing
- keep the hard-gists corpus as the comparison baseline

## Current Blockers

*None - Phase 1 completed and Phase 2 is ready to plan.*

## Active TODOs

- [ ] Run `$gsd-plan-phase 2`
- [ ] Use `01-HOTSPOT-AUDIT.md` to pick the first resolver targets
- [ ] Reuse `01-baseline.json` and `01-memory-profile.json` when measuring Phase 2 changes

## Deferred Items

- [ ] Consider async I/O or data-driven family bundles in a later milestone if v2 hotspots remain after modernization

---

## Session Continuity

### What Just Happened

- Archived milestone v1.0 and moved its phase directories into `.planning/milestones/v1.0-phases/`
- Replaced the active roadmap with milestone v2.0: Rust Codebase Modernization
- Reset phase numbering to 1 for the new milestone
- Created Phase 1 research and validation docs plus plans `01-01` and `01-02`
- Completed `01-01` with committed baseline and memory-profile artifacts
- Completed `01-02` with the regression gate, hotspot audit, and README guardrails

### What's Next

**Immediate:** Plan Phase 2 (Resolver Memory & Algorithm Efficiency)

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Use the committed Phase 1 baseline to drive the first resolver efficiency refactors

### Context for Next Session

1. Read `PROJECT.md` for active scope and boundaries
2. Read `REQUIREMENTS.md` for milestone REQ IDs
3. Read `ROADMAP.md` for phase structure
4. Read `01-HOTSPOT-AUDIT.md`, then run `$gsd-plan-phase 2`

---

## Quick Reference

**Key Files:**

- `.planning/PROJECT.md` - milestone scope, constraints, and decisions
- `.planning/REQUIREMENTS.md` - v2 requirement IDs and traceability
- `.planning/ROADMAP.md` - phase structure for modernization work
- `.planning/codebase/CONCERNS.md` - current Rust hotspots and risks
- `.planning/phases/01-baseline-and-guardrails/01-baseline.json` - committed timing and pass-rate baseline
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json` - representative memory snapshot
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md` - ranked Rust optimization targets for Phase 2 and Phase 3

**Key Commands:**

- `$gsd-plan-phase 2` - plan the first Rust optimization phase
- `$gsd-progress` - review milestone state after Phase 1 completion
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after Phase 1 completion on 2026-03-27*
