---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 1
current_plan: "01-01"
status: ready_for_execution
last_updated: "2026-03-26T23:35:08.4739999-04:00"
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 2
  completed_plans: 0
---

# Project State: APDR

**Last Updated:** 2026-03-26
**Current Phase:** 1
**Current Plan:** 01-01

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 1 - Baseline & Guardrails

---

## Current Position

Phase: 1 (baseline-and-guardrails) - PLANNED
Plan: 01-01
Status: Ready for execution

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

*None - ready to execute Phase 1*

## Active TODOs

- [ ] Execute `01-01` to add the deterministic baseline harness and memory capture workflow
- [ ] Execute `01-02` to add the regression gate, hotspot audit, and README guardrails
- [ ] Run `$gsd-execute-phase 1`

## Deferred Items

- [ ] Consider async I/O or data-driven family bundles in a later milestone if v2 hotspots remain after modernization

---

## Session Continuity

### What Just Happened

- Archived milestone v1.0 and moved its phase directories into `.planning/milestones/v1.0-phases/`
- Replaced the active roadmap with milestone v2.0: Rust Codebase Modernization
- Reset phase numbering to 1 for the new milestone
- Created Phase 1 research and validation docs plus plans `01-01` and `01-02`

### What's Next

**Immediate:** Execute Phase 1 plan `01-01` (Baseline Harness & Memory Capture)

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Finish Phase 1 so Phase 2 can optimize from measured evidence

### Context for Next Session

1. Read `PROJECT.md` for active scope and boundaries
2. Read `REQUIREMENTS.md` for milestone REQ IDs
3. Read `ROADMAP.md` for phase structure
4. Execute `$gsd-execute-phase 1`

---

## Quick Reference

**Key Files:**

- `.planning/PROJECT.md` - milestone scope, constraints, and decisions
- `.planning/REQUIREMENTS.md` - v2 requirement IDs and traceability
- `.planning/ROADMAP.md` - phase structure for modernization work
- `.planning/codebase/CONCERNS.md` - current Rust hotspots and risks
- `tools/apdr/src/resolver/mod.rs` - largest resolver hotspot
- `tools/apdr/src/docker/builder.rs` - largest validation hotspot

**Key Commands:**

- `$gsd-execute-phase 1` - execute the planned baseline and guardrail work
- `$gsd-progress` - review milestone state after planning
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after Phase 1 planning on 2026-03-26*
