---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 04
current_plan: 0
status: active
last_updated: "2026-03-27T16:36:54.000Z"
progress:
  total_phases: 6
  completed_phases: 3
  total_plans: 8
  completed_plans: 8
---

# Project State: APDR

**Last Updated:** 2026-03-27
**Current Phase:** 04
**Current Plan:** 0

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 04 - module-layout-and-boundary-cleanup

---

## Current Position

Phase: 04 (module-layout-and-boundary-cleanup) - READY TO PLAN
Plan: 0 of TBD

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
- Phase 3 confirmed that forced validation on this Windows host still bottlenecks on Docker buildx access rather than on the new telemetry layer
- Some production error paths still rely on `unwrap()`, `expect()`, or brittle string-matching logic

## Accumulated Context

### Architecture Notes

- Rust core lives under `tools/apdr/src/`
- Python LLM bridge lives under `tools/apdr/llm_py/`
- Correctness still depends on reproducible env and Docker validation
- Phase 3 now has warm-vs-forced validation benchmark artifacts and a documented Windows Docker variance note

### Completed Milestone Work

- Phase 1 established the baseline harness, memory profile, hotspot audit, and benchmark regression guardrails
- Phase 2 reduced resolver hot-path ownership churn and recomputation, added retry-loop regression coverage, and captured a bounded before/after candidate benchmark
- Phase 3 improved validation telemetry, captured warm and forced validation candidates, and closed with a regression-gated delta note against the original baseline

### Carried-Forward Constraints

- Keep Windows and Docker support intact
- Avoid benchmark accuracy regressions while optimizing
- Keep the hard-gists corpus as the comparison baseline

## Current Blockers

*None - Phase 3 is complete and Phase 4 planning is next.*

## Active TODOs

- [ ] Plan Phase `04` (`Module Layout & Boundary Cleanup`)
- [ ] Split oversized validation and resolver Rust modules behind clearer boundaries without losing the current regression gates
- [ ] Preserve the Phase 3 benchmark artifacts as the reference point for later benchmark-verification work

## Deferred Items

- [ ] Consider async I/O or data-driven family bundles in a later milestone if v2 hotspots remain after modernization

---

## Session Continuity

### What Just Happened

- Archived milestone v1.0 and reset the active roadmap to v2.0
- Completed Phase 1 with baseline, memory, hotspot, and regression artifacts
- Completed `02-01` with owned pre-solve worker aggregation and shared PyPI metadata persistence helpers
- Completed `02-02` with explicit retry-loop state, normalized dependency lookup helpers, resolver regression tests, and a restored workspace clippy pass
- Completed `02-03` with `02-resolver-candidate.json`, `02-RESOLVER-CANDIDATE.md`, and `02-RESOLVER-DELTA.md`
- Planned Phase 3 with `03-RESEARCH.md`, `03-VALIDATION.md`, and three execution plans focused on validation throughput, backend telemetry, and candidate benchmarking
- Completed `03-01` with explicit env-attempt path staging, validated-env cache-source helpers, ordered env-to-Docker retry history, and passing validation-pipeline tests
- Completed `03-02` with cached Docker-agent probing, JSON-backed agent-result parsing, richer per-sample validation benchmark reporting, and optional env-create or install or smoke regression thresholds
- Completed `03-03` with warm and forced validation candidate artifacts, a passing continuity regression gate, and a delta note that keeps the Windows Docker constraint explicit

### What's Next

**Immediate:** Plan Phase 4

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Split oversized Rust modules and tighten module boundaries for reviewability without breaking the current benchmark and lint guardrails

### Context for Next Session

1. Read `PROJECT.md` for active scope and boundaries
2. Read `REQUIREMENTS.md` for milestone REQ IDs
3. Read `ROADMAP.md` for phase structure
4. Run `$gsd-plan-phase 4`, then continue the milestone with the Phase 3 benchmark artifacts in hand

---

## Quick Reference

**Key Files:**

- `.planning/PROJECT.md` - milestone scope, constraints, and decisions
- `.planning/REQUIREMENTS.md` - v2 requirement IDs and traceability
- `.planning/ROADMAP.md` - phase structure for modernization work
- `.planning/codebase/CONCERNS.md` - current Rust hotspots and risks
- `.planning/phases/01-baseline-and-guardrails/01-baseline.json` - committed timing and pass-rate baseline
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json` - representative memory snapshot
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md` - ranked Rust optimization targets
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-03-SUMMARY.md` - bounded resolver candidate capture summary
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md` - Phase 2 benchmark comparison
- `.planning/phases/03-validation-pipeline-throughput/03-02-SUMMARY.md` - Wave 2 backend telemetry and benchmark-reporting summary
- `.planning/phases/03-validation-pipeline-throughput/03-03-SUMMARY.md` - Wave 3 validation candidate capture and delta summary
- `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json` - warm continuity candidate for validation throughput
- `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json` - forced-validation artifact for the same bounded sample
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md` - validation delta note with warm-vs-forced interpretation

**Key Commands:**

- `$gsd-plan-phase 4` - plan the next structural cleanup phase
- `$gsd-progress` - review milestone state after Phase 3 completion
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after Phase 3 completion on 2026-03-27*
