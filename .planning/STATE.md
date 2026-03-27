---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 03
current_plan: Not started
status: ready
last_updated: "2026-03-27T11:49:58.7993974-04:00"
progress:
  total_phases: 6
  completed_phases: 2
  total_plans: 8
  completed_plans: 5
---

# Project State: APDR

**Last Updated:** 2026-03-27
**Current Phase:** 03
**Current Plan:** Not started

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 03 - validation-pipeline-throughput

---

## Current Position

Phase: 03 (validation-pipeline-throughput) - READY TO EXECUTE
Plan: Not started

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
- Validation throughput remains constrained by fallback, retry, and environment creation overhead
- Some production error paths still rely on `unwrap()`, `expect()`, or brittle string-matching logic

## Accumulated Context

### Architecture Notes

- Rust core lives under `tools/apdr/src/`
- Python LLM bridge lives under `tools/apdr/llm_py/`
- Correctness still depends on reproducible env and Docker validation
- Phase 2 ended with a clean `cargo clippy --all-targets -- -D warnings` gate

### Completed Milestone Work

- Phase 1 established the baseline harness, memory profile, hotspot audit, and benchmark regression guardrails
- Phase 2 reduced resolver hot-path ownership churn and recomputation, added retry-loop regression coverage, and captured a bounded before/after candidate benchmark

### Carried-Forward Constraints

- Keep Windows and Docker support intact
- Avoid benchmark accuracy regressions while optimizing
- Keep the hard-gists corpus as the comparison baseline

## Current Blockers

*None - Phase 2 is complete and Phase 3 is ready for execution.*

## Active TODOs

- [ ] Execute Plan `03-01` (`Env Attempt Staging & Validated-Env Reuse Cleanup`)
- [ ] Preserve the `validation_pipeline_` cargo-test filter and workspace clippy pass while touching validation code paths
- [ ] Capture both the warm and forced validation candidate artifacts before closing Phase 3

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

### What's Next

**Immediate:** Run `$gsd-execute-phase 3`

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Reduce env and Docker orchestration cost, expose validation-stage metrics more clearly, and capture both warm and forced candidate artifacts

### Context for Next Session

1. Read `PROJECT.md` for active scope and boundaries
2. Read `REQUIREMENTS.md` for milestone REQ IDs
3. Read `ROADMAP.md` for phase structure
4. Read `03-RESEARCH.md`, `03-VALIDATION.md`, and the three `03-0X-PLAN.md` files, then execute Phase 3

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
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-02-SUMMARY.md` - retry-loop and lint-gate cleanup summary
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-03-SUMMARY.md` - bounded candidate capture summary
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md` - Phase 2 benchmark comparison
- `.planning/phases/03-validation-pipeline-throughput/03-RESEARCH.md` - validation hotspot and measurement guidance for Phase 3
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION.md` - verification contract for validation-throughput work
- `.planning/phases/03-validation-pipeline-throughput/03-01-PLAN.md` - env staging and validated-env reuse cleanup
- `.planning/phases/03-validation-pipeline-throughput/03-02-PLAN.md` - backend telemetry and benchmark-reporting work
- `.planning/phases/03-validation-pipeline-throughput/03-03-PLAN.md` - candidate capture and delta closeout

**Key Commands:**

- `$gsd-execute-phase 3` - run the validation-throughput plans for the next phase
- `$gsd-progress` - review milestone state after Phase 2 completion
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after Phase 3 planning on 2026-03-27*
