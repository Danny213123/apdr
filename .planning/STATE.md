---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 03
current_plan: 3
status: active
last_updated: "2026-03-27T16:29:24.000Z"
progress:
  total_phases: 6
  completed_phases: 2
  total_plans: 8
  completed_plans: 7
---

# Project State: APDR

**Last Updated:** 2026-03-27
**Current Phase:** 03
**Current Plan:** 3

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 03 — validation-pipeline-throughput

---

## Current Position

Phase: 03 (validation-pipeline-throughput) — EXECUTING
Plan: 3 of 3

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

*None - 03-02 is complete and Phase 3 is continuing into 03-03.*

## Active TODOs

- [ ] Execute Plan `03-03` (`Validation Candidate Benchmark & Delta Report`)
- [ ] Capture both the warm and forced validation candidate artifacts with the updated benchmark harness
- [ ] Run the Phase 3 regression comparison against `01-baseline.json` before closing the phase

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

### What's Next

**Immediate:** Continue Phase 3 with Plan `03-03`

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Capture warm and forced validation candidates, run the regression gate, and write the Phase 3 validation delta note

### Context for Next Session

1. Read `PROJECT.md` for active scope and boundaries
2. Read `REQUIREMENTS.md` for milestone REQ IDs
3. Read `ROADMAP.md` for phase structure
4. Read `03-03-PLAN.md`, then continue execution in Phase 3

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
- `.planning/phases/03-validation-pipeline-throughput/03-01-SUMMARY.md` - Wave 1 env-attempt staging and cache-source cleanup summary
- `.planning/phases/03-validation-pipeline-throughput/03-02-SUMMARY.md` - Wave 2 backend telemetry and benchmark-reporting summary
- `.planning/phases/03-validation-pipeline-throughput/03-02-PLAN.md` - backend telemetry and benchmark-reporting work
- `.planning/phases/03-validation-pipeline-throughput/03-03-PLAN.md` - candidate capture and delta closeout

**Key Commands:**

- `$gsd-execute-phase 3` - continue the remaining validation-throughput plans
- `$gsd-progress` - review milestone state after Phase 2 completion
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after 03-02 execution on 2026-03-27*
