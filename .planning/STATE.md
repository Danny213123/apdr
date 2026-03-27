---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: Rust Codebase Modernization
current_phase: 05
current_plan: Not started
status: ready_to_execute
last_updated: "2026-03-27T19:29:06.7674717-04:00"
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 11
  completed_plans: 11
---

# Project State: APDR

**Last Updated:** 2026-03-27
**Current Phase:** 05
**Current Plan:** Not started
**Current Phase Name:** Documentation, Error Handling & Review Readiness
**Total Phases:** 6
**Total Plans in Phase:** 3
**Status:** Ready to execute
**Progress:** 0%
**Last Activity:** Phase 5 planned
**Last Activity Description:** Added `05-RESEARCH.md`, `05-VALIDATION.md`, and three execution plans covering reviewer docs, panic-path cleanup, and verification closeout.
**Paused At:** Phase 5 execution handoff
**Last Date:** 2026-03-27T19:29:06.7674717-04:00
**Stopped At:** Phase 5 planned
**Resume File:** .planning/phases/05-documentation-error-handling-and-review-readiness/05-01-PLAN.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 05 — documentation-error-handling-and-review-readiness

---

## Current Position

Phase: 05 (documentation-error-handling-and-review-readiness) — READY TO EXECUTE
Plan: Not started (3 plans queued)

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

*None - Phase 5 is next and Phase 4 is complete.*

## Active TODOs

- [ ] Execute Phase `05` (`Documentation, Error Handling & Review Readiness`)
- [ ] Document non-obvious Rust fallback and recovery behavior now that module ownership is explicit
- [ ] Review touched Rust production paths for panic safety and style consistency using the new Phase 4 boundaries

## Deferred Items

- [ ] Consider async I/O or data-driven family bundles in a later milestone if v2 hotspots remain after modernization

---

## Session

**Last Date:** 2026-03-27T19:29:06.7674717-04:00
**Stopped At:** Phase 5 planned
**Resume File:** .planning/phases/05-documentation-error-handling-and-review-readiness/05-01-PLAN.md

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
- Planned Phase 4 with `04-RESEARCH.md`, `04-VALIDATION.md`, and three execution plans focused on resolver decomposition, validation-builder decomposition, and support-module boundary cleanup
- Completed `04-01` by splitting `resolver/mod.rs` into facade, retry-loop, diagnostics, and artifact modules
- Completed `04-02` by splitting `docker/builder.rs` into env, Docker, agent, process, and runtime modules
- Completed `04-03` by turning `family_knowledge`, `pypi_client`, and `tier3_llm` into directory-backed facades with named support modules
- Stabilized full-suite verification by making wheelhouse cache pruning deterministic when file mtimes tie on Windows
- Captured `05-CONTEXT.md` and `05-DISCUSSION-LOG.md` for Phase 5, locking the docs placement, panic cleanup policy, and reviewer-guide scope for the planning handoff
- Planned Phase 5 with `05-RESEARCH.md`, `05-VALIDATION.md`, and three execution plans focused on reviewer docs, panic-path hardening, and consistency closeout

### What's Next

**Immediate:** Execute Phase 5 from `05-01-PLAN.md`

**This milestone:** Measure first, optimize second, then clean up layout and review quality

**Next Phase:** Execute the review-readiness plans across the newly split Rust modules with clearer docs, safer error paths, and tighter style consistency

### Context for Next Session

1. Read `.planning/phases/05-documentation-error-handling-and-review-readiness/05-CONTEXT.md` for the locked decisions from discuss-phase
2. Read `PROJECT.md` for active scope and boundaries
3. Read `REQUIREMENTS.md` for milestone REQ IDs
4. Read `ROADMAP.md` for phase structure
5. Run `$gsd-execute-phase 5`, using the completed Phase 4 summaries as the module-boundary reference

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
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-RESEARCH.md` - structural split targets and module-boundary recommendations
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-VALIDATION.md` - structural and behavioral verification contract for Phase 4
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-01-PLAN.md` - resolver orchestrator split plan
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-01-SUMMARY.md` - resolver orchestrator decomposition summary
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-02-PLAN.md` - validation builder split plan
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-02-SUMMARY.md` - validation builder decomposition summary
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-03-PLAN.md` - support module boundary cleanup plan
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-03-SUMMARY.md` - support module boundary cleanup and full-suite verification summary
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-CONTEXT.md` - locked Phase 5 decisions for documentation surface, panic cleanup, and reviewer-guide scope
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-DISCUSSION-LOG.md` - audit trail of the Phase 5 discuss-phase choices
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-RESEARCH.md` - implementation research and the concentrated panic-path targets for Phase 5
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md` - Phase 5 verification contract for docs, panic cleanup, fmt, tests, and clippy
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-01-PLAN.md` - reviewer-surface documentation and guide plan
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-02-PLAN.md` - panic-path hardening and invariant-cleanup plan
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-03-PLAN.md` - consistency sweep and verification closeout plan

**Key Commands:**

- `$gsd-execute-phase 5` - execute the review-readiness plans against the completed Phase 4 boundaries
- `$gsd-progress` - review milestone state after Phase 4 completion
- `cargo test --manifest-path tools/apdr/Cargo.toml` - run Rust tests
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets` - lint touched Rust code

---

*State updated after Phase 5 planning on 2026-03-27*

