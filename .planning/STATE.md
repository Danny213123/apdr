---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Data-Driven Family Knowledge & LLM Recovery Accuracy
current_phase: 10
current_phase_name: benchmark-verification-accuracy-closeout
current_plan: 2
status: paused
stopped_at: Completed 10-01-PLAN.md
paused_at: Phase 8 completion handoff
last_updated: "2026-03-28T21:04:06.366Z"
last_activity: 2026-03-28
progress:
  total_phases: 4
  completed_phases: 3
  total_plans: 12
  completed_plans: 10
  percent: 83
---

# Project State: APDR

**Last Updated:** 2026-03-28
**Current Phase:** 10
**Current Plan:** 2
**Current Phase Name:** benchmark-verification-accuracy-closeout
**Total Phases:** 4
**Total Plans in Phase:** 3
**Status:** Ready to execute
**Progress:** [████████░░] 83%
**Last Activity:** 2026-03-28
**Last Activity Description:** Phase 10 execution started
**Paused At:** Phase 8 completion handoff
**Last Date:** 2026-03-28T21:04:06.362Z
**Stopped At:** Completed 10-01-PLAN.md
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 10 — benchmark-verification-accuracy-closeout

---

## Current Position

Phase: 10 (benchmark-verification-accuracy-closeout) — EXECUTING
Plan: 2 of 3

## Milestone Snapshot

**Current Milestone:** v2.1 - Data-Driven Family Knowledge & LLM Recovery Accuracy

**Goal:** Replace brittle hardcoded family-knowledge behavior with data-driven rules and improve APDR's LLM recovery accuracy on the stopped benchmark failures surfaced on 2026-03-27.

**Key Benchmark Context**

- `runs\20260327-150339-apdr` processed 1,257 cases with 285 failures and 297 skips
- 228 of the 285 failures were tier3
- The dominant APDR failure buckets were `module-not-found` (86), `environment-build-failed` (62), and `version-not-found` (33)
- `pllm_results\csv\summary-all-runs.csv` overlaps all 1,257 processed cases and shows 87 APDR failures where `pllm` passed at least once, including 72 strong wins and 51 `10/10` `pllm` wins

**Carried-Forward Constraints**

- Keep Windows and Docker support intact
- Preserve benchmark continuity while comparing APDR against the stopped run and the `pllm` slice
- Improve the existing recovery path before considering any provider swap

## Current Blockers

None.

## Active TODOs

- [x] Define v2.1 requirements
- [x] Create the v2.1 roadmap
- [x] Execute Phase `07` (`Failure Baseline & Parity Slice`)
- [x] Execute Phase `08` (`Data-Driven Family Knowledge Runtime`)
- [ ] Plan Phase `09` (`Targeted Tier3 Recovery Accuracy`)
- [ ] Reduce the parity-slice `module-not-found`, `version-not-found`, and dependency-mapping failures without reopening the Phase 8 migration boundary
- [ ] Preserve the Phase 7 parity baseline and the Phase 8 family-runtime checker while targeted recovery work lands

## Deferred Items

- [ ] Async I/O remains deferred unless accuracy work exposes it as the next bottleneck
- [ ] Structured tracing and CI benchmarking remain candidates for a later milestone

---

## Session

**Last Date:** 2026-03-28T18:30:00.000Z
**Stopped At:** Phase 8 complete
**Resume File:** .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md

---

## Session Continuity

### What Just Happened

- Archived milestone v2.0 and tagged it locally as `v2.0`
- Started milestone v2.1 with scope centered on data-driven family knowledge and LLM recovery accuracy
- Locked the stopped APDR benchmark baseline in `runs\20260327-150339-apdr` against the matching `pllm` parity source in `pllm_results\csv\summary-all-runs.csv`
- Completed Phase 07 with the canonical `70`-case parity slice, the `17`-case watchlist, the `17`-case touched-family fixture corpus, and the deterministic baseline checker
- Completed Phase 08 with curated touched-family JSON data, resolver runtime wiring that consumes that data, bounded Phase 7 family regression tests, and `scripts/check_phase8_family_runtime.py`
- Left unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

### What's Next

**Immediate:** Plan Phase `09` with `$gsd-plan-phase 9`.

**After that:** Land targeted recovery changes against the locked parity slice while keeping the Phase 8 family-runtime checker green.

### Context for Next Session

1. Read `.planning/PROJECT.md`
2. Read `.planning/REQUIREMENTS.md` and `.planning/ROADMAP.md`
3. Read `.planning/phases/08-data-driven-family-knowledge-runtime/08-RESEARCH.md`, `08-VALIDATION.md`, `08-FAMILY-RUNTIME.md`, `08-01-SUMMARY.md`, `08-02-SUMMARY.md`, and `08-03-SUMMARY.md`
4. Read `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`, `07-FAMILY-SNAPSHOTS.md`, and `07-family-snapshot-manifest.json`
5. Read `runs\20260327-150339-apdr\summary.json` and `pllm_results\csv\summary-all-runs.csv`
6. Keep the unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

---

## Quick Reference

**Key Files**

- `.planning/PROJECT.md` - active milestone goals and constraints
- `.planning/REQUIREMENTS.md` - v2.1 requirement IDs and traceability
- `.planning/ROADMAP.md` - v2.1 phase structure
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` - Phase 8 runtime ownership, diagnostics contract, and Phase 9 handoff note
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-01-SUMMARY.md` - curated touched-family data model and validation summary
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-02-SUMMARY.md` - runtime wiring summary
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-03-SUMMARY.md` - fixture regression and checker summary
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md` - canonical parity baseline and touched-family migration boundary
- `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` - locked 17-case family snapshot corpus
- `runs\20260327-150339-apdr\summary.json` - stopped APDR benchmark baseline
- `pllm_results\csv\summary-all-runs.csv` - `pllm` parity comparison source

**Key Commands**

- `$gsd-plan-phase 9` - plan the targeted tier3 recovery phase against the locked Phase 7 and Phase 8 artifacts
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

---

*State updated after Phase 08 completion on 2026-03-28*
