---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Data-Driven Family Knowledge & LLM Recovery Accuracy
current_phase: 07
current_phase_name: Failure Baseline & Parity Slice
current_plan: -
status: planning
stopped_at: Phase 7 context gathered
last_updated: "2026-03-28T04:29:30.782Z"
last_activity: 2026-03-28
progress:
  total_phases: 4
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State: APDR

**Last Updated:** 2026-03-28
**Current Phase:** 07
**Current Plan:** -
**Current Phase Name:** Failure Baseline & Parity Slice
**Total Phases:** 4
**Total Plans in Phase:** 0
**Status:** Planning
**Progress:** [----------] 0%
**Last Activity:** 2026-03-28
**Last Activity Description:** Captured Phase 07 context for the tier3 parity baseline and family snapshot scope
**Last Date:** 2026-03-28T04:29:30.779Z
**Stopped At:** Phase 7 context gathered
**Resume File:** .planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Plan Phase 07 from the captured tier3 parity-slice context and snapshot decisions

---

## Current Position

Phase: 07 (Failure Baseline & Parity Slice) - NOT STARTED
Plan: -

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
- [x] Discuss Phase `07`
- [ ] Plan Phase `07`
- [ ] Build the generated tier3 parity manifest and Markdown summary
- [ ] Add benchmark-derived regression snapshots for touched family-knowledge cases

## Deferred Items

- [ ] Async I/O remains deferred unless accuracy work exposes it as the next bottleneck
- [ ] Structured tracing and CI benchmarking remain candidates for a later milestone

---

## Session

**Last Date:** 2026-03-28T04:29:30.779Z
**Stopped At:** Phase 7 context gathered
**Resume File:** .planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md

---

## Session Continuity

### What Just Happened

- Archived milestone v2.0 and tagged it locally as `v2.0`
- Started milestone v2.1 with scope centered on data-driven family knowledge and LLM recovery accuracy
- Confirmed the stopped APDR benchmark baseline in `runs\20260327-150339-apdr`
- Confirmed the matching `pllm` parity source in `pllm_results\csv\summary-all-runs.csv`
- Defined 11 scoped v2.1 requirements and mapped them across 4 new phases
- Captured Phase 07 context with the canonical slice fixed to the `70` tier3 parity cases, JSON-plus-Markdown baseline artifacts, benchmark-derived touched-family snapshots, and normalized milestone buckets
- Left unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

### What's Next

**Immediate:** Plan Phase `07` (`Failure Baseline & Parity Slice`).

**After that:** Build the tier3 parity manifest, normalize milestone buckets, and lock touched family behavior before Phase 8 changes runtime behavior.

### Context for Next Session

1. Read `.planning/PROJECT.md`
2. Read `.planning/REQUIREMENTS.md` and `.planning/ROADMAP.md`
3. Read `.planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md`
4. Read `runs\20260327-150339-apdr\summary.json`
5. Read `pllm_results\csv\summary-all-runs.csv`
6. Keep the unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

---

## Quick Reference

**Key Files**

- `.planning/PROJECT.md` - active milestone goals and constraints
- `.planning/REQUIREMENTS.md` - v2.1 requirement IDs and traceability
- `.planning/ROADMAP.md` - v2.1 phase structure
- `.planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md` - locked Phase 07 decisions for the canonical slice, artifact shape, and snapshot scope
- `runs\20260327-150339-apdr\summary.json` - stopped APDR benchmark baseline
- `pllm_results\csv\summary-all-runs.csv` - `pllm` parity comparison source
- `.planning/milestones/v2.0-ROADMAP.md` - archived v2.0 roadmap
- `.planning/milestones/v2.0-REQUIREMENTS.md` - archived v2.0 requirements

**Key Commands**

- `$gsd-plan-phase 7` - create the first execution plan from the captured Phase 07 context
- `$gsd-discuss-phase 7` - revisit or revise the Phase 07 context
- `$gsd-progress` - inspect current project status

---

*State updated after Phase 07 context capture on 2026-03-28*
