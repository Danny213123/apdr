---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Data-Driven Family Knowledge & LLM Recovery Accuracy
current_phase:
current_phase_name: Defining requirements
current_plan:
status: defining-requirements
stopped_at: Milestone v2.1 initialized
last_updated: "2026-03-27T23:39:03.1784567-04:00"
last_activity: 2026-03-27
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State: APDR

**Last Updated:** 2026-03-27
**Current Phase:** Not started
**Current Plan:** -
**Current Phase Name:** Defining requirements
**Total Phases:** 0
**Total Plans in Phase:** 0
**Status:** Defining requirements
**Progress:** [----------] 0%
**Last Activity:** 2026-03-27
**Last Activity Description:** Started milestone v2.1 and anchored scope to the stopped APDR run plus `pllm` parity data
**Last Date:** 2026-03-27T23:39:03.1784567-04:00
**Stopped At:** Milestone v2.1 initialized
**Resume File:** .planning/PROJECT.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Define v2.1 requirements for data-driven family knowledge and LLM recovery accuracy

---

## Current Position

Phase: Not started (defining requirements)
Plan: -

## Milestone Snapshot

**Current Milestone:** v2.1 - Data-Driven Family Knowledge & LLM Recovery Accuracy

**Goal:** Replace brittle hardcoded family-knowledge behavior with data-driven rules and improve APDR's LLM recovery accuracy on the stopped benchmark failures surfaced on 2026-03-27.

**Key Benchmark Context**

- `runs\20260327-150339-apdr` processed 1,257 cases with 285 failures and 297 skips.
- 228 of the 285 failures were tier3.
- The dominant APDR failure buckets were `module-not-found` (86), `environment-build-failed` (62), and `version-not-found` (33).
- `pllm_results\csv\summary-all-runs.csv` overlaps all 1,257 processed cases and shows 87 APDR failures where `pllm` passed at least once, including 72 strong wins and 51 `10/10` `pllm` wins.

**Carried-Forward Constraints**

- Keep Windows and Docker support intact.
- Preserve benchmark continuity while comparing APDR against the stopped run and the `pllm` slice.
- Improve the existing recovery path before considering any provider swap.

## Current Blockers

None.

## Active TODOs

- [ ] Define v2.1 requirements
- [ ] Create the v2.1 roadmap
- [ ] Start Phase 7 work on the stopped-run failure taxonomy and parity slice

## Deferred Items

- [ ] Async I/O remains deferred unless accuracy work exposes it as the next bottleneck
- [ ] Structured tracing and CI benchmarking remain candidates for a later milestone

---

## Session

**Last Date:** 2026-03-27T23:39:03.1784567-04:00
**Stopped At:** Milestone v2.1 initialized
**Resume File:** .planning/PROJECT.md

---

## Session Continuity

### What Just Happened

- Archived milestone v2.0 and tagged it locally as `v2.0`.
- Started milestone v2.1 with scope centered on data-driven family knowledge and LLM recovery accuracy.
- Anchored the new milestone to the stopped APDR benchmark run in `runs\20260327-150339-apdr`.
- Confirmed the separate `pllm` comparison data lives in `pllm_results\csv\summary-all-runs.csv` and exposes a concrete parity slice of APDR failures that `pllm` handled.
- Left unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched.

### What's Next

**Immediate:** Define the v2.1 requirements.

**After that:** Create the roadmap and identify the first phase number and goal.

### Context for Next Session

1. Read `.planning/PROJECT.md`.
2. Read `runs\20260327-150339-apdr\summary.json`.
3. Read `pllm_results\csv\summary-all-runs.csv`.
4. Keep the unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched.

---

## Quick Reference

**Key Files**

- `.planning/PROJECT.md` - active milestone goals and constraints
- `.planning/MILESTONES.md` - milestone history
- `runs\20260327-150339-apdr\summary.json` - stopped APDR benchmark baseline
- `pllm_results\csv\summary-all-runs.csv` - parity comparison source
- `.planning/milestones/v2.0-ROADMAP.md` - archived v2.0 roadmap
- `.planning/milestones/v2.0-REQUIREMENTS.md` - archived v2.0 requirements

**Key Commands**

- `$gsd-new-milestone` - continue milestone initialization
- `$gsd-progress` - inspect current project status

---

*State updated after milestone v2.1 initialization on 2026-03-27*
