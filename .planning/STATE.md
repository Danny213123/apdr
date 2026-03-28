---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Data-Driven Family Knowledge & LLM Recovery Accuracy
current_phase: 07
current_phase_name: failure-baseline-parity-slice
current_plan: 3
status: paused
stopped_at: Completed 07-02-PLAN.md
paused_at: Phase 7 execution handoff
last_updated: "2026-03-28T16:49:37.613Z"
last_activity: 2026-03-28
progress:
  total_phases: 4
  completed_phases: 0
  total_plans: 3
  completed_plans: 2
  percent: 67
---

# Project State: APDR

**Last Updated:** 2026-03-28
**Current Phase:** 07
**Current Plan:** 3
**Current Phase Name:** failure-baseline-parity-slice
**Total Phases:** 4
**Total Plans in Phase:** 3
**Status:** Ready to execute
**Progress:** [███████░░░] 67%
**Last Activity:** 2026-03-28
**Last Activity Description:** Phase 07 execution started
**Paused At:** Phase 7 execution handoff
**Last Date:** 2026-03-28T16:49:37.610Z
**Stopped At:** Completed 07-02-PLAN.md
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 07 — failure-baseline-parity-slice

---

## Current Position

Phase: 07 (failure-baseline-parity-slice) — EXECUTING
Plan: 1 of 3

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
- [x] Plan Phase `07`
- [ ] Execute Phase `07` (`Failure Baseline & Parity Slice`)
- [ ] Build the generated tier3 parity manifest and Markdown summary
- [ ] Add benchmark-derived regression snapshots for touched family-knowledge cases
- [ ] Add the baseline checker and Phase 8 handoff note

## Deferred Items

- [ ] Async I/O remains deferred unless accuracy work exposes it as the next bottleneck
- [ ] Structured tracing and CI benchmarking remain candidates for a later milestone

---

## Session

**Last Date:** 2026-03-28T00:45:23.5979582-04:00
**Stopped At:** Phase 7 planned
**Resume File:** .planning/phases/07-failure-baseline-parity-slice/07-01-PLAN.md

---

## Session Continuity

### What Just Happened

- Archived milestone v2.0 and tagged it locally as `v2.0`
- Started milestone v2.1 with scope centered on data-driven family knowledge and LLM recovery accuracy
- Confirmed the stopped APDR benchmark baseline in `runs\20260327-150339-apdr`
- Confirmed the matching `pllm` parity source in `pllm_results\csv\summary-all-runs.csv`
- Defined 11 scoped v2.1 requirements and mapped them across 4 new phases
- Captured Phase 07 context with the canonical slice fixed to the `70` tier3 parity cases, JSON-plus-Markdown baseline artifacts, benchmark-derived touched-family snapshots, and normalized milestone buckets
- Planned Phase 07 with `07-RESEARCH.md`, `07-VALIDATION.md`, and three execution plans covering the canonical tier3 parity manifest, the touched-family snapshot corpus, and the final baseline checker plus Phase 8 handoff note
- Left unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

### What's Next

**Immediate:** Execute Phase `07` from `07-01-PLAN.md`.

**After that:** Generate the canonical parity manifest, build the touched-family snapshot corpus, and close the baseline with the local checker and targeted resolver guardrail.

### Context for Next Session

1. Read `.planning/PROJECT.md`
2. Read `.planning/REQUIREMENTS.md` and `.planning/ROADMAP.md`
3. Read `.planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md`, `07-RESEARCH.md`, and `07-VALIDATION.md`
4. Read `.planning/phases/07-failure-baseline-parity-slice/07-01-PLAN.md`, `07-02-PLAN.md`, and `07-03-PLAN.md`
5. Read `runs\20260327-150339-apdr\summary.json` and `pllm_results\csv\summary-all-runs.csv`
6. Keep the unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` untouched

---

## Quick Reference

**Key Files**

- `.planning/PROJECT.md` - active milestone goals and constraints
- `.planning/REQUIREMENTS.md` - v2.1 requirement IDs and traceability
- `.planning/ROADMAP.md` - v2.1 phase structure
- `.planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md` - locked Phase 07 decisions for the canonical slice, artifact shape, and snapshot scope
- `.planning/phases/07-failure-baseline-parity-slice/07-RESEARCH.md` - implementation research for the parity manifest, bucket normalization, and family snapshot boundary
- `.planning/phases/07-failure-baseline-parity-slice/07-VALIDATION.md` - Phase 07 validation contract for artifact generation and targeted resolver coverage
- `.planning/phases/07-failure-baseline-parity-slice/07-01-PLAN.md` - canonical tier3 parity manifest plan
- `.planning/phases/07-failure-baseline-parity-slice/07-02-PLAN.md` - touched-family snapshot corpus plan
- `.planning/phases/07-failure-baseline-parity-slice/07-03-PLAN.md` - baseline checker and Phase 8 handoff plan
- `runs\20260327-150339-apdr\summary.json` - stopped APDR benchmark baseline
- `pllm_results\csv\summary-all-runs.csv` - `pllm` parity comparison source
- `.planning/milestones/v2.0-ROADMAP.md` - archived v2.0 roadmap
- `.planning/milestones/v2.0-REQUIREMENTS.md` - archived v2.0 requirements

**Key Commands**

- `$gsd-execute-phase 7` - execute the canonical parity baseline, family snapshot, and baseline checker plans
- `$gsd-progress` - confirm that Phase 7 is planned and ready to execute
- `python scripts/build_phase7_parity_manifest.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- `python scripts/build_phase7_family_snapshots.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --cases-root runs/20260327-150339-apdr/cases --fixtures-root tools/apdr/tests/phase7_family_fixtures --output-json .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`

---

*State updated after Phase 07 planning on 2026-03-28*
