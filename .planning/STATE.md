---
gsd_state_version: 1.0
milestone: v2.2
milestone_name: Improve LLM Performance and Benchmark Performance on macOS
status: ready_to_execute
last_updated: "2026-03-29T04:48:28Z"
last_activity: 2026-03-29
---

# Project State: APDR

**Last Updated:** 2026-03-29
**Status:** Phase 14 planned; ready to execute
**Progress:** [███░░░░░░░] 25%
**Last Activity:** 2026-03-29
**Last Activity Description:** Planned Phase 14 into three execution waves covering MAC-03, MAC-04, and WIN-01
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 14 - macOS Execution-Path Optimization

---

## Current Position

Phase: 14 of 16 (macOS Execution-Path Optimization)
Plan: 0 of 3
Status: Planned
Last activity: 2026-03-29 — Phase 14 plans created and verified for execution

---

## Performance Metrics

- v2.2 plans completed: 3
- v2.2 phases completed: 1 of 4
- Active phase plan count: 3
- Phase 13 now provides the first comparable measurement contract and checker for later milestone claims.

---

## Accumulated Context

### Decisions

- v2.0 closed the Rust modernization track and remains the last fully shipped milestone.
- v2.1 delivered useful family-knowledge and verification context, but its live-proof closeout remained unfinished when v2.2 superseded it on 2026-03-28.
- v2.2 follows a fixed order: Phase 13 measurement, Phase 14 macOS execution-path optimization, Phase 15 LangChain/LangGraph tier3 intelligence improvements, then Phase 16 closeout proof.
- v2.2 accuracy gains should come from tool use, reflection, context engineering, and model-specific inference policy rather than new deterministic recovery tables.
- Phase 13 is split into three sequential plans: canonical run-contract capture, APDR per-case metadata/timing propagation, then evidence normalization plus a deterministic measurement checker.
- Plan 13-01 established `benchmark_ui/run_contract.py` as the canonical Phase 13 metadata contract and persisted it into saved benchmark runs.
- Plan 13-02 pushed that same contract into APDR through `--run-contract-json` and made `llm_duration_ms` plus `docker_startup_duration_ms` first-class per-case timing fields.
- Plan 13-03 finished the evidence layer with fixture-safe reporting, a deterministic contract checker, and reviewer-facing env-fast/docker-proof sample artifacts.
- Phase 14 is split into three sequential plans: lock the macOS and Windows replay slices, build the native macOS replay runner and fast-lane policy, then add the regression/proof checker package for macOS gains plus Windows non-regression.

### Pending Todos

- Execute Plan 14-01 first so the replay boundary is fixed before tuning macOS performance.
- Capture cold and warm macOS replay baselines once the replay runner lands.
- Produce the representative Windows guardrail artifact or import it from the Windows host before closing Phase 14.

### Blockers/Concerns

- Mixed architecture, backend, or cache-state runs will invalidate milestone comparisons unless Phase 13 lands first.
- macOS speedups must not quietly regress Windows runtime or distort correctness on preserved pass and skip cases.

---

## Session Continuity

Last session: 2026-03-29
Stopped at: Phase 14 planning complete; next step is executing the three Phase 14 plans
Resume file: None

---

*State updated after Phase 14 planning on 2026-03-29*
