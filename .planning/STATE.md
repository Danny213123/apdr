---
gsd_state_version: 1.0
milestone: v2.2
milestone_name: Improve LLM Performance and Benchmark Performance on macOS
status: in_progress
last_updated: "2026-03-29T04:20:27Z"
last_activity: 2026-03-29
---

# Project State: APDR

**Last Updated:** 2026-03-29
**Status:** Phase 13 in progress; Plans 13-01 and 13-02 complete
**Progress:** [█░░░░░░░░░] 13%
**Last Activity:** 2026-03-29
**Last Activity Description:** Completed Plan 13-02 by propagating the canonical run contract into APDR case artifacts and adding explicit LLM/Docker-startup timings
**Resume File:** .planning/phases/13-measurement-and-run-contract-hardening/13-02-SUMMARY.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 13 - Measurement and Run-Contract Hardening

---

## Current Position

Phase: 13 of 16 (Measurement and Run-Contract Hardening)
Plan: 2 of 3
Status: Executing Plan 13-03 next
Last activity: 2026-03-29 — Plans 13-01 and 13-02 completed; Plan 13-03 is next

---

## Performance Metrics

- v2.2 plans completed: 2
- v2.2 phases completed: 0 of 4
- Active phase plan count: 3
- Progress baseline remains pending until Phase 13 execution captures the first comparable artifacts.

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

### Pending Todos

- Choose the locked replay slice and baseline commands during Phase 13 and Phase 14 execution.
- Decide which model or build-profile comparisons are worth capturing once the measurement contract exists.
- Execute Plan 13-03 next so reporting and fixture-backed artifacts enforce the Phase 13 measurement contract.

### Blockers/Concerns

- Mixed architecture, backend, or cache-state runs will invalidate milestone comparisons unless Phase 13 lands first.
- macOS speedups must not quietly regress Windows runtime or distort correctness on preserved pass and skip cases.

---

## Session Continuity

Last session: 2026-03-29
Stopped at: Plan 13-02 complete; next step is Plan 13-03
Resume file: .planning/phases/13-measurement-and-run-contract-hardening/13-02-SUMMARY.md

---

*State updated after completing Plan 13-02 on 2026-03-29*
