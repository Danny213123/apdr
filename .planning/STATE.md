---
gsd_state_version: 1.0
milestone: v2.2
milestone_name: Improve LLM Performance and Benchmark Performance on macOS
status: ready_to_plan
stopped_at: Completed 14-03-PLAN.md
last_updated: "2026-03-29T18:28:12Z"
last_activity: 2026-03-29
progress:
  total_phases: 4
  completed_phases: 2
  total_plans: 6
  completed_plans: 6
  percent: 100
---

# Project State: APDR

**Last Updated:** 2026-03-29
**Status:** Ready to Plan Phase 15
**Progress:** [██████████] 100%
**Last Activity:** 2026-03-29
**Last Activity Description:** Completed Phase 14 with replay proof checkers, bounded proof artifacts, and reviewer-facing macOS and Windows comparison notes
**Resume File:** .planning/phases/14-macos-execution-path-optimization/14-03-SUMMARY.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 15 — LangChain/LangGraph Tier3 Intelligence Improvements

---

## Current Position

Phase: 15 (langchain-langgraph-tier3-intelligence-improvements) — READY TO PLAN
Plan: 0 of TBD
Status: Ready to plan Phase 15
Last activity: 2026-03-29 -- Completed Phase 14 and ready to plan Phase 15

---

## Performance Metrics

- v2.2 plans completed: 6
- v2.2 phases completed: 2 of 4
- Active phase plan count: 0
- Phase 14 now provides deterministic macOS and Windows replay proof checkers plus bounded proof-note templates for later live evidence capture.

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
- [Phase 14]: Manifest cases use fixture-relative paths for cross-platform portability
- [Phase 14]: When replay_manifest is set, snippet_limit is ignored to prevent conflicting boundary controls
- [Phase 14]: `macos-replay` defaults to `release` when a build profile is not pinned explicitly
- [Phase 14]: Replay evidence now carries effective worker count and preflight warnings for Rosetta, backend drift, cache-state drift, and missing fresh binaries
- [Phase 14]: Proof validation now compares like-for-like slice metadata and preserved pass/skip outcomes instead of total duration alone
- [Phase 14]: Reviewer-facing proof notes and machine validation now share the same bounded artifact contract

### Pending Todos

- Plan Phase 15 for LangChain/LangGraph tier3 intelligence improvements.
- Capture live `14-macos-before.json` and `14-macos-after.json` when assembling milestone evidence.
- Import representative Windows guardrail artifacts before milestone closeout.

### Blockers/Concerns

- Live milestone proof still requires real macOS and Windows artifact capture; the repo now contains the checker contract and sample schema, not the final evidence pair.
- Phase 15 should improve agent behavior without falling back to more deterministic recovery tables.

---

## Session Continuity

Last session: 2026-03-29T18:28:12Z
Stopped at: Completed 14-03-PLAN.md
Resume file: .planning/phases/14-macos-execution-path-optimization/14-03-SUMMARY.md

---

*State updated after Phase 14 execution on 2026-03-29*
