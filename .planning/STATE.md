---
gsd_state_version: 1.0
milestone: v2.2
milestone_name: Improve LLM Performance and Benchmark Performance on macOS
status: ready_to_execute
stopped_at: Phase 15 planning complete
last_updated: "2026-03-29T19:04:00Z"
last_activity: 2026-03-29
progress:
  total_phases: 4
  completed_phases: 2
  total_plans: 10
  completed_plans: 6
  percent: 60
---

# Project State: APDR

**Last Updated:** 2026-03-29
**Status:** Phase 15 planned; ready to execute
**Progress:** [██████░░░░] 60%
**Last Activity:** 2026-03-29
**Last Activity Description:** Planned Phase 15 into four execution waves covering AGT-01 through AGT-06
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 15 — LangChain/LangGraph Tier3 Intelligence Improvements

---

## Current Position

Phase: 15 (langchain-langgraph-tier3-intelligence-improvements) — READY TO EXECUTE
Plan: 0 of 4
Status: Planned
Last activity: 2026-03-29 -- Phase 15 plans created and verified for execution

---

## Performance Metrics

- v2.2 plans completed: 6
- v2.2 phases completed: 2 of 4
- Active phase plan count: 4
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
- Phase 15 is split into four sequential plans: benchmark harness and artifact contract, explicit agent-runtime seam, benchmark-fed memory plus context engineering, then small-model policy proof and quality checking.

### Pending Todos

- Execute Plan 15-01 first so the replay-slice benchmark contract exists before agent behavior changes land.
- Capture live `14-macos-before.json` and `14-macos-after.json` when assembling milestone evidence.
- Import representative Windows guardrail artifacts before milestone closeout.

### Blockers/Concerns

- Live milestone proof still requires real macOS and Windows artifact capture; the repo now contains the checker contract and sample schema, not the final evidence pair.
- Phase 15 must improve replay-slice success through agent behavior, memory, context engineering, and model policy rather than prompt-taxonomy or rule-table growth.

---

## Session Continuity

Last session: 2026-03-29T19:04:00Z
Stopped at: Phase 15 planning complete; next step is executing the four Phase 15 plans
Resume file: None

---

*State updated after Phase 15 planning on 2026-03-29*
