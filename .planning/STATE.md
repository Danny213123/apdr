---
gsd_state_version: 1.0
milestone: v2.3
milestone_name: Tier3 Validation Recovery and Reliability
status: defining-requirements
stopped_at:
last_updated: "2026-03-30T12:01:19Z"
last_activity: 2026-03-30
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State: APDR

**Last Updated:** 2026-03-30
**Status:** Defining requirements
**Progress:** [░░░░░░░░░░] 0%
**Last Activity:** 2026-03-30
**Last Activity Description:** Milestone v2.3 started from live benchmark evidence and superseded blocked v2.2 closeout work
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Define v2.3 requirements for tier3 validation recovery, backend escalation, and reporting reliability

---

## Current Position

Phase: Not started (defining requirements)
Plan: -
Status: Defining requirements
Last activity: 2026-03-30 -- Milestone v2.3 started from the March 30 live benchmark baseline

---

## Performance Metrics

- v2.3 plans completed: 0
- v2.3 phases completed: 0 of 0
- Active phase plan count: 0
- The active milestone baseline is the March 30 live benchmark surface rather than the v2.2 sample-proof package.

---

## Accumulated Context

### Decisions

- v2.0 closed the Rust modernization track and remains the last fully shipped milestone.
- v2.1 delivered useful family-knowledge and verification context, but its live-proof closeout remained unfinished when v2.2 superseded it on 2026-03-28.
- v2.3 supersedes the blocked v2.2 closeout on 2026-03-30 because the latest live run exposed fallback and backend-routing reliability gaps that should be addressed before more proof packaging.
- The active v2.3 baseline is `runs/20260330-020943-apdr` resumed from `runs/20260330-004502-apdr`, with 26 passes in 395 non-skipped tier3 cases.
- The latest live run recorded repeated LangGraph fallback crashes on duplicate `confidence` state-key registration and showed env-only attempt metadata despite `--validation-backend llm`.
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
- [Plan 15-01]: Tier3 benchmark artifacts now record agent mode, retrieval profile, tool profile, thinking mode, context window, and inference policy directly in the replay output.
- [Plan 15-01]: Probe-only mode validates artifact shape without depending on live model execution.
- [Plan 15-02]: Tier3 request metadata can now select direct, manual, LangGraph, or LangChain benchmarking paths without patching code between runs.
- [Plan 15-02]: Agent failures now propagate explicit abstain or failure reasons instead of silently converting unknown mappings into fabricated success.
- [Plan 15-03]: Replay artifacts can now update inspectable success and failure memory rather than relying on prompt-side rule growth.
- [Plan 15-03]: Retrieval profiles now control context selection and benchmark-context summarization for tier3 resolution.
- [Plan 15-04]: Small-model policy variants are now explicit and attributable for `qwen3.5:9b`, and Phase 15 quality claims can be machine-checked against baseline-versus-candidate artifacts.
- Phase 16 is split into three sequential plans: build the closeout evidence contract and readiness checker, write the reviewer-facing comparison pack, then reconcile requirement truth and milestone closeout without overstating live proof.
- [Phase 16]: Milestone closeout must distinguish sample-contract proof from live benchmark evidence and may not mark live-proof requirements complete unless the live artifacts exist and pass the carried-forward checkers.
- [Phase 16]: The final closeout package should reference the dedicated Phase 14 and Phase 15 proof notes instead of duplicating their tables inline.
- [Phase 16]: The current terminal evidence mode is `sample-contract-only`, so the phase is complete but the milestone still waits on live artifact capture for final signoff.

### Pending Todos

- Capture live `14-macos-before.json` and `14-macos-after.json` when assembling milestone evidence.
- Capture live `15-tier3-baseline.json` and `15-tier3-candidate.json` on a benchmark-capable host if you want fresh Phase 15 evidence beyond the in-repo sample proof contract.
- Import representative Windows guardrail artifacts before milestone closeout.
- Rerun the Phase 14, Phase 15, and Phase 16 checkers against the live artifact set once those files exist.

### Blockers/Concerns

- Live milestone proof still requires real macOS and Windows artifact capture; the repo now contains the checker contract and sample schema, not the final evidence pair.
- Phase 16 has to keep sample-backed proof contracts and live benchmark evidence clearly separated so `EVD-04` and `EVD-06` are not overclaimed.

---

## Session Continuity

Last session: 2026-03-30T12:01:19Z
Stopped at: Milestone v2.3 initialization in progress
Resume file: None

---

*State updated after starting milestone v2.3 on 2026-03-30*
