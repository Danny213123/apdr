---
gsd_state_version: 1.0
milestone: v2.3
milestone_name: Tier3 Validation Recovery and Reliability
status: planning
stopped_at: Phase 20 complete, Phase 21 ready to plan
last_updated: "2026-04-01T19:36:12Z"
last_activity: 2026-04-01
progress:
  total_phases: 5
  completed_phases: 4
  total_plans: 12
  completed_plans: 12
  percent: 80
---

# Project State: APDR

**Last Updated:** 2026-04-01
**Status:** Ready to plan
**Progress:** [████████░░] 80%
**Last Activity:** 2026-04-01
**Last Activity Description:** Phase 20 complete, transitioned to Phase 21
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 21 — live-evidence-and-closeout-pack

---

## Current Position

Phase: 21
Plan: Not started
Status: Ready to discuss or plan
Last activity: 2026-04-01 -- Phase 20 execution completed and verification passed

---

## Performance Metrics

- v2.3 phases completed: 4 of 5
- v2.3 plans completed: 12
- Next phase plan count: TBD
- Active live baseline: `runs/20260330-020943-apdr` resumed from `runs/20260330-004502-apdr`
- Latest completed plan: `20-03` in `17 min` across `2` tasks and `7` modified files

---

## Accumulated Context

### Decisions

- v2.3 starts at Phase 17 and replaces v2.2 as the active roadmap milestone on 2026-03-30.
- v2.2 remains historical as superseded unfinished after Phase 16 sample-contract closeout; live proof and final signoff stayed open.
- The v2.3 phase order is fallback stability -> backend escalation -> accounting integrity -> bucket recovery -> live evidence.
- The March 30 2026 live tier3 run is the benchmark baseline for v2.3, not the v2.2 sample-proof artifact set.
- Windows and Docker correctness remain hard constraints while `llm` routing changes land.
- [Phase 17-llm-fallback-stability-and-outcome-tracing]: Keep llm validation env-first and record the terminal agent outcome as a synthetic llm attempt instead of collapsing back to env-only metadata.
- [Phase 17-llm-fallback-stability-and-outcome-tracing]: Preserve env failure context on non-pass agent summaries so later artifact work can expose both the original validation failure and the fallback terminal state.
- [Phase 17]: Expose fallback summary output with exact lowercase keys so test_executor can copy artifact fields directly.
- [Phase 17]: Derive fallback invocation and terminal outcome from llm attempts plus agent invocation counts instead of overloading validation_status.
- [Phase 17]: Keep benchmark pass/skip/fail classification driven by validation_status, validation_reason, and existing success rules while surfacing fallback metadata separately.
- [Phase 17]: Keep the Phase 17 proof anchored to a fixed March 30 slice and validate that manifest order explicitly in the checker.
- [Phase 17]: Treat the frozen March 30 run as before-state evidence; probe mode is the deterministic in-repo gate, while live mode is the post-replay audit for crash removal and fallback keys.
- [Phase 18-backend-escalation-and-path-truth]: Keep `llm` routing env-first, then Docker, then final `llm-agent`; do not skip env globally for `llm` mode.
- [Phase 18-backend-escalation-and-path-truth]: Docker escalation must stay targeted and signal-based rather than retrying every env failure.
- [Phase 18-backend-escalation-and-path-truth]: Keep top-level `validation_backend` equal to the requested run mode and surface actual route truth separately.
- [Phase 18-backend-escalation-and-path-truth]: Prove Phase 18 on deterministic tests plus a small fixed March 30 live-derived replay slice.
- [Phase 18]: Persist routed backend truth as `validation_path` and `escalated_backend` rather than overloading configured backend semantics.
- [Phase 18]: Treat missing Docker in APDR `llm` mode as a targeted warning while keeping pure Docker validation as a hard requirement.
- [Phase 19-failure-classification-and-run-accounting-integrity]: Preserve Phase 18 backend-path truth while adding explicit failure-family classification for environment-specific versus dependency-resolution outcomes.
- [Phase 19-failure-classification-and-run-accounting-integrity]: Host-runtime and framework-runtime skips must remain skips, never benchmark passes.
- [Phase 19-failure-classification-and-run-accounting-integrity]: Resumed historical rows must stay separable from live rows so proof and comparison logic can compute live-only conclusions.
- [Phase 19-failure-classification-and-run-accounting-integrity]: Prove Phase 19 on a fixed March 30 live-derived slice plus a deterministic mixed-provenance fixture.
- [Phase 19]: Keep benchmark-proof accounting anchored to the March 30 wrapper summary and validate provenance through the real `BenchmarkService` snapshot path.
- [Phase 20-dominant-bucket-recovery-gains]: Prove gains on a fixed nine-case dominant-bucket slice derived from the March 30 live baseline rather than a full-corpus rerun.
- [Phase 20-dominant-bucket-recovery-gains]: Attack the dominant buckets with bounded rule/data changes first: module alias recovery and explicit bucket exits, then compatibility replacements and Python floors.
- [Phase 20-dominant-bucket-recovery-gains]: Keep Phase 20 proof like-for-like by requiring identical `slice_id`, `validation_backend: llm`, and `model_name: qwen3.5:9b` across baseline and candidate artifacts.
- [Phase 20]: Keep the dominant-bucket proof contract deterministic in-repo and defer the full live replay evidence package to Phase 21.

### Pending Todos

- Next step is `$gsd-discuss-phase 21` or `$gsd-plan-phase 21`.

### Blockers/Concerns

- Do not treat v2.2 sample-backed proof as live evidence for v2.3 closeout.
- Benchmark reporting changes must improve truthfulness without breaking comparability against the March 30 2026 baseline.
- The fixed-slice Phase 18 proof is deterministic and green, but a real replay of that slice is still useful milestone evidence for later closeout.
- Phase 21 must turn the deterministic Phase 20 proof package into reviewer-facing live evidence without changing the locked March 30 comparison contract.

---

## Session Continuity

Last session: 2026-04-01T19:36:12Z
Stopped at: Phase 20 complete, Phase 21 ready to plan
Resume file: None

---

*State updated after Phase 20 completion on 2026-04-01*
