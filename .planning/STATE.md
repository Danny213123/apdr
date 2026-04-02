---
gsd_state_version: 1.0
milestone: v2.4
milestone_name: Docker-First LLM Validation Decision and Proof
status: executing
stopped_at: Completed 22-docker-first-policy-and-safe-degradation-01-PLAN.md
last_updated: "2026-04-02T00:53:58.302Z"
last_activity: 2026-04-02
progress:
  total_phases: 5
  completed_phases: 1
  total_plans: 6
  completed_plans: 4
  percent: 67
---

# Project State: APDR

**Last Updated:** 2026-04-02
**Status:** Executing Phase 22
**Progress:** [███████░░░] 67%
**Last Activity:** 2026-04-02
**Last Activity Description:** Completed Phase 22 plan 01 and advanced to plan 02
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 22 — docker-first-policy-and-safe-degradation

---

## Current Position

Phase: 22 (docker-first-policy-and-safe-degradation) — EXECUTING
Plan: 2 of 3
Status: Executing Phase 22
Last activity: 2026-04-02 -- Completed plan 22-01 and advanced to plan 22-02

---

## Performance Metrics

- Active milestone: `v2.4 Docker-First LLM Validation Decision and Proof`
- Planned phases: 5
- Active phase plan count: 3
- Last shipped milestone: `v2.3 Tier3 Validation Recovery and Reliability`
- Shipped scope: 5 phases, 15 plans, 30 tasks
- Fixed-slice live evidence: baseline `0/9` passes -> candidate `2/9` passes
- Fixed-slice dominant bucket deltas: `module-not-found -3`, `version-not-found -3`, `environment-build-failed -3`
- Active live baseline for the shipped evidence: `runs/20260330-020943-apdr` resumed from `runs/20260330-004502-apdr`
- Final live evidence candidate: `runs/20260401-173232-apdr` resumed from `runs/20260401-162919-apdr`
- Phase 21.1 footprint proof: `source_delta -5.55GB`, `cache_delta -15.04GB`, `target_delta -20.45GB`
- Current local footprint candidate: `tools ~2.9G`, `tools/apdr/.apdr-cache ~1.3G`, `tools/apdr/target` removed
- Phase 22 plan 01 execution: `9min`, `2 tasks`, `5 files`, commits `ebd3810` and `e787cff`

---

## Accumulated Context

### Decisions

- v2.3 starts at Phase 17 and replaces v2.2 as the active roadmap milestone on 2026-03-30.
- v2.2 remains historical as superseded unfinished after Phase 16 sample-contract closeout; live proof and final signoff stayed open.
- The v2.3 phase order is fallback stability -> backend escalation -> accounting integrity -> bucket recovery -> live evidence.
- The March 30 2026 live tier3 run is the benchmark baseline for v2.3, not the v2.2 sample-proof artifact set.
- Windows and Docker correctness remain hard constraints while `llm` routing changes land.
- [Phase 17-llm-fallback-stability-and-outcome-tracing]: Keep llm validation env-first and record the terminal agent outcome as a synthetic llm attempt instead of collapsing back to env-only metadata.
- [Phase 18-backend-escalation-and-path-truth]: Keep `llm` routing env-first, then Docker, then final `llm-agent`; do not skip env globally for `llm` mode.
- [Phase 19-failure-classification-and-run-accounting-integrity]: Preserve Phase 18 backend-path truth while adding explicit failure-family classification for environment-specific versus dependency-resolution outcomes.
- [Phase 20-dominant-bucket-recovery-gains]: Prove gains on a fixed nine-case dominant-bucket slice derived from the March 30 live baseline rather than a full-corpus rerun.
- [Phase 21-live-evidence-and-closeout-pack]: Reuse the fixed Phase 20 nine-case dominant-bucket slice and delta contract rather than widening the evidence surface for closeout.
- [Phase 21-live-evidence-and-closeout-pack]: Do not treat synthetic candidate samples or pre-Phase-20 saved runs as v2.3 live closeout evidence.
- [Phase 21-live-evidence-and-closeout-pack]: `EVD-08` closes only when the live artifact pair, representative case pack, and final closeout checker pass together.
- [Phase 21]: The final candidate evidence came from `runs/20260401-173232-apdr` after resuming `runs/20260401-162919-apdr`; one tail case is preserved explicitly as `validation_status: interrupted` rather than being silently dropped.
- [Milestone closeout]: v2.3 is archived on the strength of the fixed-slice live evidence pack and should not be presented as a full-corpus rerun win.
- [Milestone start]: v2.4 begins by testing whether the repaired `llm` path still needs an env-first hop or should move to Docker-first on supported environments.
- [Milestone structure]: v2.4 now starts with an urgent repository-footprint reduction phase, then continues docker-first policy -> policy truth -> env-first-vs-docker-first comparison -> final decision closeout.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: The inserted phase should target both GitHub/source-distributed bytes and large local tool outputs, with priority on `tools/` and especially tracked `tools/apdr/target-*` directories plus local `tools/apdr/target` and `tools/apdr/.apdr-cache`.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: Prefer structural/default reduction and supported cleanup flows over docs-only advice to manually delete gigabytes.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: Phase 21.1 is planned as a three-step execution flow: source-distributed artifact cleanup, local cache/build default and cleanup improvements, then a deterministic footprint proof package.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: The proof contract must distinguish tracked source bloat from local cache/build bloat rather than collapsing all reclaimed bytes into one number.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: Cache-default work must update every direct repo-local fallback, including `ResolveConfig`, cache commands, classify-log, and learned family knowledge, and should reuse `tools/apdr/tests/test_cache.rs` for regression coverage.
- [Phase 21.1-repository-footprint-and-download-size-reduction]: Phase 21.1 completed with tracked-source cleanup, external-first APDR defaults, a supported cleanup helper, and a deterministic proof package built from a saved pre-fix snapshot plus a real post-cleanup candidate.
- [Phase 22-docker-first-policy-and-safe-degradation]: Make docker-first the standard `llm` policy now, but preserve env-first as an explicit comparison control.
- [Phase 22-docker-first-policy-and-safe-degradation]: If Docker is unavailable or unsupported, fall back to env with an explicit bypass reason instead of failing or skipping the case.
- [Phase 22-docker-first-policy-and-safe-degradation]: Apply docker-first broadly to `llm` cases except host-runtime or clearly unsuitable cases, and gate support by runtime checks rather than by platform carve-outs.
- [Phase 22-docker-first-policy-and-safe-degradation]: Each `llm` case should leave Docker-oriented debug artifacts or an explicit Docker-bypass note in its debug folder.
- [Phase 22-docker-first-policy-and-safe-degradation]: Kept docker-first versus env-first as a normalized llm_validation_policy field instead of widening validation_backend.
- [Phase 22-docker-first-policy-and-safe-degradation]: Modeled llm first-hop selection as explicit route categories so env-first control, host-runtime pre-skip, and Docker-bypass fallback stay distinct.

### Roadmap Evolution

- Phase 21.1 inserted before Phase 22: Reduce repository disk footprint and download size (URGENT)

### Pending Todos

- Next step is execute plan `22-02`
- Keep Phase 22 execution within policy, degradation, and debug-artifact scope; detailed case-row Docker build visibility remains deferred to Phase 23

### Blockers/Concerns

- Do not overstate the fixed-slice evidence as a full-corpus benchmark claim.
- Do not overstate the Phase 21.1 footprint proof as a Git history rewrite; it improves the current tree and future defaults.
- `hard-gists/1239373/snippet.py` remains an explicitly interrupted tail case in the candidate artifact and should stay visible in milestone review.

---

## Session Continuity

Last session: 2026-04-02T00:53:58.297Z
Stopped at: Completed 22-docker-first-policy-and-safe-degradation-01-PLAN.md
Resume file: None

---

*State updated after Phase 22 plan 01 completion on 2026-04-02*
