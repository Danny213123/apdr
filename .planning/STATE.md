---
gsd_state_version: 1.0
milestone: v2.4
milestone_name: Docker-First LLM Validation Decision and Proof
status: phase_context_gathered
stopped_at: Phase 22 context gathered
last_updated: "2026-04-01T23:20:00Z"
last_activity: 2026-04-01
progress:
  total_phases: 4
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State: APDR

**Last Updated:** 2026-04-01
**Status:** Phase 22 context gathered
**Progress:** [░░░░░░░░░░] 0%
**Last Activity:** 2026-04-01
**Last Activity Description:** Phase 22 context captured for docker-first `llm` policy and safe degradation
**Resume File:** .planning/phases/22-docker-first-policy-and-safe-degradation/22-CONTEXT.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Phase 22 planning

---

## Current Position

Phase: 22
Plan: —
Status: Ready to plan Phase 22
Last activity: 2026-04-01 -- Phase 22 context gathered for docker-first routing policy

---

## Performance Metrics

- Active milestone: `v2.4 Docker-First LLM Validation Decision and Proof`
- Planned phases: 4
- Last shipped milestone: `v2.3 Tier3 Validation Recovery and Reliability`
- Shipped scope: 5 phases, 15 plans, 30 tasks
- Fixed-slice live evidence: baseline `0/9` passes -> candidate `2/9` passes
- Fixed-slice dominant bucket deltas: `module-not-found -3`, `version-not-found -3`, `environment-build-failed -3`
- Active live baseline for the shipped evidence: `runs/20260330-020943-apdr` resumed from `runs/20260330-004502-apdr`
- Final live evidence candidate: `runs/20260401-173232-apdr` resumed from `runs/20260401-162919-apdr`

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
- [Milestone structure]: v2.4 phase order is docker-first policy -> policy truth -> env-first-vs-docker-first comparison -> final decision closeout.
- [Phase 22-docker-first-policy-and-safe-degradation]: Make docker-first the standard `llm` policy now, but preserve env-first as an explicit comparison control.
- [Phase 22-docker-first-policy-and-safe-degradation]: If Docker is unavailable or unsupported, fall back to env with an explicit bypass reason instead of failing or skipping the case.
- [Phase 22-docker-first-policy-and-safe-degradation]: Apply docker-first broadly to `llm` cases except host-runtime or clearly unsuitable cases, and gate support by runtime checks rather than by platform carve-outs.
- [Phase 22-docker-first-policy-and-safe-degradation]: Each `llm` case should leave Docker-oriented debug artifacts or an explicit Docker-bypass note in its debug folder.

### Pending Todos

- Plan Phase 22 with `$gsd-plan-phase 22`

### Blockers/Concerns

- Do not overstate the fixed-slice evidence as a full-corpus benchmark claim.
- `hard-gists/1239373/snippet.py` remains an explicitly interrupted tail case in the candidate artifact and should stay visible in milestone review.

---

## Session Continuity

Last session: 2026-04-01T23:20:00Z
Stopped at: Phase 22 context gathered
Resume file: .planning/phases/22-docker-first-policy-and-safe-degradation/22-CONTEXT.md

---

*State updated after Phase 22 context capture on 2026-04-01*
