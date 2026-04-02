---
gsd_state_version: 1.0
milestone: v2.5
milestone_name: LLM End-to-End Resolver and Validation
status: Ready for execution
stopped_at: Phase 26 planned
last_updated: "2026-04-02T23:52:11Z"
last_activity: 2026-04-02
progress:
  total_phases: 5
  completed_phases: 0
  total_plans: 3
  completed_plans: 0
  percent: 0
---

# Project State: APDR

**Last Updated:** 2026-04-02
**Status:** Ready for execution
**Progress:** [░░░░░░░░░░] 0%
**Last Activity:** 2026-04-02
**Last Activity Description:** Phase 26 was researched and split into three execution waves around authored intake plans, artifact truth, and deterministic proof
**Resume File:** .planning/phases/26-llm-case-intake-and-plan-authoring/26-01-PLAN.md

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Milestone v2.5 — Phase 26 is next

---

## Current Position

Phase: Phase 26 planned
Plan: 26-01 next
Status: Ready for Phase 26 execution
Last activity: 2026-04-02 -- Phase 26 planning completed with research, validation, and 3 execution plans

---

## Performance Metrics

- Active milestone: `v2.5 LLM End-to-End Resolver and Validation`
- Planned phases: 5
- Active phase plan count: 3
- Last shipped milestone: `v2.3 Tier3 Validation Recovery and Reliability`
- Shipped scope: 5 phases, 15 plans, 30 tasks
- Fixed-slice live evidence: baseline `0/9` passes -> candidate `2/9` passes
- Fixed-slice dominant bucket deltas: `module-not-found -3`, `version-not-found -3`, `environment-build-failed -3`
- Active live baseline for the shipped evidence: `runs/20260330-020943-apdr` resumed from `runs/20260330-004502-apdr`
- Final live evidence candidate: `runs/20260401-173232-apdr` resumed from `runs/20260401-162919-apdr`
- Most recent docker-backed `llm-only` run under active investigation: `runs/20260402-184821-apdr`
- Early April 2 local evidence: the latest run has only `3` successes in its first `13` results, and at least `12` case reports already show `LLM package-resolution call returned no output`
- Known Docker regression example: case `005bbad123ef309a5bef` built successfully, then failed because `docker create` could not find the freshly built `apdr-validate:*` image tag
- Known orchestration gap: `llm-only` still too often produces empty `requirements.txt` and generic failure labels instead of a usable start-to-finish case plan
- Current phase plan count: 3
- Pending upstream verification debt from prior milestone: Phase 23 browser UAT still has 2 unresolved items, but it no longer blocks the active roadmap

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
- [Phase 22-docker-first-policy-and-safe-degradation]: Keep llm_validation_policy normalized as docker-first or env-first while validation_backend remains llm.
- [Phase 22-docker-first-policy-and-safe-degradation]: Surface the selected llm policy in preview and saved-run info fields instead of widening the backend name.
- [Phase 22-docker-first-policy-and-safe-degradation]: Rewrite Doctor copy around docker-first degradation to env validation instead of targeted Docker escalation.
- [Phase 22-docker-first-policy-and-safe-degradation]: Persisted requested llm policy, route category, and bypass details in top-level APDR outputs instead of relying on debug-folder inspection alone.
- [Phase 22-docker-first-policy-and-safe-degradation]: Used a contract-shaped Phase 22 proof slice with archetype identifiers rather than implying a live comparison harness before Phase 24.
- [Phase 22-docker-first-policy-and-safe-degradation]: Verification reopened `GDR-01`; docker-first `llm` must also degrade to env when Docker is installed but unusable, not only when the CLI is missing.
- [Phase 22]: Kept llm_validation_route stable as env-first-docker-bypass while splitting the bypass reason into exact CLI versus daemon-unavailable strings.
- [Phase 22]: Extended the proof slice to five archetypes so installed-but-unusable Docker is frozen as a first-class contract case.
- [Phase 23-policy-truth-and-failure-semantics]: Surface requested policy, route label, Docker bypass reason, bypass note, and debug artifact pointers through benchmark rows and live events instead of relying on raw metadata inspection.
- [Phase 23-policy-truth-and-failure-semantics]: Keep the UI change additive by extending expanded LLM case details with a `Validation truth` section rather than redesigning the benchmark tables.
- [Phase 23-policy-truth-and-failure-semantics]: Freeze GDR-02 with a deterministic policy-truth slice that includes Docker attempt, env-first control, Docker CLI bypass, Docker daemon bypass, host-runtime pre-skip, and framework/runtime environment-specific archetypes.
- [Phase 23-policy-truth-and-failure-semantics]: Keep requested policy and route metadata additive so validationBackend and validationPath remain the actual execution truth.
- [Phase 23-policy-truth-and-failure-semantics]: Use the same camelCase policy-truth keys for saved rows and live events to avoid another inspection-schema fork.
- [Phase 23-policy-truth-and-failure-semantics]: Derived dockerStatus from existing route, bypass, and validation path truth instead of adding a new backend taxonomy.
- [Phase 23-policy-truth-and-failure-semantics]: Keep docker-first host-runtime and Docker-unavailable markers environment-specific while true package misses remain dependency-resolution.
- [Phase 23]: Freeze the Phase 23 contract around the actual camelCase saved/live truth keys instead of inventing a proof-only schema.
- [Phase 23]: Keep the proof slice scoped to inspectability and failure-family truth, explicitly excluding the Phase 24 comparison claim.
- [Phase 24-env-first-vs-docker-first-comparison-harness]: Use a fixed matched slice and normalized artifact schema so env-first and docker-first can be compared without widening backend semantics or drifting from the existing `llm` contract.
- [Phase 24-env-first-vs-docker-first-comparison-harness]: Keep Phase 24 scoped to proving the comparison harness and delta surfaces; leave the final keep/optional/reject verdict to Phase 25.
- [Milestone start]: v2.5 replaces the conditional v2.4 closeout as the active milestone on 2026-04-02 because the user wants end-to-end LLM execution quality, not more policy-verdict work.
- [Milestone start]: v2.5 should make the LLM the primary case author for both `llm` and `llm-only`, covering module extraction, dependency planning, Docker authoring, and recovery.
- [Milestone start]: Fresh April 2 runs are the baseline for v2.5, especially the failure modes around LLM no-output and Docker build-to-run image handoff.
- [Phase 26-llm-case-intake-and-plan-authoring]: Intake is plan-first: the LLM authors a structured case plan, and APDR deterministically renders later files and artifacts from it.
- [Phase 26-llm-case-intake-and-plan-authoring]: The authored case plan must include extracted modules or imports, package mappings, unresolved imports, system-dependency hints, runtime assumptions, section-level confidence, and an authored smoke strategy.
- [Phase 26-llm-case-intake-and-plan-authoring]: `llm-only` shares the authored-plan pipeline with `llm`, but it fails truthfully when no usable intake plan exists instead of silently dropping into heuristic reconstruction.
- [Phase 26-llm-case-intake-and-plan-authoring]: No-output paths must persist a structured intake-failure record that distinguishes empty output, invalid JSON, schema failure, timeout or transport failure, and provider or tooling incompatibility.

### Roadmap Evolution

- Phase 21.1 inserted before Phase 22: Reduce repository disk footprint and download size (URGENT)

### Pending Todos

- None captured yet for v2.5

### Blockers/Concerns

- Do not overstate the fixed-slice evidence as a full-corpus benchmark claim.
- Do not overstate the Phase 21.1 footprint proof as a Git history rewrite; it improves the current tree and future defaults.
- `hard-gists/1239373/snippet.py` remains an explicitly interrupted tail case in the candidate artifact and should stay visible in milestone review.
- Phase 23 browser UAT debt from v2.4 is still open and should stay visible as historical context even though it no longer blocks active execution.
- v2.5 must separate LLM no-output and Docker infrastructure failures from genuine dependency misses before claiming performance gains.
- v2.5 should improve pass rate without hiding runtime cost explosions or provider instability.

---

## Session Continuity

Last session: 2026-04-02T23:28:45.067Z
Stopped at: Phase 26 planned
Resume file: .planning/phases/26-llm-case-intake-and-plan-authoring/26-01-PLAN.md

---

*State updated after planning Phase 26 on 2026-04-02*
