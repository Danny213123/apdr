# Roadmap: APDR

**Project:** APDR v2.5 - LLM End-to-End Resolver and Validation
**Created:** 2026-04-02
**Granularity:** Standard (5 phases)

## Milestones

- [ ] **v2.5 LLM End-to-End Resolver and Validation** - active milestone focused on making `llm` and `llm-only` own case planning, Docker authoring, recovery, and benchmark-proofed reliability
- [ ] **v2.4 Docker-First LLM Validation Decision and Proof** - superseded unfinished on 2026-04-02 after Phases 21.1, 22, 24, and 25 completed; Phase 23 human-verification debt remained open and the optional-verdict posture was overtaken by the new end-to-end LLM scope
- [x] **v2.3 Tier3 Validation Recovery and Reliability** - shipped 2026-04-01, archived in `.planning/milestones/v2.3-ROADMAP.md`
- [ ] **v2.2 Improve LLM Performance and Benchmark Performance on macOS** - Phases 13-16 completed, but the milestone was superseded unfinished on 2026-03-30 after Phase 16 sample-contract closeout; live proof and milestone signoff remained open
- [ ] **v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy** - superseded unfinished on 2026-03-28 after Phase 11 completion; Phase 12 remained open and is now historical debt rather than active milestone scope
- [x] `v2.0` Rust Codebase Modernization - shipped 2026-03-28, archived in `.planning/milestones/v2.0-ROADMAP.md`
- [x] `v1.0` Accuracy & Performance - shipped 2026-03-27, archived in `.planning/milestones/v1.0-ROADMAP.md`

## Roadmap v2.5: LLM End-to-End Resolver and Validation

The docker-first policy question is no longer the main problem. Fresh April 2 benchmark runs show that `llm` and `llm-only` still fail too often because the LLM produces no usable dependency plan, the Docker path can lose the built image before runtime, and failure reporting does not clearly separate model failure from infrastructure failure. This milestone makes the LLM the primary case author from snippet intake through Docker validation and recovery, then proves whether that stronger end-to-end path actually improves benchmark outcomes.

## Phases

- [x] **Phase 26: LLM Case Intake and Plan Authoring** - Turn snippet analysis into an explicit LLM-authored case plan with dependency, system-dependency, and runtime intent artifacts
- [x] **Phase 27: LLM-Authored Docker Validation and Artifact Truth** - Let the LLM author Docker-oriented validation inputs, fix build-to-run handoff reliability, and preserve executed Docker artifacts per case
- [x] **Phase 28: LLM Recovery Loop and Failure Semantics** - Feed install, build, and runtime logs back into the LLM for bounded recovery while making non-pass failure truth explicit
- [x] **Phase 29: LLM Benchmark Gains and Regression Harness** - Compare current versus candidate `llm` and `llm-only` behavior on a fixed slice with pass, timing, and failure-rate deltas
- [ ] **Phase 30: Live Evidence and Closeout for LLM-Led Validation** - Publish reviewer-readable before/after evidence and a final recommendation on shipping the new end-to-end LLM path

## Phase Details

### Phase 26: LLM Case Intake and Plan Authoring
**Goal**: APDR writes an explicit LLM-authored case plan before validation so `llm` and `llm-only` start from structured module, dependency, system-dependency, and runtime intent instead of opaque prompt output
**Depends on**: v2.4 superseded unfinished
**Requirements**: LLM-01, TRU-02
**Success Criteria** (what must be TRUE):
  1. APDR preserves a structured per-case plan showing what modules/imports the LLM found, which packages it mapped, which items remain unresolved, and what runtime assumptions it made.
  2. `llm` and `llm-only` both use that authored plan as their first-class input rather than collapsing directly to empty `requirements.txt` on no-output paths.
  3. Saved artifacts and debug folders make it clear which plan elements came from the LLM versus deterministic fallbacks.
**Plans**: 3 completed

### Phase 27: LLM-Authored Docker Validation and Artifact Truth
**Goal**: The LLM can author Docker-oriented validation inputs that APDR can actually execute, while the Docker path itself becomes reliable enough to stop losing freshly built images before runtime
**Depends on**: Phase 26
**Requirements**: LLM-02, DKR-01, DKR-02
**Success Criteria** (what must be TRUE):
  1. Each LLM-driven case can preserve the authored Dockerfile or equivalent build/runtime plan that APDR actually executed.
  2. Docker validation no longer fails because `docker create` cannot see the just-built image in supported environments.
  3. Case debug folders preserve both authored Docker inputs and executed Docker artifacts so failures are reproducible and inspectable.
**Plans**: 3 completed

### Phase 28: LLM Recovery Loop and Failure Semantics
**Goal**: Non-pass `llm` and `llm-only` cases get bounded, log-aware LLM recovery attempts and truthful final failure labeling instead of generic `Unknown` or misleading infrastructure hints
**Depends on**: Phase 27
**Requirements**: LLM-03, TRU-01
**Success Criteria** (what must be TRUE):
  1. Recovery prompts can consume prior build, install, and runtime logs together with the authored case plan.
  2. APDR distinguishes LLM no-output, provider/tooling failure, Docker infrastructure failure, and genuine dependency/runtime failure in final case artifacts.
  3. A failed case explains whether the LLM abstained, timed out, produced invalid output, or exhausted bounded recovery attempts.
**Plans**: 3 completed

### Phase 29: LLM Benchmark Gains and Regression Harness
**Goal**: The repo can compare baseline and candidate `llm` and `llm-only` behavior on the same slice and report whether the stronger LLM-led path helps or hurts correctness and cost
**Depends on**: Phase 28
**Requirements**: BEN-01, BEN-02
**Success Criteria** (what must be TRUE):
  1. APDR can generate paired baseline-versus-candidate artifacts for both `llm` and `llm-only` on a locked slice.
  2. Comparison outputs report pass delta, LLM no-output delta, Docker handoff failure delta, and solve/validate timing deltas.
  3. A deterministic checker fails if the comparison drifts from the locked slice or omits required regression signals.
**Plans**: 3 completed

### Phase 30: Live Evidence and Closeout for LLM-Led Validation
**Goal**: v2.5 closes with a reviewer-readable answer on whether the new end-to-end LLM path is ready to ship for `llm` and `llm-only`
**Depends on**: Phase 29
**Requirements**: EVD-11
**Success Criteria** (what must be TRUE):
  1. Closeout artifacts show before or after evidence for both `llm` and `llm-only`, using representative cases that surface both wins and honest failures.
  2. The recommendation cites correctness, reliability, and runtime tradeoffs of the end-to-end LLM path.
  3. The final verdict updates requirements, roadmap, and state truth without overstating fixed-slice evidence as a full-corpus claim.
**Plans**: Not started

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 26. LLM Case Intake and Plan Authoring | 3/3 | Complete | 2026-04-02 |
| 27. LLM-Authored Docker Validation and Artifact Truth | 3/3 | Complete | 2026-04-03 |
| 28. LLM Recovery Loop and Failure Semantics | 3/3 | Complete | 2026-04-03 |
| 29. LLM Benchmark Gains and Regression Harness | 3/3 | Complete | 2026-04-03 |
| 30. Live Evidence and Closeout for LLM-Led Validation | 0/0 | Pending | — |

## Dependencies

`Phase 26 -> Phase 27 -> Phase 28 -> Phase 29 -> Phase 30`

*Roadmap created: 2026-04-02*
*Last updated: 2026-04-03 (Phase 29 complete)*
