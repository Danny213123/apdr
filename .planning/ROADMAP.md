# Roadmap: APDR

**Project:** APDR v2.3 - Tier3 Validation Recovery and Reliability
**Created:** 2026-03-30
**Granularity:** Standard (5 phases)

## Milestones

- [ ] **v2.3 Tier3 Validation Recovery and Reliability** - Phases 17-21
- [ ] **v2.2 Improve LLM Performance and Benchmark Performance on macOS** - Phases 13-16 completed, but the milestone was superseded unfinished on 2026-03-30 after Phase 16 sample-contract closeout; live proof and milestone signoff remained open
- [ ] **v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy** - superseded unfinished on 2026-03-28 after Phase 11 completion; Phase 12 remained open and is now historical debt rather than active milestone scope
- [x] `v2.0` Rust Codebase Modernization - shipped 2026-03-28, archived in `.planning/milestones/v2.0-ROADMAP.md`
- [x] `v1.0` Accuracy & Performance - shipped 2026-03-27, archived in `.planning/milestones/v1.0-ROADMAP.md`

## Roadmap v2.3: Tier3 Validation Recovery and Reliability

This milestone starts from the March 30 2026 live tier3 baseline rather than the v2.2 sample-proof package. It restores a working `llm` fallback path first, makes Docker escalation and backend accounting truthful, repairs failure classification and resumed-run reporting so comparisons can be trusted, then proves real recovery gains on the dominant tier3 buckets with live artifacts.

## Phases

- [x] **Phase 17: LLM Fallback Stability and Outcome Tracing** - Make `llm` validation mode survive post-env failures and expose inspectable agent outcomes per case (completed 2026-03-31)
- [ ] **Phase 18: Backend Escalation and Path Truth** - Route eligible `llm`-mode failures through Docker and record the actual validation backend path
- [ ] **Phase 19: Failure Classification and Run-Accounting Integrity** - Separate host or framework failures from dependency misses and make resumed-run reporting trustworthy
- [ ] **Phase 20: Dominant Bucket Recovery Gains** - Turn the repaired fallback and backend path into measurable tier3 improvements on the live baseline buckets
- [ ] **Phase 21: Live Evidence and Closeout Pack** - Publish before-and-after recovery deltas and representative case artifacts for milestone review

## Phase Details

### Phase 17: LLM Fallback Stability and Outcome Tracing
**Goal**: Benchmark operators can run `--validation-backend llm` on tier3 cases without the fallback crashing after env validation fails, and can inspect how the agent path ended for each case
**Depends on**: Nothing (first phase of v2.3)
**Requirements**: AGT-07, AGT-08
**Success Criteria** (what must be TRUE):
  1. On the selected v2.3 tier3 slice, cases that fail env validation can continue into LLM fallback without the LangGraph `confidence` state-key crash or an equivalent post-env fallback crash terminating the run.
  2. Saved per-case artifacts show whether LLM fallback was invoked and whether it passed, abstained, or failed for each attempted case.
  3. When fallback does not solve a case, the saved artifact still exposes the agent outcome instead of collapsing back into an unlabeled env-only failure.
**Plans**: `17-01`, `17-02`, and `17-03` complete

### Phase 18: Backend Escalation and Path Truth
**Goal**: Eligible tier3 failures in `llm` mode can escalate through Docker, and every validation attempt records the actual backend route without regressing Windows or Docker correctness
**Depends on**: Phase 17
**Requirements**: VAL-01, VAL-02, WIN-02
**Success Criteria** (what must be TRUE):
  1. Eligible `environment-build-failed` and `version-not-found` tier3 cases running in `--validation-backend llm` attempt Docker-backed validation before final failure.
  2. Validation attempt artifacts identify the actual backend path taken for each attempt, including `env`, `docker`, and `llm-agent`, instead of only echoing the configured run mode.
  3. The routing changes preserve Windows and Docker correctness paths rather than silently degrading supported platforms back to env-only validation.
**Plans**: TBD

### Phase 19: Failure Classification and Run-Accounting Integrity
**Goal**: Operators can trust tier3 failure categories and resumed-run summaries to separate environment-specific issues from real dependency-resolution misses
**Depends on**: Phase 17, Phase 18
**Requirements**: VAL-04, EVD-07, EVD-09
**Success Criteria** (what must be TRUE):
  1. Per-case validation results distinguish framework or host-runtime failures from dependency-resolution failures so environment-specific cases are not counted as generic mapping misses.
  2. Resumed-run summaries do not mark skipped host-runtime cases as successes.
  3. Baseline-versus-candidate comparisons for v2.3 can be produced without mixing stale historical case metadata into current-run conclusions.
**Plans**: TBD

### Phase 20: Dominant Bucket Recovery Gains
**Goal**: The selected v2.3 tier3 benchmark slice shows real recovery improvements on the dominant live failure buckets after the fallback, routing, and accounting fixes land
**Depends on**: Phase 17, Phase 18, Phase 19
**Requirements**: AGT-09, VAL-03
**Success Criteria** (what must be TRUE):
  1. On the selected v2.3 tier3 slice with the same run mode and model, APDR resolves more cases successfully than the March 30 2026 baseline.
  2. Compared with the March 30 2026 baseline, failures in `module-not-found`, `environment-build-failed`, and `version-not-found` are reduced on that same slice.
  3. The improvement is measured on like-for-like baseline and candidate runs so the recovery delta is attributable to v2.3 changes rather than configuration drift.
**Plans**: TBD

### Phase 21: Live Evidence and Closeout Pack
**Goal**: v2.3 closes with reviewer-readable live evidence that shows the shipped tier3 recovery changes and their benchmark effect
**Depends on**: Phase 19, Phase 20
**Requirements**: EVD-08
**Success Criteria** (what must be TRUE):
  1. Milestone evidence includes before-and-after tier3 bucket counts for the selected v2.3 slice and clearly labels the March 30 2026 baseline versus the v2.3 candidate run.
  2. Milestone evidence includes representative case-level artifacts that show the shipped recovery-path behavior on real tier3 cases.
**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 17. LLM Fallback Stability and Outcome Tracing | 3/3 | Complete    | 2026-03-31 |
| 18. Backend Escalation and Path Truth | 0/TBD | Not started | - |
| 19. Failure Classification and Run-Accounting Integrity | 0/TBD | Not started | - |
| 20. Dominant Bucket Recovery Gains | 0/TBD | Not started | - |
| 21. Live Evidence and Closeout Pack | 0/TBD | Not started | - |

## Dependencies

`Phase 17 -> Phase 18 -> Phase 19 -> Phase 20 -> Phase 21`

*Roadmap created: 2026-03-30*
*Last updated: 2026-03-31 (Phase 17 complete; roadmap progress updated)*
