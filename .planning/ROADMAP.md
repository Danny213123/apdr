# Roadmap: APDR

**Project:** APDR v2.4 - Docker-First LLM Validation Decision and Proof
**Created:** 2026-04-01
**Granularity:** Standard (5 phases)

## Milestones

- [ ] **v2.4 Docker-First LLM Validation Decision and Proof** - Phases 21.1 and 22 are completed, Phase 23 is awaiting human verification after automated execution, and Phases 24-25 remain upcoming
- [x] **v2.3 Tier3 Validation Recovery and Reliability** - shipped 2026-04-01, archived in `.planning/milestones/v2.3-ROADMAP.md`
- [ ] **v2.2 Improve LLM Performance and Benchmark Performance on macOS** - Phases 13-16 completed, but the milestone was superseded unfinished on 2026-03-30 after Phase 16 sample-contract closeout; live proof and milestone signoff remained open
- [ ] **v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy** - superseded unfinished on 2026-03-28 after Phase 11 completion; Phase 12 remained open and is now historical debt rather than active milestone scope
- [x] `v2.0` Rust Codebase Modernization - shipped 2026-03-28, archived in `.planning/milestones/v2.0-ROADMAP.md`
- [x] `v1.0` Accuracy & Performance - shipped 2026-03-27, archived in `.planning/milestones/v1.0-ROADMAP.md`

## Roadmap v2.4: Docker-First LLM Validation Decision and Proof

This milestone now begins with an urgent repository-footprint reduction phase because the current checkout and GitHub download size are inflated by heavyweight tool artifacts under `tools/`, especially `tools/apdr`. After that foundation work, the milestone returns to its original policy question: should supported `llm` validation go straight to Docker instead of trying env validation first? The remaining phases introduce a docker-first control path, keep safe degradation and truthful metadata, compare docker-first against env-first on a like-for-like slice, and close with a reviewer-readable recommendation rather than an assumption.

## Phases

- [x] **Phase 21.1 (INSERTED): Repository Footprint and Download Size Reduction** - Completed 2026-04-01 with tracked-source cleanup, safer APDR defaults, cleanup tooling, and a deterministic footprint proof
- [x] **Phase 22: Docker-First Policy and Safe Degradation** - Completed 2026-04-02 with truthful installed-but-unusable Docker degradation, exact bypass reasons, and a five-case proof contract
- [ ] **Phase 23: Policy Truth and Failure Semantics** - Automated execution is complete and human verification is pending for the browser-visible `Validation truth` surfaces
- [ ] **Phase 24: Env-First vs Docker-First Comparison Harness** - Compare the two first-hop policies on a fixed slice with matched model, backend, bucket, and timing contracts
- [ ] **Phase 25: Docker-First Decision Closeout** - Publish the final evidence-backed recommendation on whether docker-first should replace env-first, remain optional, or be rejected

## Phase Details

### Phase 21.1: Repository Footprint and Download Size Reduction
**Goal**: The repo becomes materially lighter to download and keep locally by removing or relocating heavyweight tool artifacts, especially under `tools/apdr`, without breaking normal APDR development flows
**Depends on**: Phase 21 complete (v2.3 archived)
**Requirements**: DSK-01, DSK-02
**Success Criteria** (what must be TRUE):
  1. Fresh source checkouts and GitHub downloads no longer include avoidable heavyweight tool build outputs that are currently inflating the repo footprint.
  2. Large local generated directories under `tools/`, especially APDR build and cache outputs, are either kept out of the repo tree by default or have a supported reclaim path.
  3. The remaining v2.4 docker-first work can proceed without depending on checked-in build artifacts or opaque gigabyte-scale local tool directories.
**Plans**: `21.1-01`, `21.1-02`, and `21.1-03` completed

### Phase 22: Docker-First Policy and Safe Degradation
**Goal**: Benchmark operators can explicitly run docker-first `llm` validation on supported hosts without losing the existing env-first control path or breaking unsupported environments
**Depends on**: Phase 21.1
**Requirements**: DFV-01, DFV-03, GDR-01
**Success Criteria** (what must be TRUE):
  1. Operators can request a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts.
  2. Operators can still run the existing env-first `llm` policy as a control path for comparison.
  3. When Docker is unavailable, unsupported, or bypassed for a case, APDR degrades clearly instead of silently breaking `llm` validation.
**Plans**: `22-01`, `22-02`, `22-03`, and gap closure `22-04`

### Phase 23: Policy Truth and Failure Semantics
**Goal**: Operators and reviewers can see which first-hop policy was requested, what path actually ran, why docker-first was bypassed when it was, and whether non-pass cases remain classified truthfully
**Depends on**: Phase 22
**Requirements**: DFV-02, GDR-02
**Success Criteria** (what must be TRUE):
  1. Saved artifacts and benchmark readers expose requested policy, actual validation path, and bypass or fallback reason per case.
  2. Docker-first evaluation preserves host-runtime and framework-failure truth instead of flattening those cases into generic dependency-resolution failures.
  3. Reviewers can distinguish docker-first policy behavior from env-first control behavior without scraping raw logs.
**Plans**: `23-01`, `23-02`, and `23-03`

### Phase 24: Env-First vs Docker-First Comparison Harness
**Goal**: The repo can compare docker-first and env-first `llm` behavior on the same slice and report whether the first-hop change helps or hurts correctness and cost
**Depends on**: Phase 22, Phase 23
**Requirements**: CMP-01, CMP-02
**Success Criteria** (what must be TRUE):
  1. APDR can generate or extract env-first and docker-first artifacts for the same fixed slice with matching model and backend contracts.
  2. Comparison outputs report pass delta, dominant-bucket delta, and timing delta between the two policies.
  3. A deterministic checker fails if the comparison drifts from the locked slice or omits required metrics.

### Phase 25: Docker-First Decision Closeout
**Goal**: v2.4 closes with a reviewer-readable answer to the docker-first policy question, backed by the actual comparison evidence
**Depends on**: Phase 24
**Requirements**: EVD-10
**Success Criteria** (what must be TRUE):
  1. Closeout artifacts state whether docker-first should replace env-first, remain optional, or be rejected for `llm` mode.
  2. The recommendation cites the comparison evidence and calls out the main correctness, compatibility, and runtime tradeoffs.
  3. The final verdict updates requirements, roadmap, and state truth without overstating fixed-slice evidence as a full-corpus result.

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 21.1. Repository Footprint and Download Size Reduction | 3/3 | Completed | 2026-04-01 |
| 22. Docker-First Policy and Safe Degradation | 4/4 | Complete   | 2026-04-02 |
| 23. Policy Truth and Failure Semantics | 3/3 | Human Verify | — |
| 24. Env-First vs Docker-First Comparison Harness | 0/0 | Not Started | — |
| 25. Docker-First Decision Closeout | 0/0 | Not Started | — |

## Dependencies

`Phase 21.1 -> Phase 22 -> Phase 23 -> Phase 24 -> Phase 25`

*Roadmap created: 2026-04-01*
*Last updated: 2026-04-02 (Phase 23 awaiting human verification)*
