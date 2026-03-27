# Roadmap: APDR

**Project:** APDR v2.0 - Rust Codebase Modernization
**Created:** 2026-03-26
**Granularity:** Standard (6 phases)

## Roadmap v2.0: Rust Codebase Modernization

## Phases

- [x] **Phase 1: Baseline & Guardrails** - Establish measurement, profiling, and regression gates before refactoring
- [x] **Phase 2: Resolver Memory & Algorithm Efficiency** - Reduce hot-path allocation, clone churn, and avoidable work in core solve flows
- [x] **Phase 3: Validation Pipeline Throughput** - Improve env and Docker validation efficiency, cache reuse, and retry cost
- [ ] **Phase 4: Module Layout & Boundary Cleanup** - Split oversized Rust modules and make responsibilities easier to review
- [ ] **Phase 5: Documentation, Error Handling & Review Readiness** - Raise clarity, docs, and standards compliance in touched Rust code
- [ ] **Phase 6: Benchmark Verification & v2 Closeout** - Prove the modernization work with before/after benchmarks and review gates

## Phase Details

### Phase 1: Baseline & Guardrails
**Goal**: Build a measured starting point so later optimization work is evidence-driven and regression-safe

**Depends on**: Nothing (foundation phase)

**Requirements**: BASE-01, BASE-02, BASE-03, BASE-04, BASE-05

**Success Criteria** (what must be TRUE):
1. A repeatable before-state benchmark exists for end-to-end runtime, validation-heavy runtime, and pass rate
2. Memory-sensitive indicators are captured for the Rust areas targeted by this milestone
3. The team has a standard command set for fmt, clippy, targeted tests, and benchmark comparison
4. Hotspots are ranked from measured evidence, not just code inspection
5. Every later optimization phase has an explicit regression check to protect correctness

**Plans**:
- [x] `01-01` - Baseline Harness & Memory Capture
- [x] `01-02` - Regression Gate, Hotspot Audit & Guardrails

---

### Phase 2: Resolver Memory & Algorithm Efficiency
**Goal**: Make the core Rust solve path cheaper by reducing unnecessary ownership churn and avoidable work

**Depends on**: Phase 1 (baseline and regression gates)

**Requirements**: EFF-01, EFF-02, EFF-03, EFF-04, EFF-05

**Success Criteria** (what must be TRUE):
1. Targeted resolver hot paths perform less cloning, allocation, or recomputation than the baseline
2. Shared-state contention is reduced in the hottest benchmark-critical Rust flows
3. Candidate selection or retry logic is simpler and measurably cheaper in the chosen targets
4. Deterministic behavior and correctness remain intact after optimization
5. The changed code is easier to reason about than the original hotspot implementation

**Plans**:
- [x] `02-01` - Pre-solve Ownership & Metadata Prefetch Cleanup
- [x] `02-02` - Resolver Retry Loop & Dependency Mutation Cleanup
- [x] `02-03` - Resolver Candidate Benchmark & Delta Report

---

### Phase 3: Validation Pipeline Throughput
**Goal**: Reduce the cost of validation-heavy benchmark cases without weakening env or Docker correctness

**Depends on**: Phase 1 (baseline), Phase 2 (core solve hot-path cleanup)

**Requirements**: VAL-01, VAL-02, VAL-03, VAL-04, VAL-05

**Success Criteria** (what must be TRUE):
1. Validation paths reuse caches, layers, or artifacts more effectively than the baseline
2. Fallback and retry flows avoid obvious duplicate environment creation or repeated work
3. Solve, env create, install, and smoke costs are clearly measurable and easier to compare
4. Windows and Docker validation remain supported after throughput changes
5. Validation-heavy benchmark cases complete faster than they did at milestone start

**Plans**:
- [x] `03-01` - Env Attempt Staging & Validated-Env Reuse Cleanup
- [x] `03-02` - Backend Attempt Telemetry & Validation Benchmark Reporting
- [x] `03-03` - Validation Candidate Benchmark & Delta Report

---

### Phase 4: Module Layout & Boundary Cleanup
**Goal**: Make the Rust codebase easier to navigate by decomposing oversized files and clarifying ownership boundaries

**Depends on**: Phase 2 (resolver cleanup), Phase 3 (validation cleanup)

**Requirements**: ARCH-01, ARCH-02, ARCH-03, ARCH-04, ARCH-05

**Success Criteria** (what must be TRUE):
1. Oversized Rust modules are split into smaller files with coherent responsibilities
2. Recovery, validation, and supporting helpers are extracted behind clear module boundaries
3. Naming better matches responsibility and ownership of the code
4. Reviewers can follow the main control flow without tracing giant monolithic files
5. Targeted tests still pass after the structural refactor

**Plans**:
- [ ] `04-01` - Resolver Orchestrator Module Split
- [ ] `04-02` - Validation Builder Module Split
- [ ] `04-03` - Support Module Boundary Cleanup

---

### Phase 5: Documentation, Error Handling & Review Readiness
**Goal**: Bring the touched Rust code up to a higher review standard for docs, panic safety, and style consistency

**Depends on**: Phase 4 (stable module boundaries)

**Requirements**: QUAL-01, QUAL-02, QUAL-03, QUAL-04, QUAL-05

**Success Criteria** (what must be TRUE):
1. Non-obvious fallbacks, invariants, and recovery behavior are documented where reviewers need context
2. Avoidable `unwrap()` and `expect()` panic paths are removed or explicitly justified in touched production code
3. Touched Rust modules pass formatting, linting, and targeted tests
4. A reviewer-facing guide exists for benchmark-critical modules and responsibilities
5. Naming and error-handling patterns look consistent across the modernized Rust areas

**Plans**: TBD

---

### Phase 6: Benchmark Verification & v2 Closeout
**Goal**: Validate that the modernization work delivered measurable benchmark and review-quality improvements

**Depends on**: Phase 2, Phase 3, Phase 4, Phase 5

**Requirements**: BENCH-01, BENCH-02, BENCH-03, BENCH-04, BENCH-05

**Success Criteria** (what must be TRUE):
1. End-to-end benchmark runtime improves versus the milestone baseline
2. Validation-heavy cases are measurably faster than the baseline
3. Memory churn or peak memory indicators improve on the targeted Rust workflows
4. Benchmark pass rate is maintained or improved after the refactor work
5. The milestone can pass a codebase review focused on performance, layout, documentation, and standards

**Plans**: TBD

---

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Baseline & Guardrails | 2/2 | Complete | 2026-03-27 |
| 2. Resolver Memory & Algorithm Efficiency | 3/3 | Complete | 2026-03-27 |
| 3. Validation Pipeline Throughput | 3/3 | Complete | 2026-03-27 |
| 4. Module Layout & Boundary Cleanup | 2/3 | In Progress|  |
| 5. Documentation, Error Handling & Review Readiness | 0/TBD | Not started | - |
| 6. Benchmark Verification & v2 Closeout | 0/TBD | Not started | - |

---

## Dependencies

```
Phase 1: Baseline & Guardrails (FOUNDATION)
   └─> Phase 2: Resolver Memory & Algorithm Efficiency
          └─> Phase 3: Validation Pipeline Throughput
                 ├─> Phase 4: Module Layout & Boundary Cleanup
                 │      └─> Phase 5: Documentation, Error Handling & Review Readiness
                 └─> Phase 6: Benchmark Verification & v2 Closeout
```

**Critical Path**: Phase 1 -> Phase 2 -> Phase 3 -> Phase 4 -> Phase 5 -> Phase 6

**Parallel Opportunities**:
- Phase 4 structural decomposition can begin in limited areas once Phase 2 hotspot refactors stabilize
- Phase 5 documentation and review hardening can start on completed Phase 4 modules before the entire milestone is finished

---

*Roadmap created: 2026-03-26*
*Last updated: 2026-03-27 after Phase 4 planning*
