# Requirements: APDR v2.0 Rust Codebase Modernization

**Defined:** 2026-03-26
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2 modernization milestone. Each maps to exactly one roadmap phase.

### Baseline & Guardrails

- [x] **BASE-01**: Benchmark baseline captures end-to-end runtime, validation runtime, and pass rate before optimization work begins
- [x] **BASE-02**: Benchmark baseline captures memory-sensitive indicators for key Rust workflows
- [x] **BASE-03**: The repo has a repeatable command set for fmt, clippy, targeted tests, and benchmark comparison
- [x] **BASE-04**: High-risk performance hotspots are ranked from measured evidence, not only code inspection
- [x] **BASE-05**: Each optimization phase defines a regression check before refactoring begins

### Memory & Algorithm Efficiency

- [x] **EFF-01**: Hot-path Rust code reduces unnecessary cloning and allocation in resolver and cache flows
- [x] **EFF-02**: Shared-state contention in benchmark-critical paths is reduced with a better ownership or aggregation strategy
- [x] **EFF-03**: Repeated metadata lookups or recomputation in solve and validate paths are reduced or eliminated
- [x] **EFF-04**: Candidate-selection and retry logic use clearer, cheaper algorithms in the hottest Rust paths
- [x] **EFF-05**: Performance-oriented refactors preserve deterministic behavior and benchmark correctness

### Validation Throughput

- [x] **VAL-01**: Validation reuses caches, layers, or artifacts more effectively to reduce repeated build work
- [x] **VAL-02**: Validation fallback and retry paths avoid unnecessary duplicate environment creation
- [x] **VAL-03**: Python-version or backend attempts use a more efficient execution strategy than the current bottlenecks
- [x] **VAL-04**: Validation telemetry clearly separates solve time, env create time, install time, and smoke or runtime cost
- [x] **VAL-05**: Validation changes preserve Windows and Docker compatibility

### Codebase Layout

- [x] **ARCH-01**: Oversized Rust modules are split into smaller files with coherent responsibilities
- [x] **ARCH-02**: Public and internal APIs between Rust modules are easier to follow and less entangled
- [x] **ARCH-03**: Complex recovery and validation logic is extracted behind named helpers or submodules instead of giant functions
- [x] **ARCH-04**: File and module naming better reflects responsibility and ownership boundaries
- [x] **ARCH-05**: Refactors reduce cognitive load for code review on the most active Rust areas

### Documentation & Review Quality

- [x] **QUAL-01**: Non-obvious Rust behavior, invariants, and fallbacks are documented where reviewers need context
- [x] **QUAL-02**: Touched production Rust code removes avoidable `unwrap()` or `expect()` panic paths or documents why they are safe
- [x] **QUAL-03**: Touched Rust modules pass formatting, linting, and targeted tests without style regressions
- [x] **QUAL-04**: The codebase has a clear reviewer-facing guide to benchmark-critical modules and their responsibilities
- [x] **QUAL-05**: Code changes align with consistent error-handling and naming conventions across Rust modules

### Benchmark Outcomes

- [x] **BENCH-01**: End-to-end benchmark runtime improves measurably versus the v2 baseline
- [x] **BENCH-02**: Validation-heavy cases complete faster than the v2 baseline
- [ ] **BENCH-03**: Memory churn or peak memory indicators improve on the targeted Rust workflows
- [x] **BENCH-04**: Benchmark pass rate is maintained or improved after modernization work
- [ ] **BENCH-05**: The final milestone package can survive a codebase review focused on performance, layout, docs, and standards

## v2 Requirements

Deferred to a later milestone after the core modernization work lands.

### Future Modernization

- **FUT-01**: Move legacy family-knowledge bundles into data-driven configuration files
- **FUT-02**: Introduce async I/O for network and subprocess-heavy paths
- **FUT-03**: Replace ad-hoc telemetry with structured tracing across the Rust core
- **FUT-04**: Add continuous performance benchmarking in CI

## Out of Scope

| Feature | Reason |
|---------|--------|
| New UI features or UX redesign | Not part of Rust modernization |
| New LLM provider integrations | Existing provider path is sufficient for this milestone |
| Benchmark dataset expansion | Would muddy before/after comparisons |
| Full rewrite of Python helpers | Only touch cross-language boundaries when required by Rust work |
| Removing legacy compatibility families wholesale | Modernization must preserve supported scenarios unless explicitly deprecated |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| BASE-01 | Phase 1 | Complete |
| BASE-02 | Phase 1 | Complete |
| BASE-03 | Phase 1 | Complete |
| BASE-04 | Phase 1 | Complete |
| BASE-05 | Phase 1 | Complete |
| EFF-01 | Phase 2 | Complete |
| EFF-02 | Phase 2 | Complete |
| EFF-03 | Phase 2 | Complete |
| EFF-04 | Phase 2 | Complete |
| EFF-05 | Phase 2 | Complete |
| VAL-01 | Phase 3 | Complete |
| VAL-02 | Phase 3 | Complete |
| VAL-03 | Phase 3 | Complete |
| VAL-04 | Phase 3 | Complete |
| VAL-05 | Phase 3 | Complete |
| ARCH-01 | Phase 4 | Complete |
| ARCH-02 | Phase 4 | Complete |
| ARCH-03 | Phase 4 | Complete |
| ARCH-04 | Phase 4 | Complete |
| ARCH-05 | Phase 4 | Complete |
| QUAL-01 | Phase 5 | Complete |
| QUAL-02 | Phase 5 | Complete |
| QUAL-03 | Phase 5 | Complete |
| QUAL-04 | Phase 5 | Complete |
| QUAL-05 | Phase 5 | Complete |
| BENCH-01 | Phase 6 | Complete |
| BENCH-02 | Phase 6 | Complete |
| BENCH-03 | Phase 6 | Mixed |
| BENCH-04 | Phase 6 | Complete |
| BENCH-05 | Phase 6 | Blocked |

**Coverage:**
- v1 requirements: 30 total
- Mapped to phases: 30
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-26*
*Last updated: 2026-03-27 after Phase 6 Plan 03 execution*
