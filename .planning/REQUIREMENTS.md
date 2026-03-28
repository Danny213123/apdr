# Requirements: APDR v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy

**Defined:** 2026-03-27
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2.1 accuracy milestone. Each maps to exactly one roadmap phase.

### Family Knowledge

- [ ] **FAM-01**: Maintainers can define touched family aliases, package mappings, and rejection hints in data files instead of hardcoded Rust tables
- [ ] **FAM-02**: APDR loads and applies data-driven family knowledge for the touched families used in the milestone accuracy slice
- [ ] **FAM-03**: Invalid or conflicting family-knowledge data fails with actionable validation errors before it can silently change recovery behavior
- [ ] **FAM-04**: Touched family-knowledge behavior is covered by regression fixtures or tests so the data migration preserves intended outcomes

### Recovery Accuracy

- [x] **REC-01**: The repo has a reproducible target slice derived from `runs\20260327-150339-apdr` and `pllm_results\csv\summary-all-runs.csv` for APDR-failed but `pllm`-passing cases
- [ ] **REC-02**: APDR reduces `module-not-found` outcomes on the targeted parity slice compared with the 2026-03-27 baseline
- [ ] **REC-03**: APDR reduces `version-not-found` and dependency-mapping failures on the targeted parity slice compared with the 2026-03-27 baseline
- [ ] **REC-04**: APDR improves the number of APDR-failed but `pllm`-passing cases it can recover on the targeted slice
- [ ] **REC-05**: Recovery changes preserve existing passed cases and expected skip behavior for host-runtime, unsolvable, and local-helper cases on the rerun

### Benchmark Evidence

- [ ] **EVD-01**: Benchmark artifacts report case-level APDR versus baseline versus `pllm` deltas for the targeted slice
- [ ] **EVD-02**: Milestone closeout records remaining unrecovered parity cases by dominant failure bucket with enough detail for follow-on planning

## v2 Requirements

Deferred to a later milestone after the focused family-knowledge and accuracy work lands.

### Future Modernization

- **ASYNC-01**: Introduce async I/O for network and subprocess-heavy paths if post-v2.1 evidence makes it the next bottleneck
- **TRACE-01**: Replace ad-hoc telemetry with structured tracing across the Rust core
- **CI-01**: Add continuous performance and accuracy benchmarking in CI

## Out of Scope

| Feature | Reason |
|---------|--------|
| New benchmark UI or UX work | This milestone is about recovery accuracy and maintainability, not interface changes |
| Full LLM provider replacement | Improve the current recovery path before evaluating a provider swap |
| Benchmark corpus or scoring-rule redesign | Comparisons must stay anchored to the stopped run and existing hard-gists corpus |
| Broad validation-pipeline performance work | v2.0 already handled the main performance modernization scope |
| Full async or Tokio migration | Too broad for the accuracy-focused scope of v2.1 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| FAM-01 | Phase 8 | Pending |
| FAM-02 | Phase 8 | Pending |
| FAM-03 | Phase 8 | Pending |
| FAM-04 | Phase 7 | Pending |
| REC-01 | Phase 7 | Complete |
| REC-02 | Phase 9 | Pending |
| REC-03 | Phase 9 | Pending |
| REC-04 | Phase 9 | Pending |
| REC-05 | Phase 10 | Pending |
| EVD-01 | Phase 10 | Pending |
| EVD-02 | Phase 10 | Pending |

**Coverage:**
- v1 requirements: 11 total
- Mapped to phases: 11
- Unmapped: 0

---
*Requirements defined: 2026-03-27*
*Last updated: 2026-03-27 after milestone roadmap creation*
