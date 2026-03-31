# Requirements: APDR v2.3 Tier3 Validation Recovery and Reliability

**Defined:** 2026-03-30
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2.3 milestone. Each will map to exactly one roadmap phase.

### Agent Reliability

- [x] **AGT-07**: Benchmark operator can run APDR with `--validation-backend llm` on tier3 cases without the LangGraph fallback crashing after env validation fails
- [x] **AGT-08**: Benchmark operator can inspect per-case artifacts to see whether the LLM fallback was invoked, passed, abstained, or failed
- [ ] **AGT-09**: APDR resolves more cases successfully on the selected v2.3 tier3 benchmark slice than the March 30 2026 baseline for the same run mode and model

### Validation Recovery

- [ ] **VAL-01**: Benchmark operator can rerun eligible `environment-build-failed` and `version-not-found` tier3 cases in `llm` mode and have APDR attempt Docker-backed validation before final failure
- [ ] **VAL-02**: Benchmark operator can inspect each validation attempt to see the actual backend path taken (`env`, `docker`, or `llm-agent`) instead of only the configured run mode
- [ ] **VAL-03**: APDR reduces failures in the `module-not-found`, `environment-build-failed`, and `version-not-found` tier3 buckets on the selected v2.3 benchmark slice compared with the March 30 2026 baseline
- [ ] **VAL-04**: Benchmark operator can distinguish framework or host-runtime failures from dependency-resolution failures in per-case validation results so environment-specific cases are not counted as generic mapping misses

### Benchmark Evidence

- [ ] **EVD-07**: Benchmark operator can trust resumed-run summaries not to mark skipped host-runtime cases as successes
- [ ] **EVD-08**: Milestone evidence shows before-and-after tier3 bucket counts and representative case-level artifacts for the recovery changes shipped in v2.3
- [ ] **EVD-09**: Milestone proof can compare live v2.3 tier3 recovery results against the March 30 2026 baseline without mixing stale historical case metadata into current-run conclusions

### Compatibility Guardrails

- [ ] **WIN-02**: Validation pipeline changes in v2.3 preserve Windows and Docker correctness paths instead of regressing support to env-only validation

## v2 Requirements

Deferred until the live tier3 recovery path is stable and benchmark evidence is trustworthy.

### Future Expansion

- **PROV-02**: Revisit broader model-provider changes only after the current fallback and backend-routing problems are fixed
- **TRAIN-02**: Evaluate fine-tuning or PEFT only if recovery gains plateau after runtime and agent-path fixes
- **DATA-01**: Expand benchmark datasets only after the existing live benchmark evidence becomes trustworthy
- **UI-01**: Rework benchmark UI summaries only after the underlying run accounting is correct
- **HIST-02**: Revisit the unfinished v2.2 live-proof closeout only if a later milestone needs that historical claim closed explicitly

## Out of Scope

| Feature | Reason |
|---------|--------|
| Benchmark UI redesign | This milestone is about live recovery reliability and truthful reporting, not interface expansion |
| Full LLM provider replacement | Fix the current fallback and backend path before changing providers |
| Broad deterministic recovery-table expansion | The milestone should improve live recovery behavior without turning into another hardcoded patch sweep |
| Benchmark dataset or scoring-rule changes | Results need to stay comparable while reliability work lands |
| Full async or architecture rewrite | The goal is to improve the current system, not replace it |
| Reopening v2.2 sample-proof packaging as active scope | The current priority is live tier3 reliability, not more closeout packaging |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| AGT-07 | Phase 17 | Complete |
| AGT-08 | Phase 17 | Complete |
| AGT-09 | Phase 20 | Pending |
| VAL-01 | Phase 18 | Pending |
| VAL-02 | Phase 18 | Pending |
| VAL-03 | Phase 20 | Pending |
| VAL-04 | Phase 19 | Pending |
| EVD-07 | Phase 19 | Pending |
| EVD-08 | Phase 21 | Pending |
| EVD-09 | Phase 19 | Pending |
| WIN-02 | Phase 18 | Pending |

**Coverage:**
- v1 requirements: 11 total
- Mapped to phases: 11
- Unmapped: 0

---
*Requirements defined: 2026-03-30*
*Last updated: 2026-03-31 after Phase 17 Plan 01 execution*
