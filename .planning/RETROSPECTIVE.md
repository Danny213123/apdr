# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v2.0 - Rust Codebase Modernization

**Shipped:** 2026-03-28
**Phases:** 6 | **Plans:** 17 | **Sessions:** not tracked

### What Was Built
- Baseline, regression, and memory guardrails for the Rust modernization effort
- Resolver and validation-path performance improvements backed by before or after candidate evidence
- Module-boundary cleanup, reviewer docs, benchmark closeout, and a green final Rust review gate

### What Worked
- Measuring baseline performance before refactoring kept optimization claims defensible
- Reusing the Phase 5 review loop made Phase 6 signoff simple and comparable
- Phase summaries and benchmark artifacts kept the milestone archive grounded in concrete evidence

### What Was Inefficient
- The milestone CLI accepted `--help` as a positional version and produced bogus archive artifacts that had to be cleaned manually
- Wrapper-level RSS profiling on Windows was too noisy to close BENCH-03 and needed a direct process-level memory comparison
- No standalone milestone audit file was recorded, so readiness had to be verified from summaries, requirements, and closeout artifacts

### Patterns Established
- Commit benchmark baselines and regression gates before touching hot Rust paths
- Split oversized Rust modules behind facade directories before the documentation and review pass
- When host-level memory signals are noisy, add a direct process-level metric instead of stretching the original artifact

### Key Lessons
1. Close performance requirements with the metric that matches the optimized layer, not the noisiest wrapper-level signal.
2. Reusing a stable verification contract across phases reduces end-of-milestone ambiguity.
3. Milestone automation needs safer argument handling before it can be trusted unattended.

### Cost Observations
- Model mix: not tracked in milestone artifacts
- Sessions: not tracked in milestone artifacts
- Notable: Phase-by-phase artifact discipline turned final closeout into mostly archive and review work instead of late engineering churn

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | not tracked | 3 | Established the initial milestone archive and benchmark-oriented planning flow |
| v2.0 | not tracked | 6 | Shifted to evidence-backed Rust modernization with stronger phase summaries and closeout artifacts |

### Cumulative Quality

| Milestone | Tests | Coverage | Zero-Dep Additions |
|-----------|-------|----------|-------------------|
| v1.0 | not tracked | not tracked | not tracked |
| v2.0 | Full Rust closeout gate green | not tracked | 0 |

### Top Lessons (Verified Across Milestones)

1. Committed milestone artifacts make later review, audit, and archival work substantially cheaper.
2. Stable benchmark and verification gates are more trustworthy than ad hoc end-of-milestone checks.
