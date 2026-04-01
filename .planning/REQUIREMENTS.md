# Requirements: APDR v2.4 Docker-First LLM Validation Decision and Proof

**Defined:** 2026-04-01
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2.4 milestone. Each will map to exactly one roadmap phase.

### Repository Footprint

- [ ] **DSK-01**: Fresh source checkouts and GitHub downloads should not include avoidable heavyweight tool build outputs or other generated artifacts under `tools/`
- [ ] **DSK-02**: Developers should be able to avoid or reclaim gigabyte-scale local tool build and cache directories through supported defaults or cleanup flows without breaking normal APDR rebuildability

### Docker-First Routing

- [ ] **DFV-01**: Benchmark operator can run APDR with a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts
- [ ] **DFV-02**: Benchmark operator can inspect each case to see whether docker-first policy was honored, bypassed, or fell back, including the requested policy, actual backend path, and bypass reason
- [ ] **DFV-03**: Benchmark operator can still run the existing env-first `llm` policy as a comparison control while docker-first is being evaluated

### Comparison Evidence

- [ ] **CMP-01**: Repo can compare env-first versus docker-first `llm` validation on the same fixed benchmark slice with matching model and backend contracts
- [ ] **CMP-02**: Comparison artifacts report pass, dominant-bucket, and timing deltas so the first-hop policy can be judged on both correctness and cost

### Compatibility Guardrails

- [ ] **GDR-01**: When Docker is unavailable, unsupported, or explicitly bypassed, APDR degrades clearly without silently breaking `llm` validation
- [ ] **GDR-02**: Docker-first evaluation preserves truthful classification for host-runtime or framework blockers instead of flattening them into generic dependency-resolution failures

### Decision Evidence

- [ ] **EVD-10**: Milestone closes with a reviewer-readable recommendation on whether docker-first should replace env-first, remain optional, or be rejected for `llm` mode

## v2 Requirements

Deferred until the docker-first policy question is answered.

### Future Expansion

- **CORP-01**: Expand the final env-first versus docker-first comparison beyond the fixed slice only after the first-hop policy is decided
- **PERF-02**: Investigate Docker image and cache prewarming if docker-first proves correct but too expensive in runtime cost
- **UI-02**: Rework operator-facing benchmark controls only after the backend policy and evidence surfaces stabilize
- **HIST-03**: Revisit unfinished superseded-milestone closeout only if the docker-first evidence needs those historical comparisons

## Out of Scope

| Feature | Reason |
|---------|--------|
| Immediate Git history rewrite or force-cleaning every developer machine | Start by removing avoidable current-tree bytes and adding supported cleanup paths before attempting a riskier repo-history intervention |
| Immediate global removal of env validation from every mode | The milestone must answer the policy question with evidence before hard-cutting existing behavior |
| Full LLM agent or provider replacement | The current question is routing policy, not model-provider churn |
| Broad deterministic recovery-table expansion unrelated to routing policy | Keep the scope on first-hop validation behavior and its evidence |
| Full benchmark-corpus rerun as the first proof surface | Start with a fixed comparable slice before widening claims |
| Benchmark UI redesign | This milestone is about backend policy and evidence truth, not interface expansion |
| Replacing the Rust or Python architecture | The goal is to evaluate routing policy inside the existing system |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DSK-01 | Phase 21.1 | Pending |
| DSK-02 | Phase 21.1 | Pending |
| DFV-01 | Phase 22 | Pending |
| DFV-02 | Phase 23 | Pending |
| DFV-03 | Phase 22 | Pending |
| CMP-01 | Phase 24 | Pending |
| CMP-02 | Phase 24 | Pending |
| GDR-01 | Phase 22 | Pending |
| GDR-02 | Phase 23 | Pending |
| EVD-10 | Phase 25 | Pending |

**Coverage:**
- v1 requirements: 10 total
- Mapped to phases: 10
- Unmapped: 0

---
*Requirements defined: 2026-04-01*
*Last updated: 2026-04-01 after urgent pre-22 footprint phase insertion*
