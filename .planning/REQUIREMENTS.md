# Requirements: APDR v2.5 LLM End-to-End Resolver and Validation

**Defined:** 2026-04-02
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2.5 milestone. Each will map to exactly one roadmap phase.

### LLM Case Authoring

- [x] **LLM-01**: In both `llm` and `llm-only` modes, APDR can ask the LLM to extract snippet modules, runtime intent, and initial dependency candidates before validation starts
- [x] **LLM-02**: In both `llm` and `llm-only` modes, APDR can ask the LLM to author Docker-oriented validation inputs, including build/runtime guidance and reproducible per-case artifacts
- [x] **LLM-03**: After install, build, or runtime failures, APDR can ask the LLM to propose and apply bounded recovery changes using prior attempt logs and artifacts

### Docker Execution Reliability

- [x] **DKR-01**: Docker validation can reliably run the image it just built in `llm` and `llm-only` modes without image-handoff or tag-visibility regressions
- [x] **DKR-02**: Each LLM-driven case debug folder records the authored plan, Docker inputs, recovery prompts/responses, and final executed artifacts needed to explain the case path

### Failure Truth

- [x] **TRU-01**: Case reports distinguish LLM no-output, provider/tooling failure, Docker infrastructure failure, and genuine dependency/runtime failure instead of collapsing them into `Unknown` or misleading `SystemDependency`
- [x] **TRU-02**: `llm` and `llm-only` keep truthful metadata about which parts of the pipeline were authored by the LLM versus deterministic fallbacks

### Benchmark Evidence

- [x] **BEN-01**: Fixed-slice comparison artifacts show whether the new LLM-led path improves pass rate for both `llm` and `llm-only` against the April 2, 2026 baseline runs
- [x] **BEN-02**: Comparison artifacts track solve or validate timing, LLM no-output rate, and Docker handoff failures so gains are not hidden behind new regressions

### Closeout Evidence

- [ ] **EVD-11**: Milestone closes with reviewer-readable evidence and a go/no-go recommendation for the LLM-led end-to-end path in both `llm` and `llm-only`

## v2 Requirements

Deferred until the end-to-end LLM path is stable.

### Future Expansion

- **PROV-01**: Replace or widen LLM providers only after the current end-to-end orchestration path is stable enough to compare fairly
- **AGT-01**: Explore multi-step or multi-agent LLM orchestration only after the single-case start-to-finish contract is reliable
- **PERF-03**: Add Docker image prewarming or cache seeding only after the new LLM-led path proves correct enough to optimize
- **UI-03**: Rework operator-facing benchmark controls only after the authored-artifact and failure-truth surfaces stabilize

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full benchmark UI redesign | This milestone is about end-to-end LLM execution behavior and evidence, not interface expansion |
| Replacing the Rust or Python architecture | The goal is to improve the existing APDR system, not rewrite it |
| Broad deterministic recovery-table growth as the main strategy | The active objective is LLM-led planning and recovery, not another large static rules pass |
| Full LLM provider swap before fixing the current path | The current issue is orchestration reliability, not picking a new vendor first |
| Changing benchmark datasets or scoring rules | Evidence must remain comparable to the April 2, 2026 baseline runs |
| Re-litigating the docker-first policy verdict as the main deliverable | The next milestone is about making the chosen path work well in practice |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| LLM-01 | Phase 26 | Complete |
| LLM-02 | Phase 27 | Complete |
| LLM-03 | Phase 28 | Complete |
| DKR-01 | Phase 27 | Complete |
| DKR-02 | Phase 27 | Complete |
| TRU-01 | Phase 28 | Complete |
| TRU-02 | Phase 26 | Complete |
| BEN-01 | Phase 29 | Complete |
| BEN-02 | Phase 29 | Complete |
| EVD-11 | Phase 30 | Pending |

**Coverage:**
- v1 requirements: 10 total
- Mapped to phases: 10
- Unmapped: 0

---
*Requirements defined: 2026-04-02*
*Last updated: 2026-04-03 after completing Phase 29*
