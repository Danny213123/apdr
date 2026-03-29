# Requirements: APDR v2.2 Improve LLM Performance and Benchmark Performance on macOS

**Defined:** 2026-03-28
**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## v1 Requirements

Requirements for the v2.2 milestone. Each maps to exactly one roadmap phase.

### Agent Intelligence

- [ ] **AGT-01**: APDR's tier3 path can use an explicit tool-calling agent loop instead of relying on a single direct completion for hard cases
- [ ] **AGT-02**: APDR can feed benchmark outcome feedback into later tier3 attempts through inspectable memory or reflection instead of new hardcoded recovery tables as the primary mechanism
- [ ] **AGT-03**: APDR improves the number of replay-slice cases the LLM path resolves successfully compared with the v2.2 baseline established for this milestone
- [ ] **AGT-04**: When the agent cannot solve a case, APDR preserves inspectable failure reasons and abstains cleanly instead of silently masking the failure behind fabricated success

### macOS Benchmark Performance

- [ ] **MAC-01**: Every saved macOS benchmark run records host architecture, APDR binary architecture, Python architecture, validation backend, run intent, and cache state
- [ ] **MAC-02**: Benchmark artifacts report stage-level timings for resolution, LLM work, env creation, package install, validation, and Docker startup when applicable
- [ ] **MAC-03**: APDR provides a fast macOS replay mode built around native env validation and a locked benchmark slice for local iteration
- [ ] **MAC-04**: APDR demonstrates substantial before-and-after macOS benchmark performance gains on the locked replay slice compared with the v2.2 baseline without lowering correctness on preserved pass or skip cases

### Cross-Platform Guardrails

- [ ] **WIN-01**: macOS-focused benchmark-performance changes do not regress benchmark runtime or sec-per-case on the representative Windows comparison slice chosen for this milestone

### Benchmark Evidence

- [ ] **EVD-03**: Saved benchmark evidence distinguishes env-fast vs Docker-proof runs and warm vs cold cache state
- [ ] **EVD-04**: Milestone closeout includes before-and-after macOS benchmark comparisons that make the claimed performance gain reviewer-readable on the reproducible replay slice
- [ ] **EVD-05**: Any model or build-profile comparison captured during the milestone records enough metadata to attribute gains to agent behavior, model choice, runtime tuning, or backend differences
- [ ] **EVD-06**: Milestone closeout includes an explicit Windows non-regression comparison for the benchmark-performance work performed in v2.2

## v2 Requirements

Deferred to a later milestone after the focused agent-quality and macOS performance work lands.

### Future Expansion

- **CROSS-01**: Normalize benchmark comparisons across multiple macOS machines once local single-machine evidence is trustworthy
- **CI-01**: Add continuous benchmark automation in CI after the local replay loop is stable and cheap enough to run regularly
- **PROV-01**: Revisit broader model-provider changes only after the current agent loop has been improved and benchmarked fairly
- **HIST-01**: Reopen the unfinished v2.1 live-proof debt only if a later milestone needs that historical claim closed explicitly

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full LLM provider replacement | The milestone should improve the current agent behavior before changing providers |
| Deterministic recovery-table expansion as the main strategy | The milestone explicitly targets more general agent intelligence instead |
| Benchmark UI redesign | This milestone is about run quality, evidence, and performance, not a broad frontend refresh |
| Running Docker for every macOS inner-loop experiment | Too slow for rapid local iteration on macOS |
| Reopening the full v2.1 live-proof closeout as active scope | The user chose to supersede v2.1 with v2.2 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| AGT-01 | Phase TBD | Pending |
| AGT-02 | Phase TBD | Pending |
| AGT-03 | Phase TBD | Pending |
| AGT-04 | Phase TBD | Pending |
| MAC-01 | Phase TBD | Pending |
| MAC-02 | Phase TBD | Pending |
| MAC-03 | Phase TBD | Pending |
| MAC-04 | Phase TBD | Pending |
| WIN-01 | Phase TBD | Pending |
| EVD-03 | Phase TBD | Pending |
| EVD-04 | Phase TBD | Pending |
| EVD-05 | Phase TBD | Pending |
| EVD-06 | Phase TBD | Pending |

**Coverage:**
- v1 requirements: 13 total
- Mapped to phases: 0
- Unmapped: 13

---
*Requirements defined: 2026-03-28*
*Last updated: 2026-03-28 after v2.2 research synthesis*
