# Phase 5: Documentation, Error Handling & Review Readiness - Context

**Gathered:** 2026-03-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Raise review readiness across the Phase 4 Rust modernization surfaces by improving reviewer-facing documentation, tightening panic-path handling, and making style and error-handling behavior more consistent. This phase hardens and explains the existing Rust internals; it does not add new product capabilities or replace the module boundaries established in Phase 4.

</domain>

<decisions>
## Implementation Decisions

### Documentation Surface
- **D-01:** Phase 5 should produce both inline Rust documentation and a reviewer-facing guide.
- **D-02:** The inline-doc pass should focus on the Phase 4 modernization surfaces first: the resolver facade, validation builder, family knowledge, PyPI client, and tier3 LLM areas.
- **D-03:** Inline Rust docs should stay focused on public API behavior and reviewer entrypoints rather than broad deep-helper commentary.
- **D-04:** Deeper fallback, escalation, and recovery explanations should live in the reviewer-facing guide, organized by module.

### Panic-Path Policy
- **D-05:** Remove runtime-facing `unwrap()` and `expect()` panic paths in touched production Rust modules; only narrow internal invariants may remain, and those must be explicitly justified.
- **D-06:** When replacing a panic caused by host state, subprocess setup, cache state, or similar runtime conditions, prefer normal error propagation and add graceful fallback behavior when it fits the module.
- **D-07:** If a formerly-panicking path cannot continue, route into that module's existing fallback behavior instead of failing hard whenever such a fallback already exists.
- **D-08:** Small helper-signature refactors inside the touched Phase 4 surfaces are allowed when needed to remove a panic cleanly.

### Reviewer Guide Scope
- **D-09:** The reviewer-facing guide should include, for each covered area, a module responsibility map, a fallback/error-handling map, and verification pointers.
- **D-10:** Mandatory guide sections should cover only the five major modernized areas: resolver facade, validation builder, family knowledge, PyPI client, and tier3 LLM.
- **D-11:** Verification guidance in the guide should reuse the existing `cargo test`, `cargo clippy`, and targeted structural checks rather than introducing a new review framework.

### the agent's Discretion
- The exact filename and placement of the reviewer-facing guide may be chosen during planning as long as it is easy for reviewers to discover from the milestone artifacts.
- The planner may choose the exact inline-doc sites within each covered module, provided the pass stays focused on public entrypoints and reviewer-relevant behavior.
- The planner may decide which narrow invariants remain as documented internal assumptions after panic cleanup, as long as runtime-facing panics are removed from touched production code.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone scope and requirements
- `.planning/PROJECT.md` - active milestone scope, constraints, and the review-quality goals for this modernization effort.
- `.planning/REQUIREMENTS.md` - Phase 5 requirement IDs `QUAL-01` through `QUAL-05` and the milestone traceability map.
- `.planning/ROADMAP.md` - Phase 5 goal, success criteria, and milestone sequencing.
- `.planning/STATE.md` - carry-forward context from Phases 1 through 4 and the current "ready to plan" state for Phase 5.

### Phase 4 module-boundary reference
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-RESEARCH.md` - rationale for the five modernized Rust areas, stable entrypoints, and why reviewability work is scoped to those surfaces.
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-VALIDATION.md` - existing verification commands and structural review checks that the Phase 5 guide should point reviewers toward.
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-01-SUMMARY.md` - resolver facade split summary and the control-flow responsibilities preserved in `resolver/mod.rs`.
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-02-SUMMARY.md` - validation builder split summary and the backend/runtime/process boundaries preserved in `docker/builder/mod.rs`.
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-03-SUMMARY.md` - family knowledge, PyPI client, and tier3 LLM facade split summary.

### Codebase conventions and concern hotspots
- `.planning/codebase/CONVENTIONS.md` - current Rust documentation, linting, naming, and error-handling conventions.
- `.planning/codebase/CONCERNS.md` - known panic-path, fragility, and reviewability hotspots that Phase 5 should resolve or explicitly document.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tools/apdr/src/resolver/mod.rs`, `tools/apdr/src/docker/builder/mod.rs`, `tools/apdr/src/resolver/family_knowledge/mod.rs`, `tools/apdr/src/resolver/pypi_client/mod.rs`, and `tools/apdr/src/resolver/tier3_llm/mod.rs`: stable facade entrypoints created in Phase 4 that now form the primary documentation and reviewer-guide targets.
- `tools/apdr/src/docker/builder/env_backend.rs` and `tools/apdr/src/resolver/recovery_diagnostics.rs`: already contain useful function-level docs that can serve as the style baseline for additional reviewer-oriented comments.
- Existing milestone verification commands in `.planning/phases/04-module-layout-and-boundary-cleanup/04-VALIDATION.md` and `.planning/STATE.md`: ready-made review pointers for the new guide.

### Established Patterns
- Keep public entrypoints stable and push heavy implementation into sibling modules behind thin `mod.rs` facades.
- Prefer standard Rust error propagation and graceful degradation over runtime-facing panics in production paths.
- Reuse the existing verification loop (`cargo test`, `cargo clippy`, targeted `rg` and structure checks) instead of creating a separate testing framework for review readiness.

### Integration Points
- `resolver::resolve_path(...)` in `tools/apdr/src/resolver/mod.rs` remains the main resolver orchestration entrypoint.
- `docker::builder::validate_requirements(...)` in `tools/apdr/src/docker/builder/mod.rs` remains the main validation entrypoint and fallback gateway.
- The `family_knowledge`, `pypi_client`, and `tier3_llm` facades remain the reviewer-facing module boundaries for their respective support domains.

</code_context>

<specifics>
## Specific Ideas

- Keep the inline-doc pass API-focused rather than turning it into a broad helper-by-helper annotation sweep.
- Organize the reviewer guide by modernized module area so fallback maps and verification pointers line up with the Phase 4 boundaries.
- Do not add a separate new review framework; the guide should point back to the commands and targeted checks the repo already uses.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 05-documentation-error-handling-and-review-readiness*
*Context gathered: 2026-03-27*
