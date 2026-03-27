# Phase 5: Documentation, Error Handling & Review Readiness - Research

**Researched:** 2026-03-27
**Domain:** Reviewer-facing Rust documentation, panic-path hardening, and consistency cleanup across the Phase 4 modernization surfaces
**Confidence:** Medium

## Summary

Phase 5 should turn the new Phase 4 Rust facades into a review-ready surface without reopening the structural decomposition work. The evidence is concentrated and actionable: the five modernized facade files are now small enough to document cleanly, but they currently expose very little module-level reviewer guidance; there is no reviewer-facing guide artifact in the repository yet; and the remaining runtime-facing panic paths are clustered in a small set of files inside or adjacent to the touched modernization surfaces. The phase should therefore split into documentation-first work on the five modernized areas, targeted panic cleanup with graceful degradation where fallback behavior already exists, and a final consistency or verification pass that leaves reviewers with a stable guide plus green lint and tests.

Primary recommendation: plan Phase 5 as three sequential plans. First, add module-level docs and reviewer-entrypoint comments to the five Phase 4 surfaces while creating one reviewer guide that maps module ownership, fallback behavior, and verification commands. Second, remove runtime-facing `unwrap()` and `expect()` paths inside the touched modernization surfaces, allowing only narrow documented invariants and preferring existing fallback behavior over hard failure. Third, close the phase with a consistency sweep and verification pass that aligns naming and error-handling patterns across the same surfaces and proves the affected Rust areas still pass formatting, linting, and targeted tests.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| QUAL-01 | Non-obvious Rust behavior, invariants, and fallbacks are documented where reviewers need context | The new facade files exist but currently have almost no `//!` module docs, and there is no reviewer guide artifact covering the new boundaries. |
| QUAL-02 | Touched production Rust code removes avoidable `unwrap()` or `expect()` panic paths or documents why they are safe | Remaining runtime-facing panics are concentrated in `tier3_llm/process.rs`, `env_backend.rs`, `pre_solve.rs`, and `kgraph_db.rs`, which makes targeted cleanup tractable. |
| QUAL-03 | Touched Rust modules pass formatting, linting, and targeted tests | Existing commands from Phase 4 already cover the modernized resolver and validation surfaces and can be reused directly. |
| QUAL-04 | The codebase has a clear reviewer-facing guide to benchmark-critical modules and their responsibilities | Repository search found no existing reviewer guide for the five modernized areas, so the phase must create one. |
| QUAL-05 | Code changes align with consistent error-handling and naming conventions across Rust modules | The codebase already expects rustfmt, clippy, stable facade entrypoints, and standard error propagation; Phase 5 can tighten the touched surfaces against those conventions without expanding milestone scope. |

## Evidence That Should Drive Planning

### The facade surfaces are now small enough for focused documentation

- `tools/apdr/src/resolver/mod.rs`: `1679` lines
- `tools/apdr/src/docker/builder/mod.rs`: `356` lines
- `tools/apdr/src/resolver/family_knowledge/mod.rs`: `11` lines
- `tools/apdr/src/resolver/pypi_client/mod.rs`: `6` lines
- `tools/apdr/src/resolver/tier3_llm/mod.rs`: `5` lines

Phase 4 reduced the main review surfaces to manageable sizes, but the resulting facades are still light on reviewer-facing orientation. Only `resolver/mod.rs` currently exposes a handful of public function docs; the other four facade modules are essentially pure re-export shells. That means Phase 5 should target module-level docs and entrypoint docs rather than restarting the decomposition.

### There is no reviewer-facing guide yet

Searches across `.planning/`, `README.md`, and `tools/apdr/` found many references to reviewability goals, but no existing guide that explains:

- what each of the five modernized Rust areas owns
- where fallback or escalation paths live
- which tests or checks reviewers should run per area

That is direct support for creating a single reviewer-facing guide in this phase rather than relying only on inline comments.

### Remaining runtime-facing panic paths are concentrated and mostly local

The highest-value production panic-path targets currently visible are:

- `tools/apdr/src/resolver/tier3_llm/process.rs`
  - `spawn_python_process()` still panics on subprocess spawn failure
  - child stdin/stdout acquisition still uses `expect(...)`
  - this is a runtime-facing boundary and should degrade to LLM-unavailable behavior instead of aborting
- `tools/apdr/src/docker/builder/env_backend.rs`
  - repeated `summary.attempts.last().unwrap()` checks around backend escalation
  - these should become explicit guarded checks instead of assuming an attempt always exists
- `tools/apdr/src/resolver/pre_solve.rs`
  - `self.undo_stack.pop().unwrap()` is likely an internal invariant
  - this may be acceptable only if clearly documented or locally reworked without widening scope
- `tools/apdr/src/resolver/kgraph_db.rs`
  - `self.conn.as_ref().unwrap()` is hidden behind RAII but still encodes an invariant that should either be tightened or documented

Other flagged `unwrap()`/`expect()` sites in `builder/mod.rs` and `pypi_client/core.rs` are test-only. `smartpip.rs` and `family_knowledge/learned.rs` include narrow helper unwraps that should be reviewed during planning, but the main requirement pressure is on the runtime-facing paths above.

### Existing docs already show the preferred style baseline

The following files already contain the kind of concrete function docs Phase 5 can extend:

- `tools/apdr/src/docker/builder/env_backend.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/kgraph_db.rs`

Those files document behavior, cache semantics, and extraction logic in a reviewer-friendly style. Phase 5 should reuse that style rather than inventing a new documentation voice.

### Existing verification commands already cover the touched surfaces

Phase 4 and the current state files already identify the right validation loop:

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

That means Phase 5 should point reviewers to existing commands instead of creating a new review framework.

## Implementation Recommendations

### 1. Keep the phase scoped to the five modernized Rust areas

The user decisions in `05-CONTEXT.md` lock the scope to:

- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/`
- `tools/apdr/src/resolver/pypi_client/`
- `tools/apdr/src/resolver/tier3_llm/`

Adjacent helpers such as `pre_solve.rs` or `kgraph_db.rs` should only be touched when they are directly required to remove a panic or clarify a reviewer-relevant invariant tied to the modernized surfaces.

### 2. Split the phase into documentation, panic cleanup, then consistency closeout

Recommended plan decomposition:

1. **Reviewer-surface documentation plan**
   - add `//!` module docs or equivalent reviewer-entrypoint comments to the five modernized areas
   - keep inline docs focused on public API behavior and ownership boundaries
   - create one reviewer guide that maps module responsibilities, fallback paths, and verification commands

2. **Runtime-facing panic cleanup plan**
   - harden `tier3_llm/process.rs`, `docker/builder/env_backend.rs`, and any directly related helpers inside the modernized surfaces
   - prefer `Result` or `Option` propagation plus existing fallback behavior over panic
   - allow small local signature changes within the touched surfaces

3. **Consistency and verification plan**
   - tighten naming or error-handling consistency across the same surfaces
   - recheck that docs, guide, and panic cleanup still align with rustfmt, clippy, and targeted tests
   - ensure the reviewer guide points back to the exact commands reviewers should run

This sequence matches the locked context decisions and keeps the phase from turning into a general Rust cleanup.

### 3. Treat fallback behavior as a documentation artifact, not just a code artifact

The repo already contains meaningful fallback paths that reviewers need help following:

- env validation escalating to Docker validation in `docker/builder/mod.rs`
- Tier 3 LLM acting as a later-stage recovery path from resolver failures
- family knowledge and learned-family data influencing recovery and compatibility behavior

Phase 5 should explicitly document these flows in the reviewer guide, with module-by-module ownership, instead of burying them only in branch comments.

### 4. Distinguish true invariants from environment failures

The research suggests a practical cleanup rule:

- subprocess creation, pipe acquisition, cache access, and backend-escalation assumptions are runtime-facing and should not panic
- internal data-structure invariants may remain only when the code path is truly internal and the invariant is documented clearly enough for review

That rule gives the planner a concrete boundary for QUAL-02 without demanding a repo-wide panic purge.

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

### Structural checks

- `rg -n '^//!|^///' tools/apdr/src/resolver/mod.rs tools/apdr/src/docker/builder/mod.rs tools/apdr/src/resolver/family_knowledge/mod.rs tools/apdr/src/resolver/pypi_client/mod.rs tools/apdr/src/resolver/tier3_llm/mod.rs`
- `rg -n 'unwrap\\(|expect\\(' tools/apdr/src/resolver tools/apdr/src/docker/builder`
- `rg -n 'reviewer|fallback|verification' .planning/phases/05-documentation-error-handling-and-review-readiness`

### Phase-close checks

- confirm the reviewer guide exists and covers the five modernized areas only
- confirm the guide includes module ownership, fallback/error-handling maps, and existing verification commands
- confirm touched runtime-facing panic paths were removed or explicitly justified as narrow invariants
- confirm the targeted Rust tests and clippy still pass after the cleanup

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/codebase/CONVENTIONS.md`
- `.planning/codebase/CONCERNS.md`
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-RESEARCH.md`
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-VALIDATION.md`
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-01-SUMMARY.md`
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-02-SUMMARY.md`
- `.planning/phases/04-module-layout-and-boundary-cleanup/04-03-SUMMARY.md`
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-CONTEXT.md`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/env_backend.rs`
- `tools/apdr/src/resolver/family_knowledge/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/learned.rs`
- `tools/apdr/src/resolver/pypi_client/mod.rs`
- `tools/apdr/src/resolver/tier3_llm/mod.rs`
- `tools/apdr/src/resolver/tier3_llm/process.rs`

## Out-of-Scope For This Phase

- new benchmark or product-surface behavior
- another structural module split comparable to Phase 4
- a repo-wide panic elimination campaign outside the touched modernization surfaces
- a new verification framework or standalone review checklist system
- unrelated performance work reserved for Phase 6 closeout

---
*Research created: 2026-03-27*
*Phase: 05-documentation-error-handling-and-review-readiness*
