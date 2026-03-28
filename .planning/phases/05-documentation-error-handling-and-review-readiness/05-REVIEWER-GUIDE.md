# Phase 5 Reviewer Guide

This guide maps the five modernized Rust facades that Phase 4 made reviewable.
Start at each facade, confirm the public entrypoint and ownership boundary, then
follow the sibling modules only when you need implementation detail.

## Resolver Facade

### Ownership

`tools/apdr/src/resolver/mod.rs` owns the public resolver facade and keeps
`resolve_path(...)` as the reviewer entrypoint. The facade is responsible for
top-level orchestration, including parse-to-resolution flow, stage ordering,
and the stable public wrapper surface that tests and callers already use.
Implementation families now live behind named siblings: `retry_loop` owns
dependency mutation and retry control flow, `recovery_diagnostics` owns failure
classification and recovery notes, and `artifacts` owns parse, solver, and
iteration output.

### Fallback and Error Handling

The resolver facade should be read as a staged recovery pipeline rather than a
single solver call. Earlier cache and heuristic paths run before later recovery
paths. Tier 3 LLM behavior is intentionally a later recovery path, not the
primary resolution path, and the facade delegates failure analysis to
`recovery_diagnostics` so retry decisions and recovery notes stay centralized.

### Reviewer Checks

Confirm `tools/apdr/src/resolver/mod.rs` documents `resolve_path(...)` as the
reviewer entrypoint and names the sibling-module ownership split. Then spot
check that `retry_loop.rs`, `recovery_diagnostics.rs`, and `artifacts.rs`
match the ownership language used in the facade docs.

## Validation Builder

### Ownership

`tools/apdr/src/docker/builder/mod.rs` owns the validation builder facade and
keeps `validate_requirements(...)` as the reviewer entrypoint. The facade owns
backend selection, attempt-history assembly, and the public summary returned to
callers. Backend and helper details live in siblings: `env_backend` owns local
environment creation and smoke tests, `docker_backend` owns container
validation, `agent_backend` owns the validation-agent path, and
`python_runtime` plus `process` own shared runtime and command helpers.

### Fallback and Error Handling

The default validation path runs env validation first. When env validation
fails in a way that qualifies for fallback, the facade hands off through the
env-to-Docker escalation path. The Docker backend also contains a later
deterministic fallback behind the optional validation-agent attempt, so agent
failures do not replace the normal validation loop.

### Reviewer Checks

Confirm `tools/apdr/src/docker/builder/mod.rs` explicitly states that env
validation runs first and uses the term `env-to-Docker escalation`. Then check
`env_backend.rs` for attempt-workspace and validated-env-cache handling, and
`docker_backend.rs` for deterministic Docker fallback after any optional
validation-agent attempt.

## Family Knowledge

### Ownership

`tools/apdr/src/resolver/family_knowledge/mod.rs` owns the public family
knowledge facade. Public entrypoints such as `apply_family_knowledge(...)`,
`recover_family_knowledge(...)`, `protects_family_version(...)`, and
`validation_candidate_versions(...)` are re-exported through the facade so
callers keep the stable module path. Detailed ownership is split across
siblings: `legacy_bundles` owns curated bundle definitions and compatibility
pinning, `learned` owns learned-family persistence and lookup, and `detection`
owns family detection and namespace helpers.

### Fallback and Error Handling

Family knowledge is a guided recovery layer, not a mandatory first step for all
packages. It applies curated or learned family rules when the resolver has
enough evidence to do so, while detection helpers keep namespace and family
matching consistent. The intent is to recover from known package-family
patterns without spreading fallback rules across unrelated resolver code.

### Reviewer Checks

Confirm the facade docs use the term `family knowledge` and that the public
entrypoints remain exposed from `mod.rs`. Then check `core.rs`, `learned.rs`,
and `legacy_bundles.rs` to verify that family-rule application, persistence,
and curated bundle logic are still separated the way the facade describes.

## PyPI Client

### Ownership

`tools/apdr/src/resolver/pypi_client/mod.rs` owns the public PyPI client facade
for package metadata, version lookup, compatibility matching, and dependency
spec loading. Public entrypoints stay re-exported through the facade while
siblings carry specialized responsibilities: `smartpip` owns SmartPip and
KGraph orchestration, `version_matching` owns version parsing and constraint
checks, and `host_python` owns host-Python helper commands.

### Fallback and Error Handling

The PyPI client is deliberately layered. Version and metadata lookups prefer
the cheapest sources first: local cache, then the in-process knowledge cache,
then native KGraph SQLite, then SmartPip, then PyPI Simple API or host-Python
subprocess fallbacks. Dependency-spec lookup follows the same general pattern
of using local and indexed sources before subprocess work. Reviewers should
expect progressively more expensive and less local fallback paths as earlier
sources fail to answer the query.

### Reviewer Checks

Confirm the facade docs use the term `PyPI client` and keep the public module
path stable. Then inspect `core.rs` for the staged lookup order and verify that
`smartpip.rs`, `version_matching.rs`, and `host_python.rs` own the specific
orchestration, comparison, and subprocess helper behavior described above.

## Tier 3 LLM

### Ownership

`tools/apdr/src/resolver/tier3_llm/mod.rs` owns the public Tier 3 LLM facade.
Public entrypoints such as `assess_solvability(...)`, `resolve(...)`,
`resolve_with_context(...)`, and `fallback_notes(...)` stay exported from the
facade so resolver call sites remain stable. Detailed implementation is split
across siblings: `process` owns Python subprocess lifecycle and IPC, `context`
owns request-context assembly, and `failure_memory` owns persisted failure
history.

### Fallback and Error Handling

Tier 3 LLM is a late-stage recovery subsystem, not the primary resolver path.
Its job is to assess whether an LLM-assisted recovery attempt is warranted and,
when it is, supply candidate packages or recovery notes back to the resolver.
`process.rs` now degrades Python service startup and pipe-capture failures into
normal Tier 3 LLM unavailability behavior, so host-state problems do not abort
the resolver process before earlier fallback paths can continue.

### Reviewer Checks

Confirm the facade docs use the term `Tier 3 LLM` and keep the public entry
surface small. Then inspect `core.rs` for the public recovery entrypoints and
`process.rs` for the warning-based unavailability path that replaced the prior
runtime-facing startup panics.

## Verification Commands

Use the existing Phase 5 validation commands instead of a new checklist:

- `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`
