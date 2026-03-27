---
phase: 04
slug: module-layout-and-boundary-cleanup
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-27
---

# Phase 4 - Validation Strategy

> Validation contract for Rust module decomposition, stable entrypoint preservation, and structural reviewability checks.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test`, `cargo clippy`, and structural `rg` or line-count checks |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml test_resolver -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` |
| **Structural command** | `rg -n "^mod |^pub\\(crate\\) mod |^pub mod " tools/apdr/src/resolver tools/apdr/src/docker` |
| **Estimated runtime** | ~4-8 minutes per full verification loop, depending on Rust test runtime |

---

## Sampling Rate

- **After every task commit:** run the quick command plus the task-specific grep or line-count check
- **After every plan wave:** run the full suite command
- **Before phase closeout:** rerun the structural command and verify the top-level orchestrator files are materially smaller than the Phase 3 starting point
- **Max feedback latency:** keep file-split feedback under 2 minutes except for the full Rust suite

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | ARCH-01 | structural grep | `rg -n "mod retry_loop;|mod recovery_diagnostics;|mod artifacts;" tools/apdr/src/resolver/mod.rs` | yes | pending |
| 04-01-02 | 01 | 1 | ARCH-02 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml test_resolver -- --nocapture` | yes | pending |
| 04-01-03 | 01 | 1 | ARCH-05 | line-count guard | `@(Get-Content tools/apdr/src/resolver/mod.rs).Count` | yes | pending |
| 04-02-01 | 02 | 2 | ARCH-01 | structural grep | `rg -n "mod env_backend;|mod docker_backend;|mod agent_backend;|mod python_runtime;|mod process;" tools/apdr/src/docker/builder/mod.rs` | no | pending |
| 04-02-02 | 02 | 2 | ARCH-03 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` | yes | pending |
| 04-02-03 | 02 | 2 | ARCH-04 | line-count guard | `@(Get-Content tools/apdr/src/docker/builder/mod.rs).Count` | no | pending |
| 04-03-01 | 03 | 3 | ARCH-03 | structural grep | `rg -n "mod legacy_bundles;|mod learned;|mod detection;|mod smartpip;|mod version_matching;|mod host_python;|mod process;|mod context;|mod failure_memory;" tools/apdr/src/resolver` | no | pending |
| 04-03-02 | 03 | 3 | ARCH-02 | full Rust suite | `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` | yes | pending |
| 04-03-03 | 03 | 3 | ARCH-05 | artifact + structure review | `rg -n "resolve_path|validate_requirements|apply_family_knowledge|fetch_versions|resolve_with_context" tools/apdr/src/resolver tools/apdr/src/docker` | yes | pending |

---

## Wave 0 Requirements

- Existing Rust test infrastructure from Phases 1 through 3 remains available.
- No new packages or services are required before execution.
- The current benchmark and validation artifacts stay unchanged and serve only as reference during this structural phase.
- Phase 4 may move files, but it must not rely on a working Docker daemon for verification.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Top-level orchestrator files read as control-flow layers rather than dumping grounds | ARCH-02, ARCH-05 | Tests can prove behavior, but they cannot judge whether the resulting module boundary is actually easier to review | Open `tools/apdr/src/resolver/mod.rs` and `tools/apdr/src/docker/builder/mod.rs`, then confirm most heavy implementation details moved into sibling modules and the remaining file reads as orchestration |
| New module names match responsibilities without hiding existing behavior | ARCH-04 | Grep can prove names exist, but a reviewer still needs to confirm the names describe what the file owns | Read the new module filenames and verify each extracted cluster matches the function families listed in `04-RESEARCH.md` |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit structure check
- [x] Sampling continuity still references the existing Rust test and lint loop
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Structural checks complement behavioral checks
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** passed
