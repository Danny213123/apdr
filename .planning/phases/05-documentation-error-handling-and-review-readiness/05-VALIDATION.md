---
phase: 05
slug: documentation-error-handling-and-review-readiness
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-27
---

# Phase 5 - Validation Strategy

> Validation contract for reviewer-facing Rust documentation, panic-path hardening, and consistency cleanup across the Phase 4 modernization surfaces.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test`, `cargo clippy`, and structural `rg` checks |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` |
| **Estimated runtime** | ~4-8 minutes for the full suite, depending on host and cached artifacts |

---

## Sampling Rate

- **After every task commit:** Run the task-specific quick command plus the relevant structural grep for docs, guide output, or panic-path removal
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** keep targeted checks under 2 minutes except for the full Rust suite

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | QUAL-01 | structural grep | `rg -n '^//!|^///' tools/apdr/src/resolver/mod.rs tools/apdr/src/docker/builder/mod.rs tools/apdr/src/resolver/family_knowledge/mod.rs tools/apdr/src/resolver/pypi_client/mod.rs tools/apdr/src/resolver/tier3_llm/mod.rs` | yes | pending |
| 05-01-02 | 01 | 1 | QUAL-04 | doc existence | `rg -n 'resolver|validation builder|family knowledge|PyPI client|tier3 LLM|fallback|verification' .planning/phases/05-documentation-error-handling-and-review-readiness/*` | yes | pending |
| 05-02-01 | 02 | 2 | QUAL-02 | panic-path grep | `rg -n 'unwrap\\(|expect\\(' tools/apdr/src/resolver tools/apdr/src/docker/builder` | yes | pending |
| 05-02-02 | 02 | 2 | QUAL-02 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` | yes | pending |
| 05-02-03 | 02 | 2 | QUAL-02 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` | yes | pending |
| 05-03-01 | 03 | 3 | QUAL-03 | full Rust suite | `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` | yes | pending |
| 05-03-02 | 03 | 3 | QUAL-05 | structural grep | `rg -n 'Result<|io::Result|Failed to|fallback|retrying with Docker' tools/apdr/src/resolver tools/apdr/src/docker/builder` | yes | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.
- No new test framework or external service is required before execution.
- The phase should reuse the existing resolver and validation-pipeline test entrypoints plus clippy.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Reviewer guide covers the five modernized areas only and explains ownership, fallback behavior, and verification pointers | QUAL-01, QUAL-04 | Grep can prove the guide exists, but a reviewer still needs to judge whether the sections actually explain the modernized boundaries clearly | Read the new reviewer guide and confirm it has sections for resolver facade, validation builder, family knowledge, PyPI client, and tier3 LLM, and that each section names ownership, fallback or escalation behavior, and the commands reviewers should run |
| Remaining panic sites are narrow internal invariants rather than runtime-facing failures | QUAL-02 | Automated grep can show where `unwrap()` and `expect()` remain, but a reviewer must judge whether the survivors are truly internal and documented | Review any remaining `unwrap()` or `expect()` in touched production files and confirm each surviving site is either inside tests or explicitly justified as a narrow invariant |
| Naming and error-handling style look consistent across the touched surfaces | QUAL-05 | Clippy and grep help, but consistency across facade docs, guide language, and error-path naming is still a review judgment | Read the touched Rust modules and the reviewer guide together and confirm terminology for fallback, escalation, retry, and reviewer entrypoints stays consistent |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity still references the existing Rust test and lint loop
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded for targeted checks
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** approved 2026-03-27
