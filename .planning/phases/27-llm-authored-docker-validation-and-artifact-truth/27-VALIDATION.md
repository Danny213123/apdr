---
phase: 27
slug: llm-authored-docker-validation-and-artifact-truth
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-02
---

# Phase 27 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust unit tests for Docker authoring and handoff logic, Python unittest coverage for benchmark metadata surfaces, deterministic proof checker, and artifact grep checks |
| **Config file** | `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json`, `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json`, `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md`, and `scripts/check_phase27_docker_artifacts.py` |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase27_ -- --nocapture && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase27_ -- --nocapture && python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events && python3 scripts/check_phase27_docker_artifacts.py --authored-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json --executed-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json --status-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-docker-artifact-proof-status.json --probe-only && rg -n 'docker_plan|Dockerfile|build_image_id|executed_image_ref|docker-build.command|docker-run.command|Phase 28' tools/apdr/src/docker/builder/docker_backend.rs tools/apdr/src/lib.rs tools/apdr/test_executor.py scripts/check_phase27_docker_artifacts.py .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md"` |
| **Estimated runtime** | ~25 seconds |

---

## Sampling Rate

- **After every task commit:** Run the task's specific verify command
- **After every plan wave:** Run the quick run command
- **Before Phase 27 verification:** Run the full suite command
- **Max feedback latency:** 25 seconds for deterministic Docker-artifact checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 27-01-01 | 01 | 1 | LLM-02 | rust/authoring | `cargo test --manifest-path tools/apdr/Cargo.toml phase27_author_ -- --nocapture` | ✅ | ⬜ pending |
| 27-01-02 | 01 | 1 | LLM-02, DKR-02 | artifact/write | `cargo test --manifest-path tools/apdr/Cargo.toml phase27_artifact_ -- --nocapture` | ✅ | ⬜ pending |
| 27-02-01 | 02 | 2 | DKR-01 | rust/handoff | `cargo test --manifest-path tools/apdr/Cargo.toml phase27_handoff_ -- --nocapture` | ✅ | ⬜ pending |
| 27-02-02 | 02 | 2 | DKR-02 | metadata/export | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events` | ✅ | ⬜ pending |
| 27-03-01 | 03 | 3 | LLM-02, DKR-01, DKR-02 | proof-contract | `python3 scripts/check_phase27_docker_artifacts.py --authored-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json --executed-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json --status-json /tmp/phase27-status.json --probe-only` | ✅ | ⬜ pending |
| 27-03-02 | 03 | 3 | DKR-02 | grep/proof | `rg -n 'docker_plan|Dockerfile|build_image_id|executed_image_ref|docker-build.command|docker-run.command|Phase 28' scripts/check_phase27_docker_artifacts.py .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- The Phase 26 authored intake contract remains the upstream truth boundary; Phase 27 may extend it but must not silently replace it.
- The existing case-output boundary through `resolution-report.txt`, summary lines, benchmark `output_metadata`, and attempt debug dirs remains the machine-readable review surface.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| A successful case clearly shows authored Docker intent versus executed Docker inputs | LLM-02, DKR-02 | Requires human inspection of case-artifact readability | Open a representative successful case and confirm the authored Docker plan, authored Dockerfile (or equivalent), executed Dockerfile, and command artifacts are easy to distinguish. |
| A build-to-run handoff failure is now explained by explicit image-reference truth instead of only a missing-image message | DKR-01 | Requires reviewer judgment across multiple artifacts | Inspect a regression-style case and confirm the artifacts show the built image reference, post-build verification result, and the exact reference used for container create/start. |
| Case details expose enough Docker artifact pointers that reviewers do not need raw directory spelunking | DKR-02 | Requires human review of saved case surfaces | Inspect a saved case detail view or output metadata and confirm the authored and executed Docker artifact paths are visible and understandable. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
