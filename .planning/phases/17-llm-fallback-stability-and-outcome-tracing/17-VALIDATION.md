---
phase: 17
slug: llm-fallback-stability-and-outcome-tracing
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-03-30
---

# Phase 17 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` + Python `unittest` |
| **Config file** | `tools/apdr/Cargo.toml` and repo-root Python test modules |
| **Quick run command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_ && python3 -m unittest benchmark_ui.test_runner_events"` |
| **Full suite command** | `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract"` |
| **Estimated runtime** | ~120 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_ && python3 -m unittest benchmark_ui.test_runner_events"`
- **After every plan wave:** Run `/bin/zsh -lc "cargo test --manifest-path tools/apdr/Cargo.toml && python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract"`
- **Before `$gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 120 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 17-01-01 | 01 | 1 | AGT-07 | syntax + unit | `python3 -m py_compile tools/apdr/docker_agent/__main__.py tools/apdr/docker_agent/graph.py tools/apdr/docker_agent/state.py` | ✅ | ⬜ pending |
| 17-01-02 | 01 | 1 | AGT-07 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_` | ✅ | ⬜ pending |
| 17-02-01 | 02 | 2 | AGT-08 | unit | `cargo test --manifest-path tools/apdr/Cargo.toml phase17_llm_` | ✅ | ⬜ pending |
| 17-02-02 | 02 | 2 | AGT-08 | unit | `python3 -m unittest benchmark_ui.test_runner_events` | ✅ | ⬜ pending |
| 17-03-01 | 03 | 3 | AGT-07 | proof-contract | `python3 scripts/check_phase17_fallback_artifacts.py --slice-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-live-fallback-slice.json --sample-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-agent-outcome-sample.json --status-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-fallback-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 17-03-02 | 03 | 3 | AGT-08 | syntax + doc | `python3 -m py_compile scripts/check_phase17_fallback_artifacts.py` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live crash signature removed on the fixed March 30 slice | AGT-07 | Requires inspecting current benchmark artifacts rather than only unit-test fixtures | Re-run the Phase 17 checker with `--run-dir runs/20260330-020943-apdr` and confirm the generated proof status does not report `ValueError: 'confidence' is already being used as a state key` for the checked run. |
| Case artifacts expose fallback truth after replay | AGT-08 | Requires human review of generated case outputs and proof note | Open the checked `output_data_*.yml` files for the fixed slice and confirm each checked case includes `fallback_invoked`, `fallback_outcome`, and `fallback_reason`, then confirm `17-FALLBACK-PROOF.md` calls out those exact keys in `## Before/After Review`. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
