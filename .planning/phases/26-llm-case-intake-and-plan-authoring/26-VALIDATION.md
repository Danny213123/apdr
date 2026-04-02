---
phase: 26
slug: llm-case-intake-and-plan-authoring
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-02
---

# Phase 26 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python unit tests for authored-plan generation, Rust resolver tests for IPC and artifact truth, deterministic proof checker, and artifact grep checks |
| **Config file** | `.planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json`, `.planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json`, `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md`, and `scripts/check_phase26_case_plan.py` |
| **Quick run command** | `/bin/zsh -lc "python3.11 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py tools/apdr/llm_py/tests/test_client_fallbacks.py -k phase26_ -q && cargo test --manifest-path tools/apdr/Cargo.toml phase26_ -- --nocapture"` |
| **Full suite command** | `/bin/zsh -lc "python3.11 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py tools/apdr/llm_py/tests/test_client_fallbacks.py -k phase26_ -q && cargo test --manifest-path tools/apdr/Cargo.toml phase26_ -- --nocapture && python3 -m unittest benchmark_ui.test_run_contract && python3 scripts/check_phase26_case_plan.py --plan-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json --failure-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json --status-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-case-plan-proof-status.json --probe-only && rg -n 'AUTHORED_PLAN|INTAKE_FAILURE|case-plan.json|intake-failure.json|authorship' tools/apdr/src/lib.rs tools/apdr/test_executor.py .planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md"` |
| **Estimated runtime** | ~20 seconds |

---

## Sampling Rate

- **After every task commit:** Run the task's specific verify command
- **After every plan wave:** Run the quick run command
- **Before Phase 26 verification:** Run the full suite command
- **Max feedback latency:** 20 seconds for deterministic authored-plan checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 26-01-01 | 01 | 1 | LLM-01 | python/unit | `python3.11 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py tools/apdr/llm_py/tests/test_client_fallbacks.py -k phase26_intake_ -q` | ✅ | ⬜ pending |
| 26-01-02 | 01 | 1 | LLM-01 | rust/protocol | `cargo test --manifest-path tools/apdr/Cargo.toml phase26_intake_ -- --nocapture` | ✅ | ⬜ pending |
| 26-02-01 | 02 | 2 | TRU-02 | rust/artifact | `cargo test --manifest-path tools/apdr/Cargo.toml phase26_truth_ -- --nocapture` | ✅ | ⬜ pending |
| 26-02-02 | 02 | 2 | LLM-01, TRU-02 | metadata/export | `python3 -m unittest benchmark_ui.test_run_contract` | ✅ | ⬜ pending |
| 26-03-01 | 03 | 3 | LLM-01, TRU-02 | proof-contract | `python3 scripts/check_phase26_case_plan.py --plan-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json --failure-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json --status-json /tmp/phase26-status.json --probe-only` | ✅ | ⬜ pending |
| 26-03-02 | 03 | 3 | TRU-02 | grep/proof | `rg -n 'AUTHORED_PLAN|INTAKE_FAILURE|smoke_strategy|deterministic fallback|llm-only' scripts/check_phase26_case_plan.py .planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md .planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json .planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- The existing tier3 LLM IPC remains a single JSON-line request and response exchange.
- The existing output artifact flow through `resolution-report.txt`, summary lines, and `output_data_*.yml` remains the case-metadata boundary for benchmark ingestion.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| A saved successful case reads like an authored plan rather than a post-hoc note dump | LLM-01 | Requires human inspection of artifact readability | Open a representative case artifact directory after execution and confirm `case-plan.json` plus `resolution-report.txt` explain imports, mappings, runtime assumptions, and smoke strategy coherently. |
| `llm-only` no-output truth is honest and not disguised as downstream validation failure | TRU-02 | Requires human judgment across multiple artifacts | Inspect a no-output `llm-only` case and confirm it contains an intake-failure artifact with a classified reason and diagnostic preview rather than only empty `requirements.txt` or generic `Unknown`. |
| Authored versus deterministic sections are understandable without reading raw trace logs | TRU-02 | Requires human review of phrasing and artifact organization | Confirm the final case artifacts make it obvious which plan sections were LLM-authored and which sections, if any, were filled by deterministic fallback behavior. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
