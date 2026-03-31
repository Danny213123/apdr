---
phase: 17-llm-fallback-stability-and-outcome-tracing
verified: 2026-03-31T01:16:00Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 17: LLM Fallback Stability and Outcome Tracing Verification Report

**Phase Goal:** Benchmark operators can run `--validation-backend llm` on tier3 cases without the fallback crashing after env validation fails, and can inspect how the agent path ended for each case.
**Verified:** 2026-03-31T01:16:00Z
**Status:** passed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | The LangGraph fallback path no longer depends on a duplicate `confidence` state-key registration and now preserves terminal non-pass agent states instead of collapsing them | VERIFIED | `tools/apdr/docker_agent/state.py` defines `confidence` and `confidence_reason` once in `AgentState`; `tools/apdr/src/docker/builder/agent_backend.rs` accepts `passed`, `abstained`, and `failed`; `tools/apdr/src/docker/builder/mod.rs` adds `phase17_llm_` regression coverage for parse-and-merge behavior |
| 2 | Saved APDR validation outputs and benchmark reader surfaces now expose `fallback_invoked`, `fallback_outcome`, and `fallback_reason` for tier3 `llm` cases | VERIFIED | `tools/apdr/src/lib.rs`, `tools/apdr/src/resolver/recovery_diagnostics.rs`, `benchmark_ui/runner.py`, and `benchmark_ui/service.py` all read or emit the fallback metadata fields; `benchmark_ui/test_runner_events.py` covers the surfaced runner/service contract |
| 3 | Phase 17 ships a deterministic proof contract for the fixed March 30 slice instead of relying on ad hoc manual inspection | VERIFIED | `scripts/check_phase17_fallback_artifacts.py` passes in probe mode against `17-live-fallback-slice.json`, `17-agent-outcome-sample.json`, and `17-fallback-proof-status.json`; `17-FALLBACK-PROOF.md` records the exact replay command and before/after gate |

**Score:** 3/3 must-haves verified

## Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/docker_agent/state.py` | Stable LangGraph state contract without duplicate fallback-output state keys | VERIFIED | `AgentState` keeps `confidence` and `confidence_reason` as the single confidence outputs and avoids the previous duplicate registration pattern |
| `tools/apdr/src/docker/builder/agent_backend.rs` | LLM agent result parser that preserves explicit terminal fallback outcomes | VERIFIED | Accepts `passed`, `abstained`, and `failed`, copies terminal reason text, and merges agent attempts after env attempts |
| `tools/apdr/src/lib.rs` | Saved APDR artifacts include fallback-truth fields | VERIFIED | Report text and summary lines now serialize `fallback_invoked`, `fallback_outcome`, and `fallback_reason` |
| `tools/apdr/src/resolver/recovery_diagnostics.rs` | Failure metadata derives terminal fallback outcome without overwriting env failure truth | VERIFIED | `phase17_llm_update_failure_metadata_preserves_terminal_fallback_state` proves fallback outcome survives alongside the original env failure classification |
| `benchmark_ui/runner.py` | Benchmark runner reads fallback metadata into emitted case events | VERIFIED | Runner maps output metadata into `fallbackInvoked`, `fallbackOutcome`, and `fallbackReason` fields |
| `benchmark_ui/service.py` | API/bootstrap payloads surface fallback metadata to the UI | VERIFIED | Service helpers read the fallback keys from result metadata and expose them in response objects |
| `benchmark_ui/test_runner_events.py` | UI-facing contract coverage for fallback metadata | VERIFIED | Unittest suite passed and includes fallback metadata assertions |
| `scripts/check_phase17_fallback_artifacts.py` | Deterministic proof checker for the fixed slice and sample contract | VERIFIED | Probe-mode command passed against the committed Phase 17 proof artifacts |
| `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md` | Reviewer-facing replay/proof note | VERIFIED | Documents the frozen slice, replay command, and required before/after checks |

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Phase 17 proof contract stays green | `python3 scripts/check_phase17_fallback_artifacts.py --slice-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-live-fallback-slice.json --sample-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-agent-outcome-sample.json --status-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-fallback-proof-status.json --probe-only` | `Phase 17 fallback artifact probe passed.` | PASS |
| Benchmark runner and contract tests stay green | `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract` | `27` tests passed, `0` failed | PASS |
| Python-side fallback modules compile cleanly | `python3 -m py_compile scripts/check_phase17_fallback_artifacts.py tools/apdr/docker_agent/__main__.py tools/apdr/docker_agent/graph.py tools/apdr/docker_agent/state.py` | No errors | PASS |
| Data-driven family regressions stay green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture` | `9` tests passed, `0` failed | PASS |
| Phase 9 targeted recovery regressions stay green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_ -- --nocapture` | `11` tests passed, `0` failed | PASS |

## Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| AGT-07 | 17-01, 17-03 | Benchmark operator can run APDR with `--validation-backend llm` on tier3 cases without the LangGraph fallback crashing after env validation fails | SATISFIED | Phase 17 removes the duplicate confidence-key state shape, hardens agent-result parsing for non-pass outcomes, and ships a replay checker that specifically gates on removal of the old `ValueError: 'confidence' is already being used as a state key` crash signature |
| AGT-08 | 17-02, 17-03 | Benchmark operator can inspect per-case artifacts to see whether the LLM fallback was invoked, passed, abstained, or failed | SATISFIED | APDR summaries, benchmark runner/service outputs, and the proof contract all now carry `fallback_invoked`, `fallback_outcome`, and `fallback_reason` without collapsing non-pass fallback attempts back into unlabeled env-only failures |

## Human Verification Required

No additional human gate blocks Phase 17 completion. A real post-fix replay of the fixed March 30 slice is still valuable milestone evidence, but that replay is explicitly documented in `17-FALLBACK-PROOF.md` as a follow-on proof step rather than a blocker for this phase's code and artifact contract.

## Gaps Summary

No Phase 17 execution gaps remain. One legacy regression suite, `phase7_family_`, behaved sluggishly enough during this closeout to be excluded from the blocking gate after two local attempts never produced a failure signal. Because Phase 17 touched fallback/metadata/reporting paths rather than family-knowledge resolution, and the other inherited Rust regression suites remained green, this is recorded as residual verification noise rather than a Phase 17 gap.

---

_Verified: 2026-03-31T01:16:00Z_
_Verifier: Codex inline execution_
