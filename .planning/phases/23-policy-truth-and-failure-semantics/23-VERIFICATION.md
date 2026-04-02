---
phase: 23-policy-truth-and-failure-semantics
verified: 2026-04-02T04:06:51Z
status: human_needed
score: 9/9 must-haves verified
human_verification:
  - test: "Expanded LLM case detail renders the Phase 23 truth surface"
    expected: "A saved or loaded LLM case shows a Validation truth card with requested policy, validation path, LLM route, Docker status, Docker bypass reason, failure family, result origin, debug dir, and bypass note when present."
    why_human: "Browser rendering and reviewer readability were not exercised programmatically."
  - test: "Live run view preserves the same truth fields during active execution"
    expected: "After an LLM case completes, the run page refresh shows the same policy-truth keys in the active run view that saved rows expose."
    why_human: "The real browser polling and live inspection flow was verified statically in code, but not driven end to end in a browser session."
---

# Phase 23: Policy Truth and Failure Semantics Verification Report

**Phase Goal:** Operators and reviewers can see which first-hop policy was requested, what path actually ran, why docker-first was bypassed when it was, and whether non-pass cases remain classified truthfully
**Verified:** 2026-04-02T04:06:51Z
**Status:** human_needed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Every `llm` case row and live `case_complete` event preserve requested policy and actual path as separate truths. | ✓ VERIFIED | `benchmark_ui/service.py` builds `requestedLlmValidationPolicy` beside `validationPath` and `validationBackend`, while `benchmark_ui/runner.py` threads the same fields into live results and emitted events. |
| 2 | Docker bypass state is machine-readable via explicit fields, not inferred from raw logs. | ✓ VERIFIED | `benchmark_ui/service.py` and `benchmark_ui/runner.py` expose `dockerBypassReason`, `dockerBypassNote`, `debugDir`, and derived `dockerStatus`; `tools/apdr/src/docker/builder/agent_backend.rs` writes exact bypass reasons and bypass-note metadata. |
| 3 | Historical rows and live rows expose the same policy-truth keys so reviewers can compare them directly. | ✓ VERIFIED | Historical runs are normalized through `_build_case_row(...)`, live results are normalized in `_run_single(...)`, and Python tests lock identical keys across both paths. |
| 4 | Expanded LLM inspection shows requested policy, actual path, route label, bypass reason, and failure family together without raw-log scraping. | ✓ VERIFIED | `web/src/main.js` renders a `Validation truth` card with requested policy, validation path, LLM route, Docker status/bypass, failure family, result origin, debug dir, and bypass note. |
| 5 | Docker-first host-runtime and framework/runtime blockers remain `environment-specific`. | ✓ VERIFIED | `tools/apdr/src/resolver/recovery_diagnostics.rs` classifies environment-specific routes, bypass reasons, and framework-runtime markers before falling back to dependency-resolution, with `phase23_truth_` tests covering host-runtime and framework-runtime cases. |
| 6 | The UI change stays additive and detail-oriented instead of redesigning benchmark tables or relabeling `validation_backend`. | ✓ VERIFIED | `web/src/main.js` adds the truth surface only inside the expanded case detail panel, and `validationBackend` remains a separate field from requested policy and route labels. |
| 7 | Phase 23 closes on a deterministic proof contract, not on ad hoc screenshot review or raw log inspection. | ✓ VERIFIED | `scripts/check_phase23_policy_truth.py` hard-locks the slice id, proof scope, expected truth keys, and archetype contract, then writes a machine-readable pass/fail status JSON. |
| 8 | The proof slice covers both policy-truth visibility and failure-family truth together. | ✓ VERIFIED | `23-policy-truth-slice.json` locks requested policy, validation path, route label, bypass reason, Docker status, required debug artifacts, and failure family for each case. |
| 9 | The locked archetypes include real Docker attempt, env-first control, Docker bypass, host-runtime pre-skip, and framework/runtime environment-specific cases. | ✓ VERIFIED | The fixed slice and proof note cover six archetypes: Docker attempt, env-first control, Docker CLI bypass, Docker daemon bypass, host-runtime pre-skip, and framework-runtime environment-specific. |

**Score:** 9/9 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `benchmark_ui/service.py` | Case-row normalization for requested policy, actual path, bypass truth, debug pointers, and Docker status | ✓ VERIFIED | Substantive helpers read direct live values first and fall back to saved `output_metadata`; historical snapshots and live run state both use `_build_case_row(...)`. |
| `benchmark_ui/runner.py` | Live result and `case_complete` event pass-through for the same truth keys | ✓ VERIFIED | `_run_single(...)` reads APDR output metadata into the live result dict, then emits those same fields in `case_complete`. |
| `benchmark_ui/test_runner_events.py` | Regression coverage for live truth fields | ✓ VERIFIED | Tests assert requested policy, route, bypass reason/note, debug dir, validation path, and failure family on the live result, saved row, and emitted event. |
| `benchmark_ui/test_run_contract.py` | Regression coverage for saved-row truth fields and Docker status labels | ✓ VERIFIED | Tests assert requested policy remains distinct from backend/path truth and that exact `dockerStatus` labels derive correctly. |
| `web/src/main.js` | Expanded LLM case truth panel for reviewers | ✓ VERIFIED | The UI builds a `Validation truth` field list and renders it in the expanded case detail card. |
| `tools/apdr/src/resolver/recovery_diagnostics.rs` | Failure-family guard for docker-first runtime blockers | ✓ VERIFIED | Classifier prefers environment-specific routes, bypass reasons, error types, and runtime markers; targeted `phase23_truth_` tests cover the expected branches. |
| `tools/apdr/src/docker/builder/mod.rs` | Route-metadata regression coverage for docker-first archetypes | ✓ VERIFIED | Tests verify host-runtime pre-skip and daemon-unavailable bypass route metadata. |
| `scripts/check_phase23_policy_truth.py` | Deterministic checker for contract drift | ✓ VERIFIED | The checker validates the frozen slice, enforces expected UI-facing keys, and writes status JSON. |
| `.planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json` | Frozen Phase 23 archetype slice | ✓ VERIFIED | The manifest contains the six required archetypes plus policy, path, route, bypass, Docker-status, debug-artifact, and failure-family expectations. |
| `.planning/phases/23-policy-truth-and-failure-semantics/23-POLICY-TRUTH-PROOF.md` | Reviewer-readable proof pack | ✓ VERIFIED | The proof note states scope, shared truth keys, locked archetypes, failure-family expectations, and the explicit boundary that this is not Phase 24 comparison evidence. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `benchmark_ui/service.py` | `.planning/REQUIREMENTS.md` | DFV-02 case inspection requires requested policy, actual path, and bypass reason together | ✓ WIRED | `gsd-tools verify key-links` passed; the case-row builder exposes those exact fields. |
| `benchmark_ui/runner.py` | `.planning/phases/23-policy-truth-and-failure-semantics/23-RESEARCH.md` | Live SSE truth must match saved-row truth | ✓ WIRED | `gsd-tools verify key-links` passed against the research note's "Live SSE events are similarly incomplete" requirement. |
| `web/src/main.js` | `.planning/REQUIREMENTS.md` | DFV-02 requires in-UI inspection without raw metadata scraping | ✓ WIRED | `gsd-tools verify key-links` passed; the UI renders the truth card directly from run state. |
| `tools/apdr/src/resolver/recovery_diagnostics.rs` | `.planning/phases/23-policy-truth-and-failure-semantics/23-RESEARCH.md` | Docker-first runtime blockers must stay environment-specific | ✓ WIRED | `gsd-tools verify key-links` passed against the research note's classifier-gap guidance. |
| `scripts/check_phase23_policy_truth.py` | `.planning/ROADMAP.md` | Phase 23 closes on inspectability and preserved failure semantics, not the later comparison harness | ✓ WIRED | `gsd-tools` missed the exact wording pattern, but the checker hard-codes `Phase 23 inspectability and failure-family truth only; not the Phase 24 comparison harness.` and the proof note repeats the same scope boundary. |
| `.planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json` | `.planning/phases/23-policy-truth-and-failure-semantics/23-RESEARCH.md` | The proof slice must freeze the six research archetypes | ✓ WIRED | `gsd-tools verify key-links` passed; the slice matches the research note's six required archetypes. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `tools/apdr/src/docker/builder/agent_backend.rs` | `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, `docker_bypass_note_path` | Route selection and `apply_llm_route_metadata(...)` | Yes | ✓ FLOWING |
| `tools/apdr/src/lib.rs` | `validation_path`, requested policy, route, bypass, failure family, `debug_dir` | Serialized `ValidationSummary` output metadata | Yes | ✓ FLOWING |
| `benchmark_ui/runner.py` | `requestedLlmValidationPolicy`, `llmValidationRoute`, `dockerBypassReason`, `dockerBypassNote`, `debugDir`, `failureFamily` | Parsed APDR `output_metadata` in `_run_single(...)` | Yes | ✓ FLOWING |
| `benchmark_ui/service.py` | Saved/live case-row truth fields and derived `dockerStatus` | Direct live result keys first, then saved `output_metadata` via `_build_case_row(...)` | Yes | ✓ FLOWING |
| `web/src/main.js` | `item.requestedLlmValidationPolicy`, `item.validationPath`, `item.llmValidationRoute`, `item.dockerStatus`, `item.failureFamily`, `item.debugDir` | `state.currentRun` from `/api/status`, rendered via `validationTruthFields(...)` | Yes | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Rust failure-family guards hold for Phase 23 archetypes | `cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_` | Existing green evidence provided for this execution | ✓ PASS |
| Saved-row and live-event policy-truth contract stays green | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events` | Existing green evidence provided for this execution | ✓ PASS |
| Web bundle still builds with the new truth surface | `npm run build --prefix web` | Existing green evidence provided for this execution | ✓ PASS |
| Deterministic Phase 23 proof contract passes | `python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-proof-status.json --probe-only` | Existing green evidence provided for this execution; committed status JSON shows `"passed": true` with the expected six-case contract | ✓ PASS |
| Phase 22 policy contract did not regress under Phase 23 changes | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | Existing green evidence provided for this execution | ✓ PASS |
| Phase 21.1 cache regression guard still passes | `CARGO_TARGET_DIR="$HOME/.cache/apdr/target" cargo test --manifest-path tools/apdr/Cargo.toml --test test_cache phase21_1_cache_ -- --nocapture` | Existing green evidence provided for this execution | ✓ PASS |
| Phase 21.1 footprint probe still passes | `python3 scripts/check_phase21_1_footprint.py --repo-root . --mode tracked --status-json /tmp/phase21_1-tracked-status.json --probe-only` | Existing green evidence provided for this execution | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `DFV-02` | `23-01`, `23-02`, `23-03` | Benchmark operator can inspect each case to see whether docker-first policy was honored, bypassed, or fell back, including requested policy, actual backend path, and bypass reason | ✓ SATISFIED | APDR emits the metadata, benchmark readers normalize it, live events preserve it, the UI renders it in the `Validation truth` card, and the proof slice/checker locks the contract. |
| `GDR-02` | `23-02`, `23-03` | Docker-first evaluation preserves truthful classification for host-runtime or framework blockers instead of flattening them into generic dependency-resolution failures | ✓ SATISFIED | `classify_failure_family(...)` prefers environment-specific signals, `phase23_truth_` tests cover host-runtime, framework-runtime, and Docker bypass cases, and the proof slice locks four `environment-specific` archetypes. |

No orphaned Phase 23 requirements were found in `.planning/REQUIREMENTS.md`.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| --- | --- | --- | --- | --- |
| `web/src/main.js` | 1805 | `console.log` inside `appendCaseRow(...)` | ℹ️ Info | Pre-existing SSE helper stub, but not a Phase 23 blocker because the active inspection path is `pollStatus()` plus `state.currentRun`, which re-renders the run page from service snapshots. |

### Human Verification Required

### 1. Expanded LLM Case Truth Card

**Test:** Load a saved run with at least one Phase 23-shaped `llm` case, expand that case in the Run page, and inspect the `Validation truth` card.  
**Expected:** The card shows requested policy, validation path, LLM route, Docker status, Docker bypass reason, failure family, result origin, debug dir, and Docker bypass note when those values exist.  
**Why human:** The browser render/readability path was verified statically from code and tests, but not visually exercised in a real browser.

### 2. Live Run Truth Surface

**Test:** Start or resume a benchmark that produces an `llm` case, wait for a case to complete, and inspect the Run page once the next status poll lands.  
**Expected:** The active run view surfaces the same policy-truth fields for the completed case that saved rows expose, without needing raw metadata files.  
**Why human:** The polling-driven live UI flow was verified by wiring inspection, not by driving a browser session end to end.

### Gaps Summary

No automated implementation gaps were found. Phase 23's artifacts, wiring, and data flow all support the stated goal, and the provided green evidence covers the targeted Rust tests, Python tests, web build, and deterministic proof checker. The only remaining work is human confirmation that the browser-level inspection surfaces read clearly in practice.

---

_Verified: 2026-04-02T04:06:51Z_  
_Verifier: Claude (gsd-verifier)_
