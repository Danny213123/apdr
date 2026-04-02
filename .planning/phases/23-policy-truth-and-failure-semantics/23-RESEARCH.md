# Phase 23: Policy Truth and Failure Semantics - Research

**Researched:** 2026-04-02
**Domain:** Making docker-first `llm` case truth inspectable end to end while preserving truthful failure-family classification under the new routing policy
**Confidence:** High

## Summary

Phase 23 should be planned as an end-to-end truth-surfacing phase, not as another routing change. Phase 22 already made docker-first the default `llm` policy, persisted `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, and guaranteed Docker or bypass artifacts in each `llm` case debug folder. The remaining gap is that benchmark readers and the UI still hide most of that truth. Reviewers can see `validation_path` and `failure_family` in some places, but they still cannot inspect a case and reliably answer "was docker-first honored, bypassed, or overridden?" without reading raw output files or debug folders by hand.

The good news is that the core metadata seam already exists. `tools/apdr/src/lib.rs` persists the exact policy-truth fields Phase 23 needs, and `tools/apdr/src/docker/builder/agent_backend.rs` already normalizes route labels and exact bypass reasons such as `explicit env-first control policy`, `host-runtime pre-skip`, `docker cli unavailable`, and `docker daemon unavailable`. That means Phase 23 should not widen the backend model again or invent a second route taxonomy. It should reuse those exact fields, thread them through `benchmark_ui/service.py` and `benchmark_ui/runner.py`, and surface them in the expandable LLM case UI.

The failure-semantics seam also already exists. `tools/apdr/src/resolver/recovery_diagnostics.rs` classifies environment-specific failures separately from dependency-resolution failures, but Phase 22 did not freeze that behavior against the new docker-first policy archetypes. Phase 23 therefore needs a narrow classification guard: host-runtime pre-skips, framework-runtime blockers, and Docker-unusable runtime failures must remain `environment-specific` even when the requested policy is docker-first. The phase should close with a deterministic proof slice that locks both the policy-truth fields and the failure-family expectation together.

The clean planning shape is three waves. First, expose the missing case-level truth fields in benchmark readers and live events. Second, add UI inspection surfaces and preserve environment-specific classification under docker-first. Third, freeze a deterministic policy-truth proof package that reviewers can trust without a full rerun.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| DFV-02 | Benchmark operator can inspect each case to see whether docker-first policy was honored, bypassed, or fell back, including the requested policy, actual backend path, and bypass reason | The Rust core already emits `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, and `docker_bypass_note`, but benchmark rows, SSE events, and the web case detail view do not expose them yet. |
| GDR-02 | Docker-first evaluation preserves truthful classification for host-runtime or framework blockers instead of flattening them into generic dependency-resolution failures | `recovery_diagnostics.rs` already has an environment-specific family seam, but docker-first archetypes are not yet frozen as regression cases under that classifier. |

## Evidence That Should Drive Planning

### APDR already writes the exact policy-truth fields we need

`tools/apdr/src/lib.rs` persists `validation_path`, `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, `docker_bypass_note`, `failure_family`, `debug_dir`, `attempts_dir`, and `llm_trace_dir` into the saved output metadata. Phase 23 should reuse those exact keys instead of inventing alternate Python- or UI-only names in the backend layer.

### Route labels and bypass reasons are already normalized in one place

`tools/apdr/src/docker/builder/agent_backend.rs` already defines the allowed route labels: `docker-first`, `env-first-control`, `env-first-host-runtime`, and `env-first-docker-bypass`. It also already maps them to exact bypass reasons like `explicit env-first control policy`, `host-runtime pre-skip`, `docker cli unavailable`, and `docker daemon unavailable`. That file should remain the single source of truth for route meaning.

### Benchmark readers still drop most of the new metadata

`benchmark_ui/service.py` currently exposes `validationBackend`, `validationPath`, `escalatedBackend`, `failureFamily`, `failureBucket`, `skipCandidate`, and fallback fields in case rows, but it does not expose the requested LLM policy, route label, exact Docker bypass reason, bypass note path, or debug directory pointers. That is the core DFV-02 gap for saved runs and historical inspection.

### Live SSE events are similarly incomplete

`benchmark_ui/runner.py` already emits `validationPath`, `escalatedBackend`, `failureFamily`, and fallback metadata in live `case_complete` events, but it still omits the requested policy, route label, and Docker bypass reason. That means live UI inspection and saved-run inspection do not share the same truth surface yet.

### The current web UI still hides case-level route truth

`web/src/main.js` already lets operators choose `llm_validation_policy`, but the case rendering paths only show generic result, dependency, timing, and log-tail data. The expandable case detail panel does not render requested policy, route label, Docker bypass reason, failure family, result origin, or debug artifact paths. Phase 23 should extend the expandable detail panel and lightweight LLM case surfaces rather than redesigning the table.

### Phase 22 already guaranteed Docker or bypass artifacts per `llm` case

Phase 22 made each `llm` case leave Docker attempt materials or an explicit Docker-bypass note in the debug folder. Phase 23 does not need to redefine that artifact contract. It should expose enough metadata in the benchmark/UI layer for operators to find and understand those artifacts without scraping logs or guessing which folder to open.

### Failure-family classification exists, but docker-first regression coverage does not

`tools/apdr/src/resolver/recovery_diagnostics.rs` already distinguishes `environment-specific` from `dependency-resolution`, and it already recognizes host-runtime and framework-runtime language. The missing piece is a docker-first-specific regression contract proving that these cases stay environment-specific even when the requested policy is docker-first and Docker is bypassed or pre-skipped.

## Implementation Recommendations

### 1. Thread policy-truth metadata through benchmark readers and live events

Recommended files:

- `benchmark_ui/service.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`

Recommended responsibilities:

- add case-row helper readers for `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, `docker_bypass_note`, and `debug_dir`
- expose those values as stable camelCase case fields such as `requestedLlmValidationPolicy`, `llmValidationRoute`, `dockerBypassReason`, `dockerBypassNote`, and `debugDir`
- keep `validationBackend=llm` and `validationPath` distinct so requested mode and actual path remain separate truths
- emit the same policy-truth fields in live `case_complete` SSE payloads so live and historical case inspection stay aligned

### 2. Surface the truth in the UI and freeze environment-specific semantics

Recommended files:

- `web/src/main.js`
- `benchmark_ui/service.py`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/docker/builder/mod.rs`

Recommended responsibilities:

- extend the expanded LLM case detail view with a `Validation truth` section that shows requested policy, actual path, route label, Docker bypass reason, failure family, result origin, and debug artifact pointers
- derive a lightweight Docker participation label for display from the existing route/path metadata instead of inventing a new backend
- keep the UI change additive and detail-oriented rather than redesigning the case table
- add Rust regression tests prefixed `phase23_truth_` that prove docker-first host-runtime and framework-runtime blockers remain `environment-specific`

### 3. Freeze a deterministic Phase 23 proof slice

Recommended files:

- `scripts/check_phase23_policy_truth.py`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-POLICY-TRUTH-PROOF.md`

Recommended responsibilities:

- create a fixed slice that covers at least one docker-first Docker-attempt case, one env-first control case, one Docker CLI bypass case, one Docker daemon-unavailable bypass case, one host-runtime pre-skip case, and one framework-runtime environment-specific case
- require the checker to validate requested policy, validation path, route label, bypass reason, failure family, and the presence of expected UI-facing truth keys
- summarize the slice in a reviewer-readable proof note so later phases can compare policies without re-arguing what Phase 23 promised

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase23_truth_`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events`
- `npm run build --prefix web`

### Artifact checks

- `rg -n 'requested_llm_validation_policy|llm_validation_route|docker_bypass_reason|docker_bypass_note|failure_family' tools/apdr/src/lib.rs tools/apdr/src/docker/builder/agent_backend.rs benchmark_ui/service.py benchmark_ui/runner.py benchmark_ui/test_run_contract.py benchmark_ui/test_runner_events.py`
- `rg -n 'Validation truth|Requested policy|Docker bypass|Failure family|debugDir|dockerBypassNote' web/src/main.js`
- `rg -n 'slice-json|status-json|requested_policy|failure_family|docker_bypass_reason' scripts/check_phase23_policy_truth.py .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json`

### Phase-close checks

- inspect a representative saved `llm` case row and confirm the UI can show requested policy, actual path, route label, bypass reason, and failure family without opening raw metadata files
- inspect a live `case_complete` event and confirm it carries the same policy-truth keys as the saved-row contract
- inspect representative host-runtime and framework-runtime cases and confirm they still resolve to `failure_family=environment-specific`
- run the deterministic Phase 23 checker and confirm it passes for the locked archetype slice

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-RESEARCH.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-VALIDATION.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md`
- `tools/apdr/src/lib.rs`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `benchmark_ui/service.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/run_contract.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`
- `web/src/main.js`

## Out of Scope For This Phase

- changing the docker-first routing order or degradation rules again
- the like-for-like env-first versus docker-first comparison harness
- the final keep/optional/reject verdict for docker-first `llm`
- redesigning benchmark tables or broader UI navigation
- redefining the Phase 22 Docker artifact contract instead of surfacing it
- full benchmark-corpus reruns for proof

## Source Base

No external browsing was required for Phase 23 planning. The source of truth is the repo's Phase 22 policy artifacts, the current benchmark/UI code, and the v2.4 milestone requirements already present in the workspace.

---
*Research created: 2026-04-02*
*Phase: 23-policy-truth-and-failure-semantics*
