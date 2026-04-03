# Phase 28: LLM Recovery Loop and Failure Semantics - Research

**Researched:** 2026-04-03
**Domain:** Turning the new authored case-plan and Docker-artifact truth into a bounded LLM recovery loop while making final non-pass failure labeling truthful and machine-readable
**Confidence:** High

## Summary

Phase 28 should be planned as a recovery-contract plus failure-truth phase, not as another intake or Docker-authoring phase. Phase 26 already froze the authored case-plan contract, and Phase 27 now freezes authored-versus-executed Docker truth plus the image-handoff seam. The remaining gap is downstream: the current recovery action in [`tools/apdr/llm_py/actions/recovery.py`](tools/apdr/llm_py/actions/recovery.py) still sees only `resolved_packages`, `error_log`, `snippet_source`, `python_version`, `error_type`, and a lightweight `previous_attempts` list. It does not consume the authored case plan, the authored Docker plan, the executed Dockerfile path, the image-inspect artifact, or the attempt metadata Phase 27 just made available.

At the same time, the current final failure semantics are too coarse for the user’s April 2 regressions. Case [`runs/20260402-150222-apdr/cases/1045108`](runs/20260402-150222-apdr/cases/1045108) ends with `validation_reason: No automatic recovery fix found for Unknown. Error: LLM unavailable, proceeding optimistically`, even though the LLM trace records repeated Ollama JSON-mode `HTTP 400` failures and `120s` timeouts in [`call-004-recovery_fix/response.json`](runs/20260402-150222-apdr/cases/1045108/.apdr-debug/llm/call-004-recovery_fix/response.json). Case [`runs/20260402-184821-apdr/cases/005bbad123ef309a5bef`](runs/20260402-184821-apdr/cases/005bbad123ef309a5bef) ends as `SystemDependency` / `dependency-resolution`, even though the deeper truth is a no-output package-resolution path followed by a Docker/runtime failure seam. Today `classify_failure_family(...)` in [`tools/apdr/src/resolver/recovery_diagnostics.rs`](tools/apdr/src/resolver/recovery_diagnostics.rs) only chooses between `environment-specific` and `dependency-resolution`, so provider/tooling and LLM no-output failures get flattened into misleading dependency misses.

The right Phase 28 shape is three waves. First, upgrade the recovery contract so each bounded recovery attempt can consume the authored plan plus real executed artifacts and emit a structured recovery outcome, not just a swap/add/remove hint. Second, add additive final-failure truth fields that distinguish `llm-no-output`, `provider-tooling-failure`, `docker-infrastructure-failure`, and true dependency/runtime failure without breaking the existing summary schema. Third, freeze one applied-recovery sample and one classified non-pass sample with a deterministic checker so Phase 29 can compare gains without reopening the meaning of failure labels.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| LLM-03 | After install, build, or runtime failures, APDR can ask the LLM to propose and apply bounded recovery changes using prior attempt logs and artifacts | Phase 27 now gives Phase 28 authored and executed Docker artifacts to feed into recovery, so the next step is to widen the recovery request contract rather than adding more ad hoc string heuristics. |
| TRU-01 | Case reports distinguish LLM no-output, provider/tooling failure, Docker infrastructure failure, and genuine dependency/runtime failure instead of collapsing them into `Unknown` or misleading `SystemDependency` | The April 2 case reports prove the current surface still collapses these categories, even when the underlying trace artifacts already contain enough detail to separate them. |

## Evidence That Should Drive Planning

### The current recovery prompt does not consume authored plan or Docker artifacts

`handle(...)` in [`tools/apdr/llm_py/actions/recovery.py`](tools/apdr/llm_py/actions/recovery.py) builds its prompt from:

- `resolved_packages`
- `error_log`
- `snippet_source`
- `python_version`
- `error_type`
- `previous_attempts`

That means the recovery path still ignores:

- the Phase 26 authored case plan
- the Phase 27 authored Docker plan
- the executed Dockerfile path
- the executed image ref and image-inspect result
- the attempt metadata path and combined log path

Phase 28 should consume those artifacts directly instead of making the LLM infer them from a single log excerpt.

### The retry loop records notes, but not a first-class recovery artifact

[`tools/apdr/src/resolver/retry_loop.rs`](tools/apdr/src/resolver/retry_loop.rs) already keeps `llm_recovery_history`, `iteration_snapshots`, and `validation.iteration_history`, and it applies recovery via `apply_llm_recovery_hint(...)`. But this state is mostly flattened into notes and final attempt metadata. There is no durable case-level artifact like `recovery-attempts.json` that records:

- what recovery input was sent
- which authored/executed artifacts were referenced
- whether the LLM applied a fix, abstained, or failed
- whether the failure was model no-output versus provider/tooling versus runtime

Phase 28 should make that recovery path inspectable and reusable for later benchmark proof.

### Current final failure semantics are too coarse for the live regressions

The two concrete April 2 regressions show why additive truth fields are needed:

- In [`runs/20260402-150222-apdr/cases/1045108/resolution-report.txt`](runs/20260402-150222-apdr/cases/1045108/resolution-report.txt), the final case surface says `Unknown`, while the LLM trace shows repeated `time: missing unit in duration "-1"` and timeout failures in Ollama.
- In [`runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/resolution-report.txt`](runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/resolution-report.txt), the final case surface says `SystemDependency` and `dependency-resolution`, even though the trace also shows `LLM package-resolution call returned no output`.

The existing `failure_family` field is still useful as a coarse bucket, but Phase 28 should add explicit additive fields such as:

- `recovery_outcome`
- `failure_truth_class`
- `failure_truth_detail`

That lets later evidence distinguish true dependency/runtime misses from model/provider or Docker-infrastructure failures without breaking older readers.

### Phase 27 created the exact upstream inputs Phase 28 needs

Phase 27 now gives the recovery phase:

- `case-plan.json`
- `docker-plan.json`
- `Dockerfile.authored`
- `Dockerfile.executed`
- `docker-build.command.txt`
- `docker-run.command.txt`
- `docker-image.inspect.txt`
- `executed_image_ref`
- `image_handoff_verified`

That means Phase 28 can finally run a genuine artifact-aware recovery loop instead of passing only `last_log`.

## Implementation Recommendations

### 1. Expand the recovery contract to use authored and executed artifacts

Recommended files:

- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/prompts.py`
- `tools/apdr/llm_py/actions/recovery.py`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/tests/test_resolver.rs`

Recommended responsibilities:

- extend the recovery request with authored-plan, authored-Docker-plan, and executed-attempt artifact context
- add structured recovery outcome fields such as `recovery_outcome`, `failure_class`, and `diagnostic_preview`
- persist ordered recovery attempts in a durable case artifact instead of only notes
- keep the recovery loop bounded so repeated provider or no-output failures stop truthfully

### 2. Add additive final-failure truth fields without breaking coarse buckets

Recommended files:

- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/recovery/classifier.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`

Recommended responsibilities:

- keep `failure_family` as the coarse environment-specific versus dependency-resolution bucket
- add additive fields that explicitly classify:
  - `llm-no-output`
  - `provider-tooling-failure`
  - `docker-infrastructure-failure`
  - `dependency-runtime-failure`
- derive those fields from intake failures, recovery diagnostics, Docker artifact truth, and final attempt metadata instead of only the last log string
- export them through case reports, summary lines, benchmark metadata, and live events

### 3. Freeze recovery and failure-truth contracts before Phase 29

Recommended files:

- `scripts/check_phase28_recovery_truth.py`
- `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json`
- `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json`
- `.planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md`

Recommended responsibilities:

- freeze one sample where bounded recovery applies a real fix and records the recovery outcome
- freeze one sample where the final failure is classified as provider/tooling or Docker infrastructure rather than `Unknown`
- add a deterministic checker that proves the recovery artifact and additive failure-truth fields stay present and consistent

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase28_ -- --nocapture`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events`

### Focused recovery checks

- `python3 -m pytest tools/apdr/llm_py/tests/test_recovery_mock.py tools/apdr/llm_py/tests/test_client_fallbacks.py -q`

### Artifact checks

- `python3 scripts/check_phase28_recovery_truth.py --recovery-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json --failure-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json --status-json /tmp/phase28-status.json --probe-only`
- `rg -n 'recovery_outcome|failure_truth_class|failure_truth_detail|recovery-attempts.json|Phase 29' scripts/check_phase28_recovery_truth.py .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-RECOVERY-TRUTH-PROOF.md tools/apdr/src/lib.rs tools/apdr/test_executor.py`

### Phase-close checks

- inspect a case where recovery applies a fix and confirm the saved artifacts show the authored plan, executed Docker artifacts, recovery attempt record, and the exact note that changed the dependency set
- inspect a case where the provider fails or the model abstains and confirm the final case report no longer collapses to `Unknown`
- inspect a case where Docker infrastructure fails and confirm the case report distinguishes that from a true dependency/runtime miss

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-VERIFICATION.md`
- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/prompts.py`
- `tools/apdr/llm_py/actions/recovery.py`
- `tools/apdr/llm_py/client.py`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/recovery/classifier.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`
- `runs/20260402-150222-apdr/cases/1045108/resolution-report.txt`
- `runs/20260402-150222-apdr/cases/1045108/.apdr-debug/llm/call-004-recovery_fix/response.json`
- `runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/resolution-report.txt`
- `runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/.apdr-debug/llm/call-002-package_resolution/response.json`

## Out of Scope For This Phase

- benchmark pass-rate or timing comparison claims against the April 2 baseline
- final live-evidence packaging or a shipping verdict for the end-to-end LLM path
- replacing the active LLM provider stack instead of stabilizing the current orchestration
- another round of Docker authoring or image-handoff work already covered by Phase 27
- broad benchmark UI redesign

## Source Base

No external browsing was required for Phase 28 planning. The source of truth is the active v2.5 milestone files, the completed Phase 26 and Phase 27 proof packages, the current recovery and diagnostics code, and the April 2 run artifacts already present in the workspace.

---
*Research created: 2026-04-03*
*Phase: 28-llm-recovery-loop-and-failure-semantics*
