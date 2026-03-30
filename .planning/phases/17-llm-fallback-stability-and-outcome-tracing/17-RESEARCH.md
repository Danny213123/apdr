# Phase 17: LLM Fallback Stability and Outcome Tracing - Research

**Researched:** 2026-03-30
**Domain:** Hardening APDR's `llm` validation path after env failure and making per-case fallback outcomes inspectable in saved artifacts
**Confidence:** High

## Summary

Phase 17 does not need broad new recovery ideas. The repo already has the right seam for this work. `tools/apdr/src/docker/builder/agent_backend.rs` owns the env-first plus LangGraph fallback path for `--validation-backend llm`, `tools/apdr/docker_agent/__main__.py` already emits structured JSON even when the graph fails, and `tools/apdr/src/lib.rs` plus `tools/apdr/test_executor.py` already define the metadata surfaces that benchmark operators inspect. The real gap is that the current Rust side discards most non-pass agent outcomes and falls back to the original env result, which makes the live run look like an unlabeled env-only failure even when the agent was invoked.

The live March 30, 2026 benchmark evidence points to one concrete production failure. `runs/20260330-020943-apdr/benchmark-context.log` records repeated post-env fallback crashes with `ValueError: 'confidence' is already being used as a state key`. The current `docker_agent` graph defines a node named `confidence` in `tools/apdr/docker_agent/graph.py`, while `tools/apdr/docker_agent/state.py` also defines a `confidence` state field. The exact LangGraph internals are not shown in-repo, so the collision mechanism is an inference from current code plus the runtime error text, but it is a strong one and should be treated as the leading fix target for `AGT-07`.

Phase 17 should therefore be planned as three bounded moves. First, remove or neutralize the post-env LangGraph crash path and make agent failure return structured summary data instead of `None`. Second, thread agent invocation and terminal outcome metadata through APDR's saved artifacts without broadening into Phase 18's backend-routing work. Third, prove the change on a fixed March 30 live-derived slice with case-level artifacts and focused regression tests, not a full moving benchmark rerun.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| AGT-07 | Benchmark operator can run APDR with `--validation-backend llm` on tier3 cases without the LangGraph fallback crashing after env validation fails | The `llm` path already routes through `agent_backend.rs` after env failure, and the live benchmark log already exposes the exact crash to reproduce and guard against. |
| AGT-08 | Benchmark operator can inspect per-case artifacts to see whether the LLM fallback was invoked, passed, abstained, or failed | `ValidationSummary`, `ValidationAttempt`, output YAML serialization, and benchmark result readers already exist; Phase 17 mainly needs to preserve agent outcomes instead of discarding them. |

## Evidence That Should Drive Planning

### The current `llm` path is already env-first and centrally owned

`tools/apdr/src/docker/builder/mod.rs` routes `VALIDATION_BACKEND_LLM` into `validate_requirements_llm`, and `tools/apdr/src/docker/builder/agent_backend.rs` performs env validation first, then invokes the LangGraph agent only after env failure. That means Phase 17 can stay narrow. It does not need to redesign all validation routing. It only needs to harden and expose the existing post-env fallback behavior.

### Non-pass agent outcomes are currently discarded

`attempt_langgraph_agent` returns `Option<ValidationSummary>`, but `parse_agent_result` only returns `Some` when the JSON result says `status == "passed"`. Any agent abstain, failure, or crash path turns into `None`, and `validate_requirements_llm` then prints `LangGraph agent unavailable or failed, returning env result`. This is the main truth-loss seam for `AGT-08`.

### The Python agent already emits useful failure structure

`tools/apdr/docker_agent/__main__.py` catches graph exceptions, converts them into a failed final state, and prints JSON with `status`, `confidence`, `confidence_reason`, `attempts`, and total duration. The Rust side therefore does not need a separate debugging channel to know the agent failed. It needs to stop throwing that structure away.

### Likely crash mechanism: graph node name collides with state key

`tools/apdr/docker_agent/graph.py` adds a node named `confidence` and sets it as the entry point, while `tools/apdr/docker_agent/state.py` also defines `confidence` and `confidence_reason` as state fields. The live error says `'confidence' is already being used as a state key`. That strongly suggests a LangGraph namespace collision between the node name and the state schema. This is an inference from the repo code plus the runtime error string, but it is the most plausible explanation currently available.

### Artifact truth can build on existing structs rather than new formats

`tools/apdr/src/lib.rs` already gives `ValidationSummary` fields for `status`, `reason`, `failure_bucket`, `validation_backend`, `escalated_backend`, `attempts`, `llm_trace_dir`, and `agent_invocations`. `tools/apdr/test_executor.py` already writes `validation_backend`, `validation_status`, `validation_reason`, `llm_trace_dir`, and related metadata into per-case output files. Phase 17 should use those existing surfaces rather than inventing a second artifact format.

### Benchmark UI result classification is a downstream integration point

`benchmark_ui/runner.py` and `benchmark_ui/service.py` derive pass/skip/fail labels from `validation_status`, `validation_reason`, `succeeded`, and host-skip rules. Phase 17 does not need to solve the full resumed-run accounting problem, but it should ensure new fallback statuses and reasons do not get flattened away before operators can inspect them.

### Phase boundary discipline matters here

The repo already maps Docker escalation and actual backend path truth to Phase 18, and broader failure-bucket plus host-runtime accounting cleanup to Phase 19. Phase 17 planning should not smuggle those in. The deliverable here is "the existing `llm` fallback survives and tells the truth," not "all validation routing is now perfect."

## Implementation Recommendations

### 1. Make LangGraph fallback return structured non-pass outcomes instead of disappearing

Recommended files:

- `tools/apdr/docker_agent/graph.py`
- `tools/apdr/docker_agent/state.py`
- `tools/apdr/docker_agent/__main__.py`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/mod.rs`

Recommended responsibilities:

- remove or avoid the `confidence` name collision that is likely causing the live crash
- preserve structured agent failure and abstain results on the Rust side instead of returning `None`
- make the combined `llm` validation summary retain env attempts plus the terminal agent outcome
- add targeted Rust unit coverage for agent-result parsing and `llm` fallback summary shaping

### 2. Thread fallback invocation and terminal outcome into saved artifacts

Recommended files:

- `tools/apdr/src/lib.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`

Recommended responsibilities:

- record whether fallback was invoked and how it ended using the locked Phase 17 vocabulary: `passed`, `abstained`, `failed`
- keep crash and unavailability detail as reason text or substatus under `failed`
- ensure final per-case artifacts show the real validation result plus fallback outcome instead of collapsing to an unlabeled env-only failure
- keep phase scope narrow by avoiding Phase 18's broader `env` versus `docker` versus `llm-agent` routing overhaul

### 3. Add focused proof and regression coverage on a fixed live-derived slice

Recommended files:

- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-VALIDATION.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md`
- `tools/apdr/src/docker/builder/mod.rs`
- `benchmark_ui/test_runner_events.py`
- `benchmark_ui/test_run_contract.py`

Recommended responsibilities:

- define one fixed March 30, 2026 live-derived slice for Phase 17 proof
- add unit tests for the crash fix and agent-result parsing
- add benchmark-side tests or focused assertions that per-case artifacts still expose fallback outcomes
- publish a small reviewer-facing proof note showing before versus after case artifacts for `passed`, `abstained`, and `failed`

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml parse_agent_result`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline`
- `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract`

### Artifact checks

- `rg -n 'confidence|confidence_reason|status|attempts' tools/apdr/docker_agent/__main__.py tools/apdr/docker_agent/graph.py tools/apdr/docker_agent/state.py`
- `rg -n 'validation_status|validation_reason|validation_backend|llm_trace_dir|agent_invocations' tools/apdr/src/lib.rs tools/apdr/test_executor.py`
- `rg -n '_result_validation_status|_result_validation_reason|_result_succeeded|_result_skipped' benchmark_ui/runner.py benchmark_ui/service.py`

### Phase-close checks

- replay one fixed March 30 live-derived crash case in `--validation-backend llm` mode and confirm the fallback no longer terminates with the `confidence` state-key crash
- inspect the resulting per-case `output_data_*.yml` and `.apdr-debug/attempts/*/metadata.txt` artifacts to confirm fallback invocation plus terminal outcome are preserved
- verify representative `passed`, `abstained`, and `failed` fallback cases still surface distinct artifact outcomes after benchmark result ingestion

## Canonical Files For Planning

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md`
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md`
- `runs/20260330-020943-apdr/benchmark-context.log`
- `runs/20260330-020943-apdr/summary.json`
- `runs/20260330-004502-apdr/summary.json`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/docker_agent/graph.py`
- `tools/apdr/docker_agent/state.py`
- `tools/apdr/docker_agent/__main__.py`
- `tools/apdr/src/lib.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`

## Out of Scope For This Phase

- Docker escalation for `environment-build-failed` and `version-not-found` cases in `llm` mode
- broad actual-backend-path truth across `env`, `docker`, and `llm-agent` for every validation attempt
- resumed-run accounting cleanup and host-runtime skip reclassification
- dominant tier3 bucket reduction work beyond stabilizing the existing fallback path
- milestone-wide closeout evidence packaging

## Source Base

No external browsing was required for Phase 17 planning. The source of truth is the repo's existing APDR fallback code, benchmark artifact readers, milestone requirements, and the March 30, 2026 live run artifacts already present in the workspace.

---
*Research created: 2026-03-30*
*Phase: 17-llm-fallback-stability-and-outcome-tracing*
