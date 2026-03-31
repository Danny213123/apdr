# Phase 18: Backend Escalation and Path Truth - Research

**Researched:** 2026-03-31
**Domain:** Routing eligible `llm`-mode validation failures through Docker before final agent fallback while keeping benchmark-visible backend truth honest
**Confidence:** High

## Summary

Phase 18 should be planned around the existing APDR builder seam, not as a new validation stack. `tools/apdr/src/docker/builder/mod.rs` already owns the top-level backend switch, the plain env flow already knows how to retry through Docker when the failure looks recoverable, and the `llm` flow already has a dedicated handoff point in `tools/apdr/src/docker/builder/agent_backend.rs`. The work is therefore mostly about inserting one missing deterministic stage for eligible `llm` failures and then exposing the actual route truth without breaking the comparison contract that keeps `validation_backend` equal to the user-requested mode.

The main design constraint is that current Docker code cannot simply be reused unchanged in the `llm` path. `tools/apdr/src/docker/builder/docker_backend.rs` already conditionally calls the LangGraph agent first when `allow_llm` is true, which would violate the Phase 18 decision that `llm` routing must be `env -> docker -> llm-agent`. Phase 18 therefore needs either a deterministic Docker entrypoint or a narrow flag that forces the Docker stage to stay Docker-only when it is being used as the middle escalation hop.

The repo already stores more truth than the benchmark surface shows. Individual `ValidationAttempt` records carry an actual `validation_backend`, and `ValidationSummary` already has `escalated_backend`, but top-level artifacts still read mostly as the configured mode rather than the route that actually happened. That makes this phase a truth-threading problem more than a data-model invention problem.

Phase 18 should therefore be planned as three waves. First, land targeted Docker escalation for eligible `llm` failures with deterministic ordering and Rust coverage. Second, preserve the requested top-level backend while adding explicit path truth through summary serialization and benchmark ingestion. Third, tighten operator messaging and freeze a fixed March 30 live-derived proof slice so the new routing can be checked without relying on a moving full-run benchmark.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| VAL-01 | Eligible `llm`-mode tier3 failures escalate through Docker before final failure | The env path already has recoverable-failure heuristics and Docker validation machinery; the `llm` path is missing only the Docker middle stage and deterministic ordering. |
| VAL-02 | Artifacts and benchmark readers surface the actual backend route without losing the requested mode | `ValidationAttempt.validation_backend` and `ValidationSummary.escalated_backend` already exist, so the missing piece is a top-level path field and consistent reader serialization. |
| WIN-02 | Routing changes preserve Windows and Docker correctness | The benchmark UI and runtime checks already distinguish Docker availability and Windows constraints; Phase 18 must update those surfaces truthfully rather than removing them. |

## Evidence That Should Drive Planning

### The builder facade is the right seam for routing changes

`tools/apdr/src/docker/builder/mod.rs` owns the top-level backend constants and dispatch. That makes it the safest place to lock the new route contract in tests and to keep pure `docker` and pure `env` semantics stable while `llm` grows a Docker middle stage.

### The current `llm` flow skips Docker entirely

`tools/apdr/src/docker/builder/agent_backend.rs` currently performs env validation and then, on failure, moves straight to agent fallback. That matches the March 30 baseline behavior but leaves a large packaging-style recovery opportunity unused for `environment-build-failed` and `version-not-found` cases.

### Current Docker backend behavior is too broad for the middle-stage contract

`tools/apdr/src/docker/builder/docker_backend.rs` can already attempt LangGraph work when `allow_llm` is enabled. That behavior is fine for older call sites, but it is the wrong shape for a deterministic middle hop inside `llm` mode. Phase 18 needs a Docker-only stage when invoked as escalation from the `llm` route.

### Eligibility should be signal-based, not only final-bucket-based

The existing Docker retry helpers in `docker_backend.rs` inspect lower-level env failure signals such as interpreter absence, build timeout, and system dependency errors. Phase 18 should keep that style because final bucket labels alone are too coarse and arrive after some of the detail that makes Docker escalation safe. The locked product decision is targeted escalation, not blanket retry.

### Actual backend truth already exists at the attempt level

`tools/apdr/src/docker/builder/env_backend.rs` creates `ValidationAttempt` values whose `validation_backend` field already records the real backend for each attempt. That means Phase 18 does not need to redesign attempt storage. It needs to derive and serialize a top-level path summary such as `env->docker` or `env->docker->llm-agent`.

### Phase 13 contract prevents overwriting the configured backend

The measurement contract from Phase 13 keeps top-level `validation_backend` meaningful for run-level comparison and dashboard summaries. Phase 18 must therefore expose path truth separately instead of mutating the configured mode into the last backend that happened to run.

### Benchmark operator messaging needs truthful wording, not a new UI model

`benchmark_ui/service.py`, `benchmark_ui/state.py`, and `tools/apdr/test_executor.py` already provide the operator-facing labels and doctor/runtime checks. The necessary change is to describe APDR `llm` mode as local env validation plus targeted Docker escalation plus agent fallback, and to warn clearly when Docker is unavailable for eligible cases.

## Implementation Recommendations

### 1. Insert a deterministic Docker middle stage into `llm` validation

Recommended files:

- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`

Recommended responsibilities:

- create or expose a Docker-only validation entrypoint that never invokes the LangGraph agent internally
- add a narrow helper that decides when an env failure from `llm` mode is eligible for Docker escalation
- preserve the required route ordering: `env -> docker -> llm-agent`
- add Rust tests that lock both eligibility and route order

### 2. Thread path truth through serialized artifacts and readers

Recommended files:

- `tools/apdr/src/lib.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`

Recommended responsibilities:

- keep top-level `validation_backend` equal to the requested backend
- add a dedicated `validation_path` summary field derived from actual attempts and escalations
- serialize `validation_path` and `escalated_backend` into output YAML/JSON and benchmark event streams
- update result reader tests so route truth survives the Rust-to-Python boundary

### 3. Add runtime guardrails and a fixed replay proof slice

Recommended files:

- `benchmark_ui/state.py`
- `benchmark_ui/test_state_backend_doctor.py`
- `scripts/check_phase18_backend_path.py`
- `.planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json`
- `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md`

Recommended responsibilities:

- warn operators when `llm` mode is selected but targeted Docker escalation cannot run on the current machine
- keep pure Docker mode failure semantics intact
- freeze a small March 30 live-derived slice with packaging-style failures that should exercise `env -> docker`
- add a deterministic checker that validates routing/path artifacts without requiring a fresh full benchmark

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase18_backend_`
- `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_run_contract`
- `python3 -m unittest benchmark_ui.test_state_backend_doctor`

### Artifact checks

- `rg -n 'validate_requirements_docker|allow_llm|phase18_backend_' tools/apdr/src/docker/builder/*.rs`
- `rg -n 'validation_path|escalated_backend|validation_backend' tools/apdr/src/lib.rs tools/apdr/test_executor.py benchmark_ui/*.py`
- `rg -n 'llm resolver|docker|agent fallback|doctor' benchmark_ui/service.py benchmark_ui/state.py`

### Phase-close checks

- run the fixed Phase 18 checker in probe mode and confirm the selected slice produces route histories that include `env->docker` for eligible cases
- inspect representative case artifacts and metadata to confirm top-level `validation_backend` still matches the requested run mode while `validation_path` reflects the actual route
- verify pure Docker mode still reports required Docker availability and Windows/runtime correctness without silently downgrading to env-only behavior

## Canonical Files For Planning

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-CONTEXT.md`
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md`
- `.planning/codebase/ARCHITECTURE.md`
- `.planning/codebase/TESTING.md`
- `.planning/codebase/CONVENTIONS.md`
- `runs/20260330-020943-apdr/summary.json`
- `runs/20260330-020943-apdr/benchmark-context.log`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/src/docker/builder/env_backend.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/state.py`

## Out of Scope For This Phase

- repairing host-runtime versus dependency failure accounting across all result classes
- milestone-wide before-versus-after recovery gains on the dominant buckets
- broader import-to-package mapping improvements
- replaying or re-baselining the entire March 30 run
- redesigning benchmark UI structure beyond truthful backend/path messaging

## Source Base

No external browsing was required for Phase 18 planning. The source of truth is the repo's current APDR routing code, benchmark UI reader code, milestone requirements, and the March 30, 2026 live run artifacts already present in the workspace.

---
*Research created: 2026-03-31*
*Phase: 18-backend-escalation-and-path-truth*
