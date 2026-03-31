# Phase 18: Backend Escalation and Path Truth - Context

**Gathered:** 2026-03-30
**Status:** Ready for planning

<domain>
## Phase Boundary

Route eligible `llm`-mode validation failures through Docker and make the actual validation path explicit in saved artifacts without regressing Windows or Docker correctness. This phase clarifies routing and backend-path truth; it does not broaden failure taxonomy, claim benchmark recovery gains, or require a full live benchmark closeout.

</domain>

<decisions>
## Implementation Decisions

### Routing Order
- **D-01:** Phase 18 keeps `llm` mode env-first and changes the eligible recovery order to `env -> docker -> llm-agent`.
- **D-02:** Docker escalation in Phase 18 is a targeted recovery step inside `llm` mode, not a global Docker-first policy for every `llm` case and not a Docker-only replacement for the repaired agent path.

### Escalation Eligibility
- **D-03:** Docker escalation should be targeted and signal-based rather than applied to all env failures.
- **D-04:** The initial eligible signals are backend or packaging failures: missing local interpreter, build timeout, system-library or build failures, and `version-not-found` style failures that are likely recoverable in Docker.
- **D-05:** Phase 18 should avoid escalating obvious non-backend failures such as general framework or host-runtime problems unless the saved signal clearly indicates a backend-specific recovery path.

### Path Truth Contract
- **D-06:** Top-level `validation_backend` must keep the requested run mode (`llm`) so Phase 13 run-contract comparisons and `llm-hybrid` execution-mode reporting stay stable.
- **D-07:** Actual route truth must be surfaced separately from the requested run mode, at minimum through per-attempt actual backend values and a top-level path or escalation field such as `escalated_backend`, `validation_path`, or an equivalent explicit summary field.
- **D-08:** Reviewer-facing artifacts must let operators tell whether a case stayed env-only, escalated to Docker, or reached the agent path without reconstructing that path from raw logs.

### Proof Depth
- **D-09:** Phase 18 proof should combine deterministic tests with a small fixed replay slice rather than relying on tests alone or requiring a full live benchmark rerun.
- **D-10:** The replay slice should come from the March 30, 2026 live baseline and focus on cases that actually exercise env-to-Docker routing and backend-path labeling.

### Compatibility Guardrails
- **D-11:** Windows and Docker correctness remain non-negotiable guardrails for this phase, so routing changes must preserve supported paths rather than silently degrading back to env-only behavior.

### the agent's Discretion
- The exact top-level field names for actual backend-path truth, as long as requested mode and actual route remain clearly separated.
- The exact helper or facade boundaries used to insert Docker escalation into the current `llm` path, as long as reviewers can still reason from `docker::builder::validate_requirements(...)`.
- The exact replay-slice membership, as long as it is fixed, March 30-derived, and demonstrates env-to-Docker routing plus truthful artifact labeling.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone Scope And Phase Boundary
- `.planning/ROADMAP.md` — Phase 18 goal, success criteria, and the explicit boundary between routing truth here and taxonomy or recovery-gain work in Phases 19-21.
- `.planning/REQUIREMENTS.md` — `VAL-01`, `VAL-02`, and `WIN-02`, plus adjacent `VAL-04`, `EVD-07`, and `EVD-09` requirements that stay deferred out of this phase.
- `.planning/PROJECT.md` — v2.3 milestone goals, hard constraints, and the carry-forward decision that Phase 17 kept `llm` env-first on purpose.

### Prior Phase Decisions
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md` — the Phase 17 routing boundary, fallback metadata contract, and explicit deferral of Docker escalation to Phase 18.
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-VERIFICATION.md` — what Phase 17 already proved, so Phase 18 can build on fallback stability without redoing that scope.
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md` — fixed-slice proof discipline that Phase 18 should mirror when it adds routing evidence.

### Run Contract And Evidence Discipline
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md` — comparison-critical run-contract fields, especially `validation_backend`, `execution_mode`, and evidence-label expectations.
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md` — prior fixed-slice proof pattern for agent or routing changes without requiring a full benchmark closeout.

### Architecture And Integration Map
- `.planning/codebase/ARCHITECTURE.md` — current validation, Docker, and multi-agent orchestration boundaries.
- `.planning/codebase/TESTING.md` — established Rust and Python verification patterns for focused regression coverage.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tools/apdr/src/docker/builder/mod.rs`: current validation facade, backend selector, and the natural place to insert or coordinate `env -> docker -> llm-agent` routing for `llm` mode.
- `tools/apdr/src/docker/builder/docker_backend.rs`: existing Docker validator plus env-to-Docker retry heuristics that can be narrowed into the targeted eligibility contract for Phase 18.
- `tools/apdr/src/lib.rs`: `ValidationSummary` and `ValidationAttempt` already have `validation_backend`, `escalated_backend`, `build_image_id`, and per-attempt telemetry surfaces that can carry actual path truth without inventing a separate artifact channel.
- `tools/apdr/src/docker/builder/env_backend.rs`: `attempt_metadata(...)` already emits per-attempt backend/status details and is the existing artifact format for builder attempts.
- `tools/apdr/test_executor.py`: wrapper output serialization already copies top-level validation fields into saved case artifacts and can extend that truth to actual backend-path fields.
- `benchmark_ui/service.py`, `benchmark_ui/runner.py`, and `benchmark_ui/state.py`: current UI and Doctor surfaces already render requested validation mode and can be taught to distinguish requested mode from actual routed backend path.

### Established Patterns
- Phase 13 treats `validation_backend` and `execution_mode` as comparison-critical run-contract fields, so Phase 18 should preserve requested-mode semantics instead of overloading those fields with route truth.
- Phase 17 established that tier3 artifact truth belongs in explicit fields, not only in logs or inferred status transitions.
- The Docker backend already records attempt-local backend values, so Phase 18 can build backend-path truth around attempt history instead of inventing a second attempt ledger.
- Fixed-slice proof with deterministic checkers is the preferred evidence style for routing or agent-path changes before later milestone closeout phases.

### Integration Points
- `tools/apdr/src/docker/builder/agent_backend.rs`: current `llm` validation path is `env -> llm-agent`; this file likely needs to cooperate with or yield to Docker escalation before the final agent attempt.
- `tools/apdr/src/docker/builder/docker_backend.rs`: `should_retry_failed_env_validation_in_docker(...)` and `env_failure_reason_for_docker_retry(...)` are the starting point for targeted eligibility logic.
- `tools/apdr/src/resolver/retry_loop.rs` and `tools/apdr/src/lib.rs`: final summary shaping, top-level backend fields, and serialization into benchmark artifacts.
- `benchmark_ui/service.py`: run summary labels currently describe `llm` as "env + agent fallback", so the operator-facing wording must become truthful once Docker becomes part of the routed `llm` path.
- `benchmark_ui/state.py`: Doctor/runtime checks currently treat `docker` as required only for pure Docker mode, so Phase 18 may need to revisit what "optional" versus "required" means for `llm` mode with targeted Docker escalation.

</code_context>

<specifics>
## Specific Ideas

- Keep requested mode stable for comparisons: `validation_backend=llm` and `execution_mode=llm-hybrid` should remain the operator-selected mode even when a specific case escalates to Docker.
- The actual route should be obvious at a glance in both saved case artifacts and attempt metadata, especially for cases that go `env -> docker` and never reach the agent.
- The small replay slice should favor March 30 cases whose current artifacts show env-only attempt metadata and empty Docker image IDs today, because those are the clearest before/after routing proofs.
- Eligibility should be strict enough that Phase 18 does not accidentally swallow the failure-taxonomy work already reserved for Phase 19.
- If a case reaches both Docker and the agent path, the artifact trail should preserve the whole route rather than only the last backend touched.

</specifics>

<deferred>
## Deferred Ideas

- A global Docker-first policy for all `llm` cases stays out of scope for this phase; if explored later, it should be treated as a separate routing experiment rather than the default Phase 18 policy.
- Framework or host-runtime failure reclassification remains Phase 19 work.
- Benchmark-yield improvement claims on dominant buckets remain Phase 20 work.
- Full live benchmark before/after proof packaging remains Phase 21 work.

</deferred>

---

*Phase: 18-backend-escalation-and-path-truth*
*Context gathered: 2026-03-30*
