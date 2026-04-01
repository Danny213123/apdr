# Phase 22: Docker-First Policy and Safe Degradation - Context

**Gathered:** 2026-04-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Change `llm` validation to use a docker-first policy on supported hosts, while keeping env-first available as an explicit comparison control and degrading safely back to env validation when Docker cannot be used. This phase changes routing policy and debug-artifact behavior; it does not yet deliver the benchmark UI truth surfaces, the full comparison harness, or the final keep-or-reject recommendation.

</domain>

<decisions>
## Implementation Decisions

### Policy rollout
- **D-01:** Phase 22 should make docker-first the standard `llm` validation policy immediately rather than waiting for a later milestone phase to flip the default.
- **D-02:** Env-first must remain available as an explicit control path for v2.4 comparisons, even though it is no longer the default `llm` route.
- **D-03:** Top-level `validation_backend` should remain `llm`; docker-first is a policy and route change inside `llm` mode, not a new public backend name.

### Safe degradation
- **D-04:** If docker-first `llm` validation is requested or implied but Docker is unavailable, unhealthy, or unsupported on the current host, APDR should fall back to env validation rather than failing or skipping the case.
- **D-05:** That docker-to-env fallback must leave an explicit bypass reason in artifacts and operator-facing diagnostics so reviewers can tell docker-first was requested but not honored.
- **D-06:** Host-runtime and obviously unsuitable cases should still pre-skip before Docker is attempted.

### Eligibility breadth
- **D-07:** In docker-first mode, Docker should be the first validation hop for all `llm` cases except pre-classified host-runtime or otherwise clearly unsuitable cases.
- **D-08:** Phase 22 should not limit docker-first to only the old Phase 18 packaging and build-failure subset, because that would not answer the broader “ditch env first” question.

### Platform rollout
- **D-09:** Docker-first should be runtime-gated anywhere the existing Docker checks pass, including Windows; do not pre-exclude whole platforms up front.

### Debug artifacts
- **D-10:** Every `llm` case should leave Docker-oriented materials in its debug folder.
- **D-11:** When Docker is actually attempted, those debug artifacts should include the generated Dockerfile, docker build command, docker run command, and Docker logs.
- **D-12:** When Docker is bypassed, the debug folder should still include an explicit note or metadata artifact describing why docker-first was skipped.

### the agent's Discretion
- Exact flag, config, or preset shape used to expose docker-first versus env-first `llm` policies, as long as env-first remains explicitly selectable for Phase 24 comparisons.
- Exact artifact filenames and folder layout for Docker debug materials, as long as operators can find Docker inputs and bypass reasons quickly.
- Exact wording of Docker-unavailable warnings in Doctor and run artifacts, as long as the fallback is explicit and machine-readable.

</decisions>

<specifics>
## Specific Ideas

- The user wants docker-first `llm` to become the new normal now, not just a hidden experiment.
- Broad docker-first should still respect the existing host-runtime pre-skip logic rather than forcing Docker onto obviously unsuitable cases.
- The user wants Docker-related debug files present for each `llm` case so case inspection does not depend on reconstructing the route from logs alone.
- The user also wants benchmark UI case rows for `llm` runs to show Docker build participation, but that operator-facing truth surface belongs to Phase 23.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone scope and requirements
- `.planning/ROADMAP.md` — Phase 22 goal, success criteria, and the boundary between policy routing here versus truth surfaces in Phase 23 and comparison proof in Phases 24-25.
- `.planning/REQUIREMENTS.md` — `DFV-01`, `DFV-03`, and `GDR-01`, plus adjacent `DFV-02`, `CMP-*`, and `EVD-10` requirements that remain outside this phase.
- `.planning/PROJECT.md` — v2.4 milestone framing, hard constraints, and the carry-forward evidence discipline from v2.3.

### Prior routing decisions
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md` — prior decision that a global docker-first `llm` policy was deferred out of Phase 17.
- `.planning/phases/18-backend-escalation-and-path-truth/18-CONTEXT.md` — Phase 18 env-first routing decision, actual-path truth contract, and Windows/Docker guardrails that Phase 22 is intentionally revisiting.
- `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md` — requested-mode stability requirement: `validation_backend` stays `llm` while actual route truth is carried separately.

### Evidence baseline
- `.planning/phases/21-live-evidence-and-closeout-pack/21-MILESTONE-CLOSEOUT.md` — v2.3 closeout scope note and fixed-slice evidence caveat that must still constrain Phase 22 claims.

### Codebase architecture
- `.planning/codebase/ARCHITECTURE.md` — validation-layer structure, Docker layer boundaries, and the current multi-agent validation flow.
- `.planning/codebase/CONVENTIONS.md` — naming, module, and error-handling conventions for Rust and Python changes in this phase.
- `.planning/codebase/STACK.md` — platform/runtime expectations for Docker, Python, and benchmark tooling.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tools/apdr/src/docker/builder/agent_backend.rs`: Current `llm` entrypoint, env-first routing seam, Docker-escalation predicate, and natural place to insert docker-first policy branching.
- `tools/apdr/src/docker/builder/docker_backend.rs`: Existing deterministic Docker validator already writes Dockerfile, command, and log artifacts that Phase 22 can reuse for docker-first runs.
- `tools/apdr/src/docker/builder/mod.rs`: Validation facade that preserves requested backend semantics and merges attempt history across backends.
- `benchmark_ui/state.py`: Existing Doctor checks already know how to distinguish pure Docker requirements from optional Docker-in-`llm` guidance.

### Established Patterns
- Requested-mode truth (`validation_backend=llm`, `execution_mode=llm-hybrid`) should remain stable even when actual routing changes underneath.
- Actual route truth belongs in dedicated fields such as `validation_path` and `escalated_backend`, not by mutating the requested backend contract.
- Host-runtime and framework blockers already have explicit skip and failure-family handling that should stay ahead of Docker-first routing.

### Integration Points
- `tools/apdr/src/main.rs` and `tools/apdr/src/lib.rs`: CLI/config normalization and run-contract defaults for exposing docker-first versus env-first `llm` policy.
- `tools/apdr/src/docker/builder/agent_backend.rs`: First-hop policy selection, Docker bypass logic, and env fallback behavior.
- `tools/apdr/src/docker/builder/docker_backend.rs`: Docker-attempt artifact generation and Docker-unavailable handling.
- `benchmark_ui/runner.py`, `benchmark_ui/service.py`, and `benchmark_ui/state.py`: Follow-on integration points for policy truth and Docker-build visibility once Phase 23 starts.

</code_context>

<deferred>
## Deferred Ideas

- Show Docker build participation in benchmark UI case rows for `llm` runs — belongs to Phase 23 policy truth and failure semantics.
- Build the like-for-like env-first versus docker-first evidence harness — belongs to Phase 24.
- Publish the final replace/optional/reject recommendation for docker-first `llm` mode — belongs to Phase 25.

</deferred>

---

*Phase: 22-docker-first-policy-and-safe-degradation*
*Context gathered: 2026-04-01*
