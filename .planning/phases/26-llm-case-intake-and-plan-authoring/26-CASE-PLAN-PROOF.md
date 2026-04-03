# Phase 26 Case Plan Proof

## What Phase 26 Proves

Phase 26 proves that APDR now has a deterministic authored intake contract for `llm` and `llm-only` before Docker authoring or bounded recovery begins.

The proof package consists of:

- [26-authored-plan-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json)
- [26-intake-failure-sample.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json)
- [26-case-plan-proof-status.json](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/26-llm-case-intake-and-plan-authoring/26-case-plan-proof-status.json)
- [check_phase26_case_plan.py](/Users/dannyguan/Documents/fse-aiware-python-dependencies/scripts/check_phase26_case_plan.py)

## Locked Contract

The authored intake contract requires:

- an explicit `authored_plan_status`
- a successful sample with `imports`, `package_mappings`, `runtime_assumptions`, `system_dependency_hints`, and `smoke_strategy`
- a structured intake-failure sample with `failure_class`, `diagnostic_preview`, and strict `llm-only` behavior
- authorship truth that distinguishes fully LLM-authored sections from deterministic fallback sections

If any of that drifts, the checker fails before later phases can consume the plan for Docker authoring or recovery.

## Authorship Truth

Phase 26 does not pretend the LLM authored every field. When deterministic fallback contributed to the final plan, the contract preserves both:

- `authored_plan_authorship`
- `authored_plan_fallback_sections`

That means the sample can honestly say `llm-authored-with-deterministic-fallback` and still show which section came from deterministic fallback rather than the model itself.

## Deterministic Fallback Boundary

The Phase 26 contract freezes deterministic fallback as metadata, not as a silent rewrite. A successful intake artifact can include a deterministic fallback section such as `tier1-cache`, but it still must retain the authored smoke strategy and the LLM-authored dependency intent that downstream phases will consume.

## Boundary Of This Proof

Phase 26 proves:

- the authored plan schema exists
- `llm-only` fails truthfully when no usable intake plan exists
- deterministic fallback remains visible instead of being collapsed into an opaque plan

Phase 26 does not prove:

- that the LLM-authored Dockerfile is correct
- that Docker image build-to-run handoff is fixed
- that recovery prompts can repair failed installs or runtime logs

Those are deferred to Phase 27 and Phase 28.

## Handoff To Phase 27

Phase 27 should consume this authored plan contract to generate Docker-oriented validation inputs and preserve the actual executed Docker artifacts. It should not widen or rewrite the Phase 26 intake truth; it should build directly on the authored plan and structured intake-failure boundary frozen here.
