verdict: optional

# Phase 25 Milestone Verdict

## Decision

The current Phase 25 verdict is `optional`.

Docker-first should remain available and evidence-favored for `llm` mode, but it should not yet fully replace env-first as the unqualified closeout recommendation.

## Evidence Used

- Phase 24 fixed-slice comparison proof on `phase24-policy-comparison-fixed-slice-v1`
- `pass delta: +2` in favor of docker-first, moving from `1` pass to `3` passes on the locked five-case slice
- dominant bucket movement of `module-not-found -1` and `environment-build-failed -1`, with `version-not-found 0`
- timing tradeoffs that still favor docker-first overall, even with `docker_startup_duration_seconds: +61.0`
- Phase 22 policy proof showing docker-first remains safe through env-first control and explicit bypass behavior
- Phase 23 human verification state still pending with `2` unresolved browser-visible checks

## Tradeoffs

The current fixed-slice evidence is positive for docker-first on both correctness and total runtime.

The most important positive signals are:

- a positive `pass delta`
- fewer failures on the same locked slice
- lower total duration, validation duration, env creation time, and install time

The main limiting signals are:

- the evidence is still fixed-slice only, not a full-corpus paired replay
- docker-first still pays a positive `docker_startup_duration_seconds` cost
- the browser-visible Phase 23 `Validation truth` UAT is still pending, so the operator-facing inspection story is not fully signed off yet

## Recommendation

Keep docker-first enabled and document it as the current evidence-favored option for supported hosts, but do not claim that it should fully replace env-first yet.

That recommendation is stronger than `reject` because the current fixed-slice evidence is clearly positive, and more conservative than `replace` because the closeout evidence remains fixed-slice scoped and Phase 23 still carries pending human verification.

If a later live paired replay confirms the same pattern and the Phase 23 browser-UAT debt is cleared, the recommendation can be revisited toward `replace`.

## Scope Boundary

This verdict is grounded in fixed-slice evidence, not a full-corpus result.

It is appropriate to say that docker-first looks promising, is safe under the Phase 22 routing contract, and currently performs better on the locked comparison slice. It is not appropriate to say that docker-first has already proven it should replace env-first everywhere in the benchmark.

Phase 23 debt remains part of the closeout truth until the two pending browser checks are resolved.
