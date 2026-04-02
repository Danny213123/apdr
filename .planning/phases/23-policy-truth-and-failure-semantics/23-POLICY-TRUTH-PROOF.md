# Phase 23 Policy Truth Proof

## Scope

Phase 23 closes on `.planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json` plus the deterministic checker in `scripts/check_phase23_policy_truth.py`.

This proof package is intentionally narrow:

- It proves Phase 23 inspectability for requested policy, actual validation path, route label, Docker bypass context, debug pointers, and failure family.
- It proves the locked `environment-specific` versus `dependency-resolution` expectations for the Phase 23 archetypes.
- It is not the Phase 24 comparison harness, and it must not be used as the env-first versus docker-first result.

## Truth Keys Reviewers Should Expect

Saved benchmark rows, live `case_complete` events, and the expanded UI `Validation truth` card should expose the same policy-truth vocabulary for LLM cases:

- `requestedLlmValidationPolicy`
- `validationPath`
- `llmValidationRoute`
- `dockerStatus`
- `dockerBypassReason`
- `failureFamily`
- `debugDir`
- `dockerBypassNote`

These keys let a reviewer answer the Phase 23 question without reopening raw metadata files:

- what requested policy was set
- what validation path actually ran
- whether Docker was attempted, bypassed, or pre-skipped
- why a Docker bypass happened when it did
- which debug location or bypass note explains the case
- whether the final failure family stayed truthful

## Locked Archetypes

The fixed slice freezes six reviewer-facing archetypes:

- `docker-first-attempted-dependency-resolution`: requested policy `docker-first`, route `docker-first`, validation path `docker->llm-agent`, Docker status `attempted`, and failure family `dependency-resolution`
- `env-first-control`: requested policy `env-first`, route `env-first-control`, validation path `env->llm-agent`, explicit control bypass note, and failure family `dependency-resolution`
- `docker-cli-unavailable-bypass`: requested policy `docker-first`, route `env-first-docker-bypass`, validation path `env`, bypass reason `docker cli unavailable`, and failure family `environment-specific`
- `docker-daemon-unavailable-bypass`: requested policy `docker-first`, route `env-first-docker-bypass`, validation path `env`, bypass reason `docker daemon unavailable`, and failure family `environment-specific`
- `host-runtime-pre-skip`: requested policy `docker-first`, route `env-first-host-runtime`, validation path `env`, bypass reason `host-runtime pre-skip`, and failure family `environment-specific`
- `framework-runtime-environment-specific`: requested policy `docker-first`, route `docker-first`, validation path `docker->llm-agent`, Docker status `attempted`, and failure family `environment-specific`

Together these cases lock the exact Phase 23 promise: reviewers can inspect requested policy truth end to end, and runtime blockers still stay visibly `environment-specific` while true package misses remain `dependency-resolution`.

## Failure Semantics

Phase 23 does not reopen routing policy. It freezes how truth should read after the Phase 22 policy is already in place.

Reviewers should expect:

- Docker-unavailable fallback cases to stay `environment-specific`, not to collapse into generic dependency-resolution misses.
- Host-runtime pre-skip cases to stay `environment-specific` because the route itself records that Docker was intentionally bypassed for host-runtime reasons.
- Framework-runtime blockers to stay `environment-specific` even when the requested policy remains `docker-first` and the visible route still reads as a real Docker attempt.
- Real dependency misses under docker-first or env-first control to keep the failure family `dependency-resolution`.

## Probe Command

```text
python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-proof-status.json --probe-only
```

The checker fails if the slice drifts on:

- requested policy
- validation path
- route label
- derived Docker status
- Docker bypass reason
- failure family
- required debug artifacts
- the expected saved/live UI truth keys

## Reviewer Guidance

If the checker passes, Phase 23 has a deterministic contract for policy-truth visibility and failure-family truth. If it fails, reviewers should treat that as contract drift in the inspectability layer, not as evidence about whether docker-first is better or worse overall.
