# Phase 22 Docker Policy Proof

## Slice Contract

Phase 22 closes on `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json`, which freezes a five-case contract for the new `llm` routing policy:

- `contracts/docker-first-default/snippet.py` keeps requested policy `docker-first`, requires a Docker first hop, and requires `Dockerfile`, `docker-build.command.txt`, `docker-run.command.txt`, `build.log`, `run.log`, and `combined.log`.
- `contracts/env-first-control/snippet.py` keeps requested policy `env-first`, requires the `env-first-control` route, and requires `docker-bypass.txt` so the control path cannot be mistaken for an accidental Docker failure.
- `contracts/docker-bypass-fallback/snippet.py` keeps requested policy `docker-first`, requires first hop `env`, and requires `docker_bypass_reason: docker cli unavailable` plus `docker-bypass.txt`.
- `contracts/docker-daemon-unavailable/snippet.py` keeps requested policy `docker-first`, requires first hop `env`, and requires `docker_bypass_reason: docker daemon unavailable` plus `docker-bypass.txt`.
- `contracts/host-runtime-pre-skip/snippet.py` keeps requested policy `docker-first`, requires first hop `env`, and requires `docker_bypass_reason: host-runtime pre-skip` plus `docker-bypass.txt`.

The contract also freezes the top-level metadata fields reviewers should expect from Phase 22 artifacts: `requested_llm_validation_policy`, `llm_validation_route`, `validation_path`, `docker_bypass_reason`, and `docker_bypass_note`.

## Probe Command

```text
python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only
```

This is the deterministic Phase 22 policy gate. It validates the frozen contract without requiring a benchmark replay, because this phase is about locking the routing and degradation promise itself. The later env-first versus docker-first outcome comparison remains Phase 24 work.

## Before/After Review

Before Plan 22-03, non-Docker `llm` cases could finish without an explicit Docker bypass note, and top-level APDR outputs did not record the requested `llm` policy or bypass reason in a machine-readable way.

After Plan 22-04, reviewers should require all of these conditions:

- docker-first default cases record requested policy `docker-first`, keep `llm_validation_route: docker-first`, and leave the standard Docker attempt artifacts in the case debug folder
- env-first control cases record requested policy `env-first`, keep `llm_validation_route: env-first-control`, and leave `docker-bypass.txt`
- Docker-CLI-unavailable fallback cases record requested policy `docker-first`, keep `llm_validation_route: env-first-docker-bypass`, record `docker_bypass_reason: docker cli unavailable`, and leave `docker-bypass.txt`
- installed-but-unusable Docker cases record requested policy `docker-first`, keep `llm_validation_route: env-first-docker-bypass`, record `docker_bypass_reason: docker daemon unavailable`, and leave `docker-bypass.txt`
- host-runtime pre-skip cases record requested policy `docker-first`, keep `llm_validation_route: env-first-host-runtime`, record `docker_bypass_reason: host-runtime pre-skip`, and leave `docker-bypass.txt`

If the checker sees any drift in requested policy, first hop, bypass reason, or required debug-artifact presence, the Phase 22 policy contract has not been met.
