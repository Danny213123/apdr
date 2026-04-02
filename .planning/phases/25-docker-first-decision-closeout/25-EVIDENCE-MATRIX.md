# Phase 25 Evidence Matrix

This matrix compares the three allowed Phase 25 verdicts against the current evidence baseline:

- fixed-slice only evidence from the locked Phase 24 comparison harness
- `pass delta: +2` in favor of docker-first on the five-case slice
- dominant-bucket movement of `module-not-found -1` and `environment-build-failed -1`
- positive `docker_startup_duration_seconds: +61.0` even while total duration improves
- Phase 23 browser UAT still pending (`pending: 2`)

## replace

### supporting evidence

- docker-first improves the fixed slice from `1` pass to `3` passes, for a `pass delta` of `+2`
- docker-first reduces `module-not-found` and `environment-build-failed` on the same slice
- docker-first lowers total duration despite the additional Docker startup cost
- Phase 22 already proved the route is safe on supported hosts and degrades cleanly when Docker is unavailable or unusable

### blocking evidence

- the current evidence is fixed-slice only, not a full-corpus paired replay
- Phase 23 browser-visible `Validation truth` verification is still open
- `docker_startup_duration_seconds` is a real positive cost that still needs honest treatment in the final recommendation
- no stronger live paired replay artifact currently proves that the fixed-slice win generalizes

### current fit

Weak fit. `replace` overstates what the repo currently knows unless execution adds stronger live paired replay evidence and/or clears the remaining Phase 23 human verification debt.

## optional

### supporting evidence

- docker-first has a positive `pass delta` and positive bucket movement on the locked slice
- the total runtime delta is favorable even after paying `docker_startup_duration_seconds`
- Phase 22 guarantees the policy remains safe through env-first control and clear Docker bypass behavior
- `optional` matches the current evidence boundary because it allows docker-first to remain available without claiming universal superiority

### blocking evidence

- `optional` is more conservative than the current fixed-slice performance win, so it may under-claim if future live paired replay confirms the same pattern
- the open Phase 23 browser UAT still leaves some operator-facing trust work incomplete

### current fit

Best fit. `optional` matches the current evidence-favored posture: docker-first looks promising and defensible, but the evidence is still fixed-slice scoped and Phase 23 verification debt remains visible.

## reject

### supporting evidence

- docker-first introduces a real Docker startup cost
- docker-first depends on supported Docker availability, even though Phase 22 made the fallback path safe

### blocking evidence

- the current fixed-slice evidence does not show a correctness or total-runtime regression
- the locked sample has a positive `pass delta` and fewer failures under docker-first
- the Phase 22 policy contract already removed the major safety objections by keeping env-first control and explicit fallback behavior

### current fit

Poor fit. `reject` would conflict with the current machine-checked comparison evidence unless new negative evidence appears during execution.

## Current Recommendation

The current evidence points to `optional` as the default verdict:

- keep docker-first available and documented as the evidence-favored option on the fixed slice
- do not claim it should fully replace env-first yet
- carry the fixed-slice boundary and Phase 23 pending browser UAT into the final closeout artifacts
