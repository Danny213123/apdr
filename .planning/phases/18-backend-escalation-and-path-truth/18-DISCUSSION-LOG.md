# Phase 18: Backend Escalation and Path Truth - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-30
**Phase:** 18-backend-escalation-and-path-truth
**Areas discussed:** Escalation order, Eligibility policy, Path truth contract, Proof depth

---

## Escalation order

| Option | Description | Selected |
|--------|-------------|----------|
| env -> docker -> llm-agent | Keep `llm` env-first, give deterministic Docker a recovery chance before the final agent path, and preserve the repaired agent fallback for unresolved cases | ✓ |
| env -> llm-agent -> docker | Keep the current fallback order longer and only use Docker later | |
| env -> docker only | Use deterministic Docker recovery but remove the repaired agent path from `llm` mode | |

**User's choice:** `env -> docker -> llm-agent`
**Notes:** This keeps the Phase 17 env-first boundary intact while making Docker a real in-path recovery step for Phase 18 instead of a separate mode.

---

## Eligibility policy

| Option | Description | Selected |
|--------|-------------|----------|
| Targeted signal-based escalation | Escalate only backend or packaging failures such as missing interpreter, build timeout, system-dep/build failures, and `version-not-found` style failures | ✓ |
| Broad build-ish escalation | Escalate any env `build-failed` or `runtime-failed` attempt | |
| Escalate all env failures | Retry every failed env validation in Docker regardless of signal quality | |

**User's choice:** `Targeted signal-based escalation`
**Notes:** Phase 18 should stay narrow and avoid obscuring the classification work planned for Phase 19.

---

## Path truth contract

| Option | Description | Selected |
|--------|-------------|----------|
| Keep requested mode at top level, add actual path separately | Preserve `validation_backend=llm` for run-contract comparability while adding explicit actual-route truth in top-level summary fields and per-attempt metadata | ✓ |
| Make top-level backend equal the final real backend | Replace the configured mode with the last backend touched | |
| Attempt-level truth only | Keep route truth only inside attempt history and avoid new top-level fields | |

**User's choice:** `Keep requested mode at top level, add actual path separately`
**Notes:** Requested mode and actual path should both be inspectable; Phase 13 run-contract comparability should not be sacrificed for route truth.

---

## Proof depth

| Option | Description | Selected |
|--------|-------------|----------|
| Deterministic tests only | Prove the routing changes entirely through focused tests | |
| Deterministic tests + small fixed replay slice | Pair focused tests with a March 30-derived replay slice that demonstrates real env-to-Docker routing and truthful artifact output | ✓ |
| Full live benchmark replay required | Require a full benchmark rerun before considering Phase 18 complete | |

**User's choice:** `Deterministic tests + small fixed replay slice`
**Notes:** This gives Phase 18 grounded routing evidence without turning it into the milestone closeout phase.

---

## the agent's Discretion

- Exact field names for the new top-level actual-path truth, as long as requested mode and actual path remain clearly separated.
- Exact helper factoring inside the builder modules, as long as the routing remains reviewer-readable.
- Exact membership of the small fixed replay slice, as long as it is March 30-derived and demonstrates env-to-Docker routing.

## Deferred Ideas

- Global Docker-first routing for all `llm` cases
- Failure taxonomy and resumed-run accounting cleanup
- Broad recovery-gain claims on dominant buckets
- Full live benchmark closeout proof
