# Phase 22: Docker-First Policy and Safe Degradation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-01
**Phase:** 22-docker-first-policy-and-safe-degradation
**Areas discussed:** Rollout mode, Docker unavailable behavior, Eligibility breadth, Platform rollout

---

## Rollout mode

| Option | Description | Selected |
|--------|-------------|----------|
| Opt-in experiment first | Add an explicit docker-first `llm` policy, but keep env-first as the normal default until comparison proof lands | |
| New default now | Make docker-first the standard `llm` mode immediately, keep env-first only as a comparison control | ✓ |
| Hard cutover | Replace env-first `llm` routing entirely in Phase 22 | |

**User's choice:** New default now
**Notes:** Docker-first should become the normal `llm` route in this phase, but env-first must still exist for comparison work later in the milestone.

---

## Docker unavailable behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Fall back to env with explicit bypass reason | If Docker is unavailable or unsupported, run env validation and record why docker-first was skipped | ✓ |
| Fail fast | If docker-first is requested and Docker is unusable, fail the case or run clearly | |
| Skip the case | Mark the case as a docker-unavailable non-pass instead of trying env | |

**User's choice:** Fall back to env with explicit bypass reason
**Notes:** Safe degradation is preferred over hard failure, but the bypass must be visible in artifacts and diagnostics.

---

## Eligibility breadth

| Option | Description | Selected |
|--------|-------------|----------|
| Broad docker-first | Start with Docker for every `llm` case except host-runtime or clearly unsuitable cases | ✓ |
| Narrow docker-first | Use Docker first only for the old Phase 18 recoverable-bucket subset | |
| Mixed heuristic | Use Docker first for packaging-heavy cases and env first for lighter cases | |

**User's choice:** Broad docker-first
**Notes:** The user wants this milestone to answer the real “ditch env first” question, so the old targeted subset is too narrow.

---

## Platform rollout

| Option | Description | Selected |
|--------|-------------|----------|
| Runtime-gated everywhere | Enable docker-first anywhere existing Docker runtime checks pass, including Windows | ✓ |
| macOS/Linux first | Keep Windows on env-first until later proof | |
| macOS-only first | Keep the first experiment as narrow as possible | |

**User's choice:** Runtime-gated everywhere
**Notes:** Do not carve out platforms up front if the existing Docker checks say they are healthy.

---

## Additional requests

- Add Docker-related materials to the debug folder for each `llm` case.
- Update `llm` case visibility in the UI to include Docker builds.

## the agent's Discretion

- Exact CLI/config surface for selecting env-first versus docker-first `llm` policy.
- Exact filenames and metadata shape for Docker debug-folder artifacts.
- Exact warning copy for Docker bypass or unavailability.

## Deferred Ideas

- UI surfacing of Docker build participation for `llm` cases belongs to Phase 23, where policy truth is already in scope.

