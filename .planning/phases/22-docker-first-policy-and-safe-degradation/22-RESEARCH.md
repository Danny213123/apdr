# Phase 22: Docker-First Policy and Safe Degradation - Research

**Researched:** 2026-04-01
**Domain:** Moving `llm` validation from env-first targeted escalation to docker-first policy with explicit env-first control and safe fallback behavior
**Confidence:** High

## Summary

Phase 22 should be planned as a policy-and-operator-control phase built on the existing Phase 18 routing seam, not as a new validation backend. `tools/apdr/src/docker/builder/agent_backend.rs` already owns the `llm` route order, `tools/apdr/src/docker/builder/docker_backend.rs` already knows how to produce deterministic Docker artifacts, and benchmark readers already preserve the requested backend contract by keeping `validation_backend=llm` distinct from the actual `validation_path`. The missing work is therefore a narrow policy surface plus a first-hop swap, not a backend rewrite.

The current code makes the policy gap explicit. APDR CLI/config only exposes `validation_backend`, so there is no dedicated way to request `docker-first` versus `env-first` within `llm` mode. The Python wrapper, benchmark runner, service normalization, and web UI all mirror that limitation: operators can choose `llm`, but they cannot choose which validation hop comes first. That means `DFV-01` and `DFV-03` cannot be satisfied by changing Rust routing alone. Phase 22 needs a narrow, explicit `llm` policy field that stays separate from the backend name.

Safe degradation also needs to be planned as an artifact contract, not just as a runtime branch. The deterministic Docker backend already writes `Dockerfile`, build/run command files, and Docker logs when Docker is attempted. Phase 22 should reuse that artifact seam and add an explicit bypass note when Docker is skipped, unavailable, or unsupported, so every `llm` case leaves behind Docker-oriented debug context. Phase 23 can then surface those details in richer UI truth surfaces without Phase 22 having to redesign case rows.

The clean planning shape is three waves. First, add a dedicated `llm` policy surface and switch the default route to docker-first while preserving env-first control. Second, expose that policy through the benchmark operator surfaces and Doctor/runtime copy without widening into per-case UI truth yet. Third, guarantee per-case Docker or bypass artifacts and freeze a deterministic policy proof contract.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| DFV-01 | Benchmark operator can run APDR with a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts | Current `llm` routing already has a dedicated first-hop seam in `agent_backend.rs`; Phase 22 needs an explicit policy control plus a route-order swap. |
| DFV-03 | Benchmark operator can still run the existing env-first `llm` policy as a comparison control while docker-first is being evaluated | No existing config or UI field distinguishes docker-first from env-first inside `llm`; a separate policy field is required. |
| GDR-01 | When Docker is unavailable, unsupported, or explicitly bypassed, APDR degrades clearly without silently breaking `llm` validation | Docker attempt artifacts already exist, and benchmark Doctor/runtime messaging already distinguishes optional versus required Docker; Phase 22 needs explicit bypass reasons and operator wording. |

## Evidence That Should Drive Planning

### The routing seam already exists in `agent_backend.rs`

`tools/apdr/src/docker/builder/agent_backend.rs` currently runs `llm` validation as `env -> targeted docker -> llm-agent`. That means the correct implementation seam is already localized: Phase 22 should branch the first hop inside the existing `llm` route rather than inventing a new backend name or moving policy into unrelated modules.

### There is no dedicated policy surface today

`tools/apdr/src/lib.rs::ResolveConfig`, `tools/apdr/src/main.rs`, and `tools/apdr/test_executor.py` only expose `validation_backend`. The benchmark stack mirrors that in `benchmark_ui/service.py`, `benchmark_ui/runner.py`, and `web/src/main.js`, where operators can select `env`, `docker`, `llm`, or `llm-only`, but not `docker-first` versus `env-first` inside `llm`. That is the central product gap for this phase.

### The benchmark UI already has the right kind of control seam

`web/src/main.js` already drives a validation-backend dropdown, loadout persistence, preview requests, and Doctor payloads from a normalized run-config object. Phase 22 can add a sibling `llm` policy control there without reopening case-row rendering. That keeps operator control in scope while respecting the explicit Phase 23 boundary around detailed per-case truth surfaces.

### Current operator wording is now wrong for the desired policy

`benchmark_ui/state.py::apdr_backend_description(...)` and `benchmark_ui/service.py` still describe APDR `llm` mode as "Local env validation + targeted Docker escalation + agent fallback". That wording matched Phase 18, but it will become misleading once docker-first is the default. Phase 22 must update summary and Doctor copy so operators understand that `llm` now prefers Docker first and falls back to env only when Docker cannot be used or when the policy is explicitly set to env-first.

### Docker attempt artifacts are already good enough to reuse

`tools/apdr/src/docker/builder/docker_backend.rs` already writes a deterministic artifact set including `Dockerfile`, `docker-build.command.txt`, `docker-run.command.txt`, `build.log`, `run.log`, and `combined.log`. Phase 22 should not invent a second Docker artifact format. It should reuse this layout for actual Docker attempts and add a bypass note or metadata artifact for non-Docker paths.

### Requested backend truth must stay separate from policy truth

Phase 18 and Phase 19 established that `validation_backend` remains the requested backend while actual route truth lives elsewhere. Phase 22 should preserve that contract. The new docker-first versus env-first choice is a policy selection inside `llm`, not a new backend string. The safest shape is therefore a dedicated policy field such as `llm_validation_policy`, while leaving `validation_backend=llm` and `execution_mode=llm-hybrid` stable unless a later phase proves those need expansion.

### Host-runtime and obviously unsuitable cases should stay ahead of Docker

The Phase 22 context locks that host-runtime and clearly unsuitable cases should pre-skip before Docker. Planning should therefore preserve the current pre-classification and skip behavior ahead of the new docker-first route. This phase is about moving the first validation hop for eligible `llm` cases, not about forcing Docker onto cases that already have known non-Docker semantics.

## Implementation Recommendations

### 1. Add an explicit `llm` validation policy surface and default it to docker-first

Recommended files:

- `tools/apdr/src/lib.rs`
- `tools/apdr/src/main.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/run_contract.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `web/index.html`
- `web/src/main.js`

Recommended responsibilities:

- add a narrow field such as `llm_validation_policy` with exact normalized values `docker-first` and `env-first`
- default that field to `docker-first` when `validation_backend=llm` and no override is provided
- keep `validation_backend=llm` stable instead of inventing a new backend name
- allow the benchmark UI and loadouts to persist and resend the chosen `llm` policy
- pass the selected policy through the Python wrapper into the Rust CLI

### 2. Change the `llm` route to honor docker-first with safe degradation

Recommended files:

- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `benchmark_ui/state.py`
- `benchmark_ui/service.py`

Recommended responsibilities:

- make docker-first the default `llm` route for eligible cases
- keep env-first as an explicit control path
- preserve host-runtime and clearly unsuitable pre-skips ahead of Docker
- when Docker is unavailable, unhealthy, unsupported, or explicitly bypassed, fall back to env validation and record an explicit bypass reason
- update operator-facing descriptions and Doctor wording to match the new route

### 3. Guarantee per-case Docker debug materials and freeze a deterministic policy proof

Recommended files:

- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/src/lib.rs`
- `scripts/check_phase22_docker_policy.py`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md`

Recommended responsibilities:

- ensure every `llm` case leaves either Docker attempt artifacts or an explicit Docker-bypass note in its debug folder
- preserve machine-readable bypass reasons so Phase 23 can surface them later without scraping logs
- freeze a deterministic proof slice that covers docker-first success, env-first control, docker-unavailable bypass, and host-runtime pre-skip
- add a checker that validates policy selection, first hop, bypass reason, and debug artifact presence without depending on a full benchmark rerun

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor`
- `npm run build --prefix web`

### Artifact checks

- `rg -n 'llm_validation_policy|docker-first|env-first|--llm-validation-policy' tools/apdr/src/lib.rs tools/apdr/src/main.rs tools/apdr/test_executor.py benchmark_ui/*.py web/src/main.js web/index.html`
- `rg -n 'Dockerfile|docker-build.command.txt|docker-run.command.txt|bypass' tools/apdr/src/docker/builder/*.rs`
- `rg -n 'slice-json|status-json|docker-first|env-first' scripts/check_phase22_docker_policy.py .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json`

### Phase-close checks

- inspect a representative docker-first `llm` case and confirm the debug directory contains Docker attempt artifacts without reconstructing the route from raw logs
- inspect a representative Docker-bypassed `llm` case and confirm the debug directory contains an explicit bypass note with a machine-readable reason
- inspect benchmark run config and loadout payloads and confirm `validation_backend` remains `llm` while the selected `llm` policy is carried separately
- run the deterministic Phase 22 policy checker and confirm it passes for docker-first, env-first control, Docker-bypass, and host-runtime pre-skip cases

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-CONTEXT.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DISCUSSION-LOG.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-CONTEXT.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md`
- `.planning/codebase/ARCHITECTURE.md`
- `.planning/codebase/CONVENTIONS.md`
- `tools/apdr/src/lib.rs`
- `tools/apdr/src/main.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/run_contract.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `benchmark_ui/state.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_state_backend_doctor.py`
- `web/index.html`
- `web/src/main.js`

## Out of Scope For This Phase

- adding benchmark UI case-row Docker-build visibility for `llm` runs
- redesigning benchmark case rows or history views
- the like-for-like env-first versus docker-first comparison harness
- the final keep/optional/reject verdict for docker-first `llm`
- broader failure-classification changes outside the docker-first routing and degradation contract

## Source Base

No external browsing was required for Phase 22 planning. The source of truth is the repo's existing Phase 18 routing code, benchmark operator surfaces, Phase 22 context decisions, and the v2.4 milestone requirements already present in the workspace.

---
*Research created: 2026-04-01*
*Phase: 22-docker-first-policy-and-safe-degradation*
