---
phase: 22-docker-first-policy-and-safe-degradation
verified: 2026-04-02T02:20:01Z
status: passed
score: 3/3 must-haves verified
re_verification: true
---

# Phase 22: Docker-First Policy and Safe Degradation Verification Report

**Phase Goal:** Benchmark operators can explicitly run docker-first `llm` validation on supported hosts without losing the existing env-first control path or breaking unsupported environments.
**Verified:** 2026-04-02T02:20:01Z
**Status:** passed
**Re-verification:** Yes - after Plan 22-04 gap closure

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Operators can request a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts. | ✓ VERIFIED | `ResolveConfig` keeps `llm_validation_policy` separate from `validation_backend`, the CLI accepts `--llm-validation-policy`, the web UI exposes the selector, and APDR still routes eligible `llm` cases to Docker first. |
| 2 | Operators can still run the existing env-first `llm` policy as a control path for comparison. | ✓ VERIFIED | The UI exposes `env-first`, the service and runner preserve it independently from backend selection, and Rust route tests still lock `EnvFirstControl`. |
| 3 | When Docker is unavailable, unsupported, or bypassed for a case, APDR degrades clearly instead of silently breaking `llm` validation. | ✓ VERIFIED | `probe_docker_validation_availability()` now checks both CLI presence and daemon usability, the router falls back to `env-first-docker-bypass` for missing CLI and daemon-unavailable states, exact bypass reasons are persisted in summaries plus `docker-bypass.txt`, and the fixed proof slice freezes both cases. |

**Score:** 3/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/src/lib.rs` | Separate `llm_validation_policy` plus persisted route and bypass metadata | ✓ VERIFIED | Phase 22 outputs preserve requested policy, route, and exact Docker bypass reason in top-level reporting. |
| `tools/apdr/src/main.rs` | CLI surface for policy selection | ✓ VERIFIED | Parses `--llm-validation-policy` without widening `validation_backend=llm`. |
| `tools/apdr/src/docker/builder/process.rs` | Concrete Docker usability probe | ✓ VERIFIED | Defines `DockerValidationAvailability`, `DockerUnavailabilityReason`, and probes `docker info --format {{.ServerVersion}}` with timeout. |
| `tools/apdr/src/docker/builder/agent_backend.rs` | Docker-first routing, env-first control, host-runtime pre-skip, and unusable-Docker bypass handling | ✓ VERIFIED | Routes daemon-unavailable and CLI-missing Docker through `EnvFirstDockerBypass(...)` with exact reason labels and persisted bypass notes. |
| `tools/apdr/src/docker/builder/mod.rs` | Regression coverage for daemon-unavailable fallback | ✓ VERIFIED | `phase22_policy_` coverage now includes daemon-unavailable route selection and bypass-note assertions. |
| `tools/apdr/src/docker/builder/docker_backend.rs` | Docker-attempt artifact layout | ✓ VERIFIED | Docker attempts still write `Dockerfile`, command files, and logs under the attempt debug area. |
| `benchmark_ui/run_contract.py` | Run-contract support for `llm_validation_policy` | ✓ VERIFIED | Normalizes and persists the policy separately from backend selection. |
| `benchmark_ui/runner.py` | Benchmark wrapper pass-through for selected policy | ✓ VERIFIED | Adds `--llm-validation-policy` for APDR `llm` runs and keeps backend naming stable. |
| `benchmark_ui/service.py` | Service normalization and operator-facing policy truth | ✓ VERIFIED | Preserves policy choice in saved run fields without widening backend names. |
| `benchmark_ui/state.py` | Doctor and runtime wording for docker-first safe degradation | ✓ VERIFIED | Backend description and Doctor rows explicitly cover `docker cli unavailable` and `docker daemon unavailable` as env-fallback cases. |
| `benchmark_ui/test_state_backend_doctor.py` | Doctor regression coverage for unusable Docker | ✓ VERIFIED | Locks both missing-CLI and daemon-unavailable warning paths. |
| `web/src/main.js` | Operator control for docker-first vs env-first | ✓ VERIFIED | Shows the selector only for APDR `llm`, defaults to `docker-first`, and preserves the value in the form payload. |
| `scripts/check_phase22_docker_policy.py` | Deterministic proof checker | ✓ VERIFIED | Freezes a five-case contract including missing-CLI and daemon-unavailable env-fallback cases. |
| `22-docker-policy-slice.json` | Stable proof slice | ✓ VERIFIED | Contains five archetypes with exact bypass reasons and required debug artifacts. |
| `22-DOCKER-POLICY-PROOF.md` | Reviewer-facing proof note | ✓ VERIFIED | Describes the full five-case contract and explicitly includes installed-but-unusable Docker. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `web/src/main.js` | `benchmark_ui/service.py` | Request payload carries `llm_validation_policy` separately from `validation_backend` | ✓ WIRED | The UI edits `state.form.llm_validation_policy`; the service normalizes the same field from incoming payloads. |
| `benchmark_ui/runner.py` | `tools/apdr/test_executor.py` | `--llm-validation-policy` CLI pass-through | ✓ WIRED | APDR `llm` case commands append the policy flag, and the Python wrapper forwards it to `apdr resolve`. |
| `tools/apdr/src/main.rs` | `tools/apdr/src/docker/builder/agent_backend.rs` | `ResolveConfig.llm_validation_policy` drives route selection | ✓ WIRED | CLI parsing stores the policy in config, and `llm_validation_route()` branches on it. |
| `tools/apdr/src/docker/builder/process.rs` | `tools/apdr/src/docker/builder/agent_backend.rs` | Docker usability probe informs first-hop route selection | ✓ WIRED | `probe_docker_validation_availability()` now feeds structured availability into `llm_validation_route()`. |
| `tools/apdr/src/docker/builder/agent_backend.rs` | `tools/apdr/src/lib.rs` | Route and exact bypass metadata persist into top-level outputs | ✓ WIRED | `apply_llm_route_metadata()` stamps requested policy, route, and exact bypass reason that `ResolveResult` writes into summary/report surfaces. |
| `benchmark_ui/state.py` | `tools/apdr/src/docker/builder/agent_backend.rs` | Docker-unavailable env degradation contract | ✓ WIRED | Doctor wording now matches the runtime contract: APDR `llm` degrades to env for both missing CLI and unusable Docker daemon states. |
| `scripts/check_phase22_docker_policy.py` | `22-docker-policy-slice.json` | Deterministic degradation contract | ✓ WIRED | The checker and slice agree on the five fixed cases, including installed-but-unusable Docker. |

### Data-Flow Trace

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `web/src/main.js` | `state.form.llm_validation_policy` | LLM policy dropdown -> current config payload -> `BenchmarkService._normalize_run_config()` -> `BenchmarkWorker` -> `test_executor.py` -> `apdr resolve` | Yes | ✓ FLOWING |
| `tools/apdr/src/docker/builder/process.rs` | `DockerValidationAvailability` | Docker CLI presence + `docker info --format {{.ServerVersion}}` probe | Yes | ✓ FLOWING |
| `tools/apdr/src/docker/builder/agent_backend.rs` | `summary.requested_llm_validation_policy`, `summary.llm_validation_route`, `summary.docker_bypass_reason` | `ResolveConfig.llm_validation_policy()` and Docker availability probe | Yes | ✓ FLOWING |
| `benchmark_ui/state.py` | Doctor degradation detail for APDR `llm` | Docker CLI / daemon checks plus APDR backend description | Yes | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Rust policy routing and metadata tests | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | 10 Phase 22 tests passed | ✓ PASS |
| Python run-contract and Doctor regressions | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor` | 14 tests passed | ✓ PASS |
| Web UI build integrity | `npm run build --prefix web` | Vite production build succeeded | ✓ PASS |
| Deterministic Phase 22 proof contract | `python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only` | Exit code `0`; proof status reports `passed: true`, `case_count: 5` | ✓ PASS |

### Cross-Phase Regression Gate

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Phase 21.1 cache-path regressions stay green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cache phase21_1_cache_ -- --nocapture` | 3 tests passed | ✓ PASS |
| Phase 21.1 tracked-footprint guard stays green | `python3 scripts/check_phase21_1_footprint.py --repo-root . --mode tracked --status-json /tmp/phase21_1-tracked-status.json --probe-only` | Exit code `0` | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `DFV-01` | `22-01`, `22-02`, `22-03` | Benchmark operator can run APDR with a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts | ✓ SATISFIED | Separate policy field flows from UI/service/runner to the Rust CLI, and eligible `llm` cases still default to Docker first. |
| `DFV-03` | `22-01`, `22-02` | Benchmark operator can still run the existing env-first `llm` policy as a comparison control | ✓ SATISFIED | UI exposes `env-first`, service preserves it, runner passes it, and Rust tests confirm `EnvFirstControl`. |
| `GDR-01` | `22-02`, `22-03`, `22-04` | When Docker is unavailable, unsupported, or explicitly bypassed, APDR degrades clearly without silently breaking `llm` validation | ✓ SATISFIED | Missing-CLI, daemon-unavailable, and host-runtime bypasses are all explicit in route selection, summary metadata, bypass notes, Doctor messaging, and the fixed proof slice. |

Phase 22 orphaned requirements: none. The phase plans account for all Phase 22 requirement IDs in `.planning/REQUIREMENTS.md` (`DFV-01`, `DFV-03`, `GDR-01`).

### Human Verification Required

None. Automated verification plus the cross-phase regression gate are sufficient for Phase 22 closeout.

### Gaps Summary

No Phase 22 execution gaps remain. The earlier unusable-Docker mismatch is closed: docker-first `llm` now only takes the Docker first hop when Docker is actually usable, and the fixed proof contract locks both missing-CLI and daemon-unavailable fallback behavior.

---

_Verified: 2026-04-02T02:20:01Z_
_Verifier: Codex inline re-verification_
