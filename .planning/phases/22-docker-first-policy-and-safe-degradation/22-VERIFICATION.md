---
phase: 22-docker-first-policy-and-safe-degradation
verified: 2026-04-02T01:33:44Z
status: gaps_found
score: 2/3 must-haves verified
gaps:
  - truth: "When Docker is unavailable, unsupported, or bypassed for a case, APDR degrades clearly instead of silently breaking `llm` validation."
    status: partial
    reason: "The `llm` router only bypasses Docker when the CLI is missing or the case is pre-classified as host-runtime. If Docker is on PATH but unusable, operator surfaces promise env degradation but runtime still selects the docker-first route."
    artifacts:
      - path: "tools/apdr/src/docker/builder/agent_backend.rs"
        issue: "`llm_validation_route()` only receives a boolean from `command_on_path(\"docker\")`, so daemon-health or broader unsupported-host checks cannot trigger the env-first bypass path."
      - path: "benchmark_ui/state.py"
        issue: "Doctor and runtime copy say APDR `llm` will degrade to env validation until Docker is available, but `validate_tool_runtime()` only enforces Docker daemon availability for pure `docker`, not for `llm`."
      - path: "scripts/check_phase22_docker_policy.py"
        issue: "The deterministic proof contract covers env-first control, missing-Docker bypass, and host-runtime pre-skip, but not installed-but-unusable Docker."
    missing:
      - "Gate docker-first `llm` routing on actual Docker usability, not just PATH presence."
      - "Add regression coverage for daemon-unavailable or otherwise unusable Docker in docker-first `llm` mode."
      - "Extend the Phase 22 proof contract so the unsupported/unhealthy Docker case is frozen and checkable."
---

# Phase 22: Docker-First Policy and Safe Degradation Verification Report

**Phase Goal:** Benchmark operators can explicitly run docker-first `llm` validation on supported hosts without losing the existing env-first control path or breaking unsupported environments.
**Verified:** 2026-04-02T01:33:44Z
**Status:** gaps_found
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Operators can request a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts. | ✓ VERIFIED | `ResolveConfig` keeps a separate `llm_validation_policy` defaulted to `docker-first`, the CLI accepts `--llm-validation-policy`, the web UI exposes the selector, and the benchmark runner passes the flag through to the Rust CLI. Rust policy tests and Python/web validation all pass. |
| 2 | Operators can still run the existing env-first `llm` policy as a control path for comparison. | ✓ VERIFIED | The UI exposes `env-first`, the service and runner preserve it separately from `validation_backend`, the Rust router returns `EnvFirstControl`, and targeted tests lock that path. |
| 3 | When Docker is unavailable, unsupported, or bypassed for a case, APDR degrades clearly instead of silently breaking `llm` validation. | ✗ FAILED | Host-runtime and missing-CLI bypasses are implemented, but the runtime route only checks Docker PATH presence. Doctor copy promises env degradation for broader Docker-unavailable states, yet `llm` runtime validation does not enforce Docker daemon usability before taking the docker-first route. |

**Score:** 2/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `tools/apdr/src/lib.rs` | Separate `llm_validation_policy` plus persisted route/bypass metadata | ✓ VERIFIED | Defines `docker-first` and `env-first`, defaults the policy to `docker-first`, and emits requested-policy / route / bypass fields in text and summary outputs. |
| `tools/apdr/src/main.rs` | CLI surface for policy selection | ✓ VERIFIED | Parses `--llm-validation-policy` without changing `validation_backend=llm`. |
| `tools/apdr/src/docker/builder/agent_backend.rs` | Docker-first routing, env-first control, host-runtime pre-skip, bypass notes | ⚠️ PARTIAL | Routing and note writing exist, but the bypass decision only considers `command_on_path("docker")` plus host-runtime markers. |
| `tools/apdr/src/docker/builder/docker_backend.rs` | Docker-attempt artifact layout | ✓ VERIFIED | Docker attempts write `Dockerfile`, build/run command files, and logs under the attempt debug area. |
| `benchmark_ui/run_contract.py` | Run-contract support for `llm_validation_policy` | ✓ VERIFIED | Normalizes and persists the policy separately from backend selection. |
| `benchmark_ui/runner.py` | Benchmark wrapper pass-through for selected policy | ✓ VERIFIED | Adds `--llm-validation-policy` for APDR `llm` runs and keeps `validation_backend` stable. |
| `benchmark_ui/service.py` | Service normalization and operator-facing policy truth | ✓ VERIFIED | Normalizes `llm_validation_policy` from payloads and shows it in run info fields. |
| `benchmark_ui/state.py` | Doctor and runtime wording for docker-first safe degradation | ⚠️ PARTIAL | Doctor wording is clear, but runtime validation for `llm` does not actually require Docker daemon readiness before runs start. |
| `web/src/main.js` | Operator control for docker-first vs env-first | ✓ VERIFIED | Shows the selector only for APDR `llm`, defaults to `docker-first`, and includes the field in the current config payload. |
| `scripts/check_phase22_docker_policy.py` | Deterministic proof checker | ⚠️ PARTIAL | Checker works and passes, but its fixed slice omits the installed-but-unusable Docker case that the phase text also claims to degrade safely. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `web/src/main.js` | `benchmark_ui/service.py` | Request payload carries `llm_validation_policy` separately from `validation_backend` | ✓ WIRED | The UI defaults and edits `state.form.llm_validation_policy`; the service normalizes the same field from incoming payloads. |
| `benchmark_ui/runner.py` | `tools/apdr/test_executor.py` | `--llm-validation-policy` CLI pass-through | ✓ WIRED | APDR `llm` case commands append the policy flag, and the Python wrapper forwards it to `apdr resolve`. |
| `tools/apdr/src/main.rs` | `tools/apdr/src/docker/builder/agent_backend.rs` | `ResolveConfig.llm_validation_policy` drives route selection | ✓ WIRED | CLI parsing stores the policy in config, and `llm_validation_route()` branches on it. |
| `tools/apdr/src/docker/builder/agent_backend.rs` | `tools/apdr/src/lib.rs` | Route and bypass metadata persist into top-level outputs | ✓ WIRED | `apply_llm_route_metadata()` stamps requested policy, route, and bypass fields that `ResolveResult` writes into reports and summary lines. |
| `benchmark_ui/state.py` | `tools/apdr/src/docker/builder/agent_backend.rs` | Docker-unavailable env degradation contract | ⚠️ PARTIAL | Doctor copy says `llm` runs degrade to env until Docker is available, but router bypass only triggers when Docker is missing from PATH or the case is host-runtime. |
| `scripts/check_phase22_docker_policy.py` | Phase 22 degradation contract | ⚠️ PARTIAL | The proof checker freezes four cases, but not the broader unusable-Docker condition implied by the requirement text and Doctor copy. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `web/src/main.js` | `state.form.llm_validation_policy` | LLM policy dropdown -> current config payload -> `BenchmarkService._normalize_run_config()` -> `BenchmarkWorker` -> `test_executor.py` -> `apdr resolve` | Yes | ✓ FLOWING |
| `tools/apdr/src/docker/builder/agent_backend.rs` | `summary.requested_llm_validation_policy`, `summary.llm_validation_route`, `summary.docker_bypass_reason` | `ResolveConfig.llm_validation_policy()` and `llm_validation_route()` | Yes | ✓ FLOWING |
| `benchmark_ui/state.py` | Doctor degradation detail for APDR `llm` | `doctor_checks()` Docker CLI / daemon probes | No runtime enforcement for `llm` | ⚠️ DISCONNECTED |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Rust policy routing and metadata tests | `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` | 8 Phase 22 tests passed | ✓ PASS |
| Python run-contract and Doctor regressions | `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_state_backend_doctor` | 13 tests passed | ✓ PASS |
| Web UI build integrity | `npm run build --prefix web` | Vite production build succeeded | ✓ PASS |
| Deterministic Phase 22 proof contract | `python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json --probe-only` | Exit 0; status JSON reports `passed: true` | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `DFV-01` | `22-01`, `22-02`, `22-03` | Benchmark operator can run APDR with a docker-first `llm` validation policy that attempts Docker before env validation on supported hosts | ✓ SATISFIED | Separate policy field flows from UI/service/runner to the Rust CLI and the router defaults eligible `llm` cases to Docker-first. |
| `DFV-03` | `22-01`, `22-02` | Benchmark operator can still run the existing env-first `llm` policy as a comparison control | ✓ SATISFIED | UI exposes `env-first`, service persists it, runner passes it, and Rust tests confirm `EnvFirstControl`. |
| `GDR-01` | `22-02`, `22-03` | When Docker is unavailable, unsupported, or explicitly bypassed, APDR degrades clearly without silently breaking `llm` validation | ✗ BLOCKED | Missing-CLI and host-runtime bypasses exist, but daemon-unavailable / otherwise unusable Docker is only described in Doctor copy. The runtime route does not gate `llm` on actual Docker usability before choosing `DockerFirst`. |

Phase 22 orphaned requirements: none. The phase plans account for all Phase 22 requirement IDs in `.planning/REQUIREMENTS.md` (`DFV-01`, `DFV-03`, `GDR-01`).

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| --- | --- | --- | --- | --- |
| None | - | No TODO, FIXME, placeholder, or empty-implementation stubs were found in the touched Phase 22 files. | ℹ️ Info | The blocker is a behavior and contract mismatch, not scaffolding. |

### Human Verification Required

None. Automated verification found a blocking degradation gap before manual signoff would add value.

### Gaps Summary

Phase 22 delivered the policy-selection seam, kept env-first as a real control path, and preserved route/bypass metadata and proof artifacts. The automated validation commands all pass, but that is not enough to mark the phase goal achieved.

The missing piece is the safe-degradation contract for unsupported Docker environments. Operator-facing surfaces say docker-first `llm` will degrade to env validation when Docker is unavailable, but the runtime router only treats Docker as unavailable when the CLI is absent from PATH. That means installed-but-unusable Docker can still take the docker-first route instead of the promised env-first bypass. Until runtime gating, tests, and the proof contract cover that case, `GDR-01` and the third roadmap truth remain open.

---

_Verified: 2026-04-02T01:33:44Z_
_Verifier: Claude (gsd-verifier)_
