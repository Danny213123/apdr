# Phase 27: LLM-Authored Docker Validation and Artifact Truth - Research

**Researched:** 2026-04-02
**Domain:** Turning the new Phase 26 authored case plan into executable Docker validation inputs while fixing the build-to-run handoff seam and preserving exact Docker artifacts per case
**Confidence:** High

## Summary

Phase 27 should be planned as a Docker authoring plus execution-truth phase, not as a recovery phase. Phase 26 already froze the intake contract around a structured authored case plan and strict `llm-only` intake failure behavior. The next gap is downstream: Docker validation still renders a deterministic `Dockerfile` directly from `requirements.txt` and inferred system deps in [`tools/apdr/src/docker/templates.rs`](tools/apdr/src/docker/templates.rs), while the LLM-authored case plan does not yet drive those Docker inputs. At the same time, the current deterministic Docker runner in [`tools/apdr/src/docker/builder/docker_backend.rs`](tools/apdr/src/docker/builder/docker_backend.rs) still has a concrete reliability bug where a successful build can be followed immediately by `docker create` failing to find the tagged image.

The current code already gives us usable seams. Phase 26 now writes `case-plan.json` and authored-plan metadata via [`tools/apdr/src/lib.rs`](tools/apdr/src/lib.rs). The deterministic Docker builder already writes a per-attempt work directory with `Dockerfile`, `requirements.txt`, `smoke_test.py`, build/run command snapshots, and logs in [`tools/apdr/src/docker/builder/docker_backend.rs`](tools/apdr/src/docker/builder/docker_backend.rs). The benchmark wrapper already exports summary-line metadata through [`tools/apdr/test_executor.py`](tools/apdr/test_executor.py) and the UI already knows how to surface per-case debug paths in [`benchmark_ui/service.py`](benchmark_ui/service.py). That means Phase 27 can remain a plan-first phase: the LLM authors Docker-oriented intent, Rust deterministically renders the final executed Docker inputs from that plan, and saved artifacts preserve both authored and executed truth.

The two highest-value problems to solve together are:

- authoring: no stable Docker-plan contract yet exists between the Phase 26 authored case plan and the Docker validation backend
- reliability: the build-to-run seam currently trusts the tag from `docker build` without proving the image is locally materialized for `docker create`

The right plan shape is three waves. First, add a structured authored Docker plan and persist it as a first-class artifact next to the authored case plan. Second, teach the deterministic Docker backend to consume that plan, record the exact executed Dockerfile or commands, and harden image handoff with an inspectable local-image contract. Third, freeze the authored-versus-executed Docker contract with deterministic sample artifacts and a checker so later recovery work can build on real Docker truth instead of inferred logs.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| LLM-02 | In both `llm` and `llm-only` modes, APDR can ask the LLM to author Docker-oriented validation inputs, including build/runtime guidance and reproducible per-case artifacts | Phase 26 already provides a structured case plan, so Phase 27 can add a second structured Docker authoring step instead of letting the LLM write final shell commands ad hoc. |
| DKR-01 | Docker validation can reliably run the image it just built in `llm` and `llm-only` modes without image-handoff or tag-visibility regressions | The current build-to-run seam in `docker_backend.rs` fails this today; the run log from case `005bbad123ef309a5bef` proves the image tag can disappear immediately after a successful build. |
| DKR-02 | Each LLM-driven case debug folder records the authored plan, Docker inputs, recovery prompts/responses, and final executed artifacts needed to explain the case path | The current attempt dirs already store build/run files; Phase 27 needs to add authored Docker artifacts and exact executed Docker references, not invent a new storage boundary. |

## Evidence That Should Drive Planning

### The current Docker renderer is deterministic and disconnected from the authored case plan

The deterministic backend writes `Dockerfile` directly from `requirements.txt`, inferred system deps, and the smoke-test import list in [`tools/apdr/src/docker/templates.rs`](tools/apdr/src/docker/templates.rs) and [`tools/apdr/src/docker/builder/docker_backend.rs`](tools/apdr/src/docker/builder/docker_backend.rs). That path has no structured input for:

- base-image or Python-image choice rationale
- authored system-package hints versus deterministic inferred packages
- install/runtime environment tweaks from the LLM
- which smoke behavior was authored by the LLM versus filled in deterministically

Without that contract, later benchmark reviewers cannot tell whether a Docker failure came from a bad authored plan, a deterministic renderer choice, or the runtime infrastructure.

### There is already a duplicate Dockerfile generator seam that can drift

The repo has one Dockerfile generator in Rust at [`tools/apdr/src/docker/templates.rs`](tools/apdr/src/docker/templates.rs) and another in Python at [`tools/apdr/docker_agent/tools/docker_ops.py`](tools/apdr/docker_agent/tools/docker_ops.py). Both currently mirror the same base-image, apt, pip, and smoke-test shape. That duplication is tolerable only if Phase 27 introduces a single authored Docker-plan contract or a single rendering source of truth; otherwise, authored Docker behavior will fork between deterministic and agent paths before Phase 28 even starts.

### The image handoff bug is real and precisely located

Case `hard-gists/005bbad123ef309a5bef/snippet.py` from [`runs/20260402-184821-apdr`](runs/20260402-184821-apdr) proves the reliability gap. The build log at [`combined.log`](runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/.apdr-debug/attempts/attempt-001-py-2_7/combined.log) shows `docker build` succeeding and naming the tag `apdr-validate:py2_7-build-2ad90a6d877cdf4c`, followed immediately by `Unable to find image 'apdr-validate:py2_7-build-2ad90a6d877cdf4c' locally`. In the code, `validate_requirements_docker_inner(...)` currently performs:

- `docker build --progress=plain -t {image_tag} {work_dir}`
- then `docker create --name {container_name} {image_tag}`

There is no post-build verification that the image tag is materialized into the local engine namespace that `docker create` uses. Phase 27 should fix this with an explicit handoff contract: capture an inspectable local image reference or image ID after build, verify it via `docker image inspect` or equivalent, and only then create or start the container.

### Exact executed artifact truth is not yet preserved

The current deterministic Docker attempt directory does preserve:

- `Dockerfile`
- `requirements.txt`
- `smoke_test.py`
- `docker-build.command.txt`
- `docker-run.command.txt`
- logs and metadata

But those files are not yet tied back to authored Docker intent, and the summary surface does not distinguish:

- authored Docker plan path
- authored Dockerfile path
- executed Dockerfile path
- executed image reference or image ID actually used for `docker create` or `docker start`

Phase 27 should preserve those paths explicitly, not force reviewers to infer them from raw attempt directories.

### Phase 26 already froze the right upstream contract

The Phase 26 proof package guarantees that later phases can rely on:

- an authored case plan with imports, package mappings, system-dependency hints, runtime assumptions, and smoke strategy
- explicit authorship and deterministic-fallback markers
- strict `llm-only` failure when no usable intake plan exists

Phase 27 should build on that contract rather than reopening the intake schema. The LLM should author Docker-oriented validation inputs from the authored case plan, not by re-parsing the raw snippet in a separate opaque prompt path.

## Implementation Recommendations

### 1. Add a structured Docker authoring contract derived from the authored case plan

Recommended files:

- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/__main__.py`
- `tools/apdr/llm_py/actions/docker_plan.py` (new)
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/lib.rs`

Recommended responsibilities:

- add a structured authored Docker plan model rather than asking the LLM to emit only raw Dockerfile text
- let the plan include base image choice, authored system packages, pip/runtime environment hints, smoke execution mode, and authored rationale
- persist the authored Docker plan and, when useful, a rendered authored Dockerfile artifact in the case directory
- keep Rust as the deterministic renderer and executor of the final Docker inputs

### 2. Make the executed Docker path consume the authored plan and prove image handoff

Recommended files:

- `tools/apdr/src/docker/templates.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/docker_agent/tools/docker_ops.py`
- `tools/apdr/tests/test_resolver.rs`

Recommended responsibilities:

- render the executed Dockerfile from the authored Docker plan plus clearly-labeled deterministic supplements
- preserve both authored and executed Docker artifacts in the attempt directory
- capture the exact local image reference or image ID after build and verify it before `docker create`
- use an inspectable handoff contract such as `--iidfile` plus `docker image inspect`, and add a load path when the active builder does not make the built tag locally runnable

### 3. Surface Docker artifact truth through summary metadata and frozen proof fixtures

Recommended files:

- `tools/apdr/test_executor.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`
- `scripts/check_phase27_docker_artifacts.py`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json`
- `.planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md`

Recommended responsibilities:

- export authored/executed Docker artifact paths and the executed image reference through summary lines and benchmark metadata
- keep the UI additive by surfacing these artifact pointers in case details rather than redesigning tables
- freeze one authored Docker sample and one executed Docker sample with a checker that proves the handoff contract and artifact truth

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml phase27_ -- --nocapture`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events`

### Artifact checks

- `python3 scripts/check_phase27_docker_artifacts.py --authored-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-authored-docker-sample.json --executed-json .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-executed-docker-sample.json --status-json /tmp/phase27-status.json --probe-only`
- `rg -n 'docker_plan|Dockerfile|build_image_id|executed_image_ref|docker-build.command|docker-run.command' tools/apdr/src/docker/builder/docker_backend.rs tools/apdr/src/lib.rs tools/apdr/test_executor.py .planning/phases/27-llm-authored-docker-validation-and-artifact-truth/27-DOCKER-ARTIFACT-PROOF.md`

### Phase-close checks

- inspect a successful case and confirm it contains both an authored Docker plan or Dockerfile artifact and the executed Dockerfile or commands actually used
- inspect a failed build-to-run case and confirm the case artifacts show the exact built image reference and the post-build verification result instead of only a missing-image message
- confirm benchmark metadata exposes enough artifact pointers that a reviewer can find the authored and executed Docker assets without raw directory spelunking

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-RESEARCH.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-VERIFICATION.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md`
- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/__main__.py`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/src/docker/templates.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/docker_agent/tools/docker_ops.py`
- `tools/apdr/test_executor.py`
- `benchmark_ui/service.py`
- `benchmark_ui/test_run_contract.py`
- `benchmark_ui/test_runner_events.py`
- `runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/.apdr-debug/attempts/attempt-001-py-2_7/combined.log`

## Out of Scope For This Phase

- bounded install/build/runtime recovery loops based on Docker logs
- final failure-semantics cleanup for model versus infrastructure versus snippet failures
- benchmark delta claims versus the April 2 baseline
- final live-evidence or shipping verdicts for the end-to-end LLM path
- broad benchmark UI redesign

## Source Base

No external browsing was required for Phase 27 planning. The source of truth is the active v2.5 milestone files, the completed Phase 26 authored-plan contract, the current deterministic and agent Docker builders, and the April 2 run artifacts already present in the workspace.

---
*Research created: 2026-04-02*
*Phase: 27-llm-authored-docker-validation-and-artifact-truth*
