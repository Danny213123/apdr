# Phase 26: LLM Case Intake and Plan Authoring - Research

**Researched:** 2026-04-02
**Domain:** Turning APDR `llm` and `llm-only` intake into a first-class authored case-plan contract before validation starts
**Confidence:** High

## Summary

Phase 26 should be planned as an intake-contract phase, not as a Docker-authoring or recovery phase. The existing Python resolve seam in [`tools/apdr/llm_py/actions/resolve.py`](tools/apdr/llm_py/actions/resolve.py) already assembles snippet context, retrieval context, notes, confidence, and abstain or failure metadata. The Rust side already has a stable single-line JSON IPC seam in [`tools/apdr/src/resolver/tier3_llm/process.rs`](tools/apdr/src/resolver/tier3_llm/process.rs), plus deterministic downstream rendering in [`tools/apdr/src/resolver/mod.rs`](tools/apdr/src/resolver/mod.rs) and the Docker builders. That means the clean Phase 26 move is to widen the authored intake payload, not to let the LLM directly write final files yet.

The main product gap is that current `resolve` responses only preserve thin mapping data. [`tools/apdr/llm_py/models.py`](tools/apdr/llm_py/models.py) exposes `ResolutionResponse` fields like `mappings`, `unresolved`, `notes`, `abstain_reason`, and `failure_reason`, but it has no first-class plan object for imports, runtime assumptions, system-dependency hints, authored smoke intent, or plan provenance. When the LLM returns no usable structure, Rust mostly sees either empty output or generic notes such as "LLM package-resolution call returned no output." That is exactly the gap shown by the current April 2 failures.

The existing artifact seams are good enough to reuse. [`tools/apdr/src/lib.rs`](tools/apdr/src/lib.rs) already writes `requirements.txt`, `resolution-report.txt`, and benchmark summary lines. The benchmark wrapper in [`tools/apdr/test_executor.py`](tools/apdr/test_executor.py) already turns those summary lines into machine-readable `output_metadata`. Phase 26 can therefore satisfy `TRU-02` without redesigning the UI: add explicit authored-plan and intake-failure artifacts, surface their paths and status in the summary output, and keep the full UI rendering of those details deferred to later phases.

The right plan shape is three waves. First, define the authored plan schema and transport it cleanly across the Python and Rust boundary. Second, persist that plan and the structured no-output truth in case artifacts while enforcing the stricter `llm-only` semantics. Third, freeze the plan and abstain contract with deterministic fixtures and a checker so later phases can build on a stable intake truth surface.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| LLM-01 | In both `llm` and `llm-only` modes, APDR can ask the LLM to extract snippet modules, runtime intent, and initial dependency candidates before validation starts | The current `resolve.py` seam already has all prompt inputs needed for this; Phase 26 needs a richer structured response contract. |
| TRU-02 | `llm` and `llm-only` keep truthful metadata about which parts of the pipeline were authored by the LLM versus deterministic fallbacks | `lib.rs`, `summary_lines()`, and `test_executor.py` already provide a metadata channel; Phase 26 needs explicit authored-plan and intake-failure fields instead of implicit notes. |

## Evidence That Should Drive Planning

### The current IPC payload is too thin for end-to-end authorship

[`tools/apdr/llm_py/models.py`](tools/apdr/llm_py/models.py) only models mappings, unresolved imports, recovery suggestions, and free-form metadata. There is no stable schema for:

- extracted imports versus runtime assumptions
- system-dependency hints
- authored smoke or validation intent
- section-level confidence
- machine-readable intake-failure classes

Without those fields, downstream phases have to infer plan intent from notes, logs, or empty `requirements.txt`, which is exactly what the milestone is trying to stop.

### `resolve.py` is already the natural authored-plan seam

[`tools/apdr/llm_py/actions/resolve.py`](tools/apdr/llm_py/actions/resolve.py) already performs:

- local and framework pre-resolution
- retrieval-augmented prompt assembly
- self-consistency and self-refine flows
- agent fallback triggers
- abstain and failure note capture

That makes it the right place to construct a first-class authored case plan. The plan should be created there once, then consumed downstream by Rust. Phase 26 should not scatter plan authorship across the resolver loop, Docker builder, and recovery system.

### The Rust side already has a stable ingestion point

[`tools/apdr/src/resolver/tier3_llm/core.rs`](tools/apdr/src/resolver/tier3_llm/core.rs) already:

- builds the request for Python
- persists request and response traces
- reads `notes`, `confidence`, `abstain_reason`, and `failure_reason`
- translates mappings into `ResolvedDependency`

That means Phase 26 can stay localized if it teaches this module to parse a richer authored plan and structured intake-failure record. The JSON-line IPC format in [`tools/apdr/src/resolver/tier3_llm/process.rs`](tools/apdr/src/resolver/tier3_llm/process.rs) is simple enough that widening the payload is low-risk as long as it stays single-object and compact.

### Case artifact writing already exists and should remain the truth boundary

[`tools/apdr/src/lib.rs`](tools/apdr/src/lib.rs) writes `resolution-report.txt` and summary lines, while [`tools/apdr/test_executor.py`](tools/apdr/test_executor.py) exports those fields into benchmark metadata. Phase 26 should use this seam to add:

- an authored plan artifact such as `case-plan.json`
- a structured intake-failure artifact when no usable plan exists
- summary keys for authored plan status and artifact paths

This is enough to satisfy saved-artifact truth without widening into a Phase 23-style UI effort.

### `llm-only` already has the right mode boundary for strict intake truth

The repo already treats `llm-only` as a mode that skips heuristic tiers and relies on the LLM path. That makes the new behavior straightforward:

- `llm` may continue with deterministic downstream fallback, but must mark which plan sections were not LLM-authored
- `llm-only` must fail truthfully when no usable intake plan exists rather than silently reconstructing one from deterministic tiers

That matches the user’s request and the Phase 26 discussion decisions without collapsing `llm-only` into merely "LLM-first."

### Current live failures show why intake-failure classes must be explicit

The April 2 runs repeatedly surface generic messages like `LLM package-resolution call returned no output`, `Unknown`, or misleading `SystemDependency`. Those failures need to be split into concrete classes such as:

- empty model output
- invalid JSON
- schema validation failure
- timeout or transport failure
- provider or tooling incompatibility

The recent LLM trace work in [`tools/apdr/llm_py/client.py`](tools/apdr/llm_py/client.py) already captures richer diagnostics. Phase 26 should convert those diagnostics into structured intake-failure truth rather than leaving them buried in logs.

## Implementation Recommendations

### 1. Add a first-class authored case-plan schema at the Python IPC boundary

Recommended files:

- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/actions/resolve.py`
- `tools/apdr/llm_py/client.py`
- `tools/apdr/src/resolver/tier3_llm/process.rs`
- `tools/apdr/src/resolver/tier3_llm/core.rs`

Recommended responsibilities:

- add a structured authored plan model with sections for imports or modules, package mappings, unresolved imports, system-dependency hints, runtime assumptions, authored smoke strategy, and section-level confidence
- add a structured intake-failure model with explicit reason class, human-readable reason, and safe diagnostic preview
- keep the payload plan-first: the LLM authors intent, while Rust remains responsible for deterministic file rendering
- make the Rust side parse and persist that plan and failure structure instead of reducing them to notes

### 2. Persist authored-plan and intake-failure truth into case artifacts and mode semantics

Recommended files:

- `tools/apdr/src/lib.rs`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/test_executor.py`
- `tools/apdr/tests/test_resolver.rs`
- `benchmark_ui/test_run_contract.py`

Recommended responsibilities:

- write a machine-readable authored plan artifact per case and an intake-failure artifact when the LLM cannot produce a usable plan
- extend `resolution-report.txt` and summary lines with authored-plan status, authored-plan path, intake-failure class, and authorship truth markers
- make `llm-only` fail truthfully when the authored plan is unusable
- keep `llm` able to continue into deterministic downstream work, but mark which plan elements came from fallback logic instead of the LLM

### 3. Freeze the authored-plan contract with deterministic fixtures and a checker

Recommended files:

- `scripts/check_phase26_case_plan.py`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CASE-PLAN-PROOF.md`

Recommended responsibilities:

- freeze a passing authored-plan fixture and a structured abstain fixture
- verify required fields, authorship markers, confidence sections, and failure classes deterministically
- document the Phase 26 proof boundary so later phases can rely on intake truth without overstating Docker or recovery progress

## Validation Architecture

### Quick checks

- `python3.11 -m pytest tools/apdr/llm_py/tests/test_resolve_agentic.py tools/apdr/llm_py/tests/test_client_fallbacks.py -k phase26_ -q`
- `cargo test --manifest-path tools/apdr/Cargo.toml phase26_`

### Artifact checks

- `rg -n 'AuthoredCasePlan|IntakeFailureRecord|authored_plan|intake_failure|smoke_strategy' tools/apdr/llm_py/models.py tools/apdr/llm_py/actions/resolve.py tools/apdr/src/resolver/tier3_llm/core.rs`
- `rg -n 'AUTHORED_PLAN|INTAKE_FAILURE|case-plan.json|intake-failure.json' tools/apdr/src/lib.rs tools/apdr/test_executor.py`
- `python3 scripts/check_phase26_case_plan.py --plan-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-authored-plan-sample.json --failure-json .planning/phases/26-llm-case-intake-and-plan-authoring/26-intake-failure-sample.json --status-json /tmp/phase26-status.json --probe-only`

### Phase-close checks

- inspect a successful `llm` or `llm-only` case artifact directory and confirm it contains a machine-readable authored plan with smoke or validation intent
- inspect a no-output case artifact directory and confirm it contains a structured intake-failure artifact instead of only empty `requirements.txt`
- confirm `resolution-report.txt` and output metadata distinguish authored-plan status from deterministic fallback status

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-CONTEXT.md`
- `.planning/phases/26-llm-case-intake-and-plan-authoring/26-DISCUSSION-LOG.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-CONTEXT.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md`
- `tools/apdr/llm_py/models.py`
- `tools/apdr/llm_py/actions/resolve.py`
- `tools/apdr/llm_py/client.py`
- `tools/apdr/src/resolver/tier3_llm/process.rs`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `tools/apdr/llm_py/tests/test_resolve_agentic.py`
- `tools/apdr/llm_py/tests/test_client_fallbacks.py`
- `tools/apdr/tests/test_resolver.rs`
- `benchmark_ui/test_run_contract.py`

## Out of Scope For This Phase

- direct LLM authorship of the final Dockerfile or final executed Docker commands
- fixing the Docker image handoff regression after build
- post-install or post-runtime LLM recovery loops
- final benchmark comparison claims or live closeout evidence
- broad benchmark UI redesign for authored-plan inspection

## Source Base

No external browsing was required for Phase 26 planning. The source of truth is the current v2.5 milestone files, the locked Phase 26 context decisions, the existing tier3 LLM resolve and IPC code, and the April 2 run artifacts already present in the workspace.

---
*Research created: 2026-04-02*
*Phase: 26-llm-case-intake-and-plan-authoring*
