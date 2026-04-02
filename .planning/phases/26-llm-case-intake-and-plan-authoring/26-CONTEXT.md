# Phase 26: LLM Case Intake and Plan Authoring - Context

**Gathered:** 2026-04-02
**Status:** Ready for planning

<domain>
## Phase Boundary

Make APDR produce an explicit LLM-authored case plan before validation starts so `llm` and `llm-only` stop beginning from opaque mappings or blank requirements. This phase defines what the intake plan contains, how APDR consumes it, and how intake failure is surfaced; it does not yet hand direct final Dockerfile authorship to the LLM or close the later Docker-runtime reliability work.

</domain>

<decisions>
## Implementation Decisions

### Case Plan Schema
- **D-01:** Intake should produce a full authored case plan, not just package mappings. The plan must include extracted modules or imports, package mappings, unresolved imports, system-dependency hints, runtime assumptions, section-level confidence, and an authored smoke or validation strategy.
- **D-02:** The authored smoke or validation strategy is part of the case plan itself. APDR should know from intake what the case is trying to prove during validation rather than relying only on a downstream default.
- **D-03:** The case plan must be rich enough that downstream phases can render `requirements.txt`, Docker inputs, and recovery context from it without reconstructing intent from raw prompts or logs.

### Authoring Contract
- **D-04:** Phase 26 should be plan-first. The LLM authors a structured case plan, and APDR deterministically renders later files and artifacts from that plan.
- **D-05:** APDR should not ask the LLM to directly author near-final intake files such as `requirements.txt` or the final Dockerfile in this phase. Deterministic rendering remains the audit boundary between LLM intent and executed artifacts.

### `llm-only` Mode Semantics
- **D-06:** `llm-only` should use the same authored-plan pipeline as `llm` so both modes share one intake contract and comparable per-case artifacts.
- **D-07:** `llm-only` remains stricter than `llm`: if the model cannot produce a usable intake plan, APDR should fail truthfully instead of dropping into heuristic recovery or silently reconstructing a plan from deterministic tiers.

### Intake Failure Truth
- **D-08:** Intake no-output should create a first-class structured abstain record rather than collapsing into empty `requirements.txt`, generic `Unknown`, or misleading downstream failure labels.
- **D-09:** Intake failure classes should distinguish at minimum empty output, invalid JSON, schema validation failure, timeout or transport failure, and provider or tooling incompatibility.
- **D-10:** When safe, APDR should persist a truncated raw-response snippet or diagnostic excerpt alongside the structured intake failure class so reviewers can see why intake failed without scraping raw logs.

### the agent's Discretion
- Exact field names and schema nesting for the authored plan and intake-failure records, as long as the decisions above remain explicit and machine-readable.
- Confidence-scale semantics and thresholds, as long as confidence stays section-level and inspectable.
- Raw-response truncation and redaction rules, as long as persisted diagnostics remain useful and safe to store.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone Scope
- `.planning/ROADMAP.md` — Phase 26 goal, success criteria, and the downstream dependency chain into Phases 27-30.
- `.planning/REQUIREMENTS.md` — `LLM-01` and `TRU-02`, plus adjacent `LLM-02`, `LLM-03`, and `TRU-01` requirements deliberately deferred out of this phase.
- `.planning/PROJECT.md` — v2.5 milestone framing and the active user goal that the LLM should extract modules, author validation intent, and drive start-to-finish case execution.

### Prior Phase Decisions
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-CONTEXT.md` — prior fallback-truth contract and the earlier decision to keep routing policy separate from fallback semantics.
- `.planning/phases/18-backend-escalation-and-path-truth/18-CONTEXT.md` — requested backend versus actual route truth and targeted Docker escalation constraints that still apply.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-CONTEXT.md` — docker-first policy and per-case Docker artifact expectations that Phase 26 should feed rather than replace.

### Active Failure Evidence
- `runs/20260402-184821-apdr/summary.json` — early April 2 baseline showing low current pass counts for the newest docker-backed `llm-only` run.
- `runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/resolution-report.txt` — current `llm-only` failure showing no authored package plan and misleading `SystemDependency` labeling.
- `runs/20260402-184821-apdr/cases/005bbad123ef309a5bef/.apdr-debug/attempts/attempt-001-py-2_7/combined.log` — build succeeded, then `docker create` could not find the just-built image; useful for keeping Phase 26 honest about what is intake versus what belongs to later Docker work.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tools/apdr/llm_py/actions/resolve.py`: current package-resolution intake already assembles context, notes, confidence, and abstain or failure metadata; it is the natural seam for upgrading into a first-class case plan.
- `tools/apdr/llm_py/models.py`: current Rust/Python IPC schema boundary where authored-plan fields and structured intake-failure classes can become first-class protocol elements.
- `tools/apdr/src/resolver/tier3_llm/process.rs`: persistent Python subprocess seam; any new authored-plan or abstain payload has to remain compatible with this single-line JSON exchange.
- `tools/apdr/src/docker/builder/docker_backend.rs`: current deterministic Docker renderer already turns structured inputs into `requirements.txt`, Dockerfile, and command artifacts, which supports the chosen plan-first contract.
- `tools/apdr/docker_agent/agents/builder.py` and `tools/apdr/docker_agent/tools/docker_ops.py`: existing Docker authoring helpers provide later integration points once Phase 27 starts consuming the authored plan.

### Established Patterns
- Requested backend truth is kept separate from actual validation route truth; Phase 26 should extend that discipline to intake authorship rather than overloading backend fields.
- The repo prefers explicit structured artifact fields over inferring meaning from raw logs, so intake should emit first-class plan and abstain metadata.
- `llm-only` already intentionally skips tier1 and tier2 heuristics, so its stricter “fail truthfully if no usable authored plan exists” semantics fit current mode boundaries.
- Docker files and smoke scripts are currently generated deterministically from structured inputs, which supports plan-first intake instead of artifact-first prompt output.

### Integration Points
- `tools/apdr/src/resolver/tier3_llm/core.rs` and `tools/apdr/src/resolver/tier3_llm/process.rs`: request assembly, Python IPC, and Rust-side interpretation of new plan or abstain payloads.
- `tools/apdr/llm_py/actions/resolve.py`: authored case-plan creation and initial no-output handling.
- `tools/apdr/llm_py/client.py`: provider diagnostics, tolerant JSON fallback, and raw-response capture for structured abstain records.
- `tools/apdr/test_executor.py` and saved case artifact writers: surfacing authored-plan metadata in per-case outputs without waiting for later UI phases.
- `benchmark_ui/service.py`: follow-on consumer for any new case-level authored-plan truth once later phases choose to display it.

</code_context>

<specifics>
## Specific Ideas

- The user wants the LLM to “extract snippet modules, create the Docker file, and basically do everything from start-to-finish,” but for Phase 26 the locked intake contract is plan-first rather than direct final file authoring.
- The authored plan should include an explicit smoke or validation strategy, because “what should be proven” is part of case intent, not just dependency mapping.
- `llm-only` should stay meaningfully stricter than `llm`: both share the authored-plan schema, but `llm-only` should not silently fall back to deterministic recovery when intake fails.
- Intake failure records need to explain whether the model returned nothing, invalid JSON, a schema-mismatched object, or a provider or tooling failure like the recent Ollama timeout and tool-support errors.

</specifics>

<deferred>
## Deferred Ideas

- Direct LLM authorship of the final Dockerfile at intake is deferred to Phase 27 evaluation; Phase 26 keeps deterministic rendering downstream of the authored plan.
- Docker image handoff reliability (`docker create` cannot see the freshly built image) is Phase 27 work.
- Recovery-loop behavior after install, build, or runtime failures is Phase 28 work.
- Fixed-slice benchmark comparison and live closeout evidence are Phases 29 and 30.
- Full benchmark UI surfacing of authored-plan details remains outside this phase.

</deferred>

---

*Phase: 26-llm-case-intake-and-plan-authoring*
*Context gathered: 2026-04-02*
