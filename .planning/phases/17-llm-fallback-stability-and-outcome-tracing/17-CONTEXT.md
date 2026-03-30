# Phase 17: LLM Fallback Stability and Outcome Tracing - Context

**Gathered:** 2026-03-30
**Status:** Ready for planning

<domain>
## Phase Boundary

Make `--validation-backend llm` survive post-env validation failures on tier3 cases and expose inspectable fallback outcomes per case. This phase stabilizes the existing env-first plus LangGraph fallback path and makes its end state visible in artifacts; it does not add Docker escalation, broad failure reclassification, or full benchmark-closeout proof.

</domain>

<decisions>
## Implementation Decisions

### Fallback Strategy
- **D-01:** Phase 17 should repair the existing LangGraph fallback path rather than introduce a second recovery mechanism or a broad replacement path.
- **D-02:** When env validation fails, APDR should still attempt the configured LLM fallback path, but internal agent exceptions must degrade into recorded fallback failure metadata instead of terminating the case or the run.

### Outcome Vocabulary
- **D-03:** Per-case artifacts must explicitly record whether fallback was invoked and, when invoked, the terminal agent outcome using the milestone vocabulary `passed`, `abstained`, or `failed`.
- **D-04:** Internal crash or availability details should be preserved as machine-readable reason text or substatus under `failed`, not as a new top-level milestone outcome class.

### Top-Level Result Semantics
- **D-05:** If env validation fails and the fallback does not solve the case, the saved artifact must continue to represent the real validation failure while also preserving the fallback outcome; it must not collapse back into an unlabeled env-only failure.
- **D-06:** Phase 17 should not invent new benchmark-wide failure buckets for fallback crashes. It should attach fallback outcome metadata to the existing validation result and leave broader failure taxonomy work to Phase 19.

### Proof Target
- **D-07:** Phase 17 proof should use a fixed March 30, 2026 live-derived tier3 slice that includes the exact post-env fallback crash path and representative cases for agent `passed`, `abstained`, and `failed` outcomes.
- **D-08:** Phase 17 verification should emphasize case-level artifacts and stable before/after evidence rather than a full moving benchmark rerun.

### the agent's Discretion
- Exact metadata field names and artifact layout, as long as downstream operators can unambiguously tell whether fallback was invoked and how it ended.
- The exact tier3 slice membership, as long as it is fixed, derived from the March 30, 2026 live baseline, and includes the known crash repro plus representative non-crash fallback outcomes.
- Whether proof is implemented through Rust tests, focused replay scripts, benchmark UI contract checks, or a combination of those, as long as AGT-07 and AGT-08 become provable.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone Scope
- `.planning/ROADMAP.md` — Phase 17 goal, success criteria, and the boundary between fallback stability work here and Docker/accounting work in Phases 18-21.
- `.planning/REQUIREMENTS.md` — `AGT-07` and `AGT-08`, plus adjacent `VAL-*` and `EVD-*` requirements that are intentionally deferred out of this phase.

### Evidence And Artifact Contract
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md` — reviewer-facing artifact contract, execution-mode labeling, and comparison metadata expectations.
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md` — prior fixed-slice proof discipline for agent-path work.
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md` — policy attribution expectations for agent-backed tier3 evaluation.

### Live Baseline Evidence
- `runs/20260330-020943-apdr/benchmark-context.log` — current live benchmark evidence, including repeated LangGraph fallback crashes after env failure.
- `runs/20260330-020943-apdr/summary.json` — current run-level baseline metadata for the live March 30 benchmark session.
- `runs/20260330-004502-apdr/summary.json` — resumed predecessor run referenced by the live session and used for tier3 outcome accounting.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tools/apdr/src/docker/builder/agent_backend.rs`: Central env-first plus agent-fallback entry point for `llm` mode and the natural place to convert fallback crashes into structured outcomes.
- `tools/apdr/src/lib.rs`: `ValidationSummary` and `ValidationAttempt` already carry `validation_backend`, `status`, `reason`, `escalated_backend`, and `agent_invocations`, so Phase 17 can extend artifact truth without inventing a separate evidence channel.
- `tools/apdr/test_executor.py`: Output serialization already writes validation metadata into per-case artifacts, making it a natural surfacing point for fallback invocation and terminal outcome.
- `benchmark_ui/runner.py`: Existing result shaping already reads `validation_status`, `validation_reason`, and `confidence`, so UI/event updates can build on current metadata surfaces.

### Established Patterns
- APDR validation is already env-first for `llm` mode, so Phase 17 should preserve that ordering and harden the post-env fallback behavior rather than redesign the overall routing contract.
- Reviewer-facing evidence in this repo favors explicit status fields and fixed-slice comparisons over log-only proof.
- Prior agent-quality work already treats runtime and policy attribution as part of the artifact contract, not optional debugging detail.

### Integration Points
- `tools/apdr/docker_agent/state.py` and `tools/apdr/docker_agent/graph.py`: LangGraph state definition and graph wiring around the `confidence` path.
- `tools/apdr/src/docker/builder/agent_backend.rs`: fallback invocation, exception handling, and final summary shaping.
- `tools/apdr/src/resolver/retry_loop.rs` and `tools/apdr/src/resolver/recovery_diagnostics.rs`: preserving final validation status and reason without losing fallback outcome details.
- `benchmark_ui/service.py` and `benchmark_ui/runner.py`: ensuring surfaced fallback outcomes are not silently flattened during run summary and event generation.

</code_context>

<specifics>
## Specific Ideas

- Keep the phase narrow: this is about making the existing `llm` path survive and tell the truth, not about adding Docker escalation yet.
- The live March 30, 2026 benchmark is the anchor. The known production failure is the repeated `ValueError: 'confidence' is already being used as a state key` after env validation failure.
- The artifact contract should answer two operator questions quickly: "Did fallback run?" and "How did it end?"
- Use the milestone vocabulary directly in saved artifacts so later phases can compare pass, abstain, and failure behavior without parsing raw logs.
- Working assumption for planning: the recommended defaults above were selected to keep momentum after discussing all four identified gray areas together.

</specifics>

<deferred>
## Deferred Ideas

- Docker escalation for eligible `environment-build-failed` and `version-not-found` cases belongs to Phase 18.
- Broader backend-path truth for `env` versus `docker` versus `llm-agent` across every validation attempt belongs to Phase 18.
- Failure-bucket repair and resumed-run accounting cleanup belong to Phase 19.
- Large-scale recovery work on dominant tier3 buckets belongs to Phase 20.
- Milestone-wide before/after evidence packaging belongs to Phase 21.

</deferred>

---

*Phase: 17-llm-fallback-stability-and-outcome-tracing*
*Context gathered: 2026-03-30*
