# Phase 17: LLM Fallback Stability and Outcome Tracing - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md. This log preserves the alternatives considered.

**Date:** 2026-03-30
**Phase:** 17-llm-fallback-stability-and-outcome-tracing
**Areas discussed:** Fallback strategy, outcome vocabulary, top-level result semantics, proof target

---

## Fallback Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Repair current LangGraph path | Keep env-first plus LangGraph fallback architecture and harden it so post-env failures become structured outcomes instead of crashes. | ✓ |
| Add backup non-LangGraph fallback | Introduce a second recovery path for use when the main agent crashes or is unavailable. | |
| Bypass fallback on env failure | Stop after env failure and treat `llm` mode as effectively env-only until later phases. | |

**User's choice:** Recommended default selected while discussing all areas together: repair the current LangGraph path.
**Notes:** Phase 17 is the first phase of the milestone and should stabilize the existing `llm` contract rather than broaden routing. Docker or alternate fallback work remains future-phase territory.

---

## Outcome Vocabulary

| Option | Description | Selected |
|--------|-------------|----------|
| `passed` / `abstained` / `failed` | Use explicit terminal outcomes aligned with `AGT-08`, with separate indication of whether fallback was invoked. | ✓ |
| Add top-level `crashed` | Make crash a first-class terminal outcome beside pass or fail. | |
| Log-only crash details | Keep artifacts simple and rely on logs for crash versus abstain versus failure details. | |

**User's choice:** Recommended default selected while discussing all areas together: explicit `passed`, `abstained`, and `failed` outcomes with an invocation indicator.
**Notes:** Crash and availability details should still be retained, but as reason text or substatus under `failed` rather than a new milestone vocabulary item.

---

## Top-Level Result Semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Preserve env failure plus fallback metadata | Final artifact reflects the real validation result and also records whether fallback ran and how it ended. | ✓ |
| Replace result with fallback-specific failure class | Promote fallback failure into a new top-level benchmark result for this phase. | |
| Collapse to env-only failure | Keep current behavior where failed fallback paths disappear into unlabeled env failures. | |

**User's choice:** Recommended default selected while discussing all areas together: keep the real validation result while attaching fallback outcome metadata.
**Notes:** This keeps Phase 17 focused on truthfulness and inspectability without pre-empting the broader taxonomy work already scoped to Phase 19.

---

## Proof Target

| Option | Description | Selected |
|--------|-------------|----------|
| Fixed live-derived March 30 slice | Use a stable tier3 slice from the live March 30, 2026 baseline with the known crash repro and representative fallback outcomes. | ✓ |
| Full live benchmark rerun | Require a full benchmark rerun before Phase 17 can be considered proven. | |
| Historical replay-only sample | Prove the phase on an older or synthetic slice that does not directly anchor to the March 30 live evidence. | |

**User's choice:** Recommended default selected while discussing all areas together: fixed live-derived March 30 slice.
**Notes:** The phase needs trustworthy, repeatable evidence quickly. A fixed slice provides that without blocking Phase 17 on the longer milestone-closeout work of Phase 21.

---

## the agent's Discretion

- Exact field names for fallback invocation and terminal outcome metadata.
- The precise case list for the Phase 17 proof slice, as long as it remains fixed and March 30 live-derived.
- The specific balance of Rust, Python, and benchmark UI tests used to prove the phase.

## Deferred Ideas

- Docker escalation in `llm` mode for packaging-style failures belongs to Phase 18.
- Cross-run summary integrity and host-runtime skip accounting belong to Phase 19.
- Large bucket-reduction work belongs to Phase 20.
- Milestone closeout comparisons and evidence packaging belong to Phase 21.
