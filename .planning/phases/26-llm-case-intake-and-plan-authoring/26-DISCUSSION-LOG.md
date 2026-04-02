# Phase 26: LLM Case Intake and Plan Authoring - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-02
**Phase:** 26-llm-case-intake-and-plan-authoring
**Areas discussed:** Case plan contents, Authoring contract, `llm-only` semantics, No-output handling

---

## Case Plan Contents

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal plan | Just package mappings and unresolved imports | |
| Full plan | Modules or imports, package mappings, unresolved imports, system-dependency hints, runtime assumptions, and confidence per section | |
| Full plan + smoke strategy | Include what validation should import or run, not just dependencies | ✓ |

**User's choice:** Full plan plus authored smoke or validation strategy
**Notes:** Intake should capture more than dependency names so downstream validation knows what the case is actually trying to prove.

---

## Authoring Contract

| Option | Description | Selected |
|--------|-------------|----------|
| Plan-first | LLM outputs a structured case plan and APDR renders files and artifacts deterministically from it | ✓ |
| Artifact-first | LLM directly writes near-final intake artifacts such as `requirements.txt` or Docker inputs | |

**User's choice:** Plan-first
**Notes:** This keeps the LLM in charge of intent while preserving a stable audit boundary between authored intent and executed files.

---

## `llm-only` Semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Same pipeline, stricter fallback | `llm-only` shares the authored-plan pipeline with `llm` but fails truthfully if no usable plan exists | ✓ |
| Same pipeline, same recovery as `llm` | `llm-only` behaves more like “LLM-first” than strict LLM-only | |
| Fully strict | If intake is not good enough, stop immediately and do not attempt recovery | |

**User's choice:** Same pipeline, stricter fallback
**Notes:** `llm-only` should remain meaningfully strict without forking the intake contract from `llm`.

---

## No-Output Handling

| Option | Description | Selected |
|--------|-------------|----------|
| Hard fail with raw diagnostics only | Fail immediately and rely on logs for details | |
| Structured abstain record | Persist an intake failure artifact with reason class and safe raw-response snippet | ✓ |
| Silent deterministic fallback | Keep going without clearly surfacing intake failure | |

**User's choice:** Structured abstain record
**Notes:** Current runs are already suffering from LLM no-output paths, so Phase 26 must make intake failure explicit and inspectable.

---

## the agent's Discretion

- Exact authored-plan field names and nesting
- Confidence-scale semantics
- Safe truncation and redaction rules for persisted raw-response snippets

## Deferred Ideas

- Direct final Dockerfile authoring by the LLM belongs in Phase 27 evaluation.
- Docker image handoff reliability belongs in Phase 27.
- Recovery-loop behavior after failed attempts belongs in Phase 28.
