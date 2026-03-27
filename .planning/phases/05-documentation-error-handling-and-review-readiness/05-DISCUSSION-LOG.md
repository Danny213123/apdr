# Phase 5: Documentation, Error Handling & Review Readiness - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-03-27
**Phase:** 05-documentation-error-handling-and-review-readiness
**Areas discussed:** Documentation surface, Panic-path policy, Reviewer guide scope

---

## Documentation Surface

| Option | Description | Selected |
|--------|-------------|----------|
| Both module docs and a reviewer guide | Put invariants and fallback behavior close to the code and also add one repo doc that explains how the major modernized modules fit together. | x |
| Mostly inline docs in Rust modules | Keep everything near the code and avoid creating another document unless necessary. | |
| Mostly one reviewer guide | Keep code comments minimal and centralize the explanation in a single guide. | |

**User's choice:** Both module docs and a reviewer guide.
**Notes:** The user narrowed the inline-doc pass to the Phase 4 modernization surfaces, preferred API-focused inline docs over deep helper commentary, and chose to keep deeper fallback and recovery explanation in the reviewer guide organized by module.

---

## Panic-Path Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Remove runtime-facing panics, allow narrow documented invariants | Eliminate panics caused by I/O, subprocess, cache, or host-environment conditions and keep only rare internal invariants when justified. | x |
| Remove all unwrap/expect in touched production modules | Rewrite even internal invariants to avoid panic paths. | |
| Fix only the riskiest panic paths | Prioritize user- or environment-facing failures and leave most internal assumptions alone. | |

**User's choice:** Remove runtime-facing panics, allow narrow documented invariants.
**Notes:** For former panic sites, the user preferred normal error propagation plus graceful fallback behavior where it makes sense, asked to degrade into the module's existing fallback path when possible, and allowed small helper-signature refactors inside the Phase 4 surfaces to support that cleanup.

---

## Reviewer Guide Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Module map + fallback/error-handling map + verification pointers | Explain ownership, fallback behavior, and how reviewers should verify each modernized area. | x |
| Module responsibility map only | Keep it short and structural with minimal behavior detail. | |
| Detailed maintainer manual | Go beyond review support and include deeper operational notes and rationale. | |

**User's choice:** Module map + fallback/error-handling map + verification pointers.
**Notes:** The guide should cover only the five major modernized areas and should point reviewers to the existing commands and targeted checks rather than introducing a new checklist framework.

---

## the agent's Discretion

- Exact reviewer-guide filename and placement.
- Exact inline-doc sites within each covered module.
- Which narrow internal invariants remain after panic cleanup, if they are explicitly justified and not runtime-facing.

## Deferred Ideas

None.
