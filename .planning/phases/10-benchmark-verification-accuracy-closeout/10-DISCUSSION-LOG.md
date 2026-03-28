# Phase 10: Benchmark Verification & Accuracy Closeout - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 10-Benchmark Verification & Accuracy Closeout
**Areas discussed:** Rerun scope, Evidence package shape, Preservation gate, Remaining gap reporting

---

## Rerun Scope

| Option | Description | Selected |
|--------|-------------|----------|
| `a` | Main report is the canonical `70`-case slice, with any `17`-case watchlist/overlap view kept in a separate appendix or companion artifact. | yes |
| `b` | Only the canonical `70`-case slice; no watchlist appendix at all. | |
| `c` | Mix the canonical slice and watchlist into one combined report. | |

**User's choice:** `1a`
**Notes:** User first asked to discuss all identified gray areas, then selected the recommended option to keep the canonical slice as the contract and any watchlist view separate.

---

## Evidence Package Shape

| Option | Description | Selected |
|--------|-------------|----------|
| `a` | Separate artifacts: machine-readable case-level delta data, reviewer-facing markdown summary, unrecovered-case report, and a repeatable rerun/check note. | yes |
| `b` | Markdown closeout only. | |
| `c` | One machine artifact plus a short note, no separate reviewer summary. | |

**User's choice:** `2a`
**Notes:** The closeout should preserve a clear split between machine evidence and reviewer-facing reporting.

---

## Preservation Gate

| Option | Description | Selected |
|--------|-------------|----------|
| `a` | Hard blocker if any previously passed targeted case regresses, or if expected `host-runtime`, `unsolvable`, or `local-helper` skip behavior changes on the rerun. | yes |
| `b` | Hard blocker only for passed-case regressions; skip-behavior drift is documented but not blocking. | |
| `c` | Treat all preservation issues as notes only. | |

**User's choice:** `3a`
**Notes:** Phase 10 closeout must stop on both passed-case regressions and expected-skip drift.

---

## Remaining Gap Reporting

| Option | Description | Selected |
|--------|-------------|----------|
| `a` | Dominant failure bucket counts only. | |
| `b` | Dominant buckets plus case IDs and short follow-on notes for each remaining unrecovered case. | yes |
| `c` | Full per-case narrative writeup for every remaining gap. | |

**User's choice:** `4b`
**Notes:** The unresolved-gap report should stay compact but still actionable for follow-on planning.

---

## the agent's Discretion

- Exact artifact filenames, markdown structure, and checker naming remain open as long as the reporting contract from the selected options is preserved.

## Deferred Ideas

None.
