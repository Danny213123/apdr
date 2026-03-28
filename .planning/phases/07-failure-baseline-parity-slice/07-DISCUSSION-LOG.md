# Phase 7: Failure Baseline & Parity Slice - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 07-failure-baseline-parity-slice
**Areas discussed:** Slice Scope and Target Bar, Artifact Shape, Family Regression Fixture Scope, Failure-Bucket Normalization, Benchmark-Derived Snapshot Coverage

---

## Slice Scope and Target Bar

| Option | Description | Selected |
|--------|-------------|----------|
| Hybrid canonical slice | Keep all `87` APDR-failed and `pllm`-passing cases in the artifact, but make the `70` tier3 cases the main improvement slice. | |
| All `87` as equal priority | Treat every APDR-failed and `pllm`-passing case as part of the main improvement target. | |
| Tier3 only | Drop the `17` tier1 parity cases and make the canonical slice only the `70` tier3 APDR-failed and `pllm`-passing cases. | x |

**User's choice:** Tier3 only
**Notes:** The milestone contract should track only the `70` tier3 parity cases. The `17` tier1 overlap cases are outside the Phase 7 improvement baseline.

---

## Artifact Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Generated manifest + summary | Create a script-generated JSON artifact as the source of truth and a Markdown summary for review. | x |
| Plain case list first | Use a lightweight checked-in list as the main contract and keep richer analysis separate. | |
| Both manifest and plain list | Commit both a generated JSON artifact and a separate lightweight case-id list. | |

**User's choice:** Generated manifest + summary
**Notes:** The canonical parity slice should be reproducible from generation code, with JSON as the source of truth and Markdown as the reviewer-facing companion.

---

## Family Regression Fixture Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Existing deterministic family fixtures, expanded if gaps appear | Use the current deterministic family fixture suite as the primary contract and add only small deterministic gaps if needed. | |
| Add benchmark-derived snapshots now | Capture benchmark-derived snapshots in Phase 7 instead of relying only on the deterministic fixture set. | x |
| Benchmark snapshots only for touched families | Keep deterministic tests, but require at least one benchmark-derived snapshot per touched family that will move in Phase 8. | |

**User's choice:** Add benchmark-derived snapshots now
**Notes:** Existing deterministic fixtures remain useful, but Phase 7 should add benchmark-derived regression protection before the data-driven migration.

---

## Failure-Bucket Normalization

| Option | Description | Selected |
|--------|-------------|----------|
| Normalize into a Phase 7 bucket field | Derive one explicit milestone bucket per canonical case using documented precedence while preserving raw APDR fields. | x |
| Use raw APDR fields only | Rely directly on `validation_status` and `FAILURE_BUCKET` without a Phase 7 normalized field. | |
| Normalize only ambiguous cases | Add a normalized bucket only when the raw stopped-run fields are blank or inconsistent. | |

**User's choice:** Normalize into a Phase 7 bucket field
**Notes:** Phase 7 should own a normalized milestone bucket label so later delta reporting is deterministic and not hostage to raw-field inconsistency.

---

## Benchmark-Derived Snapshot Coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Only touched family-knowledge cases | Add benchmark-derived snapshots only for parity cases that exercise families expected to move into data files in Phase 8. | x |
| Every family-related case in the 70-case slice | Snapshot any canonical tier3 case that looks family-relevant, even if it may not be touched in Phase 8. | |
| Representative sample only | Take a smaller labeled sample of family-related parity cases instead of trying to cover the full touched set. | |

**User's choice:** Only touched family-knowledge cases
**Notes:** Benchmark-derived snapshot coverage should stay bounded to the family-knowledge cases Phase 8 is expected to migrate.

---

## the agent's Discretion

- Exact filenames for the Phase 7 generated manifest and Markdown summary
- Exact deterministic rule for deciding which canonical parity cases count as touched family-knowledge cases
- Exact normalized-bucket precedence order and representation for missing raw fields
- Whether benchmark-derived snapshots land as structured fixture inputs, structured expected-output snapshots, or both

## Deferred Ideas

- The `17` tier1 APDR-failed but `pllm`-passing cases remain a future watchlist outside the Phase 7 contract
